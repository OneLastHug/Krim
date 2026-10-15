"""原生检查点捕获：同Registry读取、私有快照及可停止的持久生产者。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from krim_agent.persistence.workspace_checkpoints import WorkspaceCheckpointStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.tools.workspace import ReadFileInput, workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import (
    CheckpointCaptureFence,
    CheckpointCaptureInput,
    CheckpointCaptureView,
    CheckpointModelSource,
    WorkspaceCheckpointCreate,
    WorkspaceCheckpointView,
    checkpoint_content_kind,
)
from krim_agent.workspace_writer import CapturedFile, WorkspaceWriter

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

CaptureGuard = Callable[[str], Awaitable[tuple[str, WorkspaceBinding]]]
CaptureRead = Callable[[str, str, CheckpointCaptureFence, bool], Awaitable[object]]
CaptureBytesGuard = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class CheckpointInvocation:
    source: CheckpointModelSource
    authorize: CaptureGuard
    read: CaptureRead
    qualify: Callable[[], None] | None = None


class WorkspaceCheckpointService:
    def __init__(
        self,
        store: WorkspaceCheckpointStore,
        changes: WorkspaceChangeService,
        authorize: CaptureGuard,
        read: CaptureRead,
        *,
        max_active: int = 16,
        require_bytes: CaptureBytesGuard | None = None,
    ) -> None:
        if not 1 <= max_active <= 64:
            raise ValueError("检查点并发上限无效")
        self.store, self.changes = store, changes
        self.authorize, self.read, self.max_active = authorize, read, max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False
        self.require_bytes = require_bytes

    async def create(
        self, agent_id: str, request: WorkspaceCheckpointCreate, *, confirmed: bool = False
    ) -> WorkspaceCheckpointView:
        request = WorkspaceCheckpointCreate.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._create(agent_id, request, confirmed)), lambda: None
        )

    async def _create(
        self,
        agent_id: str,
        request: WorkspaceCheckpointCreate,
        confirmed: bool,
        invocation: CheckpointInvocation | None = None,
    ) -> WorkspaceCheckpointView:
        authority = invocation.authorize if invocation is not None else self.authorize
        scope, binding = await authority(agent_id)
        if request.capture_format == "bytes":
            if self.require_bytes is None:
                raise ServiceError(
                    "workspace_checkpoint_bytes_unavailable", "字节捕获未显式启用", 403
                )
            await self.require_bytes(agent_id)
        async with self._start_lock:
            if self._closed:
                raise ServiceError("workspace_checkpoint_closed", "检查点服务已关闭", 503)
            if (
                len(self._tasks) >= self.max_active
                and await self.store.by_key(scope, request.request_key) is None
            ):
                raise ServiceError("workspace_checkpoint_busy", "检查点并发已达上限", 429)
            if invocation is None:
                view, created = await self.store.reserve(
                    scope, request, binding.key, binding.fingerprint, binding.workspace.fingerprint
                )
            else:
                view, created = await self.store.reserve(
                    scope,
                    request,
                    binding.key,
                    binding.fingerprint,
                    binding.workspace.fingerprint,
                    model_source=invocation.source,
                    authorize=invocation.qualify,
                )
            if not created:
                return view
            identifier = view.checkpoint_id
            fences = await self.store.execution(scope, identifier)
            task = asyncio.create_task(
                self._run(
                    agent_id,
                    scope,
                    view.model_copy(deep=True),
                    fences,
                    binding,
                    confirmed,
                    invocation,
                )
            )
            self._tasks[identifier] = (scope, task)

            def done(result: asyncio.Task[None]) -> None:
                self._tasks.pop(identifier, None)
                if not result.cancelled():
                    result.exception()

            task.add_done_callback(done)
            return view

    async def create_model(
        self, agent_id: str, request: WorkspaceCheckpointCreate, invocation: CheckpointInvocation
    ) -> WorkspaceCheckpointView:
        request = WorkspaceCheckpointCreate.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._create(agent_id, request, False, invocation)), lambda: None
        )

    async def _renew(
        self,
        scope: str,
        fence: CheckpointCaptureFence,
        worker: asyncio.Task[None],
        invocation: CheckpointInvocation | None = None,
        agent_id: str | None = None,
    ) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                if invocation is not None:
                    assert agent_id is not None
                    await invocation.authorize(agent_id)
                await self.store.renew(scope, fence)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not worker.done() and not worker.cancelling():
                worker.cancel()

    def _callback(self, check: Callable[[], Coroutine[Any, Any, None]]) -> Callable[[], None]:
        loop = asyncio.get_running_loop()

        def authorize() -> None:
            coroutine = check()
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            except RuntimeError:
                coroutine.close()
                raise ServiceError(
                    "workspace_checkpoint_unavailable", "捕获资格无法核实", 503
                ) from None
            try:
                future.result(timeout=5)
            except ServiceError:
                raise
            except Exception:
                future.cancel()
                raise ServiceError(
                    "workspace_checkpoint_unavailable", "捕获资格无法核实", 503
                ) from None

        return authorize

    async def capture_read(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, object]:
        try:
            if self._closed or context.checkpoint_capture is None:
                raise ServiceError("workspace_checkpoint_closed", "捕获上下文不可用", 503)
            self.capture_identity(context)
            if context.checkpoint_capture.capture_format != "text":
                raise ServiceError(
                    "workspace_checkpoint_context", "字节捕获不能使用UTF-8读取工具", 403
                )
            request = ReadFileInput.model_validate(payload)
            if request.start_line != 1 or request.max_lines != 200:
                raise ServiceError("workspace_checkpoint_target", "捕获读取参数已改变", 403)
            scope, binding = await self.changes._guard(context)
            fence = context.checkpoint_capture

            async def check() -> None:
                await self.changes._guard(context)
                await self.store.assert_capture(scope, fence, request.path)

            await check()
            capture = await workspace_io(
                WorkspaceWriter(binding.workspace).capture_file,
                request.path,
                authorize=self._callback(check),
            )
            await check()
            if fence.model_source is None:
                await self.store.record(scope, fence, capture)
            else:
                await self.store.record(
                    scope, fence, capture, authorize=lambda: self.capture_budget(context)
                )
            if capture.snapshot is None:
                raise ServiceError("workspace_not_found", "工作区条目不存在", 404)
            return binding.workspace.file_view(
                capture.path, capture.snapshot.data, capture.snapshot.data.decode("utf-8")
            ).model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    @staticmethod
    def capture_identity(context: ToolContext) -> None:
        fence = context.checkpoint_capture
        if fence is None:
            raise ServiceError("workspace_checkpoint_context", "缺少原内部捕获围栏", 403)
        source = fence.model_source
        if source is None:
            valid = context.origin == "host" and context.run_id == "local"
        else:
            valid = (
                context.origin == "model"
                and context.run_id == source.run_id
                and context.source_call_id == source.call_id
            )
        if not valid:
            raise ServiceError("workspace_checkpoint_context", "捕获未保留原父调用来源", 403)

    @staticmethod
    def capture_budget(context: ToolContext) -> None:
        WorkspaceCheckpointService.capture_identity(context)
        for budget in context.execution_timeouts:
            when = budget.deadline.when()
            if (
                not budget.active
                or budget.deadline.expired()
                or (when is not None and asyncio.get_running_loop().time() >= when)
            ):
                raise ServiceError("tool_timeout", "捕获原调用预算已结束", 408)

    @staticmethod
    def _capture_view(capture: CapturedFile) -> CheckpointCaptureView:
        snapshot = capture.snapshot
        return CheckpointCaptureView(
            path=capture.path,
            exists=snapshot is not None,
            sha256=snapshot.sha256 if snapshot else None,
            byte_size=len(snapshot.data) if snapshot else 0,
            mode=snapshot.mode if snapshot else None,
            content_kind=checkpoint_content_kind(snapshot.data if snapshot else None),
        )

    async def _bytes_context(
        self, payload: dict[str, Any], context: ToolContext
    ) -> tuple[str, WorkspaceBinding, CheckpointCaptureFence, CheckpointCaptureInput]:
        fence = context.checkpoint_capture
        if (
            self._closed
            or fence is None
            or fence.capture_format != "bytes"
            or context.command_capture is not None
            or context.checkpoint_restore is not None
            or context.workspace_checkpoints is not self
        ):
            raise ServiceError("workspace_checkpoint_context", "字节捕获需要原私有捕获上下文", 403)
        self.capture_identity(context)
        request = CheckpointCaptureInput.model_validate(payload)
        scope, binding = await self.changes._guard(context)
        await self.store.assert_capture(scope, fence, request.path)
        if any(
            budget.active is False or budget.deadline.expired()
            for budget in context.execution_timeouts
        ):
            raise ServiceError("tool_timeout", "字节捕获原工具预算已结束", 408)
        return scope, binding, fence, request

    async def capture_metadata(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, object]:
        try:
            scope, binding, fence, request = await self._bytes_context(payload, context)

            async def check() -> None:
                current, actual, same, selected = await self._bytes_context(payload, context)
                if (
                    current != scope
                    or actual.manifest() != binding.manifest()
                    or same != fence
                    or selected != request
                ):
                    raise ServiceError("workspace_checkpoint_context", "捕获期间原资格改变", 403)

            capture = await workspace_io(
                WorkspaceWriter(binding.workspace).capture_file,
                request.path,
                allow_binary=True,
                authorize=self._callback(check),
            )
            await check()
            if capture.snapshot is not None and capture.snapshot.mode & ~0o777:
                raise ServiceError("workspace_checkpoint_mode", "字节捕获不支持特殊权限位", 409)
            if fence.model_source is None:
                await self.store.record(scope, fence, capture)
            else:
                await self.store.record(
                    scope, fence, capture, authorize=lambda: self.capture_budget(context)
                )
            return self._capture_view(capture).model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def validate_capture_metadata(self, value: Any, context: ToolContext) -> None:
        try:
            scope, _, fence, request = await self._bytes_context(context.arguments, context)
            capture = await self.store.captured(scope, fence)
            if (
                capture is None
                or capture.path != request.path
                or value != self._capture_view(capture).model_dump(mode="json")
            ):
                raise ServiceError(
                    "workspace_checkpoint_unverified", "字节回程与原保存事实不一致", 409
                )
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def _run(
        self,
        agent_id: str,
        scope: str,
        view: WorkspaceCheckpointView,
        fences: tuple[CheckpointCaptureFence, ...],
        binding: WorkspaceBinding,
        confirmed: bool,
        invocation: CheckpointInvocation | None = None,
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, view.checkpoint_id)
        renew = asyncio.create_task(self._renew(scope, fences[0], worker, invocation, agent_id))
        read = invocation.read if invocation is not None else self.read
        authority = invocation.authorize if invocation is not None else self.authorize
        try:
            for entry, fence in zip(view.files, fences, strict=True):
                try:
                    await read(agent_id, entry.path, fence, confirmed)
                except ServiceError as error:
                    # 原read工具如实返回不存在；只有私有账本确已捕获缺失才接受。
                    captured = await self.store.captured(scope, fence)
                    if (
                        error.code != "workspace_not_found"
                        or captured is None
                        or captured.snapshot is not None
                    ):
                        raise
                captured = await self.store.captured(scope, fence)
                if captured is None:
                    raise ServiceError(
                        "workspace_checkpoint_not_captured", "读取工具没有产生真实捕获", 409
                    )
            captures = await self.store.captures(scope, view.checkpoint_id)

            async def check() -> None:
                actual_scope, actual = await authority(agent_id)
                if actual_scope != scope or actual.manifest() != binding.manifest():
                    raise ServiceError("workspace_binding_changed", "检查点资源已改变", 403)
                await self.store.assert_capture(scope, fences[0])

            await workspace_io(
                WorkspaceWriter(binding.workspace).verify_captured,
                captures,
                authorize=self._callback(check),
                allow_binary=view.capture_format == "bytes",
            )
            if invocation is None:
                await self.store.finish(scope, fences[0])
            else:
                await self.store.finish(scope, fences[0], authorize=invocation.qualify)
        except asyncio.CancelledError:
            await _wait(
                asyncio.create_task(
                    self.store.fail(scope, fences[0], "workspace_checkpoint_cancelled")
                ),
                lambda: None,
            )
        except ServiceError as error:
            await _wait(
                asyncio.create_task(self.store.fail(scope, fences[0], error.code)), lambda: None
            )
        except Exception:
            await _wait(
                asyncio.create_task(
                    self.store.fail(scope, fences[0], "workspace_checkpoint_failed")
                ),
                lambda: None,
            )
        finally:
            renew.cancel()
            await asyncio.gather(renew, return_exceptions=True)
            leave_workspace_execution(marker)

    async def cancel(self, scope: str, identifier: str) -> WorkspaceCheckpointView:
        internal = inside_workspace_execution()

        async def stop() -> WorkspaceCheckpointView:
            await self.store.cancel(scope, identifier)
            entry = self._tasks.get(identifier)
            if entry is not None and entry[0] == scope:
                task = entry[1]
                if not task.done() and not task.cancelling():
                    task.cancel()
                if not internal:
                    await asyncio.gather(task, return_exceptions=True)
            return await self.store.get(scope, identifier)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    async def wait(
        self, scope: str, identifier: str, *, timeout_seconds: float = 30
    ) -> WorkspaceCheckpointView:
        if not 0 <= timeout_seconds <= 300:
            raise ServiceError("workspace_checkpoint_timeout", "等待时间须在0–300秒之间", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            result = await self.store.get(scope, identifier)
            remaining = deadline - asyncio.get_running_loop().time()
            if result.status != "capturing" or remaining <= 0:
                return result
            await asyncio.sleep(min(0.05, remaining))

    async def close(self) -> None:
        if inside_workspace_execution():
            raise ServiceError(
                "workspace_checkpoint_close_reentrant", "文件回调须取消任务，不能关闭服务", 409
            )
        async with self._start_lock:
            self._closed = True
            entries = list(self._tasks.items())
        for identifier, (scope, task) in entries:
            try:
                await self.store.cancel(scope, identifier)
            except Exception:
                pass
            finally:
                if not task.done() and not task.cancelling():
                    task.cancel()

        async def drain() -> None:
            await asyncio.gather(*(entry[1][1] for entry in entries), return_exceptions=True)

        await _wait(asyncio.create_task(drain()), lambda: None)
