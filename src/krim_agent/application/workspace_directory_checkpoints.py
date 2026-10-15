"""可信宿主目录捕获：原Registry与受管IO，材料/回程/完成分别核实。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass
from threading import Event
from typing import Any

from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import (
    ToolContext,
    ToolError,
    execution_budget_base,
    same_execution_budget,
)
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointCaptureFence,
    DirectoryCheckpointCaptureInput,
    DirectoryCheckpointCaptureView,
    DirectoryCheckpointMaterialCreate,
    DirectoryCheckpointView,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_checkpoints import CaptureGuard
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

DirectoryCaptureRead = Callable[
    [str, str, DirectoryCheckpointCaptureFence, bool], Coroutine[Any, Any, object]
]


@dataclass(frozen=True)
class DirectoryCheckpointInvocation:
    source: CheckpointModelSource
    authorize: CaptureGuard
    read: DirectoryCaptureRead
    qualify: Callable[[], None]


class WorkspaceDirectoryCheckpointService:
    def __init__(
        self,
        store: WorkspaceDirectoryCaptureStore,
        changes: WorkspaceChangeService,
        authorize: CaptureGuard,
        read: DirectoryCaptureRead,
        *,
        max_active: int = 16,
    ) -> None:
        if type(max_active) is not int or not 1 <= max_active <= 64:
            raise ValueError("目录捕获并发上限无效")
        self.store, self.changes = store, changes
        self.authorize, self.read, self.max_active = authorize, read, max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False
        self.model_close_guard: Callable[[], None] | None = None

    async def create(
        self, agent_id: str, request: DirectoryCheckpointMaterialCreate, *, confirmed: bool = False
    ) -> DirectoryCheckpointView:
        request = DirectoryCheckpointMaterialCreate.model_validate(request.model_dump(mode="json"))
        return await _wait(
            asyncio.create_task(self._create(agent_id, request, confirmed)), lambda: None
        )

    async def _create(
        self,
        agent_id: str,
        request: DirectoryCheckpointMaterialCreate,
        confirmed: bool,
        invocation: DirectoryCheckpointInvocation | None = None,
    ) -> DirectoryCheckpointView:
        authority = invocation.authorize if invocation is not None else self.authorize
        scope, binding = await authority(agent_id)
        async with self._start_lock:
            if self._closed:
                raise ServiceError("workspace_directory_capture_closed", "目录捕获服务已关闭", 503)
            if (
                len(self._tasks) >= self.max_active
                and await self.store.by_key(scope, request.request_key) is None
            ):
                raise ServiceError("workspace_directory_capture_busy", "目录捕获并发已满", 429)
            if invocation is None:
                view, created = await self.store.reserve(scope, binding, request)
            else:
                view, created = await self.store.reserve(
                    scope,
                    binding,
                    request,
                    model_source=invocation.source,
                    authorize=invocation.qualify,
                )
            if not created:
                return view
            fence = await self.store.execution(scope, view.checkpoint_id)
            task = asyncio.create_task(
                self._run(agent_id, scope, view.root_path, fence, confirmed, invocation)
            )
            self._tasks[view.checkpoint_id] = (scope, task)

            def done(result: asyncio.Task[None]) -> None:
                self._tasks.pop(view.checkpoint_id, None)
                if not result.cancelled():
                    result.exception()

            task.add_done_callback(done)
            return view

    async def create_model(
        self,
        agent_id: str,
        request: DirectoryCheckpointMaterialCreate,
        invocation: DirectoryCheckpointInvocation,
    ) -> DirectoryCheckpointView:
        request = DirectoryCheckpointMaterialCreate.model_validate(request.model_dump(mode="json"))
        return await _wait(
            asyncio.create_task(self._create(agent_id, request, False, invocation)), lambda: None
        )

    async def _renew(
        self,
        scope: str,
        fence: DirectoryCheckpointCaptureFence,
        worker: asyncio.Task[None],
        invocation: DirectoryCheckpointInvocation | None = None,
        agent_id: str | None = None,
    ) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                if invocation is None:
                    await self.store.renew(scope, fence)
                else:
                    assert agent_id is not None
                    await invocation.authorize(agent_id)
                    await self.store.renew(scope, fence, authorize=invocation.qualify)
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                if (await self.store.get(scope, fence.checkpoint_id)).status == "available":
                    # 完成事务已经结束原租约；正常完成不应取消尚在返回原回执的工具。
                    return
            except Exception:
                pass
            if not worker.done() and not worker.cancelling():
                worker.cancel()

    async def _run(
        self,
        agent_id: str,
        scope: str,
        root_path: str,
        fence: DirectoryCheckpointCaptureFence,
        confirmed: bool,
        invocation: DirectoryCheckpointInvocation | None = None,
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, fence.checkpoint_id)
        renew = asyncio.create_task(self._renew(scope, fence, worker, invocation, agent_id))

        async def fail(code: str) -> None:
            # 已失权的续租者不能再取消正在落盘的失败收尾；内部取消不冒充父Run取消。
            renew.cancel()
            cleanup = asyncio.create_task(self.store.fail(scope, fence, code))
            try:
                await _wait(cleanup, lambda: None)
            except asyncio.CancelledError:
                if cleanup.cancelled():
                    raise

        try:
            read = invocation.read if invocation is not None else self.read
            await read(agent_id, root_path, fence, confirmed)
            view = await self.store.get(scope, fence.checkpoint_id)
            if view.status != "available":
                raise ServiceError(
                    "workspace_directory_capture_unverified", "目录读取未通过完整回程校验", 409
                )
        except asyncio.CancelledError:
            await fail("workspace_directory_capture_cancelled")
        except ServiceError as error:
            await fail(error.code)
        except Exception:
            await fail("workspace_directory_capture_failed")
        finally:
            renew.cancel()
            await asyncio.gather(renew, return_exceptions=True)
            leave_workspace_execution(marker)

    def _qualify(self, context: ToolContext) -> None:
        fence = context.directory_checkpoint_capture
        if type(fence) is not DirectoryCheckpointCaptureFence:
            raise ServiceError("workspace_directory_capture_context", "缺少原目录捕获围栏", 403)
        self.store._valid_fence(fence)
        source = fence.model_source
        identity = (
            context.origin == "host"
            and context.run_id == "local"
            and context.source_call_id is None
            and context.directory_checkpoint_qualify is None
            if source is None
            else context.origin == "model"
            and context.run_id == source.run_id
            and context.source_call_id == source.call_id
            and callable(context.directory_checkpoint_qualify)
        )
        if (
            self._closed
            or context.workspace_directory_checkpoints is not self
            or not identity
            or context.agent_id is None
            or context.session_id is None
            or not context.execution_timeouts
            or any(
                item is not None
                for item in (
                    context.checkpoint_capture,
                    context.checkpoint_restore,
                    context.command_capture,
                    context.workspace_batch,
                    context.mixed_batch,
                    context.tree_fence,
                )
            )
        ):
            raise ServiceError(
                "workspace_directory_capture_context", "目录捕获须保留原私有来源上下文", 403
            )
        if context.directory_checkpoint_qualify is not None:
            context.directory_checkpoint_qualify()
        loop = asyncio.get_running_loop()
        for budget in context.execution_timeouts:
            when = budget.deadline.when()
            if (
                not budget.active
                or budget.deadline.expired()
                or when is not None
                and loop.time() >= when
            ):
                raise ServiceError("tool_timeout", "目录捕获原调用预算已结束", 408)

    async def _context(
        self, payload: dict[str, Any], context: ToolContext
    ) -> tuple[
        str, WorkspaceBinding, DirectoryCheckpointCaptureFence, DirectoryCheckpointCaptureInput
    ]:
        self._qualify(context)
        request = DirectoryCheckpointCaptureInput.model_validate(payload)
        scope, binding = await self.changes._guard(context)
        fence = context.directory_checkpoint_capture
        assert fence is not None
        await self.store.assert_capture(scope, fence, request.root_path)
        self._qualify(context)
        return scope, binding, fence, request

    @staticmethod
    def _callback(
        check: Callable[[], Coroutine[Any, Any, None]], abort: Event, deadline: float
    ) -> Callable[[], None]:
        loop = asyncio.get_running_loop()

        def authorize() -> None:
            if abort.is_set() or time.monotonic() >= deadline:
                raise ServiceError(
                    "workspace_directory_capture_stopped", "目录物理捕获已取消或超过5秒预算", 409
                )
            coroutine = check()
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            except RuntimeError:
                coroutine.close()
                raise ServiceError(
                    "workspace_directory_capture_unavailable", "原捕获资格无法核实", 503
                ) from None
            try:
                while True:
                    remaining = deadline - time.monotonic()
                    if abort.is_set() or remaining <= 0:
                        raise ServiceError(
                            "workspace_directory_capture_stopped",
                            "目录物理捕获已取消或超过5秒预算",
                            409,
                        )
                    try:
                        future.result(timeout=min(0.05, remaining))
                        if abort.is_set() or time.monotonic() >= deadline:
                            raise ServiceError(
                                "workspace_directory_capture_stopped", "目录物理资格已过期", 409
                            )
                        return
                    except FutureTimeout:
                        continue
            finally:
                if not future.done():
                    future.cancel()

        return authorize

    async def capture_metadata(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, object]:
        try:
            scope, binding, fence, request = await self._context(payload, context)
            base = execution_budget_base(context.execution_timeouts)

            def qualify() -> None:
                self._qualify(context)
                if not same_execution_budget(
                    base, execution_budget_base(context.execution_timeouts)
                ):
                    raise ServiceError(
                        "workspace_directory_capture_context", "目录捕获原预算被替换", 403
                    )

            async def check() -> None:
                current, actual, same, selected = await self._context(payload, context)
                if (
                    current != scope
                    or actual is not binding
                    or same != fence
                    or selected != request
                ):
                    raise ServiceError(
                        "workspace_directory_capture_context", "目录捕获原资格被替换", 403
                    )
                qualify()

            abort = Event()
            deadline = time.monotonic() + 5
            task = asyncio.create_task(
                workspace_io(
                    WorkspaceTreeSnapshotReader(binding.workspace).capture,
                    request.root_path,
                    authorize=self._callback(check, abort, deadline),
                    cancelled=abort.is_set,
                )
            )
            snapshot = await _wait(task, abort.set)
            await check()
            value = await self.store.record(scope, fence, binding, snapshot, qualify=qualify)
            return value.model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def validate_capture_metadata(self, value: Any, context: ToolContext) -> None:
        try:
            scope, binding, fence, _ = await self._context(context.arguments, context)
            result = DirectoryCheckpointCaptureView.model_validate(value)
            await self.store.finish(
                scope, fence, binding, result, qualify=lambda: self._qualify(context)
            )
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def wait(
        self, scope: str, identifier: str, *, timeout_seconds: float = 30
    ) -> DirectoryCheckpointView:
        if type(timeout_seconds) not in {int, float} or not 0 <= timeout_seconds <= 300:
            raise ServiceError("workspace_directory_capture_timeout", "等待须在0–300秒之间", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            view = await self.store.get(scope, identifier)
            remaining = deadline - asyncio.get_running_loop().time()
            if view.status != "capturing" or remaining <= 0:
                return view
            await asyncio.sleep(min(0.05, remaining))

    async def cancel(self, scope: str, identifier: str) -> DirectoryCheckpointView:
        internal = inside_workspace_execution()

        async def stop() -> DirectoryCheckpointView:
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

    def assert_external_close(self) -> None:
        if self.model_close_guard is not None:
            self.model_close_guard()
        if inside_workspace_execution(self._tasks):
            raise ServiceError(
                "workspace_directory_capture_reentrant", "捕获回调须取消原任务，不能关闭服务", 409
            )

    async def close(self) -> None:
        self.assert_external_close()
        async with self._start_lock:
            self._closed = True
            entries = list(self._tasks.items())
        for identifier, (scope, task) in entries:
            try:
                await self.store.cancel(scope, identifier)
            except Exception:
                # 即使持久层暂不可用，关闭标记仍拒绝资格，且必须收束全部本地生产者。
                pass
            finally:
                if not task.done() and not task.cancelling():
                    task.cancel()

        async def drain() -> None:
            await asyncio.gather(*(entry[1][1] for entry in entries), return_exceptions=True)

        await _wait(asyncio.create_task(drain()), lambda: None)
