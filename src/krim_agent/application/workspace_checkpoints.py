"""原生检查点捕获：同Registry读取、私有快照及可停止的持久生产者。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

from krim_agent.persistence.workspace_checkpoints import WorkspaceCheckpointStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.tools.workspace import ReadFileInput, workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import (
    CheckpointCaptureFence,
    WorkspaceCheckpointCreate,
    WorkspaceCheckpointView,
)
from krim_agent.workspace_writer import WorkspaceWriter

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

CaptureGuard = Callable[[str], Awaitable[tuple[str, WorkspaceBinding]]]
CaptureRead = Callable[[str, str, CheckpointCaptureFence, bool], Awaitable[object]]


class WorkspaceCheckpointService:
    def __init__(
        self,
        store: WorkspaceCheckpointStore,
        changes: WorkspaceChangeService,
        authorize: CaptureGuard,
        read: CaptureRead,
        *,
        max_active: int = 16,
    ) -> None:
        if not 1 <= max_active <= 64:
            raise ValueError("检查点并发上限无效")
        self.store, self.changes = store, changes
        self.authorize, self.read, self.max_active = authorize, read, max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False

    async def create(
        self, agent_id: str, request: WorkspaceCheckpointCreate, *, confirmed: bool = False
    ) -> WorkspaceCheckpointView:
        request = WorkspaceCheckpointCreate.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._create(agent_id, request, confirmed)), lambda: None
        )

    async def _create(
        self, agent_id: str, request: WorkspaceCheckpointCreate, confirmed: bool
    ) -> WorkspaceCheckpointView:
        scope, binding = await self.authorize(agent_id)
        async with self._start_lock:
            if self._closed:
                raise ServiceError("workspace_checkpoint_closed", "检查点服务已关闭", 503)
            if (
                len(self._tasks) >= self.max_active
                and await self.store.by_key(scope, request.request_key) is None
            ):
                raise ServiceError("workspace_checkpoint_busy", "检查点并发已达上限", 429)
            view, created = await self.store.reserve(
                scope, request, binding.key, binding.fingerprint, binding.workspace.fingerprint
            )
            if not created:
                return view
            identifier = view.checkpoint_id
            fences = await self.store.execution(scope, identifier)
            task = asyncio.create_task(
                self._run(agent_id, scope, view.model_copy(deep=True), fences, binding, confirmed)
            )
            self._tasks[identifier] = (scope, task)

            def done(result: asyncio.Task[None]) -> None:
                self._tasks.pop(identifier, None)
                if not result.cancelled():
                    result.exception()

            task.add_done_callback(done)
            return view

    async def _renew(
        self, scope: str, fence: CheckpointCaptureFence, worker: asyncio.Task[None]
    ) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
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
            await self.store.record(scope, fence, capture)
            if capture.snapshot is None:
                raise ServiceError("workspace_not_found", "工作区条目不存在", 404)
            return binding.workspace.file_view(
                capture.path, capture.snapshot.data, capture.snapshot.data.decode("utf-8")
            ).model_dump(mode="json")
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
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, view.checkpoint_id)
        renew = asyncio.create_task(self._renew(scope, fences[0], worker))
        try:
            for entry, fence in zip(view.files, fences, strict=True):
                try:
                    await self.read(agent_id, entry.path, fence, confirmed)
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
                actual_scope, actual = await self.authorize(agent_id)
                if actual_scope != scope or actual.manifest() != binding.manifest():
                    raise ServiceError("workspace_binding_changed", "检查点资源已改变", 403)
                await self.store.assert_capture(scope, fences[0])

            await workspace_io(
                WorkspaceWriter(binding.workspace).verify_captured,
                captures,
                authorize=self._callback(check),
            )
            await self.store.finish(scope, fences[0])
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
