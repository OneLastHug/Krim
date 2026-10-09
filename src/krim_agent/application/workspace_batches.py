"""原生批次只编排同一工具入口，结果仅来自原文件操作关联。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_change import WorkspaceChangeAction
from krim_agent.workspace_list import (
    NativeBatchExecution,
    NativeBatchFence,
    NativeBatchRequest,
    NativeBatchView,
)

from .workspace_changes import _wait
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

if TYPE_CHECKING:
    from krim_agent.persistence.workspace_batches import NativeBatchStore

AuthorizeBatch = Callable[[str], Awaitable[tuple[str, str, str]]]
ExecuteBatchChild = Callable[
    [str, WorkspaceChangeAction, NativeBatchFence, bool], Awaitable[object]
]


class WorkspaceBatchService:
    def __init__(
        self,
        store: NativeBatchStore,
        authorize: AuthorizeBatch,
        execute: ExecuteBatchChild,
        *,
        max_active: int = 16,
    ) -> None:
        if not 1 <= max_active <= 64:
            raise ValueError("原生批次并发须在1–64之间")
        self.store, self.authorize, self.execute = store, authorize, execute
        self.max_active = max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False

    async def start(
        self, agent_id: str, request: NativeBatchRequest, *, confirmed: bool = False
    ) -> NativeBatchView:
        # 接受请求的短事务完成后再传播网络任务取消；已接受批次需显式cancel。
        request = NativeBatchRequest.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._start(agent_id, request, confirmed)), lambda: None
        )

    async def _start(
        self, agent_id: str, request: NativeBatchRequest, confirmed: bool
    ) -> NativeBatchView:
        if self._closed:
            raise ServiceError("workspace_batch_service_closed", "原生批次服务已关闭", 503)
        scope, key, fingerprint = await self.authorize(agent_id)
        async with self._start_lock:
            if self._closed:
                raise ServiceError("workspace_batch_service_closed", "原生批次服务已关闭", 503)
            if len(self._tasks) >= self.max_active:
                if await self.store.by_key(scope, request.request_key) is None:
                    raise ServiceError("workspace_batch_busy", "原生批次并发已达上限", 429)
            view, created = await self.store.reserve(scope, request, fingerprint, key)
            if not created:
                return view
            identifier = view.batch_id
            execution = await self.store.execution(scope, identifier)
            task = asyncio.create_task(self._run(agent_id, scope, execution, confirmed))
            self._tasks[identifier] = (scope, task)

            def done(result: asyncio.Task[None]) -> None:
                self._tasks.pop(identifier, None)
                if not result.cancelled():
                    result.exception()

            task.add_done_callback(done)
            return view

    async def _renew(self, execution: NativeBatchExecution, worker: asyncio.Task[None]) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                await self.store.renew(execution.view.batch_id, execution.token)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not worker.done() and not worker.cancelling():
                worker.cancel()

    async def _run(
        self, agent_id: str, scope: str, execution: NativeBatchExecution, confirmed: bool
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        heartbeat = asyncio.create_task(self._renew(execution, worker))
        marker = enter_workspace_execution(self._tasks, execution.view.batch_id)
        blocked: set[str] = set()
        try:
            for child in execution.children:
                cancelled = False
                skipped = child.file_key in blocked
                error_code = "workspace_batch_dependency" if skipped else None
                if not skipped:
                    try:
                        # 返回值可能经过Hook；不能把它当作原文件操作已完成的证明。
                        await self.execute(agent_id, child.request, child.fence, confirmed)
                    except asyncio.CancelledError:
                        cancelled = True
                        error_code = "workspace_batch_cancelled"
                    except ServiceError as error:
                        error_code = error.code
                    except Exception:
                        error_code = "workspace_batch_execution_failed"
                item = await _wait(
                    asyncio.create_task(
                        self.store.finish_child(
                            scope, child.fence, error_code=error_code, skipped=skipped
                        )
                    ),
                    lambda: None,
                )
                if item.status != "done":
                    blocked.add(child.file_key)
                # 子项Hook可能在同一worker内请求取消；父fence关闭后不再启动后续文件动作。
                if (
                    await self.store.get(scope, execution.view.batch_id)
                ).producer_status != "running":
                    break
                if cancelled:
                    break
        finally:
            try:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                # 原文件动作已由自身编排收束；此处只关闭未开始子项并读取关联结果。
                await _wait(
                    asyncio.create_task(
                        self.store.finish(scope, execution.view.batch_id, execution.token)
                    ),
                    lambda: None,
                )
            finally:
                leave_workspace_execution(marker)

    async def cancel(self, scope: str, batch_id: str) -> NativeBatchView:
        internal = self._inside_active_batch()

        async def stop() -> NativeBatchView:
            await self.store.cancel(scope, batch_id)
            entry = self._tasks.get(batch_id)
            if entry is not None and entry[0] == scope:
                task = entry[1]
                if not task.done() and not task.cancelling():
                    task.cancel()
                # Hook可取消自身或相互取消；批次回调不能等待另一可能依赖自己的worker。
                if not internal:
                    await asyncio.gather(task, return_exceptions=True)
            return await self.store.get(scope, batch_id)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    async def wait(
        self, scope: str, batch_id: str, *, timeout_seconds: float = 30
    ) -> NativeBatchView:
        if not 0 <= timeout_seconds <= 300:
            raise ServiceError("workspace_batch_timeout", "等待时间须在0–300秒之间", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            value = await self.store.get(scope, batch_id)
            if value.producer_status != "running":
                return value
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return value
            # 等待只是读取进度，超时或调用方取消不会停止后台批次。
            await asyncio.sleep(min(0.05, remaining))

    def _inside_active_batch(self) -> bool:
        return inside_workspace_execution()

    def assert_external_close(self) -> None:
        if self._inside_active_batch():
            raise ServiceError(
                "workspace_batch_close_reentrant", "批次回调须使用cancel停止批次，不能关闭服务", 409
            )

    async def close(self) -> None:
        self.assert_external_close()
        async with self._start_lock:
            self._closed = True
            entries = list(self._tasks.items())
        for batch_id, (scope, task) in entries:
            try:
                await self.store.cancel(scope, batch_id)
            except Exception:
                # 存储暂不可用时仍终止所有本地生产者，持久fence到期后由原记录恢复。
                pass
            finally:
                if not task.done() and not task.cancelling():
                    task.cancel()
        await _wait(
            asyncio.create_task(self._drain([entry[1][1] for entry in entries])), lambda: None
        )

    @staticmethod
    async def _drain(tasks: list[asyncio.Task[None]]) -> None:
        await asyncio.gather(*tasks, return_exceptions=True)
