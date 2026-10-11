"""混合文件/目录批次的受控Worker；每个子项仍由原Registry和原账本完成。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_mixed_batch import (
    MixedBatchDispatch,
    MixedBatchExecution,
    MixedBatchRequest,
    MixedBatchView,
)

from .workspace_changes import _wait
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

if TYPE_CHECKING:
    from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore


AuthorizeMixedBatch = Callable[[str], Awaitable[tuple[str, str, str]]]
ExecuteMixedBatchChild = Callable[[str, MixedBatchDispatch, bool], Awaitable[object]]


class MixedBatchService:
    """只调度已持久冻结的子项，不根据Hook或返回值生成伪操作。"""

    def __init__(
        self,
        store: MixedBatchStore,
        authorize: AuthorizeMixedBatch,
        execute: ExecuteMixedBatchChild,
        *,
        max_active: int = 16,
    ) -> None:
        if not 1 <= max_active <= 64:
            raise ValueError("混合批次并发须在1–64之间")
        self.store, self.authorize, self.execute = store, authorize, execute
        self.max_active = max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False

    async def start(
        self, agent_id: str, request: MixedBatchRequest, *, confirmed: bool = False
    ) -> MixedBatchView:
        request = MixedBatchRequest.model_validate(request.model_dump())
        return await _wait(
            asyncio.create_task(self._start(agent_id, request, confirmed)), lambda: None
        )

    async def _start(
        self, agent_id: str, request: MixedBatchRequest, confirmed: bool
    ) -> MixedBatchView:
        if self._closed:
            raise ServiceError("workspace_mixed_batch_service_closed", "混合批次服务已关闭", 503)
        scope, key, fingerprint = await self.authorize(agent_id)
        async with self._start_lock:
            if self._closed:
                raise ServiceError(
                    "workspace_mixed_batch_service_closed", "混合批次服务已关闭", 503
                )
            if len(self._tasks) >= self.max_active:
                if await self.store.by_key(scope, request.request_key) is None:
                    raise ServiceError("workspace_mixed_batch_busy", "混合批次并发已达上限", 429)
            view, created = await self.store.reserve(scope, key, fingerprint, request)
            if not created:
                return view
            execution = await self.store.execution(scope, view.batch_id)
            task = asyncio.create_task(self._run(agent_id, scope, execution, confirmed))
            self._tasks[view.batch_id] = (scope, task)

            def done(result: asyncio.Task[None]) -> None:
                self._tasks.pop(view.batch_id, None)
                if not result.cancelled():
                    result.exception()

            task.add_done_callback(done)
            return view

    async def _renew(self, batch_id: str, token: str, worker: asyncio.Task[None]) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                await self.store.renew(batch_id, token)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not worker.done() and not worker.cancelling():
                worker.cancel()

    async def _run(
        self, agent_id: str, scope: str, execution: MixedBatchExecution, confirmed: bool
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        heartbeat = asyncio.create_task(
            self._renew(execution.view.batch_id, execution.token, worker)
        )
        marker = enter_workspace_execution(self._tasks, execution.view.batch_id)
        try:
            for child in execution.children:
                if (
                    await self.store.get(scope, execution.view.batch_id)
                ).producer_status != "running":
                    break
                error_code: str | None = None
                try:
                    await self.execute(agent_id, child, confirmed)
                except asyncio.CancelledError:
                    error_code = "workspace_mixed_batch_cancelled"
                    raise
                except ServiceError as error:
                    error_code = error.code
                except Exception:
                    error_code = "workspace_mixed_batch_execution_failed"
                try:
                    await _wait(
                        asyncio.create_task(
                            self.store.finish_child(scope, child.fence, error_code=error_code)
                        ),
                        lambda: None,
                    )
                except ServiceError as error:
                    if error.code == "workspace_mixed_batch_fenced":
                        break
                    raise
                if (
                    await self.store.get(scope, execution.view.batch_id)
                ).producer_status != "running":
                    break
        finally:
            try:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                try:
                    await _wait(
                        asyncio.create_task(
                            self.store.finish(scope, execution.view.batch_id, execution.token)
                        ),
                        lambda: None,
                    )
                except ServiceError as error:
                    if error.code != "workspace_mixed_batch_fenced":
                        raise
            finally:
                leave_workspace_execution(marker)

    async def cancel(self, scope: str, batch_id: str) -> MixedBatchView:
        internal = inside_workspace_execution()

        async def stop() -> MixedBatchView:
            await self.store.cancel(scope, batch_id)
            entry = self._tasks.get(batch_id)
            if entry is not None and entry[0] == scope:
                task = entry[1]
                if not task.done() and not task.cancelling():
                    task.cancel()
                if not internal:
                    await asyncio.gather(task, return_exceptions=True)
            return await self.store.get(scope, batch_id)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    async def wait(
        self, scope: str, batch_id: str, *, timeout_seconds: float = 30
    ) -> MixedBatchView:
        if not 0 <= timeout_seconds <= 300:
            raise ServiceError("workspace_mixed_batch_timeout", "等待时间须在0–300秒之间", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            value = await self.store.get(scope, batch_id)
            if value.producer_status != "running":
                return value
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return value
            await asyncio.sleep(min(0.05, remaining))

    def assert_external_close(self) -> None:
        if inside_workspace_execution():
            raise ServiceError(
                "workspace_mixed_batch_close_reentrant", "批次回调不能关闭混合批次服务", 409
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
                pass
            if not task.done() and not task.cancelling():
                task.cancel()
        await _wait(
            asyncio.create_task(self._drain([task for _batch, (_scope, task) in entries])),
            lambda: None,
        )

    @staticmethod
    async def _drain(tasks: list[asyncio.Task[None]]) -> None:
        await asyncio.gather(*tasks, return_exceptions=True)
