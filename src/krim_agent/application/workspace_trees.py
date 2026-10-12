"""已有planned目录树的可信host Worker；全部阶段沿原Registry和原回执。"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_tree import TreePlanDispatch, TreePlanExecution, TreePlanView

from .workspace_changes import _wait
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

if TYPE_CHECKING:
    from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore


AuthorizeTree = Callable[[str], Awaitable[tuple[str, str, str]]]
ExecuteTreeStep = Callable[[str, TreePlanDispatch, bool], Awaitable[object]]


class WorkspaceTreeService:
    def __init__(
        self,
        store: WorkspaceTreePlanStore,
        authorize: AuthorizeTree,
        execute: ExecuteTreeStep,
        *,
        max_active: int = 16,
    ) -> None:
        if type(max_active) is not int or not 1 <= max_active <= 64:
            raise ValueError("树Worker并发须为1–64整数")
        self.store, self.authorize, self.execute = store, authorize, execute
        self.max_active = max_active
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False

    async def start(self, agent_id: str, plan_id: str, *, confirmed: bool = False) -> TreePlanView:
        if type(confirmed) is not bool:
            raise ServiceError("workspace_tree_parameters", "树确认参数须为布尔值", 422)
        return await _wait(
            asyncio.create_task(self._start(agent_id, plan_id, confirmed)), lambda: None
        )

    async def _start(self, agent_id: str, plan_id: str, confirmed: bool) -> TreePlanView:
        if self._closed:
            raise ServiceError("workspace_tree_service_closed", "树Worker服务已关闭", 503)
        scope, key, resource = await self.authorize(agent_id)
        async with self._start_lock:
            if self._closed:
                raise ServiceError("workspace_tree_service_closed", "树Worker服务已关闭", 503)
            view = await self.store.get(scope, plan_id)
            if view.workspace_key != key or view.resource_fingerprint != resource:
                raise ServiceError("workspace_tree_resource_conflict", "树不属于原工作区绑定", 403)
            if view.producer_status != "planned":
                return view
            if len(self._tasks) >= self.max_active:
                raise ServiceError("workspace_tree_busy", "树Worker并发已达上限", 429)
            try:
                execution = await self.store.claim(scope, plan_id)
            except ServiceError as error:
                if error.code == "workspace_tree_claimed":
                    return await self.store.get(scope, plan_id)
                raise
            task = asyncio.create_task(self._run(agent_id, scope, execution, confirmed))
            self._tasks[plan_id] = (scope, task)

            def done(result: asyncio.Task[None]) -> None:
                current = self._tasks.get(plan_id)
                if current is not None and current[1] is result:
                    self._tasks.pop(plan_id, None)
                if not result.cancelled():
                    result.exception()

            task.add_done_callback(done)
            return execution.view

    async def _renew(
        self, scope: str, execution: TreePlanExecution, worker: asyncio.Task[None]
    ) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                await self.store.renew(scope, execution.view.plan_id, execution.token)
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await self.store.cancel(scope, execution.view.plan_id)
            finally:
                if not worker.done() and not worker.cancelling():
                    worker.cancel()

    async def _run(
        self, agent_id: str, scope: str, execution: TreePlanExecution, confirmed: bool
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        identifier, token = execution.view.plan_id, execution.token
        marker = enter_workspace_execution(self._tasks, identifier)
        heartbeat = asyncio.create_task(self._renew(scope, execution, worker))
        try:
            for position in range(execution.view.step_count):
                if (await self.store.get(scope, identifier)).producer_status != "running":
                    break
                error_code: str | None = None
                try:
                    dispatch = await self.store.dispatch(scope, identifier, token, position)
                    await self.execute(agent_id, dispatch, confirmed)
                    if dispatch.fence.phase == "preview":
                        dispatch = await self.store.dispatch(scope, identifier, token, position)
                        if dispatch.fence.phase != "change":
                            raise ServiceError(
                                "workspace_tree_original_proposal_missing",
                                "树预览未留下原提案",
                                409,
                            )
                        await self.execute(agent_id, dispatch, confirmed)
                except asyncio.CancelledError:
                    error_code = "workspace_tree_cancelled"
                    raise
                except ServiceError as error:
                    error_code = error.code
                except Exception:
                    error_code = "workspace_tree_execution_failed"
                finally:
                    try:
                        await _wait(
                            asyncio.create_task(
                                self.store.finish_step(
                                    scope, identifier, token, position, error_code=error_code
                                )
                            ),
                            lambda: None,
                        )
                    except ServiceError as error:
                        if error.code != "workspace_tree_fenced":
                            raise
                if (await self.store.get(scope, identifier)).producer_status != "running":
                    break
                page = await self.store.steps(
                    scope, identifier, after=position - 1 if position else None, limit=1
                )
                if error_code is not None or not page.items or page.items[0].status != "done":
                    break
        finally:
            try:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                try:
                    await _wait(
                        asyncio.create_task(self.store.finish(scope, identifier, token)),
                        lambda: None,
                    )
                except ServiceError as error:
                    if error.code != "workspace_tree_fenced":
                        raise
            finally:
                leave_workspace_execution(marker)

    async def cancel(self, scope: str, plan_id: str) -> TreePlanView:
        internal = inside_workspace_execution()

        async def stop() -> TreePlanView:
            await self.store.cancel(scope, plan_id)
            current = self._tasks.get(plan_id)
            if current is not None and current[0] == scope:
                task = current[1]
                if not task.done() and not task.cancelling():
                    task.cancel()
                if not internal:
                    await asyncio.gather(task, return_exceptions=True)
            return await self.store.get(scope, plan_id)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    async def wait(self, scope: str, plan_id: str, *, timeout_seconds: float = 30) -> TreePlanView:
        if (
            type(timeout_seconds) not in {int, float}
            or not math.isfinite(timeout_seconds)
            or not 0 <= timeout_seconds <= 300
        ):
            raise ServiceError("workspace_tree_timeout", "树等待时间须为有限的0–300秒", 422)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            value = await self.store.get(scope, plan_id)
            if value.producer_status != "running":
                return value
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return value
            await asyncio.sleep(min(0.05, remaining))

    def assert_external_close(self) -> None:
        if inside_workspace_execution(self._tasks):
            raise ServiceError(
                "workspace_tree_close_reentrant", "树回调不能关闭正在等待自身的服务", 409
            )

    async def close(self) -> None:
        self.assert_external_close()
        async with self._start_lock:
            self._closed = True
            current = list(self._tasks.items())
        for plan_id, (scope, task) in current:
            try:
                await self.store.cancel(scope, plan_id)
            except Exception:
                pass
            if not task.done() and not task.cancelling():
                task.cancel()
        await _wait(
            asyncio.create_task(self._drain([task for _, (_, task) in current])), lambda: None
        )

    @staticmethod
    async def _drain(tasks: list[asyncio.Task[None]]) -> None:
        await asyncio.gather(*tasks, return_exceptions=True)
