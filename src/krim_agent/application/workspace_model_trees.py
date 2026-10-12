"""模型目录树在原调用内执行；原上下文、Store事实和取消收束不脱离。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any
from uuid import uuid4

from pydantic import JsonValue

from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_tree import (
    TreePlanDispatch,
    TreePlanExecution,
    TreePlanModelSource,
    TreePlanRequest,
    TreePlanStartRequest,
    TreePlanView,
)

from .workspace_changes import WorkspaceChangeService, _wait
from .workspace_directories import DirectoryChangeService
from .workspace_execution import (
    enter_workspace_execution,
    inside_workspace_execution,
    leave_workspace_execution,
)

PrepareTree = Callable[[TreePlanRequest, ToolContext], Awaitable[TreePlanView]]


@dataclass
class _Invocation:
    request: TreePlanStartRequest
    context: ToolContext
    result: TreePlanView | None = None
    execution: TreePlanExecution | None = None


class ModelTreeService:
    def __init__(
        self, changes: WorkspaceChangeService, directories: DirectoryChangeService
    ) -> None:
        self.changes, self.directories = changes, directories
        self.store = WorkspaceTreePlanStore(changes.store, directories.store)
        self.prepare: PrepareTree | None = None
        self._tasks: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._plans: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._closed = False

    def _context(self, context: ToolContext) -> TreePlanModelSource:
        if self._closed:
            raise ServiceError("workspace_tree_model_closed", "模型树服务已关闭", 503)
        if (
            context.origin != "model"
            or context.run_id == "local"
            or context.session_id is None
            or context.agent_id is None
        ):
            raise ServiceError(
                "workspace_tree_model_context", "模型树需要原Agent/session/Run/call", 403
            )
        if any(
            value is not None
            for value in (
                context.tree_fence,
                context.mixed_batch,
                context.workspace_batch,
                context.checkpoint_capture,
                context.checkpoint_restore,
                context.command_capture,
            )
        ):
            raise ServiceError("workspace_tree_action", "其他事务上下文不能准备或启动新树", 403)
        return TreePlanModelSource(context.run_id, context.call_id)

    async def tool_prepare(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        try:
            self._context(context)
            if self.prepare is None:
                raise ServiceError(
                    "workspace_tree_backend_missing", "模型树缺少原来源准备回调", 503
                )
            await self.directories._source_guard(context)
            view = await self.prepare(TreePlanRequest.model_validate(payload), context)
            await self.directories._source_guard(context)
            value = view.model_dump(mode="json")
            context._tree_model_response = ("prepare", json.dumps(value, sort_keys=True))
            return value
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def tool_start(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        try:
            self._context(context)
            scope, binding, _execution = await self.directories._source_guard(context)
            request = TreePlanStartRequest.model_validate(payload)
            view = await self.store.get(scope, request.plan_id)
            if (
                view.workspace_key != binding.key
                or view.resource_fingerprint != binding.fingerprint
            ):
                raise ServiceError("workspace_tree_resource_conflict", "模型树不属于原工作区", 403)
            if view.producer_status != "planned":
                value = view.model_dump(mode="json")
                context._tree_model_response = ("start", json.dumps(value, sort_keys=True))
                return value
            if len(self._tasks) >= 16:
                raise ServiceError("workspace_tree_busy", "模型树调用已达上限", 429)
            state = _Invocation(request, context)
            identifier = str(uuid4())
            task = asyncio.create_task(self._run(identifier, state, scope, binding))
            self._tasks[identifier] = (scope, task)

            def stop() -> None:
                if not task.done() and not task.cancelling():
                    task.cancel()

            try:
                await _wait(task, stop)
                assert state.result is not None
                value = state.result.model_dump(mode="json")
                context._tree_model_response = ("start", json.dumps(value, sort_keys=True))
                return value
            finally:
                current = self._tasks.get(identifier)
                if current is not None and current[1] is task:
                    self._tasks.pop(identifier, None)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def _cancel_parent(self, scope: str, state: _Invocation) -> None:
        if state.execution is not None:
            state.result = await _wait(
                asyncio.create_task(self.store.cancel(scope, state.request.plan_id)), lambda: None
            )

    async def _renew(self, scope: str, state: _Invocation, worker: asyncio.Task[None]) -> None:
        assert state.execution is not None
        try:
            while True:
                await asyncio.sleep(max(0.02, min(0.2, self.store.lease_seconds / 4)))
                await self.store.renew(scope, state.request.plan_id, state.execution.token)
                await self.directories._source_guard(state.context)
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await self._cancel_parent(scope, state)
            finally:
                # 存储暂不可用也不能让已失去租约的执行器继续等待/派发。
                if not worker.done() and not worker.cancelling():
                    worker.cancel()

    async def _child(
        self, scope: str, state: _Invocation, dispatch: TreePlanDispatch
    ) -> str | None:
        nested = replace(
            state.context, tree_fence=dispatch.fence, source_call_id=state.context.call_id
        )
        task = asyncio.create_task(
            nested.registry.invoke(dispatch.name, dispatch.request.model_dump(), nested)
        )
        try:
            result = await asyncio.shield(task)
            return result.code if result.is_error else None
        except asyncio.CancelledError:

            def stop() -> None:
                if not task.done() and not task.cancelling():
                    task.cancel()

            try:
                await self._cancel_parent(scope, state)
            except Exception:
                # 保留本次取消；_run的最终收束会再次核实并关闭持久父。
                pass
            finally:
                # 关闭父账本失败仍须收束原child，原Writer保留自己的资格核实。
                stop()
                try:
                    await _wait(task, stop)
                except asyncio.CancelledError:
                    pass
            raise

    async def _run(
        self, identifier: str, state: _Invocation, scope: str, binding: WorkspaceBinding
    ) -> None:
        worker = asyncio.current_task()
        assert worker is not None
        marker = enter_workspace_execution(self._tasks, identifier)
        heartbeat = None
        cancelled = False
        try:

            async def accept() -> TreePlanExecution:
                execution = await self.store.claim(
                    scope, state.request.plan_id, model_source=self._context(state.context)
                )
                state.execution, state.result = execution, execution.view
                return execution

            accept_task = asyncio.create_task(accept())
            try:
                execution = await asyncio.shield(accept_task)
            except asyncio.CancelledError:
                cancelled = True
                try:
                    await _wait(accept_task, lambda: None)
                except ServiceError:
                    pass
                raise
            except ServiceError as error:
                if error.code == "workspace_tree_claimed":
                    state.result = await self.store.get(scope, state.request.plan_id)
                    return
                raise
            if (
                execution.view.workspace_key != binding.key
                or execution.view.resource_fingerprint != binding.fingerprint
            ):
                raise ServiceError(
                    "workspace_tree_resource_conflict", "模型树资格工作区已变化", 403
                )
            self._plans[state.request.plan_id] = (scope, worker)
            heartbeat = asyncio.create_task(self._renew(scope, state, worker))
            for position in range(execution.view.step_count):
                if (
                    await self.store.get(scope, state.request.plan_id)
                ).producer_status != "running":
                    break
                await self.directories._source_guard(state.context)
                error_code = None
                try:
                    dispatch = await self.store.dispatch(
                        scope, state.request.plan_id, execution.token, position
                    )
                    error_code = await self._child(scope, state, dispatch)
                    if error_code is None and dispatch.fence.phase == "preview":
                        dispatch = await self.store.dispatch(
                            scope, state.request.plan_id, execution.token, position
                        )
                        if dispatch.fence.phase != "change":
                            raise ServiceError(
                                "workspace_tree_original_proposal_missing",
                                "原树预览未保存提案",
                                409,
                            )
                        error_code = await self._child(scope, state, dispatch)
                except asyncio.CancelledError:
                    cancelled = True
                    error_code = "workspace_tree_cancelled"
                    raise
                except ServiceError as error:
                    error_code = error.code
                finally:
                    try:
                        await _wait(
                            asyncio.create_task(
                                self.store.finish_step(
                                    scope,
                                    state.request.plan_id,
                                    execution.token,
                                    position,
                                    error_code=error_code,
                                )
                            ),
                            lambda: None,
                        )
                    except ServiceError as error:
                        if error.code != "workspace_tree_fenced":
                            raise
                if (
                    error_code is not None
                    or (await self.store.get(scope, state.request.plan_id)).producer_status
                    != "running"
                ):
                    break
                page = await self.store.steps(
                    scope, state.request.plan_id, after=position - 1 if position else None, limit=1
                )
                if not page.items or page.items[0].status != "done":
                    break
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            try:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                if state.execution is not None:
                    if cancelled:
                        await self._cancel_parent(scope, state)
                    else:
                        try:
                            state.result = await _wait(
                                asyncio.create_task(
                                    self.store.finish(
                                        scope, state.request.plan_id, state.execution.token
                                    )
                                ),
                                lambda: None,
                            )
                        except ServiceError:
                            await self._cancel_parent(scope, state)
            finally:
                current = self._plans.get(state.request.plan_id)
                if current is not None and current[1] is worker:
                    self._plans.pop(state.request.plan_id, None)
                leave_workspace_execution(marker)

    async def validate_result(self, kind: str, value: Any, context: ToolContext) -> None:
        try:
            parsed = TreePlanView.model_validate_json(json.dumps(value)).model_dump(mode="json")
            if context._tree_model_response != (kind, json.dumps(parsed, sort_keys=True)):
                raise ValueError()
        except (TypeError, ValueError):
            raise ToolError(
                "workspace_tree_model_unverified", "模型树回程未关联本次原事实"
            ) from None

    def assert_external_close(self) -> None:
        if inside_workspace_execution(self._tasks):
            raise ServiceError("workspace_tree_model_reentrant", "模型树回调不能关闭当前服务", 409)

    async def cancel_if_running(self, scope: str, plan_id: str) -> bool:
        current = self._plans.get(plan_id)
        if current is None or current[0] != scope:
            return False
        await _wait(asyncio.create_task(self.store.cancel(scope, plan_id)), lambda: None)
        task = current[1]
        if not task.done() and not task.cancelling():
            task.cancel()
        if not inside_workspace_execution():
            await asyncio.gather(task, return_exceptions=True)
        return True

    async def close(self) -> None:
        self.assert_external_close()
        self._closed = True
        tasks = [task for _scope, task in self._tasks.values()]
        for task in tasks:
            if not task.done() and not task.cancelling():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._plans.clear()
