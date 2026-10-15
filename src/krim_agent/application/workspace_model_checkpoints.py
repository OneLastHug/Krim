"""模型检查点父调用内创建/查询/取消，不借宿主回调脱离原Run与预算。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from typing import Any
from uuid import uuid4

from pydantic import JsonValue

from krim_agent.persistence.agent_sessions import guard_agent_topic
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import (
    ExecutionBudgetBase,
    ToolContext,
    ToolError,
    execution_budget_base,
    same_execution_budget,
)
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import (
    CheckpointCaptureFence,
    CheckpointModelSource,
    WorkspaceCheckpointCreate,
    WorkspaceCheckpointView,
)
from krim_agent.workspace_checkpoint_control import (
    CheckpointLookup,
    CheckpointQuery,
    CheckpointQueryResult,
)

from .workspace_changes import _wait
from .workspace_checkpoints import CheckpointInvocation, WorkspaceCheckpointService
from .workspace_execution import (
    WorkspaceTasks,
    enter_workspace_execution,
    leave_workspace_execution,
)

_current: ContextVar[tuple[object, str] | None] = ContextVar(
    "krim_model_checkpoint_control", default=None
)


class ModelCheckpointService:
    def __init__(self, checkpoints: WorkspaceCheckpointService) -> None:
        self.checkpoints, self.store = checkpoints, checkpoints.store
        self._calls: WorkspaceTasks = {}
        self._closed = False

    @asynccontextmanager
    async def call_scope(self) -> AsyncIterator[None]:
        if len(self._calls) >= 64:
            raise ToolError("workspace_checkpoint_busy", "模型检查点控制并发已满")
        task = asyncio.current_task()
        assert task is not None
        identifier = str(uuid4())
        self._calls[identifier] = ("control", task)
        marker = enter_workspace_execution(self._calls, identifier)
        control = _current.set((self, identifier))
        try:
            yield
        finally:
            _current.reset(control)
            self._calls.pop(identifier, None)
            leave_workspace_execution(marker)

    def assert_external_close(self) -> None:
        current = _current.get()
        entry = self._calls.get(current[1]) if current is not None and current[0] is self else None
        if entry is not None and not entry[1].done():
            raise ServiceError(
                "workspace_checkpoint_model_reentrant", "检查点控制回调不能关闭原服务", 409
            )

    def _source(self, context: ToolContext) -> CheckpointModelSource:
        if self._closed:
            raise ServiceError("workspace_checkpoint_model_closed", "模型检查点控制已关闭", 503)
        if (
            context.origin != "model"
            or context.run_id == "local"
            or context.agent_id is None
            or context.session_id is None
            or context.workspace_checkpoint_controls is not self
            or not context.execution_timeouts
            or any(
                item is not None
                for item in (
                    context.checkpoint_capture,
                    context.checkpoint_restore,
                    context.tree_fence,
                    context.workspace_batch,
                    context.mixed_batch,
                    context.command_capture,
                    context.source_call_id,
                )
            )
        ):
            raise ServiceError(
                "workspace_checkpoint_model_context",
                "检查点控制需要原Agent/session/Run/call预算",
                403,
            )
        return CheckpointModelSource(context.run_id, context.call_id)

    def _check_budget(self, context: ToolContext, base: ExecutionBudgetBase) -> None:
        self._source(context)
        if not same_execution_budget(base, execution_budget_base(context.execution_timeouts)):
            raise ServiceError("workspace_checkpoint_model_budget", "检查点原预算基准已改变", 403)
        loop = asyncio.get_running_loop()
        for budget in context.execution_timeouts:
            when = budget.deadline.when()
            if (
                not budget.active
                or budget.deadline.expired()
                or (when is not None and loop.time() >= when)
            ):
                raise ServiceError("tool_timeout", "原检查点调用预算已结束", 408)

    async def _guard(
        self, context: ToolContext, base: ExecutionBudgetBase, *, writing: bool = True
    ) -> tuple[str, WorkspaceBinding]:
        self._check_budget(context, base)
        if writing:
            return await self.checkpoints.changes._guard(context)
        binding = context.workspace
        if binding is None:
            raise ServiceError("workspace_unbound", "检查点历史没有原工作区绑定", 409)
        scope = f"session:{context.session_id}"
        async with self.store.database.write_lock, self.store.database.sessions() as sql:
            await self.store.edits._scope(sql, scope, writing=True, run_id=context.run_id)
            row = await guard_agent_topic(
                sql, context.topic_id, context.session_id, context.agent_id, self.store.owner_id
            )
            if row is None or row.permissions.get("workspace") != binding.manifest():
                raise ServiceError(
                    "workspace_binding_changed", "检查点历史冻结资源或主体已改变", 403
                )
        return scope, binding

    async def _lookup(self, scope: str, request: CheckpointLookup) -> WorkspaceCheckpointView:
        if request.checkpoint_id is not None:
            return await self.store.get(scope, request.checkpoint_id)
        assert request.request_key is not None
        value = await self.store.by_key(scope, request.request_key)
        if value is None:
            raise ServiceError("workspace_checkpoint_not_found", "原检查点键不存在", 404)
        return value

    async def tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        try:
            source = self._source(context)
            base = execution_budget_base(context.execution_timeouts)
            scope, binding = await self._guard(context, base, writing=operation == "create")
            if operation == "create":
                result = await self._create(
                    WorkspaceCheckpointCreate.model_validate(payload),
                    context,
                    source,
                    base,
                    scope,
                    binding,
                )
                value = result.model_dump(mode="json")
            elif operation == "cancel":
                original = await self._lookup(scope, CheckpointLookup.model_validate(payload))
                result = await self.checkpoints.cancel(scope, original.checkpoint_id)
                # 根控制标记不能让外部取消跳过等待真正的捕获生产者。
                running = self.checkpoints._tasks.get(original.checkpoint_id)
                if running is not None and running[0] == scope:
                    await _wait(running[1], lambda: None)
                value = (await self.store.get(scope, result.checkpoint_id)).model_dump(mode="json")
            elif operation == "query":
                query = CheckpointQuery.model_validate(payload)
                page = CheckpointQueryResult(kind=query.kind)
                if query.kind == "history":
                    page.entries = await self.store.list(
                        scope, offset=query.offset, limit=query.limit
                    )
                elif query.kind == "id":
                    assert query.checkpoint_id is not None
                    page.checkpoint = await self.store.get(scope, query.checkpoint_id)
                else:
                    assert query.request_key is not None
                    page.checkpoint = await self.store.by_key(scope, query.request_key)
                value = page.model_dump(mode="json")
            else:
                raise ServiceError(
                    "workspace_checkpoint_model_operation", "检查点控制动作无效", 422
                )
            await self._guard(context, base, writing=operation == "create")
            context._checkpoint_control_response = (operation, json.dumps(value, sort_keys=True))
            return value
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def _create(
        self,
        request: WorkspaceCheckpointCreate,
        context: ToolContext,
        source: CheckpointModelSource,
        base: ExecutionBudgetBase,
        scope: str,
        binding: WorkspaceBinding,
    ) -> WorkspaceCheckpointView:
        assert context.agent_id is not None
        identity = (
            context.agent_id,
            context.session_id,
            context.topic_id,
            context.depth,
            context.timeout,
            context.budget_account_ids,
            id(context.workspace),
            id(context.registry),
        )

        def qualify() -> None:
            actual = (
                context.agent_id,
                context.session_id,
                context.topic_id,
                context.depth,
                context.timeout,
                context.budget_account_ids,
                id(context.workspace),
                id(context.registry),
            )
            if self._source(context) != source or actual != identity:
                raise ServiceError(
                    "workspace_checkpoint_model_context", "原模型捕获上下文被替换", 403
                )
            self._check_budget(context, base)

        if len(self.checkpoints._tasks) >= self.checkpoints.max_active:
            if await self.store.by_key(scope, request.request_key) is None:
                raise ServiceError("workspace_checkpoint_busy", "检查点捕获并发已满", 429)

        async def authorize(agent_id: str) -> tuple[str, WorkspaceBinding]:
            qualify()
            if agent_id != context.agent_id or self._source(context) != source:
                raise ServiceError("workspace_checkpoint_model_context", "检查点Agent被重定向", 403)
            actual_scope, actual = await self._guard(context, base)
            if actual_scope != scope or actual is not binding:
                raise ServiceError("workspace_binding_changed", "检查点原资源身份已变化", 403)
            return actual_scope, actual

        async def read(
            agent_id: str, path: str, fence: CheckpointCaptureFence, confirmed: bool
        ) -> object:
            await authorize(agent_id)
            if confirmed or fence.model_source != source:
                raise ServiceError(
                    "workspace_checkpoint_model_context", "模型捕获父来源不匹配", 403
                )
            child = replace(
                context,
                checkpoint_capture=fence,
                source_call_id=source.call_id,
                workspace_checkpoints=self.checkpoints,
            )
            name = (
                "workspace_checkpoint_capture"
                if fence.capture_format == "bytes"
                else "workspace_read"
            )
            response = await child.registry.invoke(name, {"path": path}, child)
            if response.is_error:
                raise ServiceError(
                    response.code or "workspace_checkpoint_not_captured",
                    "检查点内部读取未通过原工具链",
                    409,
                )
            return response.data

        task = asyncio.create_task(
            self.checkpoints.create_model(
                context.agent_id, request, CheckpointInvocation(source, authorize, read, qualify)
            )
        )
        view = None
        try:
            try:
                view = await asyncio.shield(task)
            except asyncio.CancelledError:
                view = await _wait(task, lambda: None)
                raise
            while view.status == "capturing":
                await authorize(context.agent_id)
                entry = self.checkpoints._tasks.get(view.checkpoint_id)
                if entry is not None and entry[0] == scope:
                    await asyncio.shield(entry[1])
                else:
                    await asyncio.sleep(0.02)
                view = await self.store.get(scope, view.checkpoint_id)
            return view
        except BaseException:
            if view is None:
                candidate = await self.store.by_key(scope, request.request_key)
                if (
                    candidate is not None
                    and await self.store.source(scope, candidate.checkpoint_id) == source
                ):
                    view = candidate
            if view is not None:
                await _wait(
                    asyncio.create_task(self.checkpoints.cancel(scope, view.checkpoint_id)),
                    lambda: None,
                )
                running = self.checkpoints._tasks.get(view.checkpoint_id)
                if running is not None and running[0] == scope:
                    await _wait(running[1], lambda: None)
            raise

    async def validate_result(self, operation: str, value: Any, context: ToolContext) -> None:
        if context._checkpoint_control_response != (operation, json.dumps(value, sort_keys=True)):
            raise ToolError("workspace_checkpoint_model_unverified", "检查点控制回程未保留原事实")

    async def close(self) -> None:
        self.assert_external_close()
        self._closed = True
