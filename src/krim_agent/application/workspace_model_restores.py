"""原模型恢复控制：预览基线与文件子项都继承当前父调用。"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from pydantic import JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ExecutionBudgetBase, ToolContext, execution_budget_base
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_change import WorkspaceChangeAction
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_restore import (
    CheckpointRestoreExecute,
    CheckpointRestoreFence,
    CheckpointRestorePreview,
    CheckpointRestoreView,
)
from krim_agent.workspace_restore_control import RestoreLookup, RestoreQuery, RestoreQueryResult

from .workspace_changes import _wait
from .workspace_checkpoints import CheckpointInvocation
from .workspace_restores import RestoreInvocation, WorkspaceRestoreService

if TYPE_CHECKING:
    from .workspace_model_checkpoints import ModelCheckpointService


class ModelRestoreService:
    def __init__(self, controls: ModelCheckpointService, restores: WorkspaceRestoreService) -> None:
        self.controls, self.restores, self.store = controls, restores, restores.store

    async def _lookup(self, scope: str, request: RestoreLookup) -> CheckpointRestoreView:
        if request.plan_id is not None:
            return await self.store.get(scope, request.plan_id)
        assert request.request_key is not None
        value = await self.store.by_key(scope, request.request_key)
        if value is None:
            raise ServiceError("workspace_restore_not_found", "原恢复预览键不存在", 404)
        return value

    def _invocation(
        self,
        context: ToolContext,
        source: CheckpointModelSource,
        base: ExecutionBudgetBase,
        scope: str,
        binding: WorkspaceBinding,
        plan_id: str | None,
    ) -> RestoreInvocation:
        original = self.controls._invocation(context, source, base, scope, binding)
        assert original.qualify is not None

        def baseline(identifier: str) -> CheckpointInvocation:
            if plan_id is not None:
                raise ServiceError(
                    "workspace_restore_model_context", "执行调用不能新建预览基线", 403
                )
            return self.controls._invocation(
                context, replace(source, restore_plan_id=identifier), base, scope, binding
            )

        async def execute(
            agent_id: str,
            request: WorkspaceChangeAction,
            fence: CheckpointRestoreFence,
            confirmed: bool,
        ) -> object:
            await original.authorize(agent_id)
            if confirmed or fence.model_source != source or fence.plan_id != plan_id:
                raise ServiceError("workspace_restore_model_context", "原执行来源或计划被替换", 403)
            child = replace(context, checkpoint_restore=fence, source_call_id=source.call_id)
            result = await child.registry.invoke("workspace_change", request.model_dump(), child)
            if result.is_error:
                raise ServiceError(
                    result.code or "workspace_restore_execution_failed",
                    "恢复文件子项未通过原工具链",
                    409,
                )
            return result.data

        return RestoreInvocation(source, original.authorize, baseline, execute, original.qualify)

    async def _cancel_and_drain(self, scope: str, identifier: str) -> CheckpointRestoreView:
        async def stop() -> CheckpointRestoreView:
            await self.restores.cancel(scope, identifier)
            for phase in ("prepare:", "execute:"):
                entry = self.restores._tasks.get(phase + identifier)
                if entry is not None and entry[0] == scope:
                    await asyncio.gather(entry[1], return_exceptions=True)
            view = await self.store.get(scope, identifier)
            if view.baseline_checkpoint_id is not None:
                entry = self.restores.checkpoints._tasks.get(view.baseline_checkpoint_id)
                if entry is not None and entry[0] == scope:
                    await asyncio.gather(entry[1], return_exceptions=True)
            return await self.store.get(scope, identifier)

        return await _wait(asyncio.create_task(stop()), lambda: None)

    async def _operate(
        self,
        operation: str,
        request: CheckpointRestorePreview | CheckpointRestoreExecute,
        context: ToolContext,
        source: CheckpointModelSource,
        base: ExecutionBudgetBase,
        scope: str,
        binding: WorkspaceBinding,
    ) -> CheckpointRestoreView:
        assert context.agent_id is not None
        plan_id = request.plan_id if isinstance(request, CheckpointRestoreExecute) else None
        invocation = self._invocation(context, source, base, scope, binding, plan_id)
        phase = "execute" if plan_id is not None else "preview"
        task = asyncio.create_task(
            self.restores.start_model(context.agent_id, request, invocation)
            if isinstance(request, CheckpointRestoreExecute)
            else self.restores.preview_model(context.agent_id, request, invocation)
        )
        view = None
        try:
            try:
                view = await asyncio.shield(task)
            except asyncio.CancelledError:
                view = await _wait(task, lambda: None)
                raise
            while view.phase in {"preparing", "executing"}:
                await invocation.authorize(context.agent_id)
                key = ("prepare:" if view.phase == "preparing" else "execute:") + view.plan_id
                entry = self.restores._tasks.get(key)
                if entry is not None and entry[0] == scope:
                    await asyncio.shield(entry[1])
                else:
                    await asyncio.sleep(0.02)
                view = await self.store.get(scope, view.plan_id)
            return view
        except BaseException:
            if view is None:
                candidate = (
                    await self.store.get(scope, request.plan_id)
                    if isinstance(request, CheckpointRestoreExecute)
                    else await self.store.by_key(scope, request.request_key)
                )
                if (
                    candidate is not None
                    and await self.store.source(scope, candidate.plan_id, phase) == source
                ):
                    view = candidate
            if view is not None:
                await self._cancel_and_drain(scope, view.plan_id)
            raise

    async def tool_operation(
        self, operation: str, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        source = self.controls._source(context)
        base = execution_budget_base(context.execution_timeouts)
        writing = operation in {"restore_preview", "restore_start"}
        scope, binding = await self.controls._guard(context, base, writing=writing)
        if operation in {"restore_preview", "restore_start"}:
            request = (
                CheckpointRestorePreview.model_validate(payload)
                if operation == "restore_preview"
                else CheckpointRestoreExecute.model_validate(payload)
            )
            view = await self._operate(operation, request, context, source, base, scope, binding)
            value = view.model_dump(mode="json")
        elif operation == "restore_cancel":
            original = await self._lookup(scope, RestoreLookup.model_validate(payload))
            value = (await self._cancel_and_drain(scope, original.plan_id)).model_dump(mode="json")
        elif operation == "restore_query":
            query = RestoreQuery.model_validate(payload)
            page = RestoreQueryResult(kind=query.kind)
            if query.kind == "history":
                page.entries = await self.store.history(
                    scope, offset=query.offset, limit=query.limit
                )
            elif query.kind == "id":
                assert query.plan_id is not None
                page.plan = await self.store.get(scope, query.plan_id)
            else:
                assert query.request_key is not None
                page.plan = await self.store.by_key(scope, query.request_key)
            value = page.model_dump(mode="json")
        else:
            raise ServiceError("workspace_restore_model_operation", "模型恢复动作无效", 422)
        await self.controls._guard(context, base, writing=writing)
        context._checkpoint_control_response = (operation, json.dumps(value, sort_keys=True))
        return value
