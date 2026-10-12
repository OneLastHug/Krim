"""模型目录树只读消费；原上下文守卫及本次Backend回程绑定。"""

from __future__ import annotations

import json
from typing import Any

from pydantic import JsonValue

from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.workspace_tree import TreePlanQuery, TreePlanQueryResult

from .workspace_changes import WorkspaceChangeService
from .workspace_directories import DirectoryChangeService


class TreePlanQueryService:
    def __init__(
        self, changes: WorkspaceChangeService, directories: DirectoryChangeService
    ) -> None:
        self.changes = changes
        self.store = WorkspaceTreePlanStore(changes.store, directories.store)

    async def tool_query(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        try:
            request = TreePlanQuery.model_validate(payload)
            scope, binding = await self.changes._guard(context, writing=False)
            plan = None
            steps = None
            if request.kind == "by_key":
                assert request.request_key is not None
                plan = await self.store.by_key(scope, request.request_key)
                original = plan
            else:
                assert request.plan_id is not None
                original = await self.store.get(scope, request.plan_id)
                if request.kind == "steps":
                    steps = await self.store.steps(
                        scope, request.plan_id, after=request.after, limit=request.limit
                    )
                else:
                    plan = original
            if original is not None and (
                original.workspace_key != binding.key
                or original.resource_fingerprint != binding.fingerprint
            ):
                raise ServiceError(
                    "workspace_tree_resource_conflict", "树查询不属于原工作区绑定", 403
                )
            await self.changes._guard(context, writing=False)
            value = TreePlanQueryResult(kind=request.kind, plan=plan, steps=steps).model_dump(
                mode="json"
            )
            context._tree_query_response = json.dumps(value, sort_keys=True)
            return value
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def validate_query(self, value: Any, context: ToolContext) -> None:
        try:
            parsed = TreePlanQueryResult.model_validate_json(json.dumps(value)).model_dump(
                mode="json"
            )
            if (
                context._tree_query_response is None
                or json.dumps(parsed, sort_keys=True) != context._tree_query_response
            ):
                raise ValueError()
        except (TypeError, ValueError):
            raise ToolError(
                "workspace_tree_query_unverified", "树查询回程未关联本次原账本"
            ) from None
        try:
            await self.changes._guard(context, writing=False)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error
