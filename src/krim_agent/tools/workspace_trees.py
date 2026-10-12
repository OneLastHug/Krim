"""受原Registry保护的目录树准备；仅持久冻结，不启动生产者或Writer。"""

import json
from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_tree import TreePlanRequest, TreePlanView

from .core import ToolContext, ToolDefinition, ToolError


@cache
def tree_prepare_tool() -> ToolDefinition:
    async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
        if context.workspace_trees is None:
            raise ToolError("workspace_tree_backend_missing", "当前上下文缺少目录树准备服务")
        try:
            value = await context.workspace_trees.prepare(
                TreePlanRequest.model_validate(arguments.model_dump()), context
            )
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error
        result = value.model_dump(mode="json")
        context._tree_prepare_response = json.dumps(result, sort_keys=True)
        return result

    async def guard(value: Any, context: ToolContext) -> None:
        if context._tree_prepare_response != json.dumps(value, sort_keys=True):
            raise ToolError("workspace_tree_prepare_unverified", "树准备回程未保留原持久计划事实")

    return ToolDefinition(
        "workspace_tree_prepare",
        "从原命令目录来源准备持久planned树，不发布文件或目录",
        TreePlanRequest,
        execute,
        "read",
        output_schema=TreePlanView,
        output_guard=guard,
    )
