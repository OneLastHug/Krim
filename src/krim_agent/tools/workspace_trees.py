"""受原Registry保护的目录树准备和查询；不启动生产者或Writer。"""

import json
from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_tree import (
    TreePlanQuery,
    TreePlanQueryResult,
    TreePlanRequest,
    TreePlanView,
)

from .core import ToolContext, ToolDefinition, ToolError


@cache
def tree_query_tool() -> ToolDefinition:
    async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
        if context.workspace_tree_queries is None:
            raise ToolError("workspace_tree_backend_missing", "当前上下文缺少目录树查询服务")
        return await context.workspace_tree_queries.tool_query(arguments.model_dump(), context)

    async def guard(value: Any, context: ToolContext) -> None:
        if context.workspace_tree_queries is None:
            raise ToolError("workspace_tree_backend_missing", "当前上下文缺少目录树查询服务")
        await context.workspace_tree_queries.validate_query(value, context)

    return ToolDefinition(
        "workspace_tree_query",
        "读取当前会话原目录树、原请求键或步骤页，不准备、启动或重放目录文件动作",
        TreePlanQuery,
        execute,
        "read",
        output_schema=TreePlanQueryResult,
        output_guard=guard,
    )


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
