"""模型混合批次读取工具；不提供公开fence/owner/scope或后台写入口。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.workspace_mixed_batch import MixedBatchQuery, MixedBatchQueryResult

from .core import ToolContext, ToolDefinition, ToolError


@cache
def mixed_batch_query_tool() -> ToolDefinition:
    async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
        if context.workspace_mixed_queries is None:
            raise ToolError(
                "workspace_mixed_batch_backend_missing", "当前上下文缺少混合批次查询服务"
            )
        return await context.workspace_mixed_queries.tool_query(arguments.model_dump(), context)

    async def guard(value: Any, context: ToolContext) -> None:
        if context.workspace_mixed_queries is None:
            raise ToolError(
                "workspace_mixed_batch_backend_missing", "当前上下文缺少混合批次查询服务"
            )
        await context.workspace_mixed_queries.validate_query(value, context)

    return ToolDefinition(
        "workspace_mixed_query",
        "读取当前会话原混合批次、原请求键或分页历史，不开始或重放文件目录动作",
        MixedBatchQuery,
        execute,
        "read",
        output_schema=MixedBatchQueryResult,
        output_guard=guard,
    )
