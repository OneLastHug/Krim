"""模型混合批次写工具；来源资格只能由可信AgentService提供。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchView

from .core import ToolContext, ToolDefinition, ToolError


@cache
def mixed_batch_start_tool() -> ToolDefinition:
    async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
        if context.workspace_mixed_batches is None:
            raise ToolError("workspace_mixed_batch_backend_missing", "当前上下文缺少混合批次写服务")
        return await context.workspace_mixed_batches.tool_start(arguments.model_dump(), context)

    async def guard(value: Any, context: ToolContext) -> None:
        if context.workspace_mixed_batches is None:
            raise ToolError("workspace_mixed_batch_backend_missing", "当前上下文缺少混合批次写服务")
        await context.workspace_mixed_batches.validate_start(value, context)

    return ToolDefinition(
        "workspace_mixed_start",
        "按原提案和版本启动受权限控制的文件目录混合批次；每项独立执行并保留原回执",
        MixedBatchRequest,
        execute,
        "write",
        output_schema=MixedBatchView,
        output_guard=guard,
    )
