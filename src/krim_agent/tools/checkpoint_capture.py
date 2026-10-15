"""原生bytes捕获只回真实元数据，必须原内部Fence；不扩大UTF-8读取Schema。"""

import json
from functools import cache
from typing import Any

from pydantic import BaseModel

from krim_agent.workspace_checkpoint import CheckpointCaptureInput, CheckpointCaptureView

from .core import ToolContext, ToolDefinition, ToolError


@cache
def checkpoint_capture_tool() -> ToolDefinition:
    async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, object]:
        if context.workspace_checkpoints is None:
            raise ToolError("workspace_checkpoint_backend", "当前上下文缺少检查点捕获服务")
        result = await context.workspace_checkpoints.capture_metadata(
            arguments.model_dump(), context
        )
        context._checkpoint_capture_response = json.dumps(result, sort_keys=True)
        return result

    async def guard(value: Any, context: ToolContext) -> None:
        if context.workspace_checkpoints is None:
            raise ToolError("workspace_checkpoint_backend", "当前上下文缺少检查点捕获服务")
        if context._checkpoint_capture_response != json.dumps(value, sort_keys=True):
            raise ToolError("workspace_checkpoint_unverified", "捕获回程未保留原私有快照元数据")
        await context.workspace_checkpoints.validate_capture_metadata(value, context)

    return ToolDefinition(
        "workspace_checkpoint_capture",
        "仅在原生检查点内部冻结上下文捕获有界文件字节，返回真实种类/摘要/规模，不返回内容",
        CheckpointCaptureInput,
        execute,
        "read",
        output_schema=CheckpointCaptureView,
        output_guard=guard,
    )
