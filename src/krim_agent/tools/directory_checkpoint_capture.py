"""目录捕获叶工具；仅私有Fence可进入，全部Hook后才完成原捕获任务。"""

import json
from functools import cache
from typing import Any

from pydantic import BaseModel

from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointCaptureInput,
    DirectoryCheckpointCaptureView,
)

from .core import ToolContext, ToolDefinition, ToolError


@cache
def directory_checkpoint_capture_tool() -> ToolDefinition:
    async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, object]:
        if context.workspace_directory_checkpoints is None:
            raise ToolError("workspace_directory_capture_backend", "缺少原目录捕获服务")
        value = await context.workspace_directory_checkpoints.capture_metadata(
            arguments.model_dump(), context
        )
        context._directory_checkpoint_capture_response = json.dumps(value, sort_keys=True)
        return value

    async def guard(value: Any, context: ToolContext) -> None:
        if context.workspace_directory_checkpoints is None:
            raise ToolError("workspace_directory_capture_backend", "缺少原目录捕获服务")
        if context._directory_checkpoint_capture_response != json.dumps(value, sort_keys=True):
            raise ToolError(
                "workspace_directory_capture_unverified", "目录回程未保留原完整材料摘要"
            )
        await context.workspace_directory_checkpoints.validate_capture_metadata(value, context)

    return ToolDefinition(
        "workspace_directory_checkpoint_capture",
        "仅在原私有目录检查点上下文捕获完整目录与普通文件字节；只返回安全元数据",
        DirectoryCheckpointCaptureInput,
        execute,
        "read",
        output_schema=DirectoryCheckpointCaptureView,
        output_guard=guard,
    )
