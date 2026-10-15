"""显式模型检查点控制；旧读取/字节元数据工具Schema与宿主管理保持。"""

from functools import cache, partial
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.workspace_checkpoint import WorkspaceCheckpointCreate, WorkspaceCheckpointView
from krim_agent.workspace_checkpoint_control import (
    CheckpointLookup,
    CheckpointQuery,
    CheckpointQueryResult,
)

from .core import ToolContext, ToolDefinition, ToolError


async def _execute(
    operation: str, arguments: BaseModel, context: ToolContext
) -> dict[str, JsonValue]:
    if context.workspace_checkpoint_controls is None:
        raise ToolError("workspace_checkpoint_model_backend", "缺少模型检查点控制后端")
    return await context.workspace_checkpoint_controls.tool_operation(
        operation, arguments.model_dump(), context
    )


async def _guard(operation: str, value: Any, context: ToolContext) -> None:
    if context.workspace_checkpoint_controls is None:
        raise ToolError("workspace_checkpoint_model_backend", "缺少模型检查点控制后端")
    await context.workspace_checkpoint_controls.validate_result(operation, value, context)


@cache
def checkpoint_control_tools() -> tuple[ToolDefinition, ...]:
    return (
        ToolDefinition(
            "workspace_checkpoint_create",
            "在原模型调用内捕获完整有界文件检查点，返回真实元数据",
            WorkspaceCheckpointCreate,
            partial(_execute, "create"),
            "write",
            output_schema=WorkspaceCheckpointView,
            output_guard=partial(_guard, "create"),
        ),
        ToolDefinition(
            "workspace_checkpoint_query",
            "只读原检查点ID、请求键或有界历史，不重新捕获文件",
            CheckpointQuery,
            partial(_execute, "query"),
            "read",
            output_schema=CheckpointQueryResult,
            output_guard=partial(_guard, "query"),
        ),
        ToolDefinition(
            "workspace_checkpoint_cancel",
            "关闭原检查点捕获准入并等待原生产者收束",
            CheckpointLookup,
            partial(_execute, "cancel"),
            "write",
            output_schema=WorkspaceCheckpointView,
            output_guard=partial(_guard, "cancel"),
        ),
    )
