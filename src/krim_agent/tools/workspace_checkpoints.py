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
from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointCreate,
    DirectoryCheckpointView,
)
from krim_agent.workspace_directory_checkpoint_control import DirectoryCheckpointQueryResult
from krim_agent.workspace_restore import (
    CheckpointRestoreExecute,
    CheckpointRestorePreview,
    CheckpointRestoreView,
)
from krim_agent.workspace_restore_control import RestoreLookup, RestoreQuery, RestoreQueryResult

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


@cache
def checkpoint_restore_control_tools() -> tuple[ToolDefinition, ...]:
    return (
        ToolDefinition(
            "workspace_checkpoint_restore_preview",
            "原调用内捕获恢复前基线并准备原条件恢复计划",
            CheckpointRestorePreview,
            partial(_execute, "restore_preview"),
            "write",
            output_schema=CheckpointRestoreView,
            output_guard=partial(_guard, "restore_preview"),
        ),
        ToolDefinition(
            "workspace_checkpoint_restore_start",
            "按原预览版本执行逐文件条件恢复并等待真实回执",
            CheckpointRestoreExecute,
            partial(_execute, "restore_start"),
            "write",
            output_schema=CheckpointRestoreView,
            output_guard=partial(_guard, "restore_start"),
        ),
        ToolDefinition(
            "workspace_checkpoint_restore_query",
            "只读原恢复计划ID、预览键或有界历史",
            RestoreQuery,
            partial(_execute, "restore_query"),
            "read",
            output_schema=RestoreQueryResult,
            output_guard=partial(_guard, "restore_query"),
        ),
        ToolDefinition(
            "workspace_checkpoint_restore_cancel",
            "关闭原恢复准入并等待真实基线与写入收束",
            RestoreLookup,
            partial(_execute, "restore_cancel"),
            "write",
            output_schema=CheckpointRestoreView,
            output_guard=partial(_guard, "restore_cancel"),
        ),
    )


@cache
def directory_checkpoint_control_tools() -> tuple[ToolDefinition, ...]:
    return (
        ToolDefinition(
            "workspace_directory_checkpoint_create",
            "在原模型Run/call内捕获完整目录，保留原父预算与真实元数据",
            DirectoryCheckpointCreate,
            partial(_execute, "directory_create"),
            "write",
            output_schema=DirectoryCheckpointView,
            output_guard=partial(_guard, "directory_create"),
        ),
        ToolDefinition(
            "workspace_directory_checkpoint_query",
            "只读原目录捕获ID、请求键或有界历史，零再次捕获",
            CheckpointQuery,
            partial(_execute, "directory_query"),
            "read",
            output_schema=DirectoryCheckpointQueryResult,
            output_guard=partial(_guard, "directory_query"),
        ),
        ToolDefinition(
            "workspace_directory_checkpoint_cancel",
            "关闭原目录捕获准入并等待原生产者收束",
            CheckpointLookup,
            partial(_execute, "directory_cancel"),
            "write",
            output_schema=DirectoryCheckpointView,
            output_guard=partial(_guard, "directory_cancel"),
        ),
    )
