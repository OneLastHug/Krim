"""目录条件事务工具；公开工具只取已登记workspace与原Registry上下文。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.workspace_directory import (
    DirectoryAttemptView,
    DirectoryChangeAction,
    DirectoryOperationRequest,
    DirectoryPreviewRequest,
    DirectoryPreviewView,
)

from .core import ToolContext, ToolDefinition, ToolError


@cache
def directory_change_tools() -> tuple[ToolDefinition, ...]:
    async def execute(
        operation: str, arguments: BaseModel, context: ToolContext
    ) -> dict[str, JsonValue]:
        if context.directory_changes is None:
            raise ToolError("workspace_directory_backend_missing", "当前上下文没有目录事务服务")
        return await context.directory_changes.tool_operation(
            operation, arguments.model_dump(), context
        )

    async def guard(operation: str, value: Any, context: ToolContext) -> None:
        if context.directory_changes is None:
            raise ToolError("workspace_directory_backend_missing", "当前上下文没有目录事务服务")
        await context.directory_changes.validate_result(operation, value, context)

    return (
        ToolDefinition(
            "directory_preview",
            "冻结单个已授权空目录的创建或删除条件提案，不发布目录",
            DirectoryPreviewRequest,
            lambda arguments, context: execute("preview", arguments, context),
            "read",
            output_schema=DirectoryPreviewView,
            output_guard=lambda value, context: guard("preview", value, context),
        ),
        ToolDefinition(
            "directory_change",
            "按原版本应用、撤销或保留单个空目录提案",
            DirectoryChangeAction,
            lambda arguments, context: execute("change", arguments, context),
            "write",
            output_schema=DirectoryAttemptView,
            output_guard=lambda value, context: guard("change", value, context),
        ),
        ToolDefinition(
            "directory_reconcile",
            "只读核实不确定目录操作，不重放目录系统调用",
            DirectoryOperationRequest,
            lambda arguments, context: execute("reconcile", arguments, context),
            "write",
            output_schema=DirectoryAttemptView,
            output_guard=lambda value, context: guard("reconcile", value, context),
        ),
        ToolDefinition(
            "directory_finalize",
            "清理已确认目录操作的私有恢复材料",
            DirectoryOperationRequest,
            lambda arguments, context: execute("finalize", arguments, context),
            "write",
            output_schema=DirectoryAttemptView,
            output_guard=lambda value, context: guard("finalize", value, context),
        ),
    )
