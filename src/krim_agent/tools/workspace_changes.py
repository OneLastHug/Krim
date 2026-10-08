"""工作区修改工具；所有入口共享ToolRegistry的权限、Hook与输出验证。"""

from collections.abc import Awaitable, Callable
from functools import cache

from pydantic import BaseModel, JsonValue

from krim_agent.workspace_change import (
    WorkspaceAttemptView,
    WorkspaceChangeAction,
    WorkspaceChangeView,
    WorkspaceOperationRequest,
    WorkspacePreviewRequest,
)

from .core import ToolContext, ToolDefinition, ToolError


@cache
def workspace_change_tools() -> tuple[ToolDefinition, ...]:
    def executor(
        operation: str,
    ) -> Callable[[BaseModel, ToolContext], Awaitable[dict[str, JsonValue]]]:
        async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
            if context.workspace_changes is None:
                raise ToolError("workspace_backend_missing", "当前上下文没有持久工作区修改服务")
            return await context.workspace_changes.tool_operation(
                operation, arguments.model_dump(), context
            )

        return execute

    return (
        ToolDefinition(
            "workspace_preview",
            "冻结文件修改提案与Diff，不发布文件修改",
            WorkspacePreviewRequest,
            executor("preview"),
            "read",
            output_schema=WorkspaceChangeView,
        ),
        ToolDefinition(
            "workspace_change",
            "按原版本应用、撤销或保留冻结提案；幂等重试只读取原操作",
            WorkspaceChangeAction,
            executor("change"),
            "write",
            output_schema=WorkspaceAttemptView,
        ),
        ToolDefinition(
            "workspace_reconcile",
            "只读取原目标与恢复材料核实不确定操作，不重放文件修改",
            WorkspaceOperationRequest,
            executor("reconcile"),
            "write",
            output_schema=WorkspaceAttemptView,
        ),
        ToolDefinition(
            "workspace_finalize",
            "显式清理已确认操作的私有恢复条目，保留持久旧快照",
            WorkspaceOperationRequest,
            executor("finalize"),
            "write",
            output_schema=WorkspaceAttemptView,
        ),
    )
