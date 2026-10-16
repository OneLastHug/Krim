"""显式缺失基线回退工具，复用原Move后端与同调用回程核实。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.workspace_missing_directory_rollback import (
    MissingDirectoryRollbackAction,
    MissingDirectoryRollbackAttemptView,
    MissingDirectoryRollbackPreview,
    MissingDirectoryRollbackView,
)
from krim_agent.workspace_missing_directory_rollback_control import (
    MissingDirectoryRollbackCancel,
    MissingDirectoryRollbackQuery,
    MissingDirectoryRollbackQueryResult,
)
from krim_agent.workspace_tree_move import TreeMoveOperationRequest

from .core import Risk, ToolContext, ToolDefinition, ToolError


@cache
def missing_directory_rollback_tools() -> tuple[ToolDefinition, ...]:
    def definition(
        name: str,
        operation: str,
        description: str,
        request: type[BaseModel],
        response: type[BaseModel],
        risk: Risk,
    ) -> ToolDefinition:
        async def run(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
            if context.tree_moves is None:
                raise ToolError(
                    "workspace_missing_directory_rollback_backend_missing", "当前没有缺失回退后端"
                )
            return await context.tree_moves.tool_operation(
                operation, arguments.model_dump(), context
            )

        async def verify(value: Any, context: ToolContext) -> None:
            if context.tree_moves is None:
                raise ToolError(
                    "workspace_missing_directory_rollback_backend_missing", "当前没有缺失回退后端"
                )
            await context.tree_moves.validate_result(operation, value, context)

        return ToolDefinition(
            name, description, request, run, risk, output_schema=response, output_guard=verify
        )

    specs: tuple[tuple[str, str, str, type[BaseModel], type[BaseModel], Risk], ...] = (
        (
            "missing_directory_rollback_preview",
            "missing_rollback_preview",
            "审阅原完成缺失恢复的整树保留回退，明确当前版本和保留位置",
            MissingDirectoryRollbackPreview,
            MissingDirectoryRollbackView,
            "read",
        ),
        (
            "missing_directory_rollback_query",
            "missing_rollback_query",
            "只读缺失基线回退原提案/操作/原键与分页历史，不重放副作用",
            MissingDirectoryRollbackQuery,
            MissingDirectoryRollbackQueryResult,
            "read",
        ),
        (
            "missing_directory_rollback_change",
            "missing_rollback_change",
            "按原版本完整保留回到缺失、反向恢复原树或保留核实",
            MissingDirectoryRollbackAction,
            MissingDirectoryRollbackAttemptView,
            "write",
        ),
        (
            "missing_directory_rollback_reconcile",
            "missing_rollback_reconcile",
            "只读核实同一个unknown回退操作，不重新移动或生成目录",
            TreeMoveOperationRequest,
            MissingDirectoryRollbackAttemptView,
            "write",
        ),
        (
            "missing_directory_rollback_cancel",
            "missing_rollback_cancel",
            "关闭原回退准入并等待真实worker收束，不自动回滚目录",
            MissingDirectoryRollbackCancel,
            MissingDirectoryRollbackView,
            "write",
        ),
    )
    return tuple(definition(*spec) for spec in specs)
