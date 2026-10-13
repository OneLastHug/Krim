"""显式原生成根工具，固定映射原Move后端/同调用与canonical回程。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.generated_tree_undo import (
    GeneratedTreeUndoAction,
    GeneratedTreeUndoAttemptView,
    GeneratedTreeUndoCancelRequest,
    GeneratedTreeUndoPreviewRequest,
    GeneratedTreeUndoPreviewView,
    GeneratedTreeUndoQueryResult,
)
from krim_agent.workspace_tree_move import TreeMoveOperationRequest, TreeMoveQueryRequest

from .core import Risk, ToolContext, ToolDefinition, ToolError


@cache
def generated_tree_undo_tools() -> tuple[ToolDefinition, ...]:
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
                    "workspace_generated_undo_backend_missing", "当前上下文没有原根撤销服务"
                )
            return await context.tree_moves.tool_operation(
                operation, arguments.model_dump(), context
            )

        async def verify(value: Any, context: ToolContext) -> None:
            if context.tree_moves is None:
                raise ToolError(
                    "workspace_generated_undo_backend_missing", "当前上下文没有原根撤销服务"
                )
            await context.tree_moves.validate_result(operation, value, context)

        return ToolDefinition(
            name, description, request, run, risk, output_schema=response, output_guard=verify
        )

    specs: tuple[tuple[str, str, str, type[BaseModel], type[BaseModel], Risk], ...] = (
        (
            "generated_tree_undo_preview",
            "generated_undo_preview",
            "审阅原完成生成树整根撤销与明确保留位置，不重跑生成或隐式seal",
            GeneratedTreeUndoPreviewRequest,
            GeneratedTreeUndoPreviewView,
            "read",
        ),
        (
            "generated_tree_undo_query",
            "generated_undo_query",
            "只读原生成历史/根撤销/实际还原/原key和稳定分页事实",
            TreeMoveQueryRequest,
            GeneratedTreeUndoQueryResult,
            "read",
        ),
        (
            "generated_tree_undo_change",
            "generated_undo_change",
            "按原版本remove_root/restore_root/keep，保全原生成树和历史",
            GeneratedTreeUndoAction,
            GeneratedTreeUndoAttemptView,
            "write",
        ),
        (
            "generated_tree_undo_reconcile",
            "generated_undo_reconcile",
            "只核实同一unknown原生成根operation，零namespace重放",
            TreeMoveOperationRequest,
            GeneratedTreeUndoAttemptView,
            "write",
        ),
        (
            "generated_tree_undo_cancel",
            "generated_undo_cancel",
            "取消原根提案或关闭原producer，查询原operation收束事实",
            GeneratedTreeUndoCancelRequest,
            GeneratedTreeUndoPreviewView,
            "write",
        ),
    )
    return tuple(definition(*spec) for spec in specs)
