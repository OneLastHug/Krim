"""非空树工具只使用原上下文后端；公开请求不承载私有树材料。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveAttemptView,
    TreeMoveInspectRequest,
    TreeMoveInspectView,
    TreeMoveOperationRequest,
    TreeMovePreviewRequest,
    TreeMovePreviewView,
    TreeMoveQueryRequest,
    TreeMoveQueryResult,
)

from .core import Risk, ToolContext, ToolDefinition, ToolError


@cache
def tree_move_tools() -> tuple[ToolDefinition, ...]:
    async def execute(
        operation: str, arguments: BaseModel, context: ToolContext
    ) -> dict[str, JsonValue]:
        if context.tree_moves is None:
            raise ToolError("workspace_tree_move_backend_missing", "当前上下文没有原树移动服务")
        return await context.tree_moves.tool_operation(operation, arguments.model_dump(), context)

    async def guard(operation: str, value: Any, context: ToolContext) -> None:
        if context.tree_moves is None:
            raise ToolError("workspace_tree_move_backend_missing", "当前上下文没有原树移动服务")
        await context.tree_moves.validate_result(operation, value, context)

    def definition(
        name: str,
        operation: str,
        description: str,
        request: type[BaseModel],
        response: type[BaseModel],
        risk: Risk,
    ) -> ToolDefinition:
        async def run(arguments: BaseModel, context: ToolContext) -> dict[str, JsonValue]:
            return await execute(operation, arguments, context)

        async def verify(value: Any, context: ToolContext) -> None:
            await guard(operation, value, context)

        return ToolDefinition(
            name, description, request, run, risk, output_schema=response, output_guard=verify
        )

    specifications: tuple[tuple[str, str, str, type[BaseModel], type[BaseModel], Risk], ...] = (
        (
            "tree_move_inspect",
            "inspect",
            "只读完整有界目录树摘要与原指纹，不公开字节或inode",
            TreeMoveInspectRequest,
            TreeMoveInspectView,
            "read",
        ),
        (
            "tree_move_preview",
            "preview",
            "冻结原已有目录树与双父的条件移动提案，目标须缺失",
            TreeMovePreviewRequest,
            TreeMovePreviewView,
            "read",
        ),
        (
            "tree_move_query",
            "query",
            "只读原树移动提案、操作、原键及分页历史，不重放动作",
            TreeMoveQueryRequest,
            TreeMoveQueryResult,
            "read",
        ),
        (
            "tree_move_change",
            "change",
            "按原版本应用、撤销或保留完整目录树移动提案",
            TreeMoveAction,
            TreeMoveAttemptView,
            "write",
        ),
        (
            "tree_move_reconcile",
            "reconcile",
            "核实同一unknown树移动，不重发原namespace动作",
            TreeMoveOperationRequest,
            TreeMoveAttemptView,
            "write",
        ),
    )
    return tuple(definition(*specification) for specification in specifications)
