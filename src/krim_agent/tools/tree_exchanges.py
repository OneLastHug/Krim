"""双完整树工具只使用原上下文后端；公开请求不承载私有树材料。"""

from functools import cache
from typing import Any

from pydantic import BaseModel, JsonValue

from krim_agent.persistence.completed_tree_origins import CompletedTreeOriginView
from krim_agent.workspace_tree_exchange_types import (
    TreeExchangeAction,
    TreeExchangeAttemptView,
    TreeExchangeInspectRequest,
    TreeExchangeInspectView,
    TreeExchangeOperationRequest,
    TreeExchangePreviewRequest,
    TreeExchangePreviewView,
    TreeExchangeQueryRequest,
    TreeExchangeQueryResult,
    TreeExchangeSealRequest,
)

from .core import Risk, ToolContext, ToolDefinition, ToolError


@cache
def tree_exchange_tools() -> tuple[ToolDefinition, ...]:
    async def execute(
        operation: str, arguments: BaseModel, context: ToolContext
    ) -> dict[str, JsonValue]:
        if context.tree_exchanges is None:
            raise ToolError(
                "workspace_tree_exchange_backend_missing", "当前上下文没有原双树交换服务"
            )
        return await context.tree_exchanges.tool_operation(
            operation, arguments.model_dump(), context
        )

    async def guard(operation: str, value: Any, context: ToolContext) -> None:
        if context.tree_exchanges is None:
            raise ToolError(
                "workspace_tree_exchange_backend_missing", "当前上下文没有原双树交换服务"
            )
        await context.tree_exchanges.validate_result(operation, value, context)

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
            "tree_exchange_inspect",
            "inspect",
            "只读完整有界目录树摘要与原指纹，不公开字节或inode",
            TreeExchangeInspectRequest,
            TreeExchangeInspectView,
            "read",
        ),
        (
            "tree_exchange_seal",
            "seal",
            "首次封存原已完成生成树的完整实际版本，重复仅原事实",
            TreeExchangeSealRequest,
            CompletedTreeOriginView,
            "read",
        ),
        (
            "tree_exchange_preview",
            "preview",
            "冻结两完整树原条件交换提案，可关联原已封存生成树",
            TreeExchangePreviewRequest,
            TreeExchangePreviewView,
            "read",
        ),
        (
            "tree_exchange_query",
            "query",
            "只读原双树交换提案、操作、原键及分页历史，不重放动作",
            TreeExchangeQueryRequest,
            TreeExchangeQueryResult,
            "read",
        ),
        (
            "tree_exchange_change",
            "change",
            "按原版本应用、撤销或保留完整目录双树交换提案",
            TreeExchangeAction,
            TreeExchangeAttemptView,
            "write",
        ),
        (
            "tree_exchange_reconcile",
            "reconcile",
            "核实同一unknown双树交换，不重发原namespace动作",
            TreeExchangeOperationRequest,
            TreeExchangeAttemptView,
            "write",
        ),
    )
    return tuple(definition(*specification) for specification in specifications)
