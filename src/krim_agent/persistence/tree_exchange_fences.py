"""双完整树交换完整物理namespace锁，沿旧File/Directory/Move同一事实。"""

from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.workspace_tree_exchange import PreparedTreeExchange, WorkspaceTreeExchangeWriter

from .tree_move_fences import NamespaceKeys, assert_namespace_available, move_keys


def exchange_keys(change: PreparedTreeExchange) -> NamespaceKeys:
    first, second = WorkspaceTreeExchangeWriter._moves(change)
    return move_keys(first) | move_keys(second)


async def assert_exchange_namespace_available(
    sql: AsyncSession,
    change: PreparedTreeExchange,
    *,
    exclude_operation_id: str | None = None,
) -> NamespaceKeys:
    for reference in WorkspaceTreeExchangeWriter._moves(change):
        await assert_namespace_available(sql, reference, exclude_operation_id=exclude_operation_id)
    return exchange_keys(change)
