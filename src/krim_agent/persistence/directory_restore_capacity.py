"""完整/缺失恢复共用的总量计费；调用方持有同一SQL writer事务。"""

from sqlalchemy import LargeBinary, Text, and_, case, func, select
from sqlalchemy import cast as sql_cast
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.expression import SQLColumnExpression

from .database import (
    WorkspaceContainerRetentionModelSourceRow,
    WorkspaceContainerRetentionRow,
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestorePlanRow,
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestorePlanRow,
)
from .directory_restore_codec import MAX_METADATA


async def staging_usage(sql: AsyncSession) -> tuple[int, int]:
    total_bytes = total_entries = 0
    for plan in (WorkspaceDirectoryRestorePlanRow, WorkspaceMissingDirectoryRestorePlanRow):
        used_bytes, used_entries = (
            await sql.execute(
                select(
                    func.coalesce(func.sum(plan.staging_reserved_bytes), 0),
                    func.coalesce(func.sum(plan.staging_reserved_entries), 0),
                )
            )
        ).one()
        total_bytes += int(used_bytes)
        total_entries += int(used_entries)
    return total_bytes, total_entries


def _json_bytes[Value](column: SQLColumnExpression[Value], sqlite: bool) -> ColumnElement[int]:
    value = sql_cast(column, Text)
    return func.coalesce(
        func.length(sql_cast(value, LargeBinary)) if sqlite else func.octet_length(value), 0
    )


async def material_usage(sql: AsyncSession, *, sqlite: bool) -> int:
    used = 0
    for plan, attempt, baseline in (
        (
            WorkspaceDirectoryRestorePlanRow,
            WorkspaceDirectoryRestoreAttemptRow,
            (WorkspaceDirectoryRestorePlanRow.baseline_summary,),
        ),
        (
            WorkspaceMissingDirectoryRestorePlanRow,
            WorkspaceMissingDirectoryRestoreAttemptRow,
            (WorkspaceMissingDirectoryRestorePlanRow.absence_metadata,),
        ),
    ):
        size: ColumnElement[int] = func.coalesce(func.length(plan.prepared_payload), 0)
        for column in (plan.stage_metadata, plan.prepared_metadata, plan.target_summary, *baseline):
            size = size + _json_bytes(column, sqlite)
        # 原生产/unknown预留最大回执，完成后只计真实JSON，不重复计staged BLOB。
        size = size + case(
            (
                and_(
                    plan.prepared_metadata.is_not(None),
                    attempt.status.in_(("running", "uncertain")),
                ),
                MAX_METADATA,
            ),
            else_=_json_bytes(attempt.effect_metadata, sqlite),
        )
        used += int(
            await sql.scalar(
                select(func.coalesce(func.sum(size), 0))
                .select_from(plan)
                .outerjoin(attempt, attempt.plan_id == plan.plan_id)
            )
            or 0
        )
    retention_size: ColumnElement[int] = _json_bytes(
        WorkspaceContainerRetentionRow.source_metadata, sqlite
    )
    retention_size = retention_size + _json_bytes(
        WorkspaceContainerRetentionRow.request_metadata, sqlite
    )
    retention_size = retention_size + _json_bytes(
        WorkspaceContainerRetentionRow.model_sources_metadata, sqlite
    )
    # 已准入的回收/unknown预留有界回执和核实历史，避免其他来源填满容量后无法收束。
    retention_size = retention_size + case(
        (WorkspaceContainerRetentionRow.status.in_(("running", "unknown")), 69632),
        else_=_json_bytes(WorkspaceContainerRetentionRow.receipt_metadata, sqlite)
        + _json_bytes(WorkspaceContainerRetentionRow.verification_metadata, sqlite),
    )
    source_bytes = sum(
        (
            _json_bytes(column, sqlite)
            for column in (
                WorkspaceContainerRetentionModelSourceRow.run_id,
                WorkspaceContainerRetentionModelSourceRow.call_id,
                WorkspaceContainerRetentionModelSourceRow.operation_id,
                WorkspaceContainerRetentionModelSourceRow.phase,
            )
        ),
        8,
    )
    used += int(await sql.scalar(select(func.coalesce(func.sum(source_bytes), 0))) or 0)
    return used + int(
        await sql.scalar(
            select(func.coalesce(func.sum(retention_size), 0)).select_from(
                WorkspaceContainerRetentionRow
            )
        )
        or 0
    )
