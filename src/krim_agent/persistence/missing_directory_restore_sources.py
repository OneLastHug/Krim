"""缺失目录恢复的原模型来源；与全部Checkpoint生产者双向排他。"""

from __future__ import annotations

from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource

from .database import (
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestoreModelSourceRow,
    WorkspaceMissingDirectoryRestorePlanRow,
)
from .directory_restore_sources import DirectoryRestoreSources, assert_new_call, valid_source
from .workspace_edits import WorkspaceEditStore


async def sources(
    sql: AsyncSession,
    edits: WorkspaceEditStore,
    plan: WorkspaceMissingDirectoryRestorePlanRow,
    attempt: WorkspaceMissingDirectoryRestoreAttemptRow | None,
) -> DirectoryRestoreSources:
    rows = list(
        await sql.scalars(
            select(WorkspaceMissingDirectoryRestoreModelSourceRow).where(
                WorkspaceMissingDirectoryRestoreModelSourceRow.plan_id == plan.plan_id
            )
        )
    )
    original: dict[str, CheckpointModelSource] = {}
    verification: list[tuple[str, CheckpointModelSource]] = []
    try:
        for row in rows:
            value = valid_source(CheckpointModelSource(row.run_id, row.call_id))
            assert value is not None
            if not plan.scope_id.startswith("session:"):
                raise ValueError("缺失目录恢复模型缺少原session")
            await edits._scope(sql, plan.scope_id, writing=False, run_id=value.run_id)
            if row.phase == "verify":
                if attempt is None or row.operation_id != attempt.operation_id:
                    raise ValueError("核实来源与原operation不匹配")
                verification.append((row.operation_id, value))
            elif row.phase in {"preview", "execute"}:
                if row.phase in original or (
                    row.phase == "preview"
                    and row.operation_id is not None
                    or row.phase == "execute"
                    and (attempt is None or row.operation_id != attempt.operation_id)
                ):
                    raise ValueError("原恢复阶段来源重复或错配")
                original[row.phase] = value
            else:
                raise ValueError("恢复来源阶段无效")
        if bool(plan.preview_model_bound) != ("preview" in original) or bool(
            plan.execute_model_bound
        ) != ("execute" in original):
            raise ValueError("原恢复模型来源或标记缺失")
        current = None
        if attempt is not None:
            if attempt.verification_model_count != len(
                verification
            ) or attempt.verification_count < len(verification):
                raise ValueError("原核实来源计数不一致")
            if attempt.verify_model_bound:
                if not isinstance(attempt.verification_run_id, str) or not isinstance(
                    attempt.verification_call_id, str
                ):
                    raise ValueError("当前核实来源不完整")
                current = valid_source(
                    CheckpointModelSource(attempt.verification_run_id, attempt.verification_call_id)
                )
                if current not in [value for _op, value in verification]:
                    raise ValueError("当前核实来源缺失")
            elif (
                attempt.verification_run_id is not None or attempt.verification_call_id is not None
            ):
                raise ValueError("当前核实丢失模型标记")
        return DirectoryRestoreSources(original.get("preview"), original.get("execute"), current)
    except (ValueError, TypeError, AttributeError, AssertionError, ServiceError):
        raise ServiceError(
            "workspace_missing_directory_restore_corrupt", "缺失目录恢复原阶段来源损坏或缺失", 409
        ) from None


async def admit(
    sql: AsyncSession,
    edits: WorkspaceEditStore,
    scope: str,
    plan_id: str,
    phase: Literal["preview", "execute", "verify"],
    source: CheckpointModelSource | None,
    operation_id: str | None = None,
    *,
    max_sources: int = 2000,
) -> None:
    value = valid_source(source)
    if value is None:
        return
    if not scope.startswith("session:"):
        raise ServiceError(
            "workspace_missing_directory_restore_model_context",
            "缺失目录恢复模型需要原session",
            403,
        )
    await edits._scope(sql, scope, writing=True, run_id=value.run_id)
    await assert_new_call(sql, value)
    if (
        int(
            await sql.scalar(
                select(func.count()).select_from(WorkspaceMissingDirectoryRestoreModelSourceRow)
            )
            or 0
        )
        >= max_sources
    ):
        raise ServiceError(
            "workspace_missing_directory_restore_capacity", "缺失目录恢复模型来源数量已达上限", 429
        )
    sql.add(
        WorkspaceMissingDirectoryRestoreModelSourceRow(
            run_id=value.run_id,
            call_id=value.call_id,
            plan_id=plan_id,
            phase=phase,
            operation_id=operation_id,
        )
    )
