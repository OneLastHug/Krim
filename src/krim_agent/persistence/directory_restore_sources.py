"""目录恢复各阶段原模型来源；同call不能借Host或其他Checkpoint生产者降级。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_restore import directory_restore_model_source

from .database import (
    NativeCheckpointModelSourceRow,
    WorkspaceDirectoryCaptureModelSourceRow,
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestoreModelSourceRow,
    WorkspaceDirectoryRestorePlanRow,
    WorkspaceMissingDirectoryRestoreModelSourceRow,
    WorkspaceRestoreModelSourceRow,
)
from .workspace_edits import WorkspaceEditStore


def valid_source(source: CheckpointModelSource | None) -> CheckpointModelSource | None:
    return directory_restore_model_source(source)


def source_value(source: CheckpointModelSource | None) -> dict[str, str] | None:
    value = valid_source(source)
    return {"run_id": value.run_id, "call_id": value.call_id} if value is not None else None


async def assert_new_call(sql: AsyncSession, source: CheckpointModelSource) -> None:
    """调用方先持有原SQL writer并核实活跃Scope；这里只做全类型同call排他。"""
    if valid_source(source) is None:
        raise ServiceError("workspace_directory_restore_model_context", "模型调用来源不能为空", 403)
    models: tuple[Any, ...] = (
        NativeCheckpointModelSourceRow,
        WorkspaceDirectoryCaptureModelSourceRow,
        WorkspaceRestoreModelSourceRow,
        WorkspaceDirectoryRestoreModelSourceRow,
        WorkspaceMissingDirectoryRestoreModelSourceRow,
    )
    for model in models:
        if (
            await sql.scalar(
                select(model.run_id).where(
                    model.run_id == source.run_id, model.call_id == source.call_id
                )
            )
            is not None
        ):
            raise ServiceError(
                "workspace_directory_restore_model_call_conflict",
                "原调用已绑定其他Checkpoint操作",
                409,
            )


async def assert_no_directory_restore_call(
    sql: AsyncSession, source: CheckpointModelSource
) -> None:
    """旧生产者检查两类目录恢复来源，保留其原基线/同键幂等例外。"""
    if (
        await sql.scalar(
            select(WorkspaceDirectoryRestoreModelSourceRow.run_id).where(
                WorkspaceDirectoryRestoreModelSourceRow.run_id == source.run_id,
                WorkspaceDirectoryRestoreModelSourceRow.call_id == source.call_id,
            )
        )
        is not None
    ):
        raise ServiceError(
            "workspace_directory_restore_model_call_conflict", "原调用已绑定目录恢复阶段", 409
        )
    if (
        await sql.scalar(
            select(WorkspaceMissingDirectoryRestoreModelSourceRow.run_id).where(
                WorkspaceMissingDirectoryRestoreModelSourceRow.run_id == source.run_id,
                WorkspaceMissingDirectoryRestoreModelSourceRow.call_id == source.call_id,
            )
        )
        is not None
    ):
        raise ServiceError(
            "workspace_missing_directory_restore_model_call_conflict",
            "原调用已绑定缺失目录恢复阶段",
            409,
        )


@dataclass(frozen=True)
class DirectoryRestoreSources:
    preview: CheckpointModelSource | None
    execute: CheckpointModelSource | None
    verify: CheckpointModelSource | None


async def sources(
    sql: AsyncSession,
    edits: WorkspaceEditStore,
    plan: WorkspaceDirectoryRestorePlanRow,
    attempt: WorkspaceDirectoryRestoreAttemptRow | None,
) -> DirectoryRestoreSources:
    rows = list(
        await sql.scalars(
            select(WorkspaceDirectoryRestoreModelSourceRow).where(
                WorkspaceDirectoryRestoreModelSourceRow.plan_id == plan.plan_id
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
                raise ValueError("目录恢复模型缺少原session")
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
            "workspace_directory_restore_corrupt", "目录恢复原阶段来源损坏或缺失", 409
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
            "workspace_directory_restore_model_context", "目录恢复模型需要原session", 403
        )
    await edits._scope(sql, scope, writing=True, run_id=value.run_id)
    await assert_new_call(sql, value)
    if (
        int(
            await sql.scalar(
                select(func.count()).select_from(WorkspaceDirectoryRestoreModelSourceRow)
            )
            or 0
        )
        >= max_sources
    ):
        raise ServiceError(
            "workspace_directory_restore_capacity", "目录恢复模型来源数量已达上限", 429
        )
    sql.add(
        WorkspaceDirectoryRestoreModelSourceRow(
            run_id=value.run_id,
            call_id=value.call_id,
            plan_id=plan_id,
            phase=phase,
            operation_id=operation_id,
        )
    )
