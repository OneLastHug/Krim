"""恢复阶段来源校验；所有SQL调用由调用方持有原事务与互斥。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource

from .database import (
    NativeCheckpointModelSourceRow,
    WorkspaceDirectoryCaptureModelSourceRow,
    WorkspaceRestoreModelSourceRow,
    WorkspaceRestorePlanRow,
)

if TYPE_CHECKING:
    from .workspace_edits import WorkspaceEditStore


async def restore_sources(
    session: AsyncSession, edits: WorkspaceEditStore, row: WorkspaceRestorePlanRow
) -> dict[str, CheckpointModelSource]:
    saved = list(
        await session.scalars(
            select(WorkspaceRestoreModelSourceRow).where(
                WorkspaceRestoreModelSourceRow.plan_id == row.plan_id
            )
        )
    )
    result = {item.phase: CheckpointModelSource(item.run_id, item.call_id) for item in saved}
    if (
        bool(row.preview_model_bound) != ("preview" in result)
        or bool(row.execute_model_bound) != ("execute" in result)
        or any(phase not in {"preview", "execute"} for phase in result)
        or row.execute_model_bound
        and row.execute_key is None
    ):
        raise ServiceError("workspace_restore_corrupt", "恢复阶段来源标记或材料缺失", 409)
    for source in result.values():
        if (
            not row.scope_id.startswith("session:")
            or source.run_id == "local"
            or not 1 <= len(source.call_id) <= 128
            or any(ord(char) < 32 or ord(char) == 127 for char in source.call_id)
        ):
            raise ServiceError("workspace_restore_corrupt", "恢复阶段原调用来源损坏", 409)
        await edits._scope(session, row.scope_id, writing=False, run_id=source.run_id)
    return result


async def admit_restore_source(
    session: AsyncSession,
    edits: WorkspaceEditStore,
    scope: str,
    source: CheckpointModelSource,
) -> None:
    from .workspace_checkpoints import WorkspaceCheckpointStore

    WorkspaceCheckpointStore._valid_source(source)
    if source.restore_plan_id is not None or not scope.startswith("session:"):
        raise ServiceError("workspace_restore_model_context", "恢复阶段需要原模型父调用", 403)
    await edits._scope(session, scope, writing=True, run_id=source.run_id)
    from .directory_restore_sources import assert_no_directory_restore_call

    await assert_no_directory_restore_call(session, source)
    prior = await session.scalar(
        select(WorkspaceRestoreModelSourceRow).where(
            WorkspaceRestoreModelSourceRow.run_id == source.run_id,
            WorkspaceRestoreModelSourceRow.call_id == source.call_id,
        )
    )
    capture = await session.scalar(
        select(NativeCheckpointModelSourceRow).where(
            NativeCheckpointModelSourceRow.run_id == source.run_id,
            NativeCheckpointModelSourceRow.call_id == source.call_id,
        )
    )
    directory = await session.scalar(
        select(WorkspaceDirectoryCaptureModelSourceRow).where(
            WorkspaceDirectoryCaptureModelSourceRow.run_id == source.run_id,
            WorkspaceDirectoryCaptureModelSourceRow.call_id == source.call_id,
        )
    )
    if prior is not None or capture is not None or directory is not None:
        raise ServiceError("workspace_restore_model_call_conflict", "原调用已绑定其他操作", 409)
