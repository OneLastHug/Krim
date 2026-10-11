"""混合批次内部原事务准入；公开参数不能提供fence或内部动作键。"""

from __future__ import annotations

import secrets
import time
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_change import WorkspaceChangeAction
from krim_agent.workspace_directory import DirectoryChangeAction
from krim_agent.workspace_mixed_batch import MixedBatchFence

from .database import (
    MixedWorkspaceBatchChildRow,
    MixedWorkspaceBatchRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceFileEffectRow,
)

MIXED_REQUEST_PREFIX = "krim-mixed-batch:"
MixedSource = Literal["file", "directory"]


def assert_live(row: MixedWorkspaceBatchRow, owner_id: str) -> None:
    if (
        row.producer_status != "running"
        or row.owner_id != owner_id
        or (row.lease_expires_at or 0) <= time.time()
    ):
        raise ServiceError("workspace_mixed_batch_fenced", "混合批次执行资格已失效", 409)


async def assert_mixed_request(
    sql: AsyncSession,
    owner_id: str,
    scope: str,
    resource: str,
    source: MixedSource,
    request: WorkspaceChangeAction | DirectoryChangeAction,
    fence: MixedBatchFence | None,
    existing_operation_id: str | None = None,
) -> MixedWorkspaceBatchChildRow | None:
    if fence is None:
        if existing_operation_id is None and request.request_key.startswith(MIXED_REQUEST_PREFIX):
            raise ServiceError(
                "workspace_mixed_batch_request_reserved", "内部混合批次动作键禁止公开准入", 403
            )
        return None
    parent = await sql.get(MixedWorkspaceBatchRow, fence.batch_id)
    child = await sql.get(MixedWorkspaceBatchChildRow, (fence.batch_id, fence.position))
    if (
        parent is None
        or child is None
        or fence.source != source
        or child.source != source
        or parent.scope_id != scope
        or parent.resource_fingerprint != resource
        or not secrets.compare_digest(parent.token, fence.token)
        or parent.action != request.action
        or child.proposal_id != request.proposal_id
        or child.expected_revision != request.expected_revision
        or child.request_key != request.request_key
    ):
        raise ServiceError("workspace_mixed_batch_fence_mismatch", "混合批次原冻结凭据不匹配", 403)
    associated = child.file_operation_id if source == "file" else child.directory_operation_id
    if existing_operation_id is not None:
        if associated != existing_operation_id:
            raise ServiceError(
                "workspace_mixed_batch_operation_conflict", "子项不能认领其他原操作", 409
            )
        return child
    assert_live(parent, owner_id)
    if (
        not child.admission_open
        or associated is not None
        or child.status != "pending"
        or child.error_code is not None
    ):
        raise ServiceError("workspace_mixed_batch_child_closed", "混合子项准入已关闭", 409)
    if source == "file" and request.action == "undo":
        from .workspace_batches import assert_file_undo_tip
        from .workspace_edits import digest

        proposal = await sql.get(WorkspaceEditProposalRow, child.proposal_id)
        if proposal is None:
            raise ServiceError("workspace_mixed_batch_corrupt", "混合文件原提案缺失", 409)
        physical_key = digest(
            {
                "parent": [proposal.prepared["parent_device"], proposal.prepared["parent_inode"]],
                "name": proposal.path.split("/")[-1],
            }
        )
        sequence = await sql.scalar(
            select(WorkspaceFileEffectRow.effect_sequence).where(
                WorkspaceFileEffectRow.proposal_id == child.proposal_id,
                WorkspaceFileEffectRow.action == "apply",
            )
        )
        await assert_file_undo_tip(sql, child.proposal_id, physical_key, sequence)
    return child


async def assert_mixed_execution(
    sql: AsyncSession,
    owner_id: str,
    source: MixedSource,
    operation_id: str,
) -> None:
    column = (
        MixedWorkspaceBatchChildRow.file_operation_id
        if source == "file"
        else MixedWorkspaceBatchChildRow.directory_operation_id
    )
    child = await sql.scalar(select(MixedWorkspaceBatchChildRow).where(column == operation_id))
    if child is None:
        return
    parent = await sql.get(MixedWorkspaceBatchRow, child.batch_id)
    if parent is None or child.source != source:
        raise ServiceError("workspace_mixed_batch_corrupt", "混合操作关联来源不完整", 409)
    attempt = (
        await sql.get(WorkspaceEditAttemptRow, operation_id)
        if source == "file"
        else await sql.get(WorkspaceDirectoryAttemptRow, operation_id)
    )
    if (
        attempt is None
        or attempt.scope_id != parent.scope_id
        or attempt.proposal_id != child.proposal_id
        or attempt.action != parent.action
        or attempt.expected_revision != child.expected_revision
        or attempt.request_key != child.request_key
    ):
        raise ServiceError("workspace_mixed_batch_corrupt", "混合执行关联与原操作不符", 409)
    assert_live(parent, owner_id)
