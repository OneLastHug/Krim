"""检查点恢复编排账本；真实文件效果仍由原提案与操作账本保存。"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC
from typing import TYPE_CHECKING, Literal, cast
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace_change import RestoreFileSpec, WorkspaceChangeAction
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_restore import (
    CheckpointRestoreDispatch,
    CheckpointRestoreExecute,
    CheckpointRestoreFence,
    CheckpointRestoreItem,
    CheckpointRestorePreparation,
    CheckpointRestorePreview,
    CheckpointRestoreView,
)
from krim_agent.workspace_writer import PreparedFileChange

from .database import (
    NativeWorkspaceCheckpointFileRow,
    NativeWorkspaceCheckpointRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceFileEffectRow,
    WorkspaceFileLockRow,
    WorkspaceRestoreItemRow,
    WorkspaceRestoreModelSourceRow,
    WorkspaceRestorePlanRow,
)
from .workspace_restore_sources import admit_restore_source, restore_sources

if TYPE_CHECKING:
    from .workspace_edits import WorkspaceEditStore

INTERNAL_REQUEST_PREFIX = "krim-restore:"
RestorePhase = Literal[
    "preparing", "ready", "executing", "finished", "failed", "cancelled", "interrupted", "expired"
]


def _assert_live(row: WorkspaceRestorePlanRow, owner: str, phase: str) -> None:
    if (
        row.phase != phase
        or row.owner_id != owner
        or (row.lease_expires_at or 0) <= time.time()
        or row.expires_at <= time.time()
    ):
        raise ServiceError("workspace_restore_fenced", "恢复计划执行资格已失效", 409)


async def _effect_tip(session: AsyncSession, key: str) -> int | None:
    value = await session.scalar(
        select(func.max(WorkspaceFileEffectRow.effect_sequence)).where(
            WorkspaceFileEffectRow.file_key == key
        )
    )
    return int(value) if value is not None else None


async def _restore_formats(
    session: AsyncSession, row: WorkspaceRestorePlanRow, edits: WorkspaceEditStore
) -> None:
    from .workspace_checkpoints import WorkspaceCheckpointStore

    await restore_sources(session, edits, row)
    target = await session.get(NativeWorkspaceCheckpointRow, row.checkpoint_id)
    baseline = (
        await session.get(NativeWorkspaceCheckpointRow, row.baseline_checkpoint_id)
        if row.baseline_checkpoint_id is not None
        else None
    )
    if (
        row.baseline_capture_format not in {"text", "bytes"}
        or target is None
        or target.capture_format not in {"text", "bytes"}
        or target.status != "available"
        or target.scope_id != row.scope_id
        or (target.workspace_key, target.resource_fingerprint, target.workspace_fingerprint)
        != (row.workspace_key, row.resource_fingerprint, row.workspace_fingerprint)
        or target.capture_format == "bytes"
        and row.baseline_capture_format != "bytes"
        or row.baseline_checkpoint_id is not None
        and (
            baseline is None
            or baseline.scope_id != row.scope_id
            or baseline.capture_format != row.baseline_capture_format
            or baseline.request_key != row.baseline_key
            or (
                baseline.workspace_key,
                baseline.resource_fingerprint,
                baseline.workspace_fingerprint,
            )
            != (row.workspace_key, row.resource_fingerprint, row.workspace_fingerprint)
        )
    ):
        raise ServiceError("workspace_restore_baseline", "恢复基线未保留原冻结格式与来源", 409)
    checkpoints = WorkspaceCheckpointStore(edits)
    await checkpoints._source(session, target)
    if baseline is not None:
        source = await checkpoints._source(session, baseline)
        preview = (await restore_sources(session, edits, row)).get("preview")
        expected = replace(preview, restore_plan_id=row.plan_id) if preview is not None else None
        if source != expected:
            raise ServiceError("workspace_restore_baseline", "恢复基线原调用来源不匹配", 403)


async def assert_restore_request(
    session: AsyncSession,
    owner_id: str,
    scope: str,
    request: WorkspaceChangeAction,
    resource_fingerprint: str,
    fence: CheckpointRestoreFence | None,
    existing_operation_id: str | None = None,
    *,
    edits: WorkspaceEditStore,
    run_id: str | None = None,
    call_id: str | None = None,
) -> WorkspaceRestoreItemRow | None:
    """与原reserve同事务关联；公开参数不能认领或绕过恢复计划。"""
    child = await session.scalar(
        select(WorkspaceRestoreItemRow).where(
            WorkspaceRestoreItemRow.proposal_id == request.proposal_id
        )
    )
    if fence is None:
        if request.action == "apply" and child is not None:
            if existing_operation_id is None or child.operation_id != existing_operation_id:
                raise ServiceError("workspace_restore_required", "恢复提案须通过原计划执行", 403)
            return child
        if existing_operation_id is None and request.request_key.startswith(
            INTERNAL_REQUEST_PREFIX
        ):
            raise ServiceError(
                "workspace_restore_request_reserved", "该操作键仅供恢复计划使用", 403
            )
        return None
    row = await session.get(WorkspaceRestorePlanRow, fence.plan_id)
    if (
        row is None
        or child is None
        or row.scope_id != scope
        or child.plan_id != fence.plan_id
        or child.position != fence.position
        or not secrets.compare_digest(row.token, fence.token)
        or row.resource_fingerprint != resource_fingerprint
        or request.action != "apply"
        or child.expected_revision != request.expected_revision
        or child.request_key != request.request_key
    ):
        raise ServiceError("workspace_restore_target", "恢复凭据或冻结目标不匹配", 403)
    source = (await restore_sources(session, edits, row)).get("execute")
    if (
        source != fence.model_source
        or source is not None
        and (source.run_id != run_id or source.call_id != call_id)
    ):
        raise ServiceError("workspace_restore_model_context", "恢复子项未保留原执行调用", 403)
    if existing_operation_id is not None:
        if child.operation_id != existing_operation_id:
            raise ServiceError("workspace_restore_operation_conflict", "不能认领其他文件操作", 409)
        return child
    _assert_live(row, owner_id, "executing")
    await _restore_formats(session, row, edits)
    if not child.admission_open or child.operation_id is not None or child.status != "pending":
        raise ServiceError("workspace_restore_child_closed", "恢复子项准入已关闭", 409)
    assert child.file_key is not None
    if await _effect_tip(session, child.file_key) != child.effect_sequence:
        raise ServiceError("workspace_restore_version_conflict", "预览后文件已有受控修改", 409)
    return child


async def assert_restore_execution(
    session: AsyncSession, owner: str, operation_id: str, edits: WorkspaceEditStore
) -> None:
    child = await session.scalar(
        select(WorkspaceRestoreItemRow).where(WorkspaceRestoreItemRow.operation_id == operation_id)
    )
    if child is not None:
        row = await session.get(WorkspaceRestorePlanRow, child.plan_id)
        assert row is not None
        _assert_live(row, owner, "executing")
        await _restore_formats(session, row, edits)
        source = (await restore_sources(session, edits, row)).get("execute")
        if source is not None:
            attempt = await session.get(WorkspaceEditAttemptRow, operation_id)
            if attempt is None or (attempt.run_id, attempt.call_id) != (
                source.run_id,
                source.call_id,
            ):
                raise ServiceError("workspace_restore_model_context", "原恢复操作来源不匹配", 403)
            await edits._scope(session, row.scope_id, writing=True, run_id=source.run_id)
        assert child.file_key is not None
        if await _effect_tip(session, child.file_key) != child.effect_sequence:
            raise ServiceError("workspace_restore_version_conflict", "恢复文件版本已变化", 409)


class WorkspaceRestoreStore:
    def __init__(
        self, edits: WorkspaceEditStore, *, lease_seconds: float = 60, max_plans: int = 500
    ) -> None:
        if not 0.2 <= lease_seconds <= 600 or not 1 <= max_plans <= 5000:
            raise ValueError("恢复计划租约或容量无效")
        self.edits, self.database, self.owner_id = edits, edits.database, edits.owner_id
        self.lease_seconds, self.max_plans = lease_seconds, max_plans

    @staticmethod
    async def _row(session: AsyncSession, scope: str, identifier: str) -> WorkspaceRestorePlanRow:
        row = await session.get(WorkspaceRestorePlanRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError("workspace_restore_not_found", "恢复计划不在该作用域中", 404)
        return row

    @staticmethod
    async def _children(session: AsyncSession, identifier: str) -> list[WorkspaceRestoreItemRow]:
        return list(
            await session.scalars(
                select(WorkspaceRestoreItemRow)
                .where(WorkspaceRestoreItemRow.plan_id == identifier)
                .order_by(WorkspaceRestoreItemRow.position)
            )
        )

    async def _close(
        self,
        session: AsyncSession,
        row: WorkspaceRestorePlanRow,
        phase: RestorePhase,
        code: str | None,
    ) -> None:
        row.phase, row.error_code = phase, code
        row.owner_id, row.lease_expires_at = None, None
        row.revision += 1
        row.updated_at = utc_now()
        for child in await self._children(session, row.plan_id):
            child.admission_open = False
            if child.operation_id is None and child.status == "pending":
                child.status, child.error_code = "skipped", code or "workspace_restore_not_started"
        await session.flush()

    async def _expire(self, session: AsyncSession, row: WorkspaceRestorePlanRow) -> None:
        if row.phase in {"preparing", "ready", "executing"} and row.expires_at <= time.time():
            await self._close(session, row, "expired", "workspace_restore_expired")
        elif row.phase in {"preparing", "executing"} and (row.lease_expires_at or 0) <= time.time():
            await self._close(session, row, "interrupted", "workspace_restore_interrupted")

    async def _live(
        self, session: AsyncSession, scope: str, identifier: str, token: str, phase: str
    ) -> WorkspaceRestorePlanRow:
        row = await self._row(session, scope, identifier)
        if not secrets.compare_digest(row.token, token):
            raise ServiceError("workspace_restore_fenced", "恢复计划凭据已失效", 403)
        _assert_live(row, self.owner_id, phase)
        await _restore_formats(session, row, self.edits)
        stage = "preview" if phase == "preparing" else "execute"
        source = (await restore_sources(session, self.edits, row)).get(stage)
        if source is not None:
            await self.edits._scope(session, scope, writing=True, run_id=source.run_id)
        await self.edits._scope(session, scope, writing=True)
        await self.edits._resource(session, scope, row.workspace_key, row.resource_fingerprint)
        return row

    async def _item(
        self, session: AsyncSession, child: WorkspaceRestoreItemRow
    ) -> CheckpointRestoreItem:
        proposal = None
        operation = None
        status = child.status
        code = child.error_code
        if child.proposal_id is not None:
            row = await session.get(WorkspaceEditProposalRow, child.proposal_id)
            assert row is not None
            proposal = self.edits._change(row).view
        if child.operation_id is not None:
            attempt = await session.get(WorkspaceEditAttemptRow, child.operation_id)
            assert attempt is not None
            operation = self.edits._attempt(attempt).view
            status = operation.status
            code = operation.receipt.error_code if operation.receipt else None
        return CheckpointRestoreItem(
            position=child.position,
            path=child.path,
            proposal=proposal,
            operation=operation,
            status=cast(
                Literal["pending", "running", "done", "rejected", "uncertain", "skipped"], status
            ),
            current_mode=child.current_mode,
            target_mode=child.target_mode,
            error_code=code,
        )

    async def _view(
        self, session: AsyncSession, row: WorkspaceRestorePlanRow
    ) -> CheckpointRestoreView:
        await restore_sources(session, self.edits, row)
        items = [
            await self._item(session, child) for child in await self._children(session, row.plan_id)
        ]
        status = row.phase
        if row.phase == "executing":
            status = "running"
        elif row.phase not in {"preparing", "ready"}:
            if any(item.status in {"running", "uncertain"} for item in items):
                status = "uncertain"
            elif all(item.status == "done" for item in items):
                status = "done"
            elif row.phase == "finished":
                status = "partial" if any(item.status == "done" for item in items) else "failed"
        return CheckpointRestoreView.model_validate(
            dict(
                plan_id=row.plan_id,
                scope_id=row.scope_id,
                checkpoint_id=row.checkpoint_id,
                baseline_checkpoint_id=row.baseline_checkpoint_id,
                baseline_capture_format=row.baseline_capture_format,
                request_key=row.request_key,
                execute_key=row.execute_key,
                revision=row.revision,
                phase=row.phase,
                status=status,
                items=items,
                expires_at=row.expires_at,
                error_code=row.error_code,
                created_at=row.created_at.replace(tzinfo=UTC),
                updated_at=max(
                    [row.updated_at.replace(tzinfo=UTC)]
                    + [item.operation.updated_at for item in items if item.operation is not None]
                ),
            )
        )

    async def reserve(
        self,
        scope: str,
        request: CheckpointRestorePreview,
        workspace_key: str,
        resource_fingerprint: str,
        workspace_fingerprint: str,
        *,
        model_source: CheckpointModelSource | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> tuple[CheckpointRestoreView, bool]:
        from .workspace_checkpoints import WorkspaceCheckpointStore
        from .workspace_edits import digest

        request = CheckpointRestorePreview.model_validate(request.model_dump())
        self.edits._key(request.request_key)
        fingerprint = digest(
            {
                "request": request.model_dump(mode="json"),
                "key": workspace_key,
                "resource": resource_fingerprint,
                "workspace": workspace_fingerprint,
            }
        )
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            if authorize is not None:
                authorize()
            topic = await self.edits._scope(session, scope, writing=False)
            previous = await session.scalar(
                select(WorkspaceRestorePlanRow).where(
                    WorkspaceRestorePlanRow.scope_id == scope,
                    WorkspaceRestorePlanRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != fingerprint:
                    raise ServiceError(
                        "workspace_restore_key_conflict", "恢复预览键已绑定其他请求", 409
                    )
                if (await restore_sources(session, self.edits, previous)).get(
                    "preview"
                ) != model_source:
                    raise ServiceError(
                        "workspace_restore_model_conflict", "预览绑定其他原调用", 409
                    )
                await self._expire(session, previous)
                return await self._view(session, previous), False
            await self.edits._scope(session, scope, writing=True)
            await self.edits._resource(session, scope, workspace_key, resource_fingerprint)
            if model_source is not None:
                await admit_restore_source(session, self.edits, scope, model_source)
            target = await session.get(NativeWorkspaceCheckpointRow, request.checkpoint_id)
            if target is None or target.scope_id != scope:
                raise ServiceError("workspace_checkpoint_not_found", "检查点不在该作用域中", 404)
            if target.status != "available":
                raise ServiceError("workspace_checkpoint_unavailable", "检查点尚不可用于恢复", 409)
            await WorkspaceCheckpointStore(self.edits)._source(session, target)
            if (
                target.workspace_key,
                target.resource_fingerprint,
                target.workspace_fingerprint,
            ) != (workspace_key, resource_fingerprint, workspace_fingerprint):
                raise ServiceError("workspace_binding_changed", "检查点工作区资源已变化", 403)
            count = await session.scalar(select(func.count()).select_from(WorkspaceRestorePlanRow))
            if (count or 0) >= self.max_plans:
                raise ServiceError("workspace_restore_limit", "恢复计划历史已达上限", 429)
            files = list(
                await session.scalars(
                    select(NativeWorkspaceCheckpointFileRow)
                    .where(NativeWorkspaceCheckpointFileRow.checkpoint_id == request.checkpoint_id)
                    .order_by(NativeWorkspaceCheckpointFileRow.position)
                )
            )
            if not 1 <= len(files) <= 32 or any(not item.captured for item in files):
                raise ServiceError("workspace_checkpoint_corrupt", "检查点文件集不完整", 409)
            identifier = str(uuid4())
            row = WorkspaceRestorePlanRow(
                plan_id=identifier,
                scope_id=scope,
                topic_id=topic,
                workspace_key=workspace_key,
                resource_fingerprint=resource_fingerprint,
                workspace_fingerprint=workspace_fingerprint,
                checkpoint_id=target.checkpoint_id,
                baseline_capture_format="bytes"
                if target.capture_format == "bytes" or request.baseline_capture_format == "bytes"
                else "text",
                baseline_key="krim-backup:" + secrets.token_hex(32),
                request_key=request.request_key,
                request_hash=fingerprint,
                phase="preparing",
                revision=0,
                expires_at=time.time() + request.ttl_seconds,
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                token=secrets.token_hex(32),
                preview_model_bound=model_source is not None,
            )
            session.add(row)
            await session.flush()
            if model_source is not None:
                session.add(
                    WorkspaceRestoreModelSourceRow(
                        plan_id=identifier,
                        phase="preview",
                        run_id=model_source.run_id,
                        call_id=model_source.call_id,
                    )
                )
            for position, entry in enumerate(files):
                metadata = WorkspaceCheckpointStore._metadata(target, entry.capture_metadata)
                session.add(
                    WorkspaceRestoreItemRow(
                        plan_id=identifier,
                        position=position,
                        path=entry.path,
                        request_key=f"{INTERNAL_REQUEST_PREFIX}{identifier}:{position}",
                        target_mode=metadata.snapshot.mode if metadata.snapshot else None,
                    )
                )
            await session.flush()
            if authorize is not None:
                authorize()
            return await self._view(session, row), True

    async def preparation(self, scope: str, identifier: str) -> CheckpointRestorePreparation:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await self._row(session, scope, identifier)
            await self._live(session, scope, identifier, row.token, "preparing")
            return CheckpointRestorePreparation(
                await self._view(session, row), row.token, row.baseline_key
            )

    async def attach_baseline(
        self,
        scope: str,
        identifier: str,
        token: str,
        baseline_id: str,
        *,
        model_source: CheckpointModelSource | None = None,
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._row(session, scope, identifier)
            if (await restore_sources(session, self.edits, row)).get("preview") != model_source:
                raise ServiceError("workspace_restore_model_conflict", "基线原预览来源不匹配", 403)
            baseline = await session.get(NativeWorkspaceCheckpointRow, baseline_id)
            if (
                not secrets.compare_digest(row.token, token)
                or baseline is None
                or baseline.scope_id != scope
                or baseline.request_key != row.baseline_key
                or baseline.workspace_key != row.workspace_key
                or baseline.resource_fingerprint != row.resource_fingerprint
                or baseline.workspace_fingerprint != row.workspace_fingerprint
                or baseline.capture_format != row.baseline_capture_format
                or row.baseline_checkpoint_id not in {None, baseline_id}
            ):
                raise ServiceError("workspace_restore_baseline", "恢复前备份未绑定原计划", 409)
            # 即使准备阶段刚取消，也记录已接受的捕获，供取消/重启查询定位。
            row.baseline_checkpoint_id = baseline_id
            row.updated_at = utc_now()

    async def ready(
        self,
        scope: str,
        identifier: str,
        token: str,
        prepared: tuple[PreparedFileChange, ...],
        *,
        model_source: CheckpointModelSource | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> None:
        from .workspace_checkpoints import WorkspaceCheckpointStore
        from .workspace_edits import digest, encode_prepared, file_key

        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._live(session, scope, identifier, token, "preparing")
            if (await restore_sources(session, self.edits, row)).get("preview") != model_source:
                raise ServiceError("workspace_restore_model_conflict", "准备原预览来源不匹配", 403)
            if authorize is not None:
                authorize()
            baseline = await session.get(NativeWorkspaceCheckpointRow, row.baseline_checkpoint_id)
            target = await session.get(NativeWorkspaceCheckpointRow, row.checkpoint_id)
            if (
                baseline is None
                or target is None
                or baseline.status != "available"
                or target.status != "available"
            ):
                raise ServiceError(
                    "workspace_restore_baseline", "恢复需要完整可用的原目标和当前备份", 409
                )
            children = await self._children(session, identifier)
            if len(children) != len(prepared):
                raise ServiceError("workspace_restore_target", "恢复文件集已变化", 409)
            count = await session.scalar(select(func.count()).select_from(WorkspaceEditProposalRow))
            if (count or 0) + len(children) > self.edits.max_changes:
                raise ServiceError("workspace_proposal_limit", "修改提案历史已达上限", 429)
            await self.edits._bytes_available(
                session,
                sum(
                    len(value.before.data if value.before else b"") + len(value.after or b"")
                    for value in prepared
                ),
            )
            for child, value in zip(children, prepared, strict=True):
                old_row = await session.get(
                    NativeWorkspaceCheckpointFileRow, (baseline.checkpoint_id, child.position)
                )
                new_row = await session.get(
                    NativeWorkspaceCheckpointFileRow, (target.checkpoint_id, child.position)
                )
                if (
                    old_row is None
                    or new_row is None
                    or not old_row.captured
                    or not new_row.captured
                ):
                    raise ServiceError("workspace_checkpoint_corrupt", "恢复快照缺少文件", 409)
                old = WorkspaceCheckpointStore._decode(baseline, old_row)
                new = WorkspaceCheckpointStore._decode(target, new_row)
                spec = value.spec
                self.edits._validate_prepared(value, original_intent=False)
                if (
                    not isinstance(spec, RestoreFileSpec)
                    or old.path != child.path
                    or new.path != child.path
                    or spec.path != child.path
                    or old.workspace_fingerprint != row.workspace_fingerprint
                    or new.workspace_fingerprint != row.workspace_fingerprint
                    or value.workspace_fingerprint != row.workspace_fingerprint
                    or value.parent_identity != old.parent_identity
                    or old.parent_identity != new.parent_identity
                    or value.before != old.snapshot
                    or value.after != (new.snapshot.data if new.snapshot else None)
                    or (spec.mode, spec.uid, spec.gid)
                    != (
                        (new.snapshot.mode, new.snapshot.uid, new.snapshot.gid)
                        if new.snapshot
                        else (None, None, None)
                    )
                    or any(
                        item.snapshot is not None and item.snapshot.mode & ~0o777
                        for item in (old, new)
                    )
                ):
                    raise ServiceError(
                        "workspace_restore_target", "恢复意图未绑定两份可信快照", 409
                    )
                key = file_key(value)
                if await session.get(WorkspaceFileLockRow, key) is not None:
                    raise ServiceError("workspace_file_busy", "恢复文件有未收束的操作", 409)
                proposal = WorkspaceEditProposalRow(
                    proposal_id=str(uuid4()),
                    scope_id=scope,
                    topic_id=row.topic_id,
                    workspace_key=row.workspace_key,
                    resource_fingerprint=row.resource_fingerprint,
                    request_key=f"{INTERNAL_REQUEST_PREFIX}{identifier}:preview:{child.position}",
                    request_hash=digest(
                        {
                            "plan": identifier,
                            "position": child.position,
                            "target": target.checkpoint_id,
                            "baseline": baseline.checkpoint_id,
                        }
                    ),
                    path=child.path,
                    prepared=encode_prepared(value),
                    preview=value.preview().model_dump(mode="json"),
                    before_content=value.before.data if value.before else None,
                    after_content=value.after,
                    status="proposed",
                    revision=0,
                    expires_at=row.expires_at,
                )
                session.add(proposal)
                await session.flush()
                child.proposal_id, child.file_key = proposal.proposal_id, key
                child.effect_sequence = await _effect_tip(session, key)
                child.current_mode = old.snapshot.mode if old.snapshot else None
            if authorize is not None:
                authorize()
            row.phase, row.owner_id, row.lease_expires_at = "ready", None, None
            row.revision += 1
            row.updated_at = utc_now()

    async def start(
        self,
        scope: str,
        request: CheckpointRestoreExecute,
        key: str,
        resource: str,
        workspace: str,
        *,
        model_source: CheckpointModelSource | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> tuple[CheckpointRestoreView, bool]:
        from .workspace_edits import digest

        self.edits._key(request.request_key)
        fingerprint = digest(
            {
                "request": request.model_dump(mode="json"),
                "key": key,
                "resource": resource,
                "workspace": workspace,
            }
        )
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            if authorize is not None:
                authorize()
            await self.edits._scope(session, scope, writing=False)
            row = await self._row(session, scope, request.plan_id)
            if row.execute_key is not None:
                if row.execute_key != request.request_key or row.execute_hash != fingerprint:
                    raise ServiceError(
                        "workspace_restore_execute_conflict", "计划已绑定原执行请求", 409
                    )
                if (await restore_sources(session, self.edits, row)).get("execute") != model_source:
                    raise ServiceError(
                        "workspace_restore_model_conflict", "执行绑定其他原调用", 409
                    )
                await self._expire(session, row)
                return await self._view(session, row), False
            await self.edits._scope(session, scope, writing=True)
            if model_source is not None:
                await admit_restore_source(session, self.edits, scope, model_source)
            await self.edits._resource(session, scope, key, resource)
            if (row.workspace_key, row.resource_fingerprint, row.workspace_fingerprint) != (
                key,
                resource,
                workspace,
            ):
                raise ServiceError("workspace_binding_changed", "恢复计划资源版本已变化", 403)
            existing = await session.scalar(
                select(WorkspaceRestorePlanRow.plan_id).where(
                    WorkspaceRestorePlanRow.scope_id == scope,
                    WorkspaceRestorePlanRow.execute_key == request.request_key,
                )
            )
            if existing is not None:
                raise ServiceError(
                    "workspace_restore_execute_conflict", "执行键已绑定其他计划", 409
                )
            await self._expire(session, row)
            if row.phase != "ready":
                raise ServiceError("workspace_restore_state", "恢复计划当前不可执行", 409)
            if row.revision != request.expected_revision:
                raise ServiceError("workspace_restore_revision", "恢复计划版本已变化", 409)
            await _restore_formats(session, row, self.edits)
            row.execute_key, row.execute_hash = request.request_key, fingerprint
            row.execute_model_bound = model_source is not None
            if model_source is not None:
                session.add(
                    WorkspaceRestoreModelSourceRow(
                        plan_id=row.plan_id,
                        phase="execute",
                        run_id=model_source.run_id,
                        call_id=model_source.call_id,
                    )
                )
            row.phase, row.owner_id = "executing", self.owner_id
            row.lease_expires_at, row.token = (
                time.time() + self.lease_seconds,
                secrets.token_hex(32),
            )
            row.revision += 1
            row.updated_at = utc_now()
            for child in await self._children(session, row.plan_id):
                child.admission_open = True
            await session.flush()
            if authorize is not None:
                authorize()
            return await self._view(session, row), True

    async def execution(self, scope: str, identifier: str) -> tuple[CheckpointRestoreDispatch, ...]:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await self._row(session, scope, identifier)
            await self._live(session, scope, identifier, row.token, "executing")
            children = await self._children(session, identifier)
            source = (await restore_sources(session, self.edits, row)).get("execute")
            result = []
            for child in children:
                assert child.proposal_id is not None
                result.append(
                    CheckpointRestoreDispatch(
                        CheckpointRestoreFence(identifier, child.position, row.token, source),
                        WorkspaceChangeAction(
                            proposal_id=child.proposal_id,
                            action="apply",
                            expected_revision=child.expected_revision,
                            request_key=child.request_key,
                        ),
                    )
                )
            return tuple(result)

    async def renew(self, scope: str, identifier: str, token: str, phase: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._live(session, scope, identifier, token, phase)
            row.lease_expires_at = time.time() + self.lease_seconds

    async def finish_child(
        self, scope: str, fence: CheckpointRestoreFence, code: str | None
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._row(session, scope, fence.plan_id)
            child = await session.get(WorkspaceRestoreItemRow, (fence.plan_id, fence.position))
            if child is None or not secrets.compare_digest(row.token, fence.token):
                raise ServiceError("workspace_restore_fenced", "恢复子项凭据不匹配", 403)
            await self._expire(session, row)
            child.admission_open = False
            if child.operation_id is None and child.status == "pending":
                child.status, child.error_code = (
                    "rejected",
                    code or "workspace_restore_not_executed",
                )
            row.updated_at = utc_now()

    async def finish(
        self, scope: str, identifier: str, token: str, code: str | None = None
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._row(session, scope, identifier)
            if not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_restore_fenced", "恢复计划凭据不匹配", 403)
            await self._expire(session, row)
            if row.phase in {"preparing", "executing"}:
                await self._close(
                    session, row, "failed" if code or row.phase == "preparing" else "finished", code
                )

    async def get(self, scope: str, identifier: str) -> CheckpointRestoreView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            await self._expire(session, row)
            return await self._view(session, row)

    async def source(self, scope: str, identifier: str, phase: str) -> CheckpointModelSource | None:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            return (await restore_sources(session, self.edits, row)).get(phase)

    async def by_key(self, scope: str, key: str) -> CheckpointRestoreView | None:
        self.edits._key(key)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope, writing=False)
            row = await session.scalar(
                select(WorkspaceRestorePlanRow).where(
                    WorkspaceRestorePlanRow.scope_id == scope,
                    WorkspaceRestorePlanRow.request_key == key,
                )
            )
            if row is None:
                return None
            await self._expire(session, row)
            return await self._view(session, row)

    async def history(
        self, scope: str, *, offset: int = 0, limit: int = 32
    ) -> list[CheckpointRestoreView]:
        if offset < 0 or not 1 <= limit <= 32:
            raise ServiceError("workspace_restore_pagination", "恢复计划分页无效", 422)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope, writing=False)
            rows = await session.scalars(
                select(WorkspaceRestorePlanRow)
                .where(WorkspaceRestorePlanRow.scope_id == scope)
                .order_by(
                    WorkspaceRestorePlanRow.created_at.desc(),
                    WorkspaceRestorePlanRow.plan_id.desc(),
                )
                .offset(offset)
                .limit(limit)
            )
            result = []
            for row in rows:
                await self._expire(session, row)
                result.append(await self._view(session, row))
            return result

    async def cancel(
        self, scope: str, identifier: str, *, include_ready: bool = True
    ) -> CheckpointRestoreView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            await self._expire(session, row)
            if row.phase in (
                {"preparing", "ready", "executing"} if include_ready else {"preparing", "executing"}
            ):
                await self._close(session, row, "cancelled", "workspace_restore_cancelled")
            return await self._view(session, row)

    async def recover_interrupted(self) -> int:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            rows = list(
                await session.scalars(
                    select(WorkspaceRestorePlanRow).where(
                        WorkspaceRestorePlanRow.phase.in_({"preparing", "executing"})
                    )
                )
            )
            count = 0
            for row in rows:
                await self._expire(session, row)
                count += row.phase not in {"preparing", "executing"}
            return count
