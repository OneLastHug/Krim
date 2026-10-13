"""非空树原账本：原scope/快照/CAS/来源/lease与双向共享namespace锁。"""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter
from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.generated_tree_undo import (
    GeneratedTreeRootState,
    GeneratedTreeUndoPage,
    GeneratedTreeUndoPreviewView,
    GeneratedTreeUndoSource,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_tree_move import (
    TreeMoveAction,
    TreeMoveActionKind,
    TreeMoveAttemptPage,
    TreeMoveAttemptView,
    TreeMoveModelKind,
    TreeMoveModelSource,
    TreeMovePage,
    TreeMovePreviewRequest,
    TreeMovePreviewView,
    TreeMoveReceiptView,
    TreeMoveStatus,
)
from krim_agent.workspace_tree_writer import (
    PreparedTreeMove,
    TreeMoveEffect,
    TreeMoveReceipt,
    WorkspaceTreeWriter,
)

from .completed_tree_seals import CompletedTreeSealStore, SealedTreeOrigin
from .database import (
    WorkspaceGeneratedTreeUndoRow,
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveLockRow,
    WorkspaceTreeMoveModelSourceRow,
    WorkspaceTreeMoveProposalRow,
    WorkspaceTreePlanRow,
)
from .tree_model_sources import run_scope
from .tree_move_codec import decode_move, encode_move
from .tree_move_fences import assert_namespace_available, move_keys
from .workspace_directories import WorkspaceDirectoryStore, decode
from .workspace_edits import WorkspaceEditStore, digest
from .workspace_tree_plans import WorkspaceTreePlanStore


def _corrupt() -> ServiceError:
    return ServiceError("workspace_tree_move_corrupt", "原树移动账本或关联损坏", 409)


def _date(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@dataclass(frozen=True)
class StoredTreeMove:
    view: TreeMovePreviewView
    prepared: PreparedTreeMove


@dataclass(frozen=True)
class StoredTreeMoveAttempt:
    view: TreeMoveAttemptView
    prepared: PreparedTreeMove
    effect: TreeMoveEffect | None
    intent_recorded: bool
    model_source: TreeMoveModelSource | None


class WorkspaceTreeMoveStore:
    def __init__(
        self,
        files: WorkspaceEditStore,
        *,
        max_proposals: int = 500,
        max_snapshot_bytes: int = 536_870_912,
        lease_seconds: float = 60,
    ) -> None:
        if type(max_proposals) is not int or not 1 <= max_proposals <= 5000:
            raise ValueError("树移动历史容量无效")
        if type(max_snapshot_bytes) is not int or not 1024 <= max_snapshot_bytes <= 2_147_483_648:
            raise ValueError("树移动私有材料总容量无效")
        if (
            type(lease_seconds) not in {int, float}
            or not math.isfinite(lease_seconds)
            or not 0.2 <= lease_seconds <= 600
        ):
            raise ValueError("树移动租约无效")
        self.files, self.database, self.owner_id = files, files.database, files.owner_id
        self.seals = CompletedTreeSealStore(
            WorkspaceTreePlanStore(
                files, WorkspaceDirectoryStore(files.database, owner_id=files.owner_id)
            )
        )
        self.max_proposals, self.max_snapshot_bytes, self.lease_seconds = (
            max_proposals,
            max_snapshot_bytes,
            lease_seconds,
        )

    @staticmethod
    def _wanted(request: TreeMovePreviewRequest | TreeMoveAction, fingerprint: str) -> str:
        return digest({"request": request.model_dump(mode="json"), "resource": fingerprint})

    @staticmethod
    def _source_valid(source: TreeMoveModelSource) -> None:
        if (
            not isinstance(source, TreeMoveModelSource)
            or not isinstance(source.run_id, str)
            or not 1 <= len(source.run_id) <= 36
            or source.run_id == "local"
            or not isinstance(source.call_id, str)
            or not 1 <= len(source.call_id) <= 128
            or any(ord(char) < 32 or ord(char) == 127 for char in source.call_id)
        ):
            raise ServiceError(
                "workspace_tree_move_model_context", "树移动须绑定原模型Run/call", 403
            )

    async def _new_source(
        self, sql: AsyncSession, scope: str, source: TreeMoveModelSource | None
    ) -> None:
        if source is None:
            return
        self._source_valid(source)
        await run_scope(sql, scope, source.run_id, live=True)
        if (
            await sql.get(WorkspaceTreeMoveModelSourceRow, (source.run_id, source.call_id))
            is not None
        ):
            raise ServiceError(
                "workspace_tree_move_call_conflict", "原模型调用已绑定其他树移动准入", 409
            )

    async def _bind_source(
        self,
        sql: AsyncSession,
        proposal: WorkspaceTreeMoveProposalRow,
        kind: TreeMoveModelKind,
        source: TreeMoveModelSource | None,
        operation_id: str | None = None,
    ) -> None:
        if source is not None:
            sql.add(
                WorkspaceTreeMoveModelSourceRow(
                    run_id=source.run_id,
                    call_id=source.call_id,
                    proposal_id=proposal.proposal_id,
                    kind=kind,
                    operation_id=operation_id,
                )
            )
            await sql.flush()

    async def _stored_source(
        self,
        sql: AsyncSession,
        proposal: WorkspaceTreeMoveProposalRow,
        operation: WorkspaceTreeMoveAttemptRow | None = None,
        *,
        live: bool = False,
    ) -> TreeMoveModelSource | None:
        query = select(WorkspaceTreeMoveModelSourceRow).where(
            WorkspaceTreeMoveModelSourceRow.proposal_id == proposal.proposal_id,
            WorkspaceTreeMoveModelSourceRow.kind == (operation.action if operation else "preview"),
        )
        if operation is not None:
            query = query.where(
                WorkspaceTreeMoveModelSourceRow.operation_id == operation.operation_id
            )
        rows = list(await sql.scalars(query))
        if len(rows) > 1:
            raise _corrupt()
        source = TreeMoveModelSource(rows[0].run_id, rows[0].call_id) if rows else None
        if source is not None:
            self._source_valid(source)
            await run_scope(sql, proposal.scope_id, source.run_id, live=live)
        if operation is not None and (
            (operation.run_id, operation.call_id)
            != ((source.run_id, source.call_id) if source else (None, None))
            or source is not None
            and rows[0].operation_id != operation.operation_id
        ):
            raise _corrupt()
        return source

    async def _retry_source(
        self,
        sql: AsyncSession,
        proposal: WorkspaceTreeMoveProposalRow,
        source: TreeMoveModelSource | None,
        operation: WorkspaceTreeMoveAttemptRow | None = None,
    ) -> None:
        if source is not None:
            self._source_valid(source)
            if await self._stored_source(sql, proposal, operation) != source:
                raise ServiceError(
                    "workspace_tree_move_source_conflict", "原树移动已绑定其他Run/call来源", 409
                )

    @staticmethod
    def _generated_source_valid(source: GeneratedTreeUndoSource) -> None:
        if not isinstance(source, GeneratedTreeUndoSource) or any(
            not isinstance(value, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,35}", value) is None
            for value in (
                source.plan_id,
                *(
                    [source.previous_restore_operation_id]
                    if source.previous_restore_operation_id is not None
                    else []
                ),
            )
        ):
            raise ServiceError("workspace_generated_undo_source", "原生成根撤销来源无效", 403)

    async def _generated_seal(
        self, sql: AsyncSession, scope: str, key: str, fingerprint: str, plan_id: str
    ) -> SealedTreeOrigin:
        sealed = await self.seals.get_material_in_transaction(
            sql, scope, plan_id, workspace_key=key, resource_fingerprint=fingerprint
        )
        if sealed is None:
            raise ServiceError(
                "workspace_generated_undo_seal_required", "生成根撤销须原完成树首次封存", 409
            )
        plan = await sql.get(WorkspaceTreePlanRow, plan_id)
        try:
            if plan is None:
                raise _corrupt()
            material = plan.frozen["root_prepared"]
            if not isinstance(material, dict):
                raise _corrupt()
            root = decode(material)
            spec = json.loads(root.spec_json)
            if (
                root.before is not None
                or spec["operation"] != "create"
                or spec["path"] != sealed.origin.snapshot.root_path
            ):
                raise _corrupt()
        except (ValueError, TypeError, KeyError):
            raise _corrupt() from None
        return sealed

    async def _restore_origin(
        self,
        sql: AsyncSession,
        scope: str,
        key: str,
        fingerprint: str,
        plan_id: str,
        identifier: str,
    ) -> tuple[WorkspaceTreeMoveAttemptRow, WorkspaceTreeMoveProposalRow, TreeMoveEffect]:
        attempt = await sql.get(WorkspaceTreeMoveAttemptRow, identifier)
        if (
            attempt is None
            or attempt.scope_id != scope
            or attempt.action != "undo"
            or attempt.status != "done"
        ):
            raise ServiceError(
                "workspace_generated_undo_restore_conflict",
                "前驱必须是同作用域实际完成的根还原",
                409,
            )
        proposal = await sql.get(WorkspaceTreeMoveProposalRow, attempt.proposal_id)
        if proposal is None or (
            proposal.scope_id != scope
            or proposal.generated_plan_id != plan_id
            or proposal.workspace_key != key
            or proposal.resource_fingerprint != fingerprint
            or proposal.active_operation_id is not None
            or proposal.status != "undone"
        ):
            raise ServiceError(
                "workspace_generated_undo_restore_conflict", "前驱还原不属于原生成根来源", 409
            )
        stored = await self._attempt_value(sql, attempt, proposal)
        if (
            stored.effect is None
            or stored.effect.observed_after is None
            or not stored.intent_recorded
        ):
            raise _corrupt()
        return attempt, proposal, stored.effect

    async def _generated_candidate(
        self,
        sql: AsyncSession,
        scope: str,
        key: str,
        fingerprint: str,
        source: GeneratedTreeUndoSource,
        prepared: PreparedTreeMove,
    ) -> SealedTreeOrigin:
        self._generated_source_valid(source)
        sealed = await self._generated_seal(sql, scope, key, fingerprint, source.plan_id)
        approved = sealed.origin.snapshot
        if source.previous_restore_operation_id is not None:
            _restore, prior, effect = await self._restore_origin(
                sql, scope, key, fingerprint, source.plan_id, source.previous_restore_operation_id
            )
            await self._generated_origin(sql, prior)
            assert effect.observed_after is not None
            approved = effect.observed_after
        if (
            prepared.before != approved
            or prepared.before.root_path != sealed.origin.snapshot.root_path
            or prepared.before.parent_identity != sealed.origin.snapshot.parent_identity
            or prepared.before.workspace_fingerprint != sealed.origin.snapshot.workspace_fingerprint
        ):
            raise ServiceError(
                "workspace_generated_undo_version_conflict",
                "根撤销须原seal或前驱实际还原的完整版本",
                409,
            )
        return sealed

    async def _generated_origin(
        self, sql: AsyncSession, proposal: WorkspaceTreeMoveProposalRow
    ) -> None:
        link = await sql.get(WorkspaceGeneratedTreeUndoRow, proposal.proposal_id)
        if proposal.generated_plan_id is None:
            if link is not None:
                raise _corrupt()
            return
        sealed = await self._generated_seal(
            sql,
            proposal.scope_id,
            proposal.workspace_key,
            proposal.resource_fingerprint,
            proposal.generated_plan_id,
        )
        seen: set[str] = set()
        current = proposal
        # 显式向前遍历，避免不可信来源链递归或循环导致资格绕过。
        while True:
            if current.proposal_id in seen or len(seen) >= 5000:
                raise _corrupt()
            seen.add(current.proposal_id)
            link = await sql.get(WorkspaceGeneratedTreeUndoRow, current.proposal_id)
            prepared = self._proposal_value(current).prepared
            if link is None or (
                current.generated_plan_id != sealed.origin.view.plan_id
                or (
                    current.scope_id,
                    current.workspace_key,
                    current.resource_fingerprint,
                    current.topic_id,
                )
                != (
                    proposal.scope_id,
                    proposal.workspace_key,
                    proposal.resource_fingerprint,
                    proposal.topic_id,
                )
                or link.source_plan_id != current.generated_plan_id
                or (link.scope_id, link.workspace_key, link.resource_fingerprint)
                != (current.scope_id, current.workspace_key, current.resource_fingerprint)
                or link.tree_fingerprint != sealed.origin.snapshot.fingerprint
                or link.proof_hash != sealed.origin.view.proof_hash
                or link.approved_fingerprint != prepared.before.fingerprint
                or prepared.before.root_path != sealed.origin.snapshot.root_path
                or prepared.before.parent_identity != sealed.origin.snapshot.parent_identity
                or prepared.before.workspace_fingerprint
                != sealed.origin.snapshot.workspace_fingerprint
                or _date(link.created_at) != _date(current.created_at)
            ):
                raise _corrupt()
            await self._stored_source(sql, current)
            if link.previous_restore_operation_id is None:
                if prepared.before != sealed.origin.snapshot:
                    raise _corrupt()
                return
            restore, prior, effect = await self._restore_origin(
                sql,
                current.scope_id,
                current.workspace_key,
                current.resource_fingerprint,
                link.source_plan_id,
                link.previous_restore_operation_id,
            )
            if (
                effect.observed_after != prepared.before
                or _date(prior.created_at) >= _date(current.created_at)
                or _date(restore.updated_at) > _date(current.created_at)
            ):
                raise _corrupt()
            current = prior

    async def _bytes(self, sql: AsyncSession, added: int) -> None:
        before = await sql.scalar(
            select(
                func.coalesce(
                    func.sum(
                        func.length(WorkspaceTreeMoveProposalRow.payload)
                        + func.length(WorkspaceTreeMoveProposalRow.prepared)
                        + func.length(WorkspaceTreeMoveProposalRow.request)
                    ),
                    0,
                )
            )
        )
        operations = await sql.scalar(
            select(
                func.coalesce(
                    func.sum(
                        func.length(WorkspaceTreeMoveAttemptRow.payload)
                        + func.length(WorkspaceTreeMoveAttemptRow.prepared)
                        + func.coalesce(func.length(WorkspaceTreeMoveAttemptRow.after_payload), 0)
                        + func.coalesce(func.length(WorkspaceTreeMoveAttemptRow.after_prepared), 0)
                        + func.length(WorkspaceTreeMoveAttemptRow.receipt_history)
                    ),
                    0,
                )
            )
        )
        origins = await sql.scalar(
            select(
                func.coalesce(
                    func.sum(
                        func.length(WorkspaceGeneratedTreeUndoRow.proposal_id)
                        + func.length(WorkspaceGeneratedTreeUndoRow.source_plan_id)
                        + func.length(WorkspaceGeneratedTreeUndoRow.scope_id)
                        + func.length(WorkspaceGeneratedTreeUndoRow.workspace_key)
                        + func.length(WorkspaceGeneratedTreeUndoRow.resource_fingerprint)
                        + func.length(WorkspaceGeneratedTreeUndoRow.tree_fingerprint)
                        + func.length(WorkspaceGeneratedTreeUndoRow.approved_fingerprint)
                        + func.length(WorkspaceGeneratedTreeUndoRow.proof_hash)
                        + func.coalesce(
                            func.length(
                                WorkspaceGeneratedTreeUndoRow.previous_restore_operation_id
                            ),
                            0,
                        )
                    ),
                    0,
                )
            )
        )
        if (
            int(before or 0) + int(operations or 0) + int(origins or 0) + added
            > self.max_snapshot_bytes
        ):
            raise ServiceError(
                "workspace_tree_move_snapshot_limit", "原树移动私有材料总容量已达上限", 413
            )

    @staticmethod
    def _proposal_value(row: WorkspaceTreeMoveProposalRow) -> StoredTreeMove:
        try:
            prepared = decode_move(row.prepared, row.payload)
            request = TreeMovePreviewRequest.model_validate_json(json.dumps(row.request))
            if (
                row.source_path != prepared.before.root_path
                or row.target_path != prepared.target_path
                or request.source_path != row.source_path
                or request.target_path != row.target_path
                or request.expected_fingerprint != prepared.before.fingerprint
                or request.request_key != row.request_key
                or row.request_hash
                != WorkspaceTreeMoveStore._wanted(request, row.resource_fingerprint)
            ):
                raise _corrupt()
            return StoredTreeMove(
                TreeMovePreviewView(
                    proposal_id=row.proposal_id,
                    scope_id=row.scope_id,
                    workspace_key=row.workspace_key,
                    resource_fingerprint=row.resource_fingerprint,
                    source_path=row.source_path,
                    target_path=row.target_path,
                    before_fingerprint=prepared.before.fingerprint,
                    entry_count=len(prepared.before.entries),
                    byte_size=prepared.before.total_bytes,
                    revision=row.revision,
                    status=cast(TreeMoveStatus, row.status),
                    expires_at=row.expires_at,
                    active_operation_id=row.active_operation_id,
                    created_at=_date(row.created_at),
                    updated_at=_date(row.updated_at),
                ),
                prepared,
            )
        except (TypeError, ValueError, KeyError):
            raise _corrupt() from None

    async def _proposal(
        self, sql: AsyncSession, scope: str, identifier: str
    ) -> WorkspaceTreeMoveProposalRow:
        topic = await self.files._scope(sql, scope, writing=False)
        row = await sql.get(WorkspaceTreeMoveProposalRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError("workspace_tree_move_not_found", "原树移动提案不在此作用域", 404)
        if row.topic_id != topic:
            raise _corrupt()
        await self.files._resource(sql, scope, row.workspace_key, row.resource_fingerprint)
        self._proposal_value(row)
        await self._stored_source(sql, row)
        await self._generated_origin(sql, row)
        pending = {"applying", "undoing", "keeping", "uncertain"}
        if (row.active_operation_id is not None) != (row.status in pending):
            raise _corrupt()
        if row.active_operation_id is not None:
            active = await sql.get(WorkspaceTreeMoveAttemptRow, row.active_operation_id)
            if active is None or active.proposal_id != row.proposal_id or active.scope_id != scope:
                raise _corrupt()
            expected_status = (
                "uncertain"
                if active.status == "uncertain"
                else {
                    "apply": "applying",
                    "undo": "undoing",
                    "keep": "keeping",
                }.get(active.action)
            )
            if active.status not in {"running", "uncertain"} or row.status != expected_status:
                raise _corrupt()
        return row

    async def preview(
        self,
        scope: str,
        binding: WorkspaceBinding,
        request: TreeMovePreviewRequest,
        prepared: PreparedTreeMove | None,
        *,
        model_source: TreeMoveModelSource | None = None,
        generated_origin: GeneratedTreeUndoSource | None = None,
    ) -> tuple[StoredTreeMove, bool]:
        request = TreeMovePreviewRequest.model_validate(request.model_dump())
        if generated_origin is not None:
            self._generated_source_valid(generated_origin)
        wanted = self._wanted(request, binding.fingerprint)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            topic = await self.files._scope(sql, scope, writing=True)
            await self.files._resource(sql, scope, binding.key, binding.fingerprint)
            previous = await sql.scalar(
                select(WorkspaceTreeMoveProposalRow).where(
                    WorkspaceTreeMoveProposalRow.scope_id == scope,
                    WorkspaceTreeMoveProposalRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != wanted:
                    raise ServiceError(
                        "workspace_tree_move_idempotency_conflict", "原树移动键参数/资源已变化", 409
                    )
                await self._retry_source(sql, previous, model_source)
                await self._generated_origin(sql, previous)
                if generated_origin is not None:
                    origin = await sql.get(WorkspaceGeneratedTreeUndoRow, previous.proposal_id)
                    if origin is None or (
                        origin.source_plan_id,
                        origin.previous_restore_operation_id,
                    ) != (generated_origin.plan_id, generated_origin.previous_restore_operation_id):
                        raise ServiceError(
                            "workspace_generated_undo_source_conflict",
                            "原键已经绑定其他生成根来源",
                            409,
                        )
                return self._proposal_value(previous), False
            await self._new_source(sql, scope, model_source)
            if prepared is None or (
                prepared.before.root_path != request.source_path
                or prepared.target_path != request.target_path
                or prepared.before.fingerprint != request.expected_fingerprint
                or prepared.before.workspace_fingerprint != binding.workspace.fingerprint
            ):
                raise ServiceError(
                    "workspace_tree_move_proposal_changed", "原树移动准备不匹配参数或资源", 409
                )
            if not binding.require().writable:
                raise ServiceError("workspace_read_only", "未授权原树移动写能力", 403)
            sealed = (
                await self._generated_candidate(
                    sql, scope, binding.key, binding.fingerprint, generated_origin, prepared
                )
                if generated_origin is not None
                else None
            )
            metadata, payload = encode_move(prepared)
            count = int(
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeMoveProposalRow))
                or 0
            )
            if count >= self.max_proposals:
                raise ServiceError("workspace_tree_move_limit", "原树移动提案历史已达上限", 429)
            raw_request = request.model_dump(mode="json")
            await self._bytes(
                sql,
                len(payload)
                + len(json.dumps(metadata))
                + len(json.dumps(raw_request))
                + (
                    36
                    + len(sealed.origin.view.plan_id)
                    + len(scope)
                    + len(binding.key)
                    + 256
                    + len(generated_origin.previous_restore_operation_id or "")
                    if sealed is not None and generated_origin is not None
                    else 0
                ),
            )
            now = utc_now()
            row = WorkspaceTreeMoveProposalRow(
                proposal_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                request_key=request.request_key,
                request_hash=wanted,
                request=raw_request,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                source_path=request.source_path,
                target_path=request.target_path,
                prepared=cast(dict[str, JsonValue], metadata),
                payload=payload,
                generated_plan_id=generated_origin.plan_id
                if generated_origin is not None
                else None,
                status="proposed",
                revision=0,
                expires_at=time.time() + request.ttl_seconds,
                active_operation_id=None,
                created_at=now,
                updated_at=now,
            )
            sql.add(row)
            await sql.flush()
            await self._bind_source(sql, row, "preview", model_source)
            if sealed is not None and generated_origin is not None:
                sql.add(
                    WorkspaceGeneratedTreeUndoRow(
                        proposal_id=row.proposal_id,
                        source_plan_id=generated_origin.plan_id,
                        scope_id=scope,
                        workspace_key=binding.key,
                        resource_fingerprint=binding.fingerprint,
                        tree_fingerprint=sealed.origin.snapshot.fingerprint,
                        approved_fingerprint=prepared.before.fingerprint,
                        proof_hash=sealed.origin.view.proof_hash,
                        previous_restore_operation_id=generated_origin.previous_restore_operation_id,
                        created_at=now,
                    )
                )
                await sql.flush()
            await self._generated_origin(sql, row)
            return self._proposal_value(row), True

    async def proposal(self, scope: str, identifier: str) -> StoredTreeMove:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._proposal(sql, scope, identifier)
            if row.active_operation_id is not None:
                operation, _ = await self._operation(sql, scope, row.active_operation_id)
                await self._expire(sql, operation, row)
            return self._proposal_value(row)

    async def _generated_view(
        self, sql: AsyncSession, row: WorkspaceTreeMoveProposalRow
    ) -> GeneratedTreeUndoPreviewView:
        link = await sql.get(WorkspaceGeneratedTreeUndoRow, row.proposal_id)
        if row.generated_plan_id is None or link is None:
            raise ServiceError("workspace_generated_undo_required", "原提案不是生成根撤销", 409)
        sealed = await self._generated_seal(
            sql, row.scope_id, row.workspace_key, row.resource_fingerprint, row.generated_plan_id
        )
        source = sealed.origin.view
        states: dict[str, GeneratedTreeRootState] = {
            "proposed": "awaiting_approval",
            "applying": "removing",
            "applied": "preserved",
            "undoing": "restoring",
            "undone": "restored",
            "keeping": "keeping",
            "kept": "kept",
            "rejected": "rejected",
            "uncertain": "uncertain",
            "cancelled": "cancelled",
        }
        return GeneratedTreeUndoPreviewView(
            source_plan_id=source.plan_id,
            source_command_id=source.command_id,
            source_hash=source.source_hash,
            command_outcome=source.outcome,
            accept_failed=source.accept_failed,
            snapshot_revision=source.snapshot_revision,
            sealed_fingerprint=source.tree_fingerprint,
            proof_hash=source.proof_hash,
            previous_restore_operation_id=link.previous_restore_operation_id,
            root_state=states[row.status],
            move=self._proposal_value(row).view,
        )

    async def generated_proposal(self, scope: str, identifier: str) -> GeneratedTreeUndoPreviewView:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._proposal(sql, scope, identifier)
            if row.active_operation_id is not None:
                operation, _ = await self._operation(sql, scope, row.active_operation_id)
                await self._expire(sql, operation, row)
            return await self._generated_view(sql, row)

    async def generated_list(
        self, scope: str, *, after: str | None = None, limit: int = 32
    ) -> GeneratedTreeUndoPage:
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceError("workspace_tree_move_page", "原生成根历史页大小无效", 422)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            linked = (
                select(WorkspaceGeneratedTreeUndoRow.proposal_id)
                .where(
                    WorkspaceGeneratedTreeUndoRow.proposal_id
                    == WorkspaceTreeMoveProposalRow.proposal_id
                )
                .exists()
            )
            query = select(WorkspaceTreeMoveProposalRow).where(
                WorkspaceTreeMoveProposalRow.scope_id == scope,
                or_(WorkspaceTreeMoveProposalRow.generated_plan_id.is_not(None), linked),
            )
            if after is not None:
                await self._generated_view(sql, await self._proposal(sql, scope, after))
                query = query.where(WorkspaceTreeMoveProposalRow.proposal_id > after)
            rows = list(
                await sql.scalars(
                    query.order_by(WorkspaceTreeMoveProposalRow.proposal_id).limit(limit + 1)
                )
            )
            items = []
            for selected in rows[:limit]:
                row = await self._proposal(sql, scope, selected.proposal_id)
                if row.active_operation_id is not None:
                    operation, _ = await self._operation(sql, scope, row.active_operation_id)
                    await self._expire(sql, operation, row)
                items.append(await self._generated_view(sql, row))
            return GeneratedTreeUndoPage(
                items=items, next_after=rows[limit - 1].proposal_id if len(rows) > limit else None
            )

    async def proposal_by_key(self, scope: str, key: str) -> StoredTreeMove | None:
        self.files._key(key)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(WorkspaceTreeMoveProposalRow).where(
                    WorkspaceTreeMoveProposalRow.scope_id == scope,
                    WorkspaceTreeMoveProposalRow.request_key == key,
                )
            )
            if row is None:
                return None
            row = await self._proposal(sql, scope, row.proposal_id)
            if row.active_operation_id is not None:
                operation, _ = await self._operation(sql, scope, row.active_operation_id)
                await self._expire(sql, operation, row)
            return self._proposal_value(row)

    @staticmethod
    def _effect_value(
        row: WorkspaceTreeMoveAttemptRow, prepared: PreparedTreeMove
    ) -> TreeMoveEffect | None:
        if row.receipt is None:
            if (
                row.status in {"done", "rejected"}
                or row.after_prepared is not None
                or row.after_payload is not None
            ):
                raise _corrupt()
            return None
        try:
            receipt = TypeAdapter(TreeMoveReceipt).validate_json(
                json.dumps(row.receipt), strict=True
            )
            after_move = (
                decode_move(row.after_prepared, row.after_payload)
                if row.after_prepared is not None and row.after_payload is not None
                else None
            )
            after = after_move.before if after_move else None
            if (
                receipt.status != row.status
                or receipt.operation_token != prepared.token
                or receipt.source_path != prepared.before.root_path
                or receipt.target_path != prepared.target_path
                or receipt.before_fingerprint != prepared.before.fingerprint
                or receipt.after_fingerprint != (after.fingerprint if after else None)
                or receipt.recovery_required != (row.status == "uncertain")
                or receipt.status == "rejected"
                and receipt.changed
                or receipt.status == "done"
                and (
                    after is None
                    or not WorkspaceTreeWriter._moved(prepared, after)
                    or receipt.changed != (row.action != "keep")
                )
                or after_move is not None
                and (
                    after_move.target_path != prepared.before.root_path
                    or after_move.target_parent_identity != prepared.before.parent_identity
                    or after_move.token != prepared.token
                )
                or not row.receipt_history
                or row.receipt_history[-1] != row.receipt
            ):
                raise _corrupt()
            return TreeMoveEffect(receipt, after)
        except (TypeError, ValueError, KeyError):
            raise _corrupt() from None

    async def _attempt_value(
        self,
        sql: AsyncSession,
        row: WorkspaceTreeMoveAttemptRow,
        proposal: WorkspaceTreeMoveProposalRow,
    ) -> StoredTreeMoveAttempt:
        prepared = decode_move(row.prepared, row.payload)
        request = TreeMoveAction(
            proposal_id=row.proposal_id,
            action=cast(TreeMoveActionKind, row.action),
            expected_revision=row.expected_revision,
            request_key=row.request_key,
        )
        original = self._proposal_value(proposal).prepared
        if (
            row.scope_id != proposal.scope_id
            or row.request_hash != self._wanted(request, proposal.resource_fingerprint)
            or row.action in {"apply", "keep"}
            and prepared != original
            or row.action == "undo"
            and (
                prepared.before.root_path != original.target_path
                or prepared.target_path != original.before.root_path
                or prepared.before.parent_identity != original.target_parent_identity
                or prepared.target_parent_identity != original.before.parent_identity
                or not WorkspaceTreeWriter._moved(original, prepared.before)
            )
        ):
            raise _corrupt()
        source = await self._stored_source(sql, proposal, row)
        effect = self._effect_value(row, prepared)
        receipt = None
        if effect is not None:
            raw = asdict(effect.receipt)
            raw.pop("operation_token")
            receipt = TreeMoveReceiptView.model_validate(raw)
        return StoredTreeMoveAttempt(
            TreeMoveAttemptView(
                operation_id=row.operation_id,
                proposal_id=row.proposal_id,
                scope_id=row.scope_id,
                action=cast(TreeMoveActionKind, row.action),
                expected_revision=row.expected_revision,
                status=cast(Literal["running", "done", "rejected", "uncertain"], row.status),
                receipt=receipt,
                cancelled=row.cancelled,
                recovery_required=row.status == "uncertain",
                created_at=_date(row.created_at),
                updated_at=_date(row.updated_at),
            ),
            prepared,
            effect,
            row.intent_recorded,
            source,
        )

    async def _operation(
        self, sql: AsyncSession, scope: str, identifier: str
    ) -> tuple[WorkspaceTreeMoveAttemptRow, WorkspaceTreeMoveProposalRow]:
        row = await sql.get(WorkspaceTreeMoveAttemptRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError(
                "workspace_tree_move_operation_not_found", "原树移动操作不在此作用域", 404
            )
        proposal = await self._proposal(sql, scope, row.proposal_id)
        await self._attempt_value(sql, row, proposal)
        return row, proposal

    async def _locks(
        self, sql: AsyncSession, row: WorkspaceTreeMoveAttemptRow, prepared: PreparedTreeMove
    ) -> None:
        keys = {
            (lock.kind, lock.lock_key)
            for lock in await sql.scalars(
                select(WorkspaceTreeMoveLockRow).where(
                    WorkspaceTreeMoveLockRow.operation_id == row.operation_id
                )
            )
        }
        if keys != move_keys(prepared):
            raise _corrupt()

    async def _live(
        self,
        sql: AsyncSession,
        row: WorkspaceTreeMoveAttemptRow,
        proposal: WorkspaceTreeMoveProposalRow,
    ) -> None:
        await self.files._scope(sql, row.scope_id, writing=True, run_id=row.run_id)
        await self.files._resource(
            sql, row.scope_id, proposal.workspace_key, proposal.resource_fingerprint
        )
        if (
            row.status != "running"
            or row.owner_id != self.owner_id
            or (row.lease_expires_at or 0) <= time.time()
            or row.cancelled
            or proposal.active_operation_id != row.operation_id
        ):
            raise ServiceError("workspace_tree_move_fenced", "原树移动生产者/租约/准入已失效", 409)
        await self._stored_source(sql, proposal, row, live=True)
        prepared = decode_move(row.prepared, row.payload)
        await self._locks(sql, row, prepared)
        await assert_namespace_available(sql, prepared, exclude_operation_id=row.operation_id)

    async def reserve(
        self,
        scope: str,
        request: TreeMoveAction,
        resource_fingerprint: str,
        prepared: PreparedTreeMove,
        *,
        model_source: TreeMoveModelSource | None = None,
    ) -> tuple[StoredTreeMoveAttempt, bool]:
        request = TreeMoveAction.model_validate(request.model_dump())
        wanted = self._wanted(request, resource_fingerprint)
        metadata, payload = encode_move(prepared)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=True)
            proposal = await self._proposal(sql, scope, request.proposal_id)
            await self.files._resource(sql, scope, proposal.workspace_key, resource_fingerprint)
            if proposal.resource_fingerprint != resource_fingerprint:
                raise ServiceError("workspace_binding_changed", "原树动作资源版本不匹配", 403)
            previous = await sql.scalar(
                select(WorkspaceTreeMoveAttemptRow).where(
                    WorkspaceTreeMoveAttemptRow.scope_id == scope,
                    WorkspaceTreeMoveAttemptRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != wanted or previous.proposal_id != proposal.proposal_id:
                    raise ServiceError(
                        "workspace_tree_move_idempotency_conflict", "原树动作键已绑定其他请求", 409
                    )
                await self._retry_source(sql, proposal, model_source, previous)
                return await self._attempt_value(sql, previous, proposal), False
            await self._new_source(sql, scope, model_source)
            allowed = {"apply": {"proposed"}, "undo": {"applied", "kept"}, "keep": {"applied"}}
            if (
                proposal.status not in allowed[request.action]
                or proposal.active_operation_id is not None
            ):
                raise ServiceError("workspace_tree_move_state", "原树提案状态不允许此动作", 409)
            if proposal.revision != request.expected_revision:
                raise ServiceError("workspace_revision_conflict", "原树提案版本已变化", 409)
            if request.action == "apply" and proposal.expires_at <= time.time():
                raise ServiceError("workspace_proposal_expired", "原树提案已过期", 410)
            original = self._proposal_value(proposal).prepared
            if request.action in {"apply", "keep"}:
                if prepared != original:
                    raise ServiceError(
                        "workspace_tree_move_proposal_changed", "原动作必须使用冻结树材料", 409
                    )
            else:
                last = await sql.scalar(
                    select(WorkspaceTreeMoveAttemptRow).where(
                        WorkspaceTreeMoveAttemptRow.proposal_id == proposal.proposal_id,
                        WorkspaceTreeMoveAttemptRow.action == "apply",
                        WorkspaceTreeMoveAttemptRow.status == "done",
                    )
                )
                if last is None:
                    raise _corrupt()
                effect = self._effect_value(last, original)
                if (
                    effect is None
                    or effect.observed_after != prepared.before
                    or (
                        prepared.target_path != original.before.root_path
                        or prepared.target_parent_identity != original.before.parent_identity
                        or not WorkspaceTreeWriter._moved(original, prepared.before)
                    )
                ):
                    raise ServiceError(
                        "workspace_tree_move_proposal_changed", "反向材料未绑定原完整after", 409
                    )
            keys = await assert_namespace_available(sql, prepared)
            count = int(
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeMoveAttemptRow)) or 0
            )
            if count >= self.max_proposals * 8:
                raise ServiceError(
                    "workspace_tree_move_operation_limit", "原树动作历史已达上限", 429
                )
            await self._bytes(sql, len(payload) + len(json.dumps(metadata)) + 2)
            now = utc_now()
            row = WorkspaceTreeMoveAttemptRow(
                operation_id=str(uuid4()),
                proposal_id=proposal.proposal_id,
                scope_id=scope,
                request_key=request.request_key,
                request_hash=wanted,
                action=request.action,
                expected_revision=request.expected_revision,
                previous_status=proposal.status,
                status="running",
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                prepared=cast(dict[str, JsonValue], metadata),
                payload=payload,
                after_prepared=None,
                after_payload=None,
                receipt=None,
                receipt_history=[],
                intent_recorded=False,
                cancelled=False,
                run_id=model_source.run_id if model_source else None,
                call_id=model_source.call_id if model_source else None,
                created_at=now,
                updated_at=now,
            )
            sql.add(row)
            await sql.flush()
            revision = await sql.scalar(
                update(WorkspaceTreeMoveProposalRow)
                .where(
                    WorkspaceTreeMoveProposalRow.proposal_id == proposal.proposal_id,
                    WorkspaceTreeMoveProposalRow.revision == request.expected_revision,
                    WorkspaceTreeMoveProposalRow.active_operation_id.is_(None),
                    WorkspaceTreeMoveProposalRow.status == proposal.status,
                )
                .values(
                    active_operation_id=row.operation_id,
                    revision=request.expected_revision + 1,
                    status={"apply": "applying", "undo": "undoing", "keep": "keeping"}[
                        request.action
                    ],
                    updated_at=now,
                )
                .returning(WorkspaceTreeMoveProposalRow.revision)
            )
            if revision is None:
                raise ServiceError("workspace_revision_conflict", "原树提案接受发生竞争", 409)
            for kind, key in keys:
                sql.add(
                    WorkspaceTreeMoveLockRow(operation_id=row.operation_id, kind=kind, lock_key=key)
                )
            await self._bind_source(sql, proposal, request.action, model_source, row.operation_id)
            await sql.flush()
            return await self._attempt_value(sql, row, proposal), True

    async def _expire(
        self,
        sql: AsyncSession,
        row: WorkspaceTreeMoveAttemptRow,
        proposal: WorkspaceTreeMoveProposalRow,
    ) -> None:
        if row.status == "running" and (row.lease_expires_at or 0) <= time.time():
            if proposal.active_operation_id != row.operation_id:
                raise _corrupt()
            await self._locks(sql, row, decode_move(row.prepared, row.payload))
            row.status, row.owner_id, row.lease_expires_at = "uncertain", None, None
            row.updated_at = utc_now()
            proposal.status, proposal.updated_at = "uncertain", row.updated_at
            proposal.revision += 1

    async def operation(self, scope: str, identifier: str) -> StoredTreeMoveAttempt:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            await self._expire(sql, row, proposal)
            return await self._attempt_value(sql, row, proposal)

    async def operation_by_key(self, scope: str, key: str) -> StoredTreeMoveAttempt | None:
        self.files._key(key)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(WorkspaceTreeMoveAttemptRow).where(
                    WorkspaceTreeMoveAttemptRow.scope_id == scope,
                    WorkspaceTreeMoveAttemptRow.request_key == key,
                )
            )
            if row is None:
                return None
            row, proposal = await self._operation(sql, scope, row.operation_id)
            await self._expire(sql, row, proposal)
            return await self._attempt_value(sql, row, proposal)

    async def find_action(
        self,
        scope: str,
        request: TreeMoveAction,
        fingerprint: str,
        *,
        model_source: TreeMoveModelSource | None = None,
    ) -> StoredTreeMoveAttempt | None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            proposal = await self._proposal(sql, scope, request.proposal_id)
            if proposal.resource_fingerprint != fingerprint:
                raise ServiceError("workspace_binding_changed", "原树动作资源版本不匹配", 403)
            row = await sql.scalar(
                select(WorkspaceTreeMoveAttemptRow).where(
                    WorkspaceTreeMoveAttemptRow.scope_id == scope,
                    WorkspaceTreeMoveAttemptRow.request_key == request.request_key,
                )
            )
            if row is None:
                return None
            if (
                row.request_hash != self._wanted(request, fingerprint)
                or row.proposal_id != proposal.proposal_id
            ):
                raise ServiceError(
                    "workspace_tree_move_idempotency_conflict", "原树动作键已绑定其他请求", 409
                )
            await self._retry_source(sql, proposal, model_source, row)
            await self._expire(sql, row, proposal)
            return await self._attempt_value(sql, row, proposal)

    async def assert_execution(self, scope: str, identifier: str) -> None:
        async with self.database.write_lock, self.database.sessions() as sql:
            row, proposal = await self._operation(sql, scope, identifier)
            await self._live(sql, row, proposal)

    async def renew(self, scope: str, identifier: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            await self._live(sql, row, proposal)
            row.lease_expires_at, row.updated_at = time.time() + self.lease_seconds, utc_now()

    async def record_intent(self, scope: str, identifier: str, prepared: PreparedTreeMove) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            await self._live(sql, row, proposal)
            if decode_move(row.prepared, row.payload) != prepared:
                raise ServiceError("workspace_tree_move_reference", "原intent材料不匹配", 409)
            row.intent_recorded, row.updated_at = True, utc_now()

    async def cancel(self, scope: str, identifier: str) -> StoredTreeMove:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=True)
            proposal = await self._proposal(sql, scope, identifier)
            if proposal.active_operation_id is not None:
                row, _ = await self._operation(sql, scope, proposal.active_operation_id)
                if row.status == "running":
                    row.cancelled, row.updated_at = True, utc_now()
            elif proposal.status == "proposed":
                proposal.status, proposal.revision, proposal.updated_at = (
                    "cancelled",
                    proposal.revision + 1,
                    utc_now(),
                )
            return self._proposal_value(proposal)

    async def settle(
        self,
        scope: str,
        identifier: str,
        effect: TreeMoveEffect,
        *,
        resolving: bool = False,
    ) -> StoredTreeMoveAttempt:
        raw = cast(dict[str, JsonValue], asdict(effect.receipt))
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            if row.status in {"done", "rejected"}:
                if row.receipt != raw:
                    raise ServiceError("workspace_tree_move_resolved", "原树操作终态不能改写", 409)
                return await self._attempt_value(sql, row, proposal)
            if resolving:
                await self.files._scope(sql, scope, writing=True)
                if row.status != "uncertain":
                    raise ServiceError("workspace_tree_move_state", "仅原unknown允许核实", 409)
            elif (
                row.status != "running"
                or row.owner_id != self.owner_id
                or (row.lease_expires_at or 0) <= time.time()
            ):
                raise ServiceError("workspace_tree_move_fenced", "原回执未由当前producer持有", 409)
            if proposal.active_operation_id != row.operation_id or len(row.receipt_history) >= 16:
                raise _corrupt()
            prepared = decode_move(row.prepared, row.payload)
            await self._locks(sql, row, prepared)
            after_metadata, after_payload = None, None
            if effect.observed_after is not None:
                after_metadata_raw, after_payload = encode_move(
                    PreparedTreeMove(
                        effect.observed_after,
                        prepared.before.root_path,
                        prepared.before.parent_identity,
                        prepared.token,
                    )
                )
                after_metadata = cast(dict[str, JsonValue], after_metadata_raw)
            candidate_history = [*row.receipt_history, raw]
            added = (
                len(json.dumps(raw))
                + len(json.dumps(candidate_history))
                - len(json.dumps(row.receipt_history))
            )
            added += len(after_payload or b"") + (
                len(json.dumps(after_metadata)) if after_metadata else 0
            )
            added -= len(row.after_payload or b"") + (
                len(json.dumps(row.after_prepared)) if row.after_prepared else 0
            )
            await self._bytes(sql, added)
            if row.action == "keep" and effect.receipt.status == "done" and not resolving:
                await self._live(sql, row, proposal)
            row.status, row.receipt, row.receipt_history = (
                effect.receipt.status,
                raw,
                candidate_history,
            )
            row.after_prepared, row.after_payload = after_metadata, after_payload
            self._effect_value(row, prepared)
            row.owner_id, row.lease_expires_at, row.updated_at = None, None, utc_now()
            if row.status == "done":
                proposal.status = {"apply": "applied", "undo": "undone", "keep": "kept"}[row.action]
            elif row.status == "uncertain":
                proposal.status = "uncertain"
            else:
                proposal.status = (
                    "cancelled" if row.cancelled and row.action == "apply" else row.previous_status
                )
            proposal.revision += 1
            proposal.updated_at = row.updated_at
            if row.status != "uncertain":
                proposal.active_operation_id = None
                await sql.execute(
                    delete(WorkspaceTreeMoveLockRow).where(
                        WorkspaceTreeMoveLockRow.operation_id == identifier
                    )
                )
            await sql.flush()
            return await self._attempt_value(sql, row, proposal)

    async def recover_interrupted(self) -> int:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            rows = list(
                await sql.scalars(
                    select(WorkspaceTreeMoveAttemptRow).where(
                        WorkspaceTreeMoveAttemptRow.status == "running",
                        WorkspaceTreeMoveAttemptRow.lease_expires_at <= time.time(),
                    )
                )
            )
            for row in rows:
                proposal = await self._proposal(sql, row.scope_id, row.proposal_id)
                await self._attempt_value(sql, row, proposal)
                await self._expire(sql, row, proposal)
            return len(rows)

    async def history(self, scope: str, identifier: str) -> list[TreeMoveAttemptView]:
        async with self.database.write_lock, self.database.sessions() as sql:
            proposal = await self._proposal(sql, scope, identifier)
            rows = await sql.scalars(
                select(WorkspaceTreeMoveAttemptRow)
                .where(WorkspaceTreeMoveAttemptRow.proposal_id == identifier)
                .order_by(
                    WorkspaceTreeMoveAttemptRow.created_at, WorkspaceTreeMoveAttemptRow.operation_id
                )
            )
            return [(await self._attempt_value(sql, row, proposal)).view for row in rows]

    async def history_page(
        self,
        scope: str,
        identifier: str,
        *,
        after: str | None = None,
        limit: int = 32,
    ) -> TreeMoveAttemptPage:
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceError("workspace_tree_move_page", "原树历史页大小无效", 422)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            proposal = await self._proposal(sql, scope, identifier)
            query = select(WorkspaceTreeMoveAttemptRow).where(
                WorkspaceTreeMoveAttemptRow.proposal_id == identifier
            )
            if after is not None:
                cursor, _ = await self._operation(sql, scope, after)
                if cursor.proposal_id != identifier:
                    raise ServiceError("workspace_tree_move_page", "原树历史游标不属于该提案", 422)
                query = query.where(WorkspaceTreeMoveAttemptRow.operation_id > after)
            rows = list(
                await sql.scalars(
                    query.order_by(WorkspaceTreeMoveAttemptRow.operation_id).limit(limit + 1)
                )
            )
            values = []
            for row in rows[:limit]:
                await self._expire(sql, row, proposal)
                values.append((await self._attempt_value(sql, row, proposal)).view)
            return TreeMoveAttemptPage(
                items=values, next_after=rows[limit - 1].operation_id if len(rows) > limit else None
            )

    async def mark_uncertain(self, scope: str, identifier: str) -> StoredTreeMoveAttempt:
        """回执持久失败时关闭原producer，保留材料/锁供同operation核实。"""
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            if row.status != "running":
                return await self._attempt_value(sql, row, proposal)
            if row.owner_id != self.owner_id:
                raise ServiceError("workspace_tree_move_fenced", "未知收束不属于原owner", 409)
            row.lease_expires_at = time.time() - 1
            await self._expire(sql, row, proposal)
            return await self._attempt_value(sql, row, proposal)

    async def list(self, scope: str, *, after: str | None = None, limit: int = 32) -> TreeMovePage:
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceError("workspace_tree_move_page", "原树历史页大小无效", 422)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            query = select(WorkspaceTreeMoveProposalRow).where(
                WorkspaceTreeMoveProposalRow.scope_id == scope
            )
            if after is not None:
                await self._proposal(sql, scope, after)
                query = query.where(WorkspaceTreeMoveProposalRow.proposal_id > after)
            rows = list(
                await sql.scalars(
                    query.order_by(WorkspaceTreeMoveProposalRow.proposal_id).limit(limit + 1)
                )
            )
            items = []
            for row in rows[:limit]:
                row = await self._proposal(sql, scope, row.proposal_id)
                if row.active_operation_id is not None:
                    operation, _ = await self._operation(sql, scope, row.active_operation_id)
                    await self._expire(sql, operation, row)
                items.append(self._proposal_value(row).view)
            return TreeMovePage(
                items=items, next_after=rows[limit - 1].proposal_id if len(rows) > limit else None
            )
