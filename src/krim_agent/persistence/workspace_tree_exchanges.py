"""双完整树交换原账本：原scope/快照/CAS/来源/lease与双向共享namespace锁。"""

from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import uuid4

from pydantic import JsonValue
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_tree_exchange import (
    PreparedTreeExchange,
    TreeExchangeEffect,
    WorkspaceTreeExchangeWriter,
)
from krim_agent.workspace_tree_exchange_types import (
    TreeExchangeAction,
    TreeExchangeActionKind,
    TreeExchangeAttemptPage,
    TreeExchangeAttemptView,
    TreeExchangeModelKind,
    TreeExchangeModelSource,
    TreeExchangePage,
    TreeExchangePreviewRequest,
    TreeExchangePreviewView,
    TreeExchangeReceiptView,
    TreeExchangeStatus,
)

from .completed_tree_seals import CompletedTreeSealStore, SealedTreeOrigin
from .database import (
    WorkspaceTreeExchangeAttemptRow,
    WorkspaceTreeExchangeLockRow,
    WorkspaceTreeExchangeModelSourceRow,
    WorkspaceTreeExchangeOriginRow,
    WorkspaceTreeExchangeProposalRow,
)
from .tree_exchange_codec import (
    decode_exchange,
    decode_exchange_effect,
    encode_exchange,
    encode_exchange_effect,
)
from .tree_exchange_fences import assert_exchange_namespace_available, exchange_keys
from .tree_model_sources import run_scope
from .workspace_directories import WorkspaceDirectoryStore
from .workspace_edits import WorkspaceEditStore, digest
from .workspace_tree_plans import WorkspaceTreePlanStore


def _corrupt() -> ServiceError:
    return ServiceError("workspace_tree_exchange_corrupt", "原双树交换账本或关联损坏", 409)


def _date(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@dataclass(frozen=True)
class StoredTreeExchange:
    view: TreeExchangePreviewView
    prepared: PreparedTreeExchange


@dataclass(frozen=True)
class StoredTreeExchangeAttempt:
    view: TreeExchangeAttemptView
    prepared: PreparedTreeExchange
    effect: TreeExchangeEffect | None
    intent_recorded: bool
    model_source: TreeExchangeModelSource | None


class WorkspaceTreeExchangeStore:
    def __init__(
        self,
        files: WorkspaceEditStore,
        *,
        max_proposals: int = 500,
        max_snapshot_bytes: int = 536_870_912,
        lease_seconds: float = 60,
    ) -> None:
        if type(max_proposals) is not int or not 1 <= max_proposals <= 5000:
            raise ValueError("双树交换历史容量无效")
        if type(max_snapshot_bytes) is not int or not 1024 <= max_snapshot_bytes <= 2_147_483_648:
            raise ValueError("双树交换私有材料总容量无效")
        if (
            type(lease_seconds) not in {int, float}
            or not math.isfinite(lease_seconds)
            or not 0.2 <= lease_seconds <= 600
        ):
            raise ValueError("双树交换租约无效")
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
    def _wanted(request: TreeExchangePreviewRequest | TreeExchangeAction, fingerprint: str) -> str:
        return digest(
            {"request": request.model_dump(mode="json", exclude_none=True), "resource": fingerprint}
        )

    @staticmethod
    def _source_valid(source: TreeExchangeModelSource) -> None:
        if (
            not isinstance(source, TreeExchangeModelSource)
            or not isinstance(source.run_id, str)
            or not 1 <= len(source.run_id) <= 36
            or source.run_id == "local"
            or not isinstance(source.call_id, str)
            or not 1 <= len(source.call_id) <= 128
            or any(ord(char) < 32 or ord(char) == 127 for char in source.call_id)
        ):
            raise ServiceError(
                "workspace_tree_exchange_model_context", "双树交换须绑定原模型Run/call", 403
            )

    async def _new_source(
        self, sql: AsyncSession, scope: str, source: TreeExchangeModelSource | None
    ) -> None:
        if source is None:
            return
        self._source_valid(source)
        await run_scope(sql, scope, source.run_id, live=True)
        if (
            await sql.get(WorkspaceTreeExchangeModelSourceRow, (source.run_id, source.call_id))
            is not None
        ):
            raise ServiceError(
                "workspace_tree_exchange_call_conflict", "原模型调用已绑定其他双树交换准入", 409
            )

    async def _bind_source(
        self,
        sql: AsyncSession,
        proposal: WorkspaceTreeExchangeProposalRow,
        kind: TreeExchangeModelKind,
        source: TreeExchangeModelSource | None,
        operation_id: str | None = None,
    ) -> None:
        if source is not None:
            sql.add(
                WorkspaceTreeExchangeModelSourceRow(
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
        proposal: WorkspaceTreeExchangeProposalRow,
        operation: WorkspaceTreeExchangeAttemptRow | None = None,
        *,
        live: bool = False,
    ) -> TreeExchangeModelSource | None:
        query = select(WorkspaceTreeExchangeModelSourceRow).where(
            WorkspaceTreeExchangeModelSourceRow.proposal_id == proposal.proposal_id,
            WorkspaceTreeExchangeModelSourceRow.kind
            == (operation.action if operation else "preview"),
        )
        if operation is not None:
            query = query.where(
                WorkspaceTreeExchangeModelSourceRow.operation_id == operation.operation_id
            )
        rows = list(await sql.scalars(query))
        if len(rows) > 1:
            raise _corrupt()
        source = TreeExchangeModelSource(rows[0].run_id, rows[0].call_id) if rows else None
        if source is not None:
            self._source_valid(source)
            await run_scope(sql, proposal.scope_id, source.run_id, live=live)
        expected_source = (source.run_id, source.call_id) if source else (None, None)
        recorded_source = (
            (operation.run_id, operation.call_id)
            if operation is not None
            else (proposal.run_id, proposal.call_id)
        )
        if recorded_source != expected_source or (
            operation is not None
            and source is not None
            and rows[0].operation_id != operation.operation_id
        ):
            raise _corrupt()
        return source

    async def _retry_source(
        self,
        sql: AsyncSession,
        proposal: WorkspaceTreeExchangeProposalRow,
        source: TreeExchangeModelSource | None,
        operation: WorkspaceTreeExchangeAttemptRow | None = None,
    ) -> None:
        stored = await self._stored_source(sql, proposal, operation)
        if source is not None:
            self._source_valid(source)
            if stored != source:
                raise ServiceError(
                    "workspace_tree_exchange_source_conflict",
                    "原双树交换已绑定其他Run/call来源",
                    409,
                )

    async def _origin_candidate(
        self,
        sql: AsyncSession,
        scope: str,
        workspace_key: str,
        resource_fingerprint: str,
        source_plan_id: str | None,
        prepared: PreparedTreeExchange,
    ) -> SealedTreeOrigin | None:
        if source_plan_id is None:
            return None
        sealed = await self.seals.get_material_in_transaction(
            sql,
            scope,
            source_plan_id,
            workspace_key=workspace_key,
            resource_fingerprint=resource_fingerprint,
        )
        if sealed is None:
            raise ServiceError(
                "workspace_tree_exchange_origin_required", "生成树交换须原首次封存", 409
            )
        if sealed.origin.snapshot != prepared.right:
            raise ServiceError(
                "workspace_tree_exchange_origin_conflict", "原右before不是已封存完整生成树", 409
            )
        return sealed

    async def _origin(self, sql: AsyncSession, proposal: WorkspaceTreeExchangeProposalRow) -> None:
        original = self._proposal_value(proposal)
        request = TreeExchangePreviewRequest.model_validate_json(json.dumps(proposal.request))
        link = await sql.get(WorkspaceTreeExchangeOriginRow, proposal.proposal_id)
        if request.source_plan_id is None:
            if link is not None:
                raise _corrupt()
            return
        if link is None or (
            link.source_plan_id != request.source_plan_id
            or link.scope_id != proposal.scope_id
            or link.workspace_key != proposal.workspace_key
            or link.resource_fingerprint != proposal.resource_fingerprint
        ):
            raise _corrupt()
        sealed = await self._origin_candidate(
            sql,
            proposal.scope_id,
            proposal.workspace_key,
            proposal.resource_fingerprint,
            request.source_plan_id,
            original.prepared,
        )
        if (
            sealed is None
            or link.tree_fingerprint != sealed.origin.snapshot.fingerprint
            or link.proof_hash != sealed.origin.view.proof_hash
        ):
            raise _corrupt()

    async def _bytes(self, sql: AsyncSession, added: int) -> None:
        before = await sql.scalar(
            select(
                func.coalesce(
                    func.sum(
                        func.length(WorkspaceTreeExchangeProposalRow.payload)
                        + func.length(WorkspaceTreeExchangeProposalRow.prepared)
                        + func.length(WorkspaceTreeExchangeProposalRow.request)
                    ),
                    0,
                )
            )
        )
        operations = await sql.scalar(
            select(
                func.coalesce(
                    func.sum(
                        func.length(WorkspaceTreeExchangeAttemptRow.payload)
                        + func.length(WorkspaceTreeExchangeAttemptRow.prepared)
                        + func.coalesce(
                            func.length(WorkspaceTreeExchangeAttemptRow.after_payload), 0
                        )
                        + func.coalesce(
                            func.length(WorkspaceTreeExchangeAttemptRow.after_prepared), 0
                        )
                        + func.coalesce(func.length(WorkspaceTreeExchangeAttemptRow.receipt), 0)
                        + func.length(WorkspaceTreeExchangeAttemptRow.receipt_history)
                    ),
                    0,
                )
            )
        )
        origins = await sql.scalar(
            select(
                func.coalesce(
                    func.sum(
                        func.length(WorkspaceTreeExchangeOriginRow.proposal_id)
                        + func.length(WorkspaceTreeExchangeOriginRow.source_plan_id)
                        + func.length(WorkspaceTreeExchangeOriginRow.scope_id)
                        + func.length(WorkspaceTreeExchangeOriginRow.workspace_key)
                        + func.length(WorkspaceTreeExchangeOriginRow.resource_fingerprint)
                        + func.length(WorkspaceTreeExchangeOriginRow.tree_fingerprint)
                        + func.length(WorkspaceTreeExchangeOriginRow.proof_hash)
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
                "workspace_tree_exchange_snapshot_limit", "原双树交换私有材料总容量已达上限", 413
            )

    @staticmethod
    def _proposal_value(row: WorkspaceTreeExchangeProposalRow) -> StoredTreeExchange:
        try:
            prepared = decode_exchange(row.prepared, row.payload)
            request = TreeExchangePreviewRequest.model_validate_json(json.dumps(row.request))
            if (
                row.left_path != prepared.left.root_path
                or row.right_path != prepared.right.root_path
                or request.left_path != row.left_path
                or request.right_path != row.right_path
                or request.expected_left != prepared.left.fingerprint
                or request.expected_right != prepared.right.fingerprint
                or request.request_key != row.request_key
                or row.request_hash
                != WorkspaceTreeExchangeStore._wanted(request, row.resource_fingerprint)
            ):
                raise _corrupt()
            return StoredTreeExchange(
                TreeExchangePreviewView(
                    proposal_id=row.proposal_id,
                    scope_id=row.scope_id,
                    workspace_key=row.workspace_key,
                    resource_fingerprint=row.resource_fingerprint,
                    left_path=row.left_path,
                    right_path=row.right_path,
                    left_before_fingerprint=prepared.left.fingerprint,
                    right_before_fingerprint=prepared.right.fingerprint,
                    left_entry_count=len(prepared.left.entries),
                    right_entry_count=len(prepared.right.entries),
                    left_byte_size=prepared.left.total_bytes,
                    right_byte_size=prepared.right.total_bytes,
                    origin="completed_tree"
                    if request.source_plan_id is not None
                    else "existing_trees",
                    source_plan_id=request.source_plan_id,
                    revision=row.revision,
                    status=cast(TreeExchangeStatus, row.status),
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
    ) -> WorkspaceTreeExchangeProposalRow:
        topic = await self.files._scope(sql, scope, writing=False)
        row = await sql.get(WorkspaceTreeExchangeProposalRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError(
                "workspace_tree_exchange_not_found", "原双树交换提案不在此作用域", 404
            )
        if row.topic_id != topic:
            raise _corrupt()
        await self.files._resource(sql, scope, row.workspace_key, row.resource_fingerprint)
        self._proposal_value(row)
        await self._stored_source(sql, row)
        await self._origin(sql, row)
        pending = {"applying", "undoing", "keeping", "uncertain"}
        if (row.active_operation_id is not None) != (row.status in pending):
            raise _corrupt()
        if row.active_operation_id is not None:
            active = await sql.get(WorkspaceTreeExchangeAttemptRow, row.active_operation_id)
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
        request: TreeExchangePreviewRequest,
        prepared: PreparedTreeExchange | None,
        *,
        model_source: TreeExchangeModelSource | None = None,
    ) -> tuple[StoredTreeExchange, bool]:
        request = TreeExchangePreviewRequest.model_validate(request.model_dump())
        wanted = self._wanted(request, binding.fingerprint)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            topic = await self.files._scope(sql, scope, writing=True)
            await self.files._resource(sql, scope, binding.key, binding.fingerprint)
            previous = await sql.scalar(
                select(WorkspaceTreeExchangeProposalRow).where(
                    WorkspaceTreeExchangeProposalRow.scope_id == scope,
                    WorkspaceTreeExchangeProposalRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != wanted:
                    raise ServiceError(
                        "workspace_tree_exchange_idempotency_conflict",
                        "原双树交换键参数/资源已变化",
                        409,
                    )
                await self._retry_source(sql, previous, model_source)
                await self._origin(sql, previous)
                return self._proposal_value(previous), False
            await self._new_source(sql, scope, model_source)
            if prepared is None or (
                prepared.left.root_path != request.left_path
                or prepared.right.root_path != request.right_path
                or prepared.left.fingerprint != request.expected_left
                or prepared.right.fingerprint != request.expected_right
                or prepared.left.workspace_fingerprint != binding.workspace.fingerprint
                or prepared.right.workspace_fingerprint != binding.workspace.fingerprint
            ):
                raise ServiceError(
                    "workspace_tree_exchange_proposal_changed",
                    "原双树交换准备不匹配参数或资源",
                    409,
                )
            if not binding.require().writable:
                raise ServiceError("workspace_read_only", "未授权原双树交换写能力", 403)
            metadata, payload = encode_exchange(prepared)
            count = int(
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeExchangeProposalRow))
                or 0
            )
            if count >= self.max_proposals:
                raise ServiceError(
                    "workspace_tree_exchange_limit", "原双树交换提案历史已达上限", 429
                )
            sealed = await self._origin_candidate(
                sql, scope, binding.key, binding.fingerprint, request.source_plan_id, prepared
            )
            raw_request = request.model_dump(mode="json", exclude_none=True)
            await self._bytes(
                sql,
                len(payload)
                + len(json.dumps(metadata))
                + len(json.dumps(raw_request))
                + (
                    36 + len(sealed.origin.view.plan_id) + len(scope) + len(binding.key) + 192
                    if sealed is not None
                    else 0
                ),
            )
            now = utc_now()
            row = WorkspaceTreeExchangeProposalRow(
                proposal_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                request_key=request.request_key,
                request_hash=wanted,
                request=raw_request,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                left_path=request.left_path,
                right_path=request.right_path,
                prepared=cast(dict[str, JsonValue], metadata),
                payload=payload,
                run_id=model_source.run_id if model_source else None,
                call_id=model_source.call_id if model_source else None,
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
            if sealed is not None:
                sql.add(
                    WorkspaceTreeExchangeOriginRow(
                        proposal_id=row.proposal_id,
                        source_plan_id=sealed.origin.view.plan_id,
                        scope_id=scope,
                        workspace_key=binding.key,
                        resource_fingerprint=binding.fingerprint,
                        tree_fingerprint=sealed.origin.snapshot.fingerprint,
                        proof_hash=sealed.origin.view.proof_hash,
                        created_at=now,
                    )
                )
                await sql.flush()
            await self._origin(sql, row)
            return self._proposal_value(row), True

    async def proposal(self, scope: str, identifier: str) -> StoredTreeExchange:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._proposal(sql, scope, identifier)
            if row.active_operation_id is not None:
                operation, _ = await self._operation(sql, scope, row.active_operation_id)
                await self._expire(sql, operation, row)
            return self._proposal_value(row)

    async def proposal_by_key(self, scope: str, key: str) -> StoredTreeExchange | None:
        self.files._key(key)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(WorkspaceTreeExchangeProposalRow).where(
                    WorkspaceTreeExchangeProposalRow.scope_id == scope,
                    WorkspaceTreeExchangeProposalRow.request_key == key,
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
        row: WorkspaceTreeExchangeAttemptRow, prepared: PreparedTreeExchange
    ) -> TreeExchangeEffect | None:
        if row.receipt is None:
            if (
                row.status in {"done", "rejected"}
                or row.after_prepared is not None
                or row.after_payload is not None
                or row.receipt_history
            ):
                raise _corrupt()
            return None
        if row.after_prepared is None or row.after_payload is None or row.status == "running":
            raise _corrupt()
        effect = decode_exchange_effect(prepared, row.after_prepared, row.after_payload)
        expected = asdict(effect.receipt)
        if row.action == "keep" and effect.receipt.status == "done":
            # 物理证据仍是原apply的双after；keep仅投影为逻辑changed=false。
            expected["changed"] = False
        if (
            effect.receipt.status != row.status
            or expected != row.receipt
            or not row.receipt_history
            or row.receipt_history[-1] != row.receipt
            or row.action in {"apply", "undo"}
            and effect.receipt.status in {"done", "uncertain"}
            and not row.intent_recorded
        ):
            raise _corrupt()
        return effect

    async def _attempt_value(
        self,
        sql: AsyncSession,
        row: WorkspaceTreeExchangeAttemptRow,
        proposal: WorkspaceTreeExchangeProposalRow,
    ) -> StoredTreeExchangeAttempt:
        prepared = decode_exchange(row.prepared, row.payload)
        request = TreeExchangeAction(
            proposal_id=row.proposal_id,
            action=cast(TreeExchangeActionKind, row.action),
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
                prepared.left.root_path != original.left.root_path
                or prepared.right.root_path != original.right.root_path
                or prepared.left.parent_identity != original.left.parent_identity
                or prepared.right.parent_identity != original.right.parent_identity
                or prepared.token == original.token
                or not WorkspaceTreeExchangeWriter._swapped(original, prepared.left, prepared.right)
            )
        ):
            raise _corrupt()
        source = await self._stored_source(sql, proposal, row)
        effect = self._effect_value(row, prepared)
        if row.action == "undo" or row.action == "keep" and row.status == "done":
            last = await sql.scalar(
                select(WorkspaceTreeExchangeAttemptRow).where(
                    WorkspaceTreeExchangeAttemptRow.proposal_id == proposal.proposal_id,
                    WorkspaceTreeExchangeAttemptRow.action == "apply",
                    WorkspaceTreeExchangeAttemptRow.status == "done",
                )
            )
            original_effect = self._effect_value(last, original) if last is not None else None
            if original_effect is None or (
                row.action == "undo"
                and (
                    original_effect.observed_left != prepared.left
                    or original_effect.observed_right != prepared.right
                )
                or row.action == "keep"
                and original_effect != effect
            ):
                raise _corrupt()
        receipt = None
        if effect is not None:
            raw = dict(row.receipt or {})
            raw.pop("operation_token")
            receipt = TreeExchangeReceiptView.model_validate(raw)
        return StoredTreeExchangeAttempt(
            TreeExchangeAttemptView(
                operation_id=row.operation_id,
                proposal_id=row.proposal_id,
                scope_id=row.scope_id,
                action=cast(TreeExchangeActionKind, row.action),
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
    ) -> tuple[WorkspaceTreeExchangeAttemptRow, WorkspaceTreeExchangeProposalRow]:
        row = await sql.get(WorkspaceTreeExchangeAttemptRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError(
                "workspace_tree_exchange_operation_not_found", "原双树交换操作不在此作用域", 404
            )
        proposal = await self._proposal(sql, scope, row.proposal_id)
        await self._attempt_value(sql, row, proposal)
        return row, proposal

    async def _locks(
        self,
        sql: AsyncSession,
        row: WorkspaceTreeExchangeAttemptRow,
        prepared: PreparedTreeExchange,
    ) -> None:
        keys = {
            (lock.kind, lock.lock_key)
            for lock in await sql.scalars(
                select(WorkspaceTreeExchangeLockRow).where(
                    WorkspaceTreeExchangeLockRow.operation_id == row.operation_id
                )
            )
        }
        if keys != exchange_keys(prepared):
            raise _corrupt()

    async def _live(
        self,
        sql: AsyncSession,
        row: WorkspaceTreeExchangeAttemptRow,
        proposal: WorkspaceTreeExchangeProposalRow,
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
            raise ServiceError(
                "workspace_tree_exchange_fenced", "原双树交换生产者/租约/准入已失效", 409
            )
        await self._stored_source(sql, proposal, row, live=True)
        prepared = decode_exchange(row.prepared, row.payload)
        await self._locks(sql, row, prepared)
        await assert_exchange_namespace_available(
            sql, prepared, exclude_operation_id=row.operation_id
        )

    async def reserve(
        self,
        scope: str,
        request: TreeExchangeAction,
        resource_fingerprint: str,
        prepared: PreparedTreeExchange,
        *,
        model_source: TreeExchangeModelSource | None = None,
    ) -> tuple[StoredTreeExchangeAttempt, bool]:
        request = TreeExchangeAction.model_validate(request.model_dump())
        wanted = self._wanted(request, resource_fingerprint)
        metadata, payload = encode_exchange(prepared)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=True)
            proposal = await self._proposal(sql, scope, request.proposal_id)
            await self.files._resource(sql, scope, proposal.workspace_key, resource_fingerprint)
            if proposal.resource_fingerprint != resource_fingerprint:
                raise ServiceError("workspace_binding_changed", "原树动作资源版本不匹配", 403)
            previous = await sql.scalar(
                select(WorkspaceTreeExchangeAttemptRow).where(
                    WorkspaceTreeExchangeAttemptRow.scope_id == scope,
                    WorkspaceTreeExchangeAttemptRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != wanted or previous.proposal_id != proposal.proposal_id:
                    raise ServiceError(
                        "workspace_tree_exchange_idempotency_conflict",
                        "原树动作键已绑定其他请求",
                        409,
                    )
                await self._retry_source(sql, proposal, model_source, previous)
                return await self._attempt_value(sql, previous, proposal), False
            await self._new_source(sql, scope, model_source)
            allowed = {"apply": {"proposed"}, "undo": {"applied", "kept"}, "keep": {"applied"}}
            if (
                proposal.status not in allowed[request.action]
                or proposal.active_operation_id is not None
            ):
                raise ServiceError("workspace_tree_exchange_state", "原树提案状态不允许此动作", 409)
            if proposal.revision != request.expected_revision:
                raise ServiceError("workspace_revision_conflict", "原树提案版本已变化", 409)
            if request.action == "apply" and proposal.expires_at <= time.time():
                raise ServiceError("workspace_proposal_expired", "原树提案已过期", 410)
            original = self._proposal_value(proposal).prepared
            if request.action in {"apply", "keep"}:
                if prepared != original:
                    raise ServiceError(
                        "workspace_tree_exchange_proposal_changed", "原动作必须使用冻结树材料", 409
                    )
            else:
                last = await sql.scalar(
                    select(WorkspaceTreeExchangeAttemptRow).where(
                        WorkspaceTreeExchangeAttemptRow.proposal_id == proposal.proposal_id,
                        WorkspaceTreeExchangeAttemptRow.action == "apply",
                        WorkspaceTreeExchangeAttemptRow.status == "done",
                    )
                )
                if last is None:
                    raise _corrupt()
                effect = self._effect_value(last, original)
                if (
                    effect is None
                    or effect.observed_left != prepared.left
                    or effect.observed_right != prepared.right
                    or prepared.token == original.token
                    or not WorkspaceTreeExchangeWriter._swapped(
                        original, prepared.left, prepared.right
                    )
                ):
                    raise ServiceError(
                        "workspace_tree_exchange_proposal_changed",
                        "反向材料未绑定原完整双after",
                        409,
                    )
            keys = await assert_exchange_namespace_available(sql, prepared)
            count = int(
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeExchangeAttemptRow))
                or 0
            )
            if count >= self.max_proposals * 8:
                raise ServiceError(
                    "workspace_tree_exchange_operation_limit", "原树动作历史已达上限", 429
                )
            await self._bytes(sql, len(payload) + len(json.dumps(metadata)) + 2)
            now = utc_now()
            row = WorkspaceTreeExchangeAttemptRow(
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
                update(WorkspaceTreeExchangeProposalRow)
                .where(
                    WorkspaceTreeExchangeProposalRow.proposal_id == proposal.proposal_id,
                    WorkspaceTreeExchangeProposalRow.revision == request.expected_revision,
                    WorkspaceTreeExchangeProposalRow.active_operation_id.is_(None),
                    WorkspaceTreeExchangeProposalRow.status == proposal.status,
                )
                .values(
                    active_operation_id=row.operation_id,
                    revision=request.expected_revision + 1,
                    status={"apply": "applying", "undo": "undoing", "keep": "keeping"}[
                        request.action
                    ],
                    updated_at=now,
                )
                .returning(WorkspaceTreeExchangeProposalRow.revision)
            )
            if revision is None:
                raise ServiceError("workspace_revision_conflict", "原树提案接受发生竞争", 409)
            for kind, key in keys:
                sql.add(
                    WorkspaceTreeExchangeLockRow(
                        operation_id=row.operation_id, kind=kind, lock_key=key
                    )
                )
            await self._bind_source(sql, proposal, request.action, model_source, row.operation_id)
            await sql.flush()
            return await self._attempt_value(sql, row, proposal), True

    async def _expire(
        self,
        sql: AsyncSession,
        row: WorkspaceTreeExchangeAttemptRow,
        proposal: WorkspaceTreeExchangeProposalRow,
    ) -> None:
        if row.status == "running" and (row.lease_expires_at or 0) <= time.time():
            if proposal.active_operation_id != row.operation_id:
                raise _corrupt()
            await self._locks(sql, row, decode_exchange(row.prepared, row.payload))
            row.status, row.owner_id, row.lease_expires_at = "uncertain", None, None
            row.updated_at = utc_now()
            proposal.status, proposal.updated_at = "uncertain", row.updated_at
            proposal.revision += 1

    async def operation(self, scope: str, identifier: str) -> StoredTreeExchangeAttempt:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            await self._expire(sql, row, proposal)
            return await self._attempt_value(sql, row, proposal)

    async def operation_by_key(self, scope: str, key: str) -> StoredTreeExchangeAttempt | None:
        self.files._key(key)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(WorkspaceTreeExchangeAttemptRow).where(
                    WorkspaceTreeExchangeAttemptRow.scope_id == scope,
                    WorkspaceTreeExchangeAttemptRow.request_key == key,
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
        request: TreeExchangeAction,
        fingerprint: str,
        *,
        model_source: TreeExchangeModelSource | None = None,
    ) -> StoredTreeExchangeAttempt | None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            proposal = await self._proposal(sql, scope, request.proposal_id)
            if proposal.resource_fingerprint != fingerprint:
                raise ServiceError("workspace_binding_changed", "原树动作资源版本不匹配", 403)
            row = await sql.scalar(
                select(WorkspaceTreeExchangeAttemptRow).where(
                    WorkspaceTreeExchangeAttemptRow.scope_id == scope,
                    WorkspaceTreeExchangeAttemptRow.request_key == request.request_key,
                )
            )
            if row is None:
                return None
            if (
                row.request_hash != self._wanted(request, fingerprint)
                or row.proposal_id != proposal.proposal_id
            ):
                raise ServiceError(
                    "workspace_tree_exchange_idempotency_conflict", "原树动作键已绑定其他请求", 409
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

    async def record_intent(
        self, scope: str, identifier: str, prepared: PreparedTreeExchange
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            await self._live(sql, row, proposal)
            if row.action == "keep":
                raise ServiceError(
                    "workspace_tree_exchange_state", "keep不能记录namespace intent", 409
                )
            if decode_exchange(row.prepared, row.payload) != prepared:
                raise ServiceError("workspace_tree_exchange_reference", "原intent材料不匹配", 409)
            row.intent_recorded, row.updated_at = True, utc_now()

    async def cancel(self, scope: str, identifier: str) -> StoredTreeExchange:
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
        effect: TreeExchangeEffect,
        *,
        resolving: bool = False,
    ) -> StoredTreeExchangeAttempt:
        raw = cast(dict[str, JsonValue], asdict(effect.receipt))
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            if row.action == "keep" and effect.receipt.status == "done":
                raw = {**raw, "changed": False}
            if row.status in {"done", "rejected"}:
                if (
                    row.receipt != raw
                    or self._effect_value(row, decode_exchange(row.prepared, row.payload)) != effect
                ):
                    raise ServiceError(
                        "workspace_tree_exchange_resolved", "原树操作终态不能改写", 409
                    )
                return await self._attempt_value(sql, row, proposal)
            if resolving:
                await self.files._scope(sql, scope, writing=True)
                if row.status != "uncertain":
                    raise ServiceError("workspace_tree_exchange_state", "仅原unknown允许核实", 409)
            elif (
                row.status != "running"
                or row.owner_id != self.owner_id
                or (row.lease_expires_at or 0) <= time.time()
            ):
                raise ServiceError(
                    "workspace_tree_exchange_fenced", "原回执未由当前producer持有", 409
                )
            if proposal.active_operation_id != row.operation_id or len(row.receipt_history) >= 16:
                raise _corrupt()
            prepared = decode_exchange(row.prepared, row.payload)
            await self._locks(sql, row, prepared)
            if (
                row.action in {"apply", "undo"}
                and effect.receipt.status in {"done", "uncertain"}
                and not row.intent_recorded
            ):
                raise ServiceError(
                    "workspace_tree_exchange_intent", "原交换效果须有已记录intent", 409
                )
            if row.action == "keep" and effect.receipt.status == "done":
                last = await sql.scalar(
                    select(WorkspaceTreeExchangeAttemptRow).where(
                        WorkspaceTreeExchangeAttemptRow.proposal_id == proposal.proposal_id,
                        WorkspaceTreeExchangeAttemptRow.action == "apply",
                        WorkspaceTreeExchangeAttemptRow.status == "done",
                    )
                )
                if (
                    last is None
                    or self._effect_value(last, self._proposal_value(proposal).prepared) != effect
                ):
                    raise ServiceError(
                        "workspace_tree_exchange_reference", "keep须原成功apply完整双after", 409
                    )
            after_metadata_raw, after_payload = encode_exchange_effect(prepared, effect)
            after_metadata = cast(dict[str, JsonValue], after_metadata_raw)
            candidate_history = [*row.receipt_history, raw]
            added = (
                len(json.dumps(raw))
                - (len(json.dumps(row.receipt)) if row.receipt is not None else 0)
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
                    delete(WorkspaceTreeExchangeLockRow).where(
                        WorkspaceTreeExchangeLockRow.operation_id == identifier
                    )
                )
            await sql.flush()
            return await self._attempt_value(sql, row, proposal)

    async def recover_interrupted(self) -> int:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            rows = list(
                await sql.scalars(
                    select(WorkspaceTreeExchangeAttemptRow).where(
                        WorkspaceTreeExchangeAttemptRow.status == "running",
                        WorkspaceTreeExchangeAttemptRow.lease_expires_at <= time.time(),
                    )
                )
            )
            for row in rows:
                proposal = await self._proposal(sql, row.scope_id, row.proposal_id)
                await self._attempt_value(sql, row, proposal)
                await self._expire(sql, row, proposal)
            return len(rows)

    async def history(self, scope: str, identifier: str) -> list[TreeExchangeAttemptView]:
        async with self.database.write_lock, self.database.sessions() as sql:
            proposal = await self._proposal(sql, scope, identifier)
            rows = await sql.scalars(
                select(WorkspaceTreeExchangeAttemptRow)
                .where(WorkspaceTreeExchangeAttemptRow.proposal_id == identifier)
                .order_by(
                    WorkspaceTreeExchangeAttemptRow.created_at,
                    WorkspaceTreeExchangeAttemptRow.operation_id,
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
    ) -> TreeExchangeAttemptPage:
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceError("workspace_tree_exchange_page", "原树历史页大小无效", 422)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            proposal = await self._proposal(sql, scope, identifier)
            query = select(WorkspaceTreeExchangeAttemptRow).where(
                WorkspaceTreeExchangeAttemptRow.proposal_id == identifier
            )
            if after is not None:
                cursor, _ = await self._operation(sql, scope, after)
                if cursor.proposal_id != identifier:
                    raise ServiceError(
                        "workspace_tree_exchange_page", "原树历史游标不属于该提案", 422
                    )
                query = query.where(WorkspaceTreeExchangeAttemptRow.operation_id > after)
            rows = list(
                await sql.scalars(
                    query.order_by(WorkspaceTreeExchangeAttemptRow.operation_id).limit(limit + 1)
                )
            )
            values = []
            for row in rows[:limit]:
                await self._expire(sql, row, proposal)
                values.append((await self._attempt_value(sql, row, proposal)).view)
            return TreeExchangeAttemptPage(
                items=values, next_after=rows[limit - 1].operation_id if len(rows) > limit else None
            )

    async def mark_uncertain(self, scope: str, identifier: str) -> StoredTreeExchangeAttempt:
        """回执持久失败时关闭原producer，保留材料/锁供同operation核实。"""
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row, proposal = await self._operation(sql, scope, identifier)
            if row.status != "running":
                return await self._attempt_value(sql, row, proposal)
            if row.owner_id != self.owner_id:
                raise ServiceError("workspace_tree_exchange_fenced", "未知收束不属于原owner", 409)
            row.lease_expires_at = time.time() - 1
            await self._expire(sql, row, proposal)
            return await self._attempt_value(sql, row, proposal)

    async def list(
        self, scope: str, *, after: str | None = None, limit: int = 32
    ) -> TreeExchangePage:
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceError("workspace_tree_exchange_page", "原树历史页大小无效", 422)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            query = select(WorkspaceTreeExchangeProposalRow).where(
                WorkspaceTreeExchangeProposalRow.scope_id == scope
            )
            if after is not None:
                await self._proposal(sql, scope, after)
                query = query.where(WorkspaceTreeExchangeProposalRow.proposal_id > after)
            rows = list(
                await sql.scalars(
                    query.order_by(WorkspaceTreeExchangeProposalRow.proposal_id).limit(limit + 1)
                )
            )
            items = []
            for row in rows[:limit]:
                row = await self._proposal(sql, scope, row.proposal_id)
                if row.active_operation_id is not None:
                    operation, _ = await self._operation(sql, scope, row.active_operation_id)
                    await self._expire(sql, operation, row)
                items.append(self._proposal_value(row).view)
            return TreeExchangePage(
                items=items, next_after=rows[limit - 1].proposal_id if len(rows) > limit else None
            )
