"""0036目录提案/attempt/CAS账本；同scope/owner/lease及原目录材料。"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import UTC
from typing import Literal, cast
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter
from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace_directory import (
    DirectoryAttemptView,
    DirectoryChangeAction,
    DirectoryChangeReceipt,
    DirectoryChangeSpec,
    DirectoryEffect,
    DirectoryMoveRestoreSpec,
    DirectoryPreviewView,
    DirectoryRecovery,
    DirectoryRestoreSpec,
    DirectorySnapshot,
    PreparedDirectoryChange,
)

from .database import (
    Database,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryLockRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceFileLockRow,
)
from .directory_fences import directory_key, entry_key
from .workspace_edits import WorkspaceEditStore, decode_prepared, digest


def encode(change: PreparedDirectoryChange) -> dict[str, JsonValue]:
    value: dict[str, JsonValue] = {
        "spec_json": change.spec_json,
        "workspace_fingerprint": change.workspace_fingerprint,
        "parent_identity": list(change.parent_identity),
        "before": asdict(change.before) if change.before is not None else None,
        "token": change.token,
        "restore_from": encode_recovery(change.restore_from)
        if change.restore_from is not None
        else None,
        "target_parent_identity": list(change.target_parent_identity)
        if change.target_parent_identity is not None
        else None,
        "target_before": asdict(change.target_before) if change.target_before is not None else None,
    }
    value = json.loads(json.dumps(value))
    value["checksum"] = digest(value)
    return value


def validate(change: PreparedDirectoryChange, *, original: bool = False) -> None:
    spec = change.spec
    if len(change.token) != 32 or any(c not in "0123456789abcdef" for c in change.token):
        raise ServiceError("workspace_directory_corrupt", "目录私有token无效", 409)
    if len(change.workspace_fingerprint) != 64 or any(
        c not in "0123456789abcdef" for c in change.workspace_fingerprint
    ):
        raise ServiceError("workspace_directory_corrupt", "目录工作区指纹无效", 409)
    if any(type(value) is not int or value < 0 for value in change.parent_identity):
        raise ServiceError("workspace_directory_corrupt", "目录父身份无效", 409)
    if isinstance(spec, DirectoryChangeSpec):
        if spec.operation == "create" and (
            change.before is not None or change.restore_from is not None
        ):
            raise ServiceError("workspace_directory_corrupt", "创建基线无效", 409)
        if spec.operation == "delete" and (
            change.before is None
            or not change.before.empty
            or spec.expected_fingerprint != change.before.fingerprint
            or change.restore_from is not None
        ):
            raise ServiceError("workspace_directory_corrupt", "删除基线无效", 409)
        if spec.operation == "move" and (
            change.before is None
            or not change.before.empty
            or spec.expected_fingerprint != change.before.fingerprint
            or spec.target_path is None
            or change.target_parent_identity is None
            or change.target_before is not None
            or change.restore_from is not None
        ):
            raise ServiceError("workspace_directory_corrupt", "移动基线无效", 409)
    elif isinstance(spec, DirectoryMoveRestoreSpec):
        if (
            change.before is None
            or not change.before.empty
            or spec.expected_fingerprint != change.before.fingerprint
            or change.target_parent_identity is None
            or change.target_before is not None
            or change.restore_from is not None
        ):
            raise ServiceError("workspace_directory_corrupt", "移动撤销基线无效", 409)
    elif (
        original
        or change.before is not None
        or change.restore_from is None
        or change.restore_from.expected is None
        or spec.mode != change.restore_from.expected.mode
    ):
        raise ServiceError("workspace_directory_corrupt", "内部恢复基线无效", 409)


def decode(value: dict[str, JsonValue]) -> PreparedDirectoryChange:
    raw = dict(value)
    checksum = raw.pop("checksum", None)
    if checksum != digest(raw):
        raise ServiceError("workspace_directory_corrupt", "目录私有快照摘要不匹配", 409)
    try:
        before = (
            TypeAdapter(DirectorySnapshot).validate_json(json.dumps(raw["before"]), strict=True)
            if raw["before"] is not None
            else None
        )
        recovery = (
            decode_recovery(cast(dict[str, JsonValue], raw["restore_from"]))
            if raw.get("restore_from") is not None
            else None
        )
        change = PreparedDirectoryChange(
            cast(str, raw["spec_json"]),
            cast(str, raw["workspace_fingerprint"]),
            TypeAdapter(tuple[int, int]).validate_json(
                json.dumps(raw["parent_identity"]), strict=True
            ),
            before,
            cast(str, raw["token"]),
            recovery,
            TypeAdapter(tuple[int, int]).validate_json(
                json.dumps(raw["target_parent_identity"]), strict=True
            )
            if raw.get("target_parent_identity") is not None
            else None,
            TypeAdapter(DirectorySnapshot).validate_json(
                json.dumps(raw["target_before"]), strict=True
            )
            if raw.get("target_before") is not None
            else None,
        )
        validate(change)
        return change
    except (KeyError, TypeError, ValueError):
        raise ServiceError("workspace_directory_corrupt", "目录私有快照结构无效", 409) from None


def encode_recovery(reference: DirectoryRecovery) -> dict[str, JsonValue]:
    value = json.loads(json.dumps(asdict(reference)))
    value["checksum"] = digest(value)
    return cast(dict[str, JsonValue], value)


def decode_recovery(value: dict[str, JsonValue]) -> DirectoryRecovery:
    raw = dict(value)
    checksum = raw.pop("checksum", None)
    if checksum != digest(raw):
        raise ServiceError("workspace_directory_corrupt", "目录恢复材料摘要不匹配", 409)
    return DirectoryRecovery(
        cast(str, raw["workspace_fingerprint"]),
        cast(str, raw["path"]),
        cast(str, raw["stage_name"]),
        TypeAdapter(tuple[int, int]).validate_json(json.dumps(raw["parent_identity"]), strict=True),
        TypeAdapter(DirectorySnapshot).validate_json(json.dumps(raw["expected"]), strict=True)
        if raw["expected"] is not None
        else None,
        cast(str | None, raw.get("target_path")) if raw.get("target_path") is not None else None,
        TypeAdapter(tuple[int, int]).validate_json(
            json.dumps(raw["target_parent_identity"]), strict=True
        )
        if raw.get("target_parent_identity") is not None
        else None,
    )


@dataclass(frozen=True)
class StoredDirectory:
    view: DirectoryPreviewView
    prepared: PreparedDirectoryChange


@dataclass(frozen=True)
class StoredDirectoryAttempt:
    view: DirectoryAttemptView
    prepared: PreparedDirectoryChange
    recovery: DirectoryRecovery | None


class WorkspaceDirectoryStore:
    def __init__(
        self,
        database: Database,
        *,
        owner_id: str | None = None,
        lease_seconds: float = 60,
        max_changes: int = 500,
    ) -> None:
        self.files = WorkspaceEditStore(
            database, owner_id=owner_id, lease_seconds=lease_seconds, max_changes=max_changes
        )
        self.database, self.owner_id = database, self.files.owner_id
        self.lease_seconds, self.max_changes = lease_seconds, max_changes

    @staticmethod
    def _view(row: WorkspaceDirectoryProposalRow) -> StoredDirectory:
        change = decode(row.prepared)
        spec = change.spec
        if not isinstance(spec, DirectoryChangeSpec):
            raise ServiceError("workspace_directory_corrupt", "目录提案不得使用内部恢复规格", 409)
        if row.path != spec.path:
            raise ServiceError("workspace_directory_corrupt", "目录提案路径与冻结快照不符", 409)
        return StoredDirectory(
            DirectoryPreviewView(
                proposal_id=row.proposal_id,
                scope_id=row.scope_id,
                workspace_key=row.workspace_key,
                resource_fingerprint=row.resource_fingerprint,
                path=spec.path,
                operation=spec.operation,
                target_path=spec.target_path,
                before_fingerprint=change.before.fingerprint if change.before else None,
                mode=spec.mode,
                revision=row.revision,
                status=cast(
                    Literal[
                        "proposed",
                        "applying",
                        "applied",
                        "undoing",
                        "undone",
                        "keeping",
                        "kept",
                        "rejected",
                        "uncertain",
                    ],
                    row.status,
                ),
                expires_at=row.expires_at,
            ),
            change,
        )

    @staticmethod
    def _attempt(row: WorkspaceDirectoryAttemptRow) -> StoredDirectoryAttempt:
        receipt = (
            TypeAdapter(DirectoryChangeReceipt).validate_json(json.dumps(row.receipt))
            if row.receipt is not None
            else None
        )
        return StoredDirectoryAttempt(
            DirectoryAttemptView(
                operation_id=row.operation_id,
                proposal_id=row.proposal_id,
                scope_id=row.scope_id,
                action=cast(Literal["apply", "undo", "keep"], row.action),
                expected_revision=row.expected_revision,
                status=cast(Literal["running", "done", "rejected", "uncertain"], row.status),
                receipt=receipt,
                recovery_required=row.status == "uncertain",
                cleanup_pending=row.cleanup_pending,
                created_at=row.created_at.replace(tzinfo=UTC).timestamp(),
                updated_at=row.updated_at.replace(tzinfo=UTC).timestamp(),
            ),
            decode(row.prepared),
            decode_recovery(row.recovery) if row.recovery else None,
        )

    async def preview(
        self,
        scope: str,
        workspace_key: str,
        fingerprint: str,
        key: str,
        change: PreparedDirectoryChange,
        *,
        ttl_seconds: float = 3600,
        run_id: str | None = None,
    ) -> tuple[StoredDirectory, bool]:
        self.files._key(key)
        validate(change, original=True)
        if type(change.spec) is not DirectoryChangeSpec or not 0 < ttl_seconds <= 86400:
            raise ServiceError("workspace_directory_proposal", "目录提案规格、资源或TTL无效", 422)
        if len(fingerprint) != 64 or any(c not in "0123456789abcdef" for c in fingerprint):
            raise ServiceError("workspace_binding_changed", "工作区资源指纹无效", 422)
        request_hash = digest(
            {
                "spec": change.spec.model_dump(mode="json"),
                "resource": fingerprint,
                "ttl": ttl_seconds,
            }
        )
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            topic = await self.files._scope(session, scope, writing=True, run_id=run_id)
            await self.files._resource(session, scope, workspace_key, fingerprint)
            previous = await session.scalar(
                select(WorkspaceDirectoryProposalRow).where(
                    WorkspaceDirectoryProposalRow.scope_id == scope,
                    WorkspaceDirectoryProposalRow.request_key == key,
                )
            )
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise ServiceError(
                        "workspace_idempotency_conflict", "目录提案键已绑定其他意图", 409
                    )
                return self._view(previous), False
            if (
                await session.scalar(
                    select(func.count()).select_from(WorkspaceDirectoryProposalRow)
                )
                or 0
            ) >= self.max_changes:
                raise ServiceError("workspace_directory_limit", "目录提案历史已达上限", 429)
            row = WorkspaceDirectoryProposalRow(
                proposal_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                request_key=key,
                request_hash=request_hash,
                resource_fingerprint=fingerprint,
                workspace_key=workspace_key,
                path=change.spec.path,
                prepared=encode(change),
                status="proposed",
                revision=0,
                expires_at=time.time() + ttl_seconds,
                active_operation_id=None,
                created_at=utc_now(),
                updated_at=utc_now(),
            )
            session.add(row)
            await session.flush()
            return self._view(row), True

    async def _proposal(
        self, session: AsyncSession, scope: str, identifier: str
    ) -> WorkspaceDirectoryProposalRow:
        row = await session.get(WorkspaceDirectoryProposalRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError("workspace_directory_not_found", "目录提案不在该作用域", 404)
        return row

    async def get(self, scope: str, identifier: str) -> StoredDirectory:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.files._scope(session, scope, writing=False)
            return self._view(await self._proposal(session, scope, identifier))

    async def find_preview(
        self,
        scope: str,
        workspace_key: str,
        fingerprint: str,
        key: str,
        spec: DirectoryChangeSpec,
        *,
        ttl_seconds: float = 3600,
    ) -> StoredDirectory | None:
        wanted = digest(
            {"spec": spec.model_dump(mode="json"), "resource": fingerprint, "ttl": ttl_seconds}
        )
        async with self.database.write_lock, self.database.sessions() as session:
            await self.files._scope(session, scope, writing=False)
            await self.files._resource(session, scope, workspace_key, fingerprint)
            row = await session.scalar(
                select(WorkspaceDirectoryProposalRow).where(
                    WorkspaceDirectoryProposalRow.scope_id == scope,
                    WorkspaceDirectoryProposalRow.request_key == key,
                )
            )
            if row is not None and row.request_hash != wanted:
                raise ServiceError(
                    "workspace_idempotency_conflict", "目录提案键已绑定其他意图", 409
                )
            return self._view(row) if row is not None else None

    async def find_action(
        self, scope: str, request: DirectoryChangeAction, fingerprint: str
    ) -> StoredDirectoryAttempt | None:
        wanted = digest({"request": request.model_dump(mode="json"), "resource": fingerprint})
        async with self.database.write_lock, self.database.sessions() as session:
            await self.files._scope(session, scope, writing=False)
            row = await session.scalar(
                select(WorkspaceDirectoryAttemptRow).where(
                    WorkspaceDirectoryAttemptRow.scope_id == scope,
                    WorkspaceDirectoryAttemptRow.request_key == request.request_key,
                )
            )
            if row is not None and row.request_hash != wanted:
                raise ServiceError(
                    "workspace_idempotency_conflict", "目录动作键已绑定其他意图", 409
                )
            return self._attempt(row) if row is not None else None

    async def history(self, scope: str, proposal_id: str) -> list[StoredDirectoryAttempt]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.files._scope(session, scope, writing=False)
            await self._proposal(session, scope, proposal_id)
            rows = await session.scalars(
                select(WorkspaceDirectoryAttemptRow)
                .where(WorkspaceDirectoryAttemptRow.proposal_id == proposal_id)
                .order_by(
                    WorkspaceDirectoryAttemptRow.created_at,
                    WorkspaceDirectoryAttemptRow.operation_id,
                )
            )
            return [self._attempt(row) for row in rows]

    async def proposals(
        self, scope: str, *, after: str | None = None, limit: int = 32
    ) -> tuple[list[StoredDirectory], str | None]:
        if not 1 <= limit <= 32:
            raise ServiceError("workspace_directory_page", "目录列表容量须为1–32", 422)
        async with self.database.write_lock, self.database.sessions() as session:
            await self.files._scope(session, scope, writing=False)
            query = (
                select(WorkspaceDirectoryProposalRow)
                .where(WorkspaceDirectoryProposalRow.scope_id == scope)
                .order_by(
                    WorkspaceDirectoryProposalRow.created_at,
                    WorkspaceDirectoryProposalRow.proposal_id,
                )
            )
            if after is not None:
                cursor = await self._proposal(session, scope, after)
                query = query.where(
                    or_(
                        WorkspaceDirectoryProposalRow.created_at > cursor.created_at,
                        and_(
                            WorkspaceDirectoryProposalRow.created_at == cursor.created_at,
                            WorkspaceDirectoryProposalRow.proposal_id > cursor.proposal_id,
                        ),
                    )
                )
            rows = list(await session.scalars(query.limit(limit + 1)))
            page = rows[:limit]
            return [self._view(row) for row in page], (
                page[-1].proposal_id if len(rows) > limit else None
            )

    async def history_page(
        self, scope: str, proposal_id: str, *, after: str | None = None, limit: int = 32
    ) -> tuple[list[StoredDirectoryAttempt], str | None]:
        if not 1 <= limit <= 32:
            raise ServiceError("workspace_directory_page", "目录历史容量须为1–32", 422)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            await self.files._scope(session, scope, writing=False)
            await self._proposal(session, scope, proposal_id)
            query = (
                select(WorkspaceDirectoryAttemptRow)
                .where(
                    WorkspaceDirectoryAttemptRow.scope_id == scope,
                    WorkspaceDirectoryAttemptRow.proposal_id == proposal_id,
                )
                .order_by(
                    WorkspaceDirectoryAttemptRow.created_at,
                    WorkspaceDirectoryAttemptRow.operation_id,
                )
            )
            if after is not None:
                cursor = await session.get(WorkspaceDirectoryAttemptRow, after)
                if cursor is None or cursor.scope_id != scope or cursor.proposal_id != proposal_id:
                    raise ServiceError(
                        "workspace_directory_operation_not_found", "目录历史游标不属于原提案", 404
                    )
                query = query.where(
                    or_(
                        WorkspaceDirectoryAttemptRow.created_at > cursor.created_at,
                        and_(
                            WorkspaceDirectoryAttemptRow.created_at == cursor.created_at,
                            WorkspaceDirectoryAttemptRow.operation_id > cursor.operation_id,
                        ),
                    )
                )
            rows = list(await session.scalars(query.limit(limit + 1)))
            page = rows[:limit]
            for row in page:
                await self._expire_attempt(session, row)
            return [self._attempt(row) for row in page], (
                page[-1].operation_id if len(rows) > limit else None
            )

    async def proposal_by_key(self, scope: str, key: str) -> StoredDirectory | None:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.files._scope(session, scope, writing=False)
            row = await session.scalar(
                select(WorkspaceDirectoryProposalRow).where(
                    WorkspaceDirectoryProposalRow.scope_id == scope,
                    WorkspaceDirectoryProposalRow.request_key == key,
                )
            )
            return self._view(row) if row is not None else None

    async def operation_by_key(self, scope: str, key: str) -> StoredDirectoryAttempt | None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            await self.files._scope(session, scope, writing=False)
            row = await session.scalar(
                select(WorkspaceDirectoryAttemptRow).where(
                    WorkspaceDirectoryAttemptRow.scope_id == scope,
                    WorkspaceDirectoryAttemptRow.request_key == key,
                )
            )
            if row is None:
                return None
            await self._expire_attempt(session, row)
            return self._attempt(row)

    async def _expire_attempt(
        self, session: AsyncSession, row: WorkspaceDirectoryAttemptRow
    ) -> None:
        if row.status == "running" and (row.lease_expires_at or 0) <= time.time():
            row.status, row.owner_id, row.lease_expires_at = "uncertain", None, None
            row.updated_at = utc_now()
            proposal = await self._proposal(session, row.scope_id, row.proposal_id)
            if proposal.active_operation_id == row.operation_id:
                proposal.status = "uncertain"

    async def effect(self, scope: str, identifier: str) -> DirectoryEffect:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.files._scope(session, scope, writing=False)
            row = await session.get(WorkspaceDirectoryAttemptRow, identifier)
            if row is None or row.scope_id != scope or row.status != "done" or row.receipt is None:
                raise ServiceError("workspace_directory_state", "目录操作尚无已完成事实", 409)
            receipt = TypeAdapter(DirectoryChangeReceipt).validate_json(
                json.dumps(row.receipt), strict=True
            )
            observed = (
                TypeAdapter(DirectorySnapshot).validate_json(
                    json.dumps(row.observed_after), strict=True
                )
                if row.observed_after is not None
                else None
            )
            if receipt.after_fingerprint != (observed.fingerprint if observed else None):
                raise ServiceError("workspace_directory_corrupt", "目录观察与回执不符", 409)
            return DirectoryEffect(
                receipt, observed, decode_recovery(row.recovery) if row.recovery else None
            )

    async def operation(self, scope: str, identifier: str) -> StoredDirectoryAttempt:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            await self.files._scope(session, scope, writing=False)
            row = await session.get(WorkspaceDirectoryAttemptRow, identifier)
            if row is None or row.scope_id != scope:
                raise ServiceError(
                    "workspace_directory_operation_not_found", "目录操作不在该作用域", 404
                )
            await self._expire_attempt(session, row)
            return self._attempt(row)

    async def _fence(self, session: AsyncSession, change: PreparedDirectoryChange) -> str:
        key = entry_key(change.parent_identity, change.spec.path)
        move_spec = change.spec
        move_target_path = (
            move_spec.target_path
            if isinstance(move_spec, DirectoryChangeSpec) and move_spec.operation == "move"
            else move_spec.target_path
            if isinstance(move_spec, DirectoryMoveRestoreSpec)
            else None
        )
        target_key = (
            entry_key(change.target_parent_identity, move_target_path)
            if move_target_path is not None and change.target_parent_identity is not None
            else None
        )
        keys = [key, target_key] if target_key is not None else [key]
        if await session.scalar(
            select(WorkspaceDirectoryLockRow.operation_id).where(
                (WorkspaceDirectoryLockRow.lock_key.in_(keys))
                | (WorkspaceDirectoryLockRow.target_lock_key.in_(keys))
            )
        ) or await session.scalar(
            select(WorkspaceFileLockRow.operation_id).where(WorkspaceFileLockRow.file_key.in_(keys))
        ):
            raise ServiceError("workspace_directory_busy", "目录移动entry有执行中或未核实操作", 409)
        parent_keys = [directory_key(change.parent_identity)]
        if change.target_parent_identity is not None:
            parent_keys.append(directory_key(change.target_parent_identity))
        if await session.scalar(
            select(WorkspaceDirectoryLockRow.operation_id).where(
                WorkspaceDirectoryLockRow.directory_key.in_(parent_keys)
            )
        ):
            raise ServiceError("workspace_directory_busy", "目录父级有执行中或未核实操作", 409)
        if change.before is not None:
            pending = (
                await session.scalars(
                    select(WorkspaceEditAttemptRow).join(
                        WorkspaceFileLockRow,
                        WorkspaceFileLockRow.operation_id == WorkspaceEditAttemptRow.operation_id,
                    )
                )
            ).all()
            for item in pending:
                prepared = decode_prepared(item.prepared, item.before_content, item.after_content)
                if prepared.parent_identity == change.before.identity:
                    raise ServiceError("workspace_file_busy", "目标目录内有未收束文件操作", 409)
        return key

    async def reserve(
        self,
        scope: str,
        request: DirectoryChangeAction,
        fingerprint: str,
        prepared: PreparedDirectoryChange,
        *,
        run_id: str | None = None,
        call_id: str | None = None,
    ) -> tuple[StoredDirectoryAttempt, bool]:
        self.files._key(request.request_key)
        validate(prepared)
        wanted_hash = digest({"request": request.model_dump(mode="json"), "resource": fingerprint})
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            await self.files._scope(session, scope, writing=True, run_id=run_id)
            previous = await session.scalar(
                select(WorkspaceDirectoryAttemptRow).where(
                    WorkspaceDirectoryAttemptRow.scope_id == scope,
                    WorkspaceDirectoryAttemptRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != wanted_hash:
                    raise ServiceError(
                        "workspace_idempotency_conflict", "目录动作键已绑定其他请求", 409
                    )
                return self._attempt(previous), False
            proposal = await self._proposal(session, scope, request.proposal_id)
            from .command_directory_proposals import assert_directory_source

            await assert_directory_source(session, proposal)
            original = decode(proposal.prepared)
            await self.files._resource(session, scope, proposal.workspace_key, fingerprint)
            expected_path = original.spec.path
            expected_parent = original.parent_identity
            if (
                request.action == "undo"
                and isinstance(original.spec, DirectoryChangeSpec)
                and original.spec.operation == "move"
            ):
                assert (
                    original.spec.target_path is not None
                    and original.target_parent_identity is not None
                )
                expected_path = original.spec.target_path
                expected_parent = original.target_parent_identity
            if (
                proposal.resource_fingerprint != fingerprint
                or prepared.workspace_fingerprint != original.workspace_fingerprint
                or prepared.spec.path != expected_path
                or prepared.parent_identity != expected_parent
            ):
                raise ServiceError("workspace_binding_changed", "目录动作不属于原资源/提案", 409)
            if proposal.revision != request.expected_revision:
                raise ServiceError("workspace_revision_conflict", "目录提案版本已变化", 409)
            allowed = {"apply": {"proposed"}, "undo": {"applied", "kept"}, "keep": {"applied"}}
            if (
                proposal.status not in allowed[request.action]
                or proposal.active_operation_id is not None
            ):
                raise ServiceError("workspace_directory_state", "目录提案状态不允许该动作", 409)
            if request.action == "apply" and (
                prepared != original or proposal.expires_at <= time.time()
            ):
                raise ServiceError("workspace_directory_proposal", "目录提案已过期或被替换", 409)
            if request.action == "keep" and prepared != original:
                raise ServiceError("workspace_directory_proposal", "保留须绑定原目录提案", 409)
            if request.action == "undo":
                source = await session.scalar(
                    select(WorkspaceDirectoryAttemptRow)
                    .where(
                        WorkspaceDirectoryAttemptRow.proposal_id == proposal.proposal_id,
                        WorkspaceDirectoryAttemptRow.action == "apply",
                        WorkspaceDirectoryAttemptRow.status == "done",
                    )
                    .order_by(WorkspaceDirectoryAttemptRow.created_at.desc())
                )
                if source is None:
                    raise ServiceError("workspace_directory_proposal", "撤销缺少原应用事实", 409)
                if original.spec.operation == "move":
                    if (
                        source.observed_after is None
                        or source.receipt is None
                        or not isinstance(prepared.spec, DirectoryMoveRestoreSpec)
                    ):
                        raise ServiceError(
                            "workspace_directory_proposal", "原移动缺少完整观察回执", 409
                        )
                    if prepared.spec.expected_fingerprint != source.receipt.get(
                        "after_fingerprint"
                    ):
                        raise ServiceError(
                            "workspace_directory_proposal", "原移动撤销目标不匹配", 409
                        )
                elif original.spec.operation == "create":
                    if source.observed_after is None or source.receipt is None:
                        raise ServiceError(
                            "workspace_directory_proposal", "原创建缺少完整观察回执", 409
                        )
                    observed = TypeAdapter(DirectorySnapshot).validate_json(
                        json.dumps(source.observed_after), strict=True
                    )
                    if source.receipt.get("after_fingerprint") != observed.fingerprint:
                        raise ServiceError(
                            "workspace_directory_proposal", "原创建观察与回执摘要不符", 409
                        )
                    if (
                        prepared.before != observed
                        or not isinstance(prepared.spec, DirectoryChangeSpec)
                        or prepared.spec.operation != "delete"
                    ):
                        raise ServiceError(
                            "workspace_directory_proposal", "撤销未绑定原创建identity", 409
                        )
                else:
                    if (
                        not isinstance(prepared.spec, DirectoryRestoreSpec)
                        or prepared.spec.original_token != original.token
                        or source.recovery is None
                        or prepared.restore_from != decode_recovery(source.recovery)
                    ):
                        raise ServiceError(
                            "workspace_directory_proposal", "撤销未绑定原删除材料", 409
                        )
            key = await self._fence(session, prepared)
            count = await session.scalar(
                select(func.count()).select_from(WorkspaceDirectoryAttemptRow)
            )
            if (count or 0) >= self.max_changes * 8:
                raise ServiceError("workspace_directory_limit", "目录动作历史已达上限", 429)
            identifier = str(uuid4())
            row = WorkspaceDirectoryAttemptRow(
                operation_id=identifier,
                proposal_id=proposal.proposal_id,
                scope_id=scope,
                request_key=request.request_key,
                request_hash=wanted_hash,
                action=request.action,
                expected_revision=request.expected_revision,
                previous_status=proposal.status,
                status="running",
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                prepared=encode(prepared),
                observed_after=None,
                receipt=None,
                recovery=None,
                cleanup_pending=False,
                run_id=run_id,
                call_id=call_id,
                receipt_history=[],
                created_at=utc_now(),
                updated_at=utc_now(),
            )
            session.add(row)
            await session.flush()
            prepared_spec = prepared.spec
            prepared_target_path = (
                prepared_spec.target_path
                if isinstance(prepared_spec, DirectoryChangeSpec)
                and prepared_spec.operation == "move"
                else prepared_spec.target_path
                if isinstance(prepared_spec, DirectoryMoveRestoreSpec)
                else None
            )
            session.add(
                WorkspaceDirectoryLockRow(
                    lock_key=key,
                    operation_id=identifier,
                    directory_key=directory_key(prepared.before.identity)
                    if prepared.before
                    else None,
                    target_lock_key=(
                        entry_key(prepared.target_parent_identity, prepared_target_path)
                        if prepared_target_path is not None
                        and prepared.target_parent_identity is not None
                        else None
                    ),
                )
            )
            proposal.active_operation_id = identifier
            proposal.status = {"apply": "applying", "undo": "undoing", "keep": "keeping"}[
                request.action
            ]
            proposal.revision += 1
            proposal.updated_at = utc_now()
            return self._attempt(row), True

    async def assert_execution(self, scope: str, identifier: str) -> None:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(WorkspaceDirectoryAttemptRow, identifier)
            if row is None or row.scope_id != scope:
                raise ServiceError("workspace_directory_operation_not_found", "目录操作不存在", 404)
            await self.files._scope(session, scope, writing=True, run_id=row.run_id)
            if (
                row.status != "running"
                or row.owner_id != self.owner_id
                or (row.lease_expires_at or 0) <= time.time()
            ):
                raise ServiceError("workspace_directory_lease_lost", "目录原生产者租约失效", 409)
            proposal = await self._proposal(session, scope, row.proposal_id)
            from .command_directory_proposals import assert_directory_source

            await assert_directory_source(session, proposal)
            if proposal.active_operation_id != identifier:
                raise ServiceError("workspace_directory_state", "目录发布资格已变化", 409)
            await self.files._resource(
                session, scope, proposal.workspace_key, proposal.resource_fingerprint
            )
            lock = await session.scalar(
                select(WorkspaceDirectoryLockRow).where(
                    WorkspaceDirectoryLockRow.operation_id == identifier
                )
            )
            prepared = decode(row.prepared)
            expected_lock = entry_key(prepared.parent_identity, prepared.spec.path)
            prepared_target_path = (
                prepared.spec.target_path
                if isinstance(prepared.spec, DirectoryChangeSpec)
                and prepared.spec.operation == "move"
                else prepared.spec.target_path
                if isinstance(prepared.spec, DirectoryMoveRestoreSpec)
                else None
            )
            expected_target = (
                entry_key(prepared.target_parent_identity, prepared_target_path)
                if prepared_target_path is not None and prepared.target_parent_identity is not None
                else None
            )
            if (
                lock is None
                or lock.lock_key != expected_lock
                or lock.target_lock_key != expected_target
            ):
                raise ServiceError("workspace_directory_lease_lost", "目录逻辑锁不匹配", 409)

    async def record_intent(
        self, scope: str, identifier: str, reference: DirectoryRecovery
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            row = await session.get(WorkspaceDirectoryAttemptRow, identifier)
            if (
                row is None
                or row.scope_id != scope
                or row.status != "running"
                or row.owner_id != self.owner_id
                or (row.lease_expires_at or 0) <= time.time()
            ):
                raise ServiceError("workspace_directory_lease_lost", "目录意图不属于原生产者", 409)
            prepared = decode(row.prepared)
            await self.files._scope(session, scope, writing=True, run_id=row.run_id)
            if (
                reference.path != prepared.spec.path
                or reference.parent_identity != prepared.parent_identity
                or reference.workspace_fingerprint != prepared.workspace_fingerprint
            ):
                raise ServiceError("workspace_directory_reference", "目录意图来源不匹配", 409)
            row.recovery = encode_recovery(reference)
            row.cleanup_pending = True
            row.updated_at = utc_now()
            lock = await session.scalar(
                select(WorkspaceDirectoryLockRow).where(
                    WorkspaceDirectoryLockRow.operation_id == identifier
                )
            )
            if lock is None:
                raise ServiceError("workspace_directory_lease_lost", "目录逻辑锁已丢失", 409)
            if reference.expected is not None:
                pending = (
                    await session.scalars(
                        select(WorkspaceEditAttemptRow).join(
                            WorkspaceFileLockRow,
                            WorkspaceFileLockRow.operation_id
                            == WorkspaceEditAttemptRow.operation_id,
                        )
                    )
                ).all()
                for item in pending:
                    frozen = decode_prepared(item.prepared, item.before_content, item.after_content)
                    if frozen.parent_identity == reference.expected.identity:
                        raise ServiceError("workspace_file_busy", "目录内有未核实文件操作", 409)
                lock.directory_key = directory_key(reference.expected.identity)

    async def renew(self, scope: str, identifier: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            result = await session.scalar(
                update(WorkspaceDirectoryAttemptRow)
                .where(
                    WorkspaceDirectoryAttemptRow.operation_id == identifier,
                    WorkspaceDirectoryAttemptRow.scope_id == scope,
                    WorkspaceDirectoryAttemptRow.status == "running",
                    WorkspaceDirectoryAttemptRow.owner_id == self.owner_id,
                    WorkspaceDirectoryAttemptRow.lease_expires_at > time.time(),
                )
                .values(lease_expires_at=time.time() + self.lease_seconds)
                .returning(WorkspaceDirectoryAttemptRow.operation_id)
            )
            if result is None:
                raise ServiceError("workspace_directory_lease_lost", "目录租约不能续期", 409)

    async def settle(
        self, scope: str, identifier: str, effect: DirectoryEffect, *, resolving: bool = False
    ) -> StoredDirectoryAttempt:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            row = await session.get(WorkspaceDirectoryAttemptRow, identifier)
            if row is None or row.scope_id != scope:
                raise ServiceError("workspace_directory_operation_not_found", "目录操作不存在", 404)
            await self.files._scope(session, scope, writing=False, run_id=row.run_id)
            prepared = decode(row.prepared)
            if (
                effect.receipt.operation_token != prepared.token
                or effect.receipt.path != prepared.spec.path
            ):
                raise ServiceError("workspace_directory_receipt", "目录回执未绑定原操作", 409)
            if effect.receipt.before_fingerprint != (
                prepared.before.fingerprint if prepared.before else None
            ) or effect.receipt.after_fingerprint != (
                effect.observed_after.fingerprint if effect.observed_after else None
            ):
                raise ServiceError("workspace_directory_receipt", "目录回执与完整观察不符", 409)
            TypeAdapter(DirectoryChangeReceipt).validate_json(
                json.dumps(asdict(effect.receipt)), strict=True
            )
            raw = cast(dict[str, JsonValue], json.loads(json.dumps(asdict(effect.receipt))))
            if row.status in {"done", "rejected"}:
                if row.receipt != raw:
                    raise ServiceError("workspace_directory_receipt", "原目录终态不可改写", 409)
                return self._attempt(row)
            if not resolving and row.owner_id != self.owner_id:
                raise ServiceError("workspace_directory_lease_lost", "目录回执不属于原owner", 409)
            if resolving and row.status != "uncertain":
                raise ServiceError("workspace_directory_state", "只有unknown可核实", 409)
            row.receipt_history = [*row.receipt_history, raw]
            row.receipt, row.status = raw, effect.receipt.status
            row.observed_after = (
                cast(dict[str, JsonValue], json.loads(json.dumps(asdict(effect.observed_after))))
                if effect.observed_after is not None
                else None
            )
            row.recovery = (
                encode_recovery(effect.recovery) if effect.recovery is not None else row.recovery
            )
            row.cleanup_pending = row.recovery is not None
            row.owner_id, row.lease_expires_at = None, None
            row.updated_at = utc_now()
            proposal = await self._proposal(session, scope, row.proposal_id)
            proposal.status = (
                {"apply": "applied", "undo": "undone", "keep": "kept"}[row.action]
                if row.status == "done"
                else row.previous_status
                if row.status == "rejected"
                else "uncertain"
            )
            proposal.revision += 1
            proposal.updated_at = utc_now()
            if row.status != "uncertain":
                proposal.active_operation_id = None
                await session.execute(
                    delete(WorkspaceDirectoryLockRow).where(
                        WorkspaceDirectoryLockRow.operation_id == identifier
                    )
                )
            return self._attempt(row)

    async def mark_cleaned(self, scope: str, identifier: str) -> StoredDirectoryAttempt:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.files._writer(session)
            row = await session.get(WorkspaceDirectoryAttemptRow, identifier)
            if row is None or row.scope_id != scope or row.status not in {"done", "rejected"}:
                raise ServiceError("workspace_directory_state", "未确认目录结果不得清理", 409)
            row.cleanup_pending = False
            row.updated_at = utc_now()
            return self._attempt(row)
