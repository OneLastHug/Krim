"""持久提案/副作用账本；SQL CAS与owner/租约，不执行任何文件动作。"""

from __future__ import annotations

import hashlib
import json
import re
import time
from builtins import list as builtin_list
from dataclasses import dataclass
from datetime import UTC
from typing import Literal, cast
from uuid import uuid4

from pydantic import Field, JsonValue, ValidationError
from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel, utc_now
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import (
    ArtifactFileSpec,
    FileChangePreview,
    FileChangeReceipt,
    FileChangeSpec,
    PreparedChangeMetadata,
    RecoveryMetadata,
    RestoreFileSpec,
    SnapshotMetadata,
    WorkspaceAttemptView,
    WorkspaceChangeAction,
    WorkspaceChangeView,
    WorkspaceFileState,
    WorkspaceOperationProofView,
)
from krim_agent.workspace_list import NativeBatchFence
from krim_agent.workspace_restore import CheckpointRestoreFence
from krim_agent.workspace_writer import FileRecovery, FileSnapshot, PreparedFileChange

from .agent_sessions import assert_session_open, assert_session_owner
from .command_proposals import assert_command_proposal
from .database import (
    AgentSessionRow,
    Database,
    RunRow,
    TopicRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceFileEffectRow,
    WorkspaceFileLockRow,
    WorkspaceLedgerGuardRow,
)
from .workspace_batches import assert_batch_execution, assert_batch_request
from .workspace_restores import assert_restore_execution, assert_restore_request

ChangeStatus = Literal[
    "proposed",
    "applying",
    "applied",
    "undoing",
    "undone",
    "keeping",
    "kept",
    "rejected",
    "uncertain",
]
AttemptStatus = Literal["running", "done", "rejected", "uncertain"]
Action = Literal["apply", "undo", "keep"]


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def snapshot_metadata(snapshot: FileSnapshot | None) -> SnapshotMetadata | None:
    if snapshot is None:
        return None
    return SnapshotMetadata(
        mode=snapshot.mode,
        device=snapshot.device,
        inode=snapshot.inode,
        mtime_ns=snapshot.mtime_ns,
        ctime_ns=snapshot.ctime_ns,
        uid=snapshot.uid,
        gid=snapshot.gid,
    )


def snapshot_hash(metadata: dict[str, JsonValue], before: bytes | None, after: bytes | None) -> str:
    return digest(
        {
            "metadata": metadata,
            "before": hashlib.sha256(before).hexdigest() if before is not None else None,
            "after": hashlib.sha256(after).hexdigest() if after is not None else None,
        }
    )


def encode_prepared(prepared: PreparedFileChange) -> dict[str, JsonValue]:
    value = PreparedChangeMetadata(
        spec_json=prepared.spec_json,
        workspace_fingerprint=prepared.workspace_fingerprint,
        parent_device=prepared.parent_identity[0],
        parent_inode=prepared.parent_identity[1],
        token=prepared.token,
        before=snapshot_metadata(prepared.before),
        snapshot_hash="0" * 64,
    ).model_dump(mode="json")
    value["snapshot_hash"] = snapshot_hash(
        {k: v for k, v in value.items() if k != "snapshot_hash"},
        prepared.before.data if prepared.before is not None else None,
        prepared.after,
    )
    return value


def decode_prepared(
    value: dict[str, JsonValue], before: bytes | None, after: bytes | None
) -> PreparedFileChange:
    metadata = PreparedChangeMetadata.model_validate(value)
    if metadata.snapshot_hash != snapshot_hash(
        {k: v for k, v in value.items() if k != "snapshot_hash"},
        before,
        after,
    ):
        raise ServiceError("workspace_snapshot_corrupt", "修改私有快照校验失败", 409)
    if (metadata.before is None) != (before is None):
        raise ServiceError("workspace_snapshot_corrupt", "修改旧内容与身份不匹配", 409)
    old = None
    if metadata.before is not None and before is not None:
        identity = metadata.before
        old = FileSnapshot(
            before,
            identity.mode,
            identity.device,
            identity.inode,
            identity.mtime_ns,
            identity.ctime_ns,
            identity.uid,
            identity.gid,
        )
    return PreparedFileChange(
        metadata.spec_json,
        metadata.workspace_fingerprint,
        (metadata.parent_device, metadata.parent_inode),
        old,
        after,
        metadata.token,
    )


def file_key(prepared: PreparedFileChange) -> str:
    return digest(
        {"parent": list(prepared.parent_identity), "name": prepared.spec.path.split("/")[-1]}
    )


@dataclass(frozen=True)
class StoredWorkspaceChange:
    view: WorkspaceChangeView
    prepared: PreparedFileChange


@dataclass(frozen=True)
class StoredWorkspaceAttempt:
    view: WorkspaceAttemptView
    prepared: PreparedFileChange
    owner_id: str | None
    lease_expires_at: float | None
    recovery: FileRecovery | None


class _KeepNode(StrictModel):
    proposal_id: str
    operation_id: str
    revision: int = Field(ge=0)
    effect_sequence: int = Field(ge=1, le=9007199254740991)
    snapshot_hash: str
    observed_after: WorkspaceFileState


class _KeepWitness(StrictModel):
    version: Literal[1] = 1
    scope_id: str
    resource_fingerprint: str
    file_key: str
    nodes: list[_KeepNode] = Field(min_length=2, max_length=5000)
    witness_hash: str


@dataclass(frozen=True)
class _EffectNode:
    effect: WorkspaceFileEffectRow
    attempt: WorkspaceEditAttemptRow
    proposal: WorkspaceEditProposalRow
    prepared: PreparedFileChange
    observed: WorkspaceFileState


def _before_state(prepared: PreparedFileChange) -> WorkspaceFileState:
    before = prepared.before
    return WorkspaceFileState(
        sha256=prepared.before_sha256,
        mode=before.mode if before else None,
        uid=before.uid if before else None,
        gid=before.gid if before else None,
    )


def _same_binding(left: _EffectNode, right: _EffectNode) -> bool:
    return (
        left.proposal.scope_id == right.proposal.scope_id
        and left.proposal.workspace_key == right.proposal.workspace_key
        and left.proposal.resource_fingerprint == right.proposal.resource_fingerprint
        and left.prepared.workspace_fingerprint == right.prepared.workspace_fingerprint
        and left.prepared.parent_identity == right.prepared.parent_identity
        and left.prepared.spec.path == right.prepared.spec.path
    )


class WorkspaceEditStore:
    def __init__(
        self,
        database: Database,
        *,
        owner_id: str | None = None,
        lease_seconds: float = 60,
        max_changes: int = 500,
        max_snapshot_bytes: int = 67_108_864,
    ) -> None:
        if not 0.2 <= lease_seconds <= 600 or not 1 <= max_changes <= 5000:
            raise ValueError("修改账本租约或容量无效")
        if not 1024 <= max_snapshot_bytes <= 536_870_912:
            raise ValueError("修改快照容量无效")
        self.database = database
        self.owner_id = owner_id or str(uuid4())
        self.lease_seconds, self.max_changes = lease_seconds, max_changes
        self.max_snapshot_bytes = max_snapshot_bytes

    async def _writer(self, session: AsyncSession) -> None:
        # 首条SQL写语句取得SQLite写锁；跨连接不能依赖asyncio锁。
        await session.execute(
            insert(WorkspaceLedgerGuardRow)
            .values(guard_id="ledger", updated_at=utc_now())
            .on_conflict_do_nothing()
        )
        await session.execute(
            update(WorkspaceLedgerGuardRow)
            .where(WorkspaceLedgerGuardRow.guard_id == "ledger")
            .values(updated_at=utc_now())
        )

    async def _scope(
        self, session: AsyncSession, scope_id: str, *, writing: bool, run_id: str | None = None
    ) -> str:
        if scope_id.startswith("session:"):
            row = await session.get(AgentSessionRow, scope_id.removeprefix("session:"))
            if row is None:
                raise ServiceError("agent_session_not_found", "修改会话不存在", 404)
            if writing:
                await assert_session_owner(session, row, self.owner_id)
                await assert_session_open(session, row)
            topic_id = row.topic_id
        elif scope_id.startswith("topic:"):
            topic_id = scope_id.removeprefix("topic:")
            if await session.get(TopicRow, topic_id) is None:
                raise ServiceError("topic_not_found", "修改话题不存在", 404)
            if (
                await session.scalar(
                    select(AgentSessionRow.session_id).where(AgentSessionRow.topic_id == topic_id)
                )
                is not None
            ):
                raise ServiceError("workspace_scope_managed", "Agent修改须使用自身会话作用域", 403)
        else:
            raise ServiceError("workspace_scope_invalid", "修改作用域无效", 422)
        if run_id is not None:
            run = await session.get(RunRow, run_id)
            if run is None or run.topic_id != topic_id:
                raise ServiceError("workspace_run_mismatch", "修改Run不属于该作用域", 403)
            if writing and (run.status not in {"queued", "running"} or run.stop_requested):
                raise ServiceError("workspace_run_closed", "修改所属Run已关闭", 409)
        return topic_id

    async def _resource(
        self,
        session: AsyncSession,
        scope_id: str,
        workspace_key: str,
        resource_fingerprint: str,
    ) -> None:
        if (
            re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", workspace_key) is None
            or re.fullmatch(r"[0-9a-f]{64}", resource_fingerprint) is None
        ):
            raise ServiceError("workspace_binding_changed", "工作区公开资源标识无效", 422)
        if scope_id.startswith("session:"):
            row = await session.get(AgentSessionRow, scope_id.removeprefix("session:"))
            assert row is not None
            manifest = row.permissions.get("workspace")
            if (
                not isinstance(manifest, dict)
                or manifest.get("key") != workspace_key
                or manifest.get("fingerprint") != resource_fingerprint
            ):
                raise ServiceError("workspace_binding_changed", "会话未绑定此工作区资源版本", 403)

    @staticmethod
    def _validate_prepared(prepared: PreparedFileChange, *, original_intent: bool) -> None:
        spec = prepared.spec
        if not Workspace._parts(spec.path):
            raise ServiceError("workspace_path", "修改须指定文件路径", 422)
        old = prepared.before.data if prepared.before is not None else None
        if spec.expected_sha256 != prepared.before_sha256:
            raise ServiceError("workspace_snapshot_corrupt", "原版本与快照不一致", 409)
        if isinstance(spec, ArtifactFileSpec) and (
            (spec.mode is None) != (prepared.after is None)
            or (prepared.before is not None and prepared.before.mode & ~0o777)
        ):
            raise ServiceError("execution_artifact_mode", "产物模式与快照不匹配", 409)
        for content in (old, prepared.after):
            if content is not None:
                if len(content) > 8_388_608 or b"\0" in content:
                    raise ServiceError("workspace_snapshot_invalid", "私有快照类型或大小无效", 422)
                try:
                    content.decode("utf-8")
                except UnicodeDecodeError:
                    raise ServiceError("workspace_encoding", "私有快照须为UTF-8", 422) from None
        if original_intent:
            if not isinstance(spec, FileChangeSpec):
                raise ServiceError(
                    "workspace_snapshot_invalid", "内部恢复规格不可作为公开提案", 422
                )
            desired: bytes | None = None
            if spec.operation == "write":
                assert spec.content is not None
                desired = spec.content.encode("utf-8")
            elif spec.operation == "replace":
                if old is None:
                    raise ServiceError("workspace_snapshot_corrupt", "替换缺少原快照", 409)
                text = old.decode("utf-8")
                for replacement in spec.replacements:
                    if text.count(replacement.old_text) != replacement.occurrences:
                        raise ServiceError("workspace_replace_conflict", "原文替换次数不符", 409)
                    text = text.replace(replacement.old_text, replacement.new_text)
                desired = text.encode("utf-8")
            if desired != prepared.after:
                raise ServiceError("workspace_snapshot_corrupt", "新快照与冻结意图不一致", 409)

    async def _bytes_available(self, session: AsyncSession, added: int) -> None:
        total = 0
        for model in (WorkspaceEditProposalRow, WorkspaceEditAttemptRow):
            used = await session.scalar(
                select(
                    func.coalesce(
                        func.sum(
                            func.coalesce(func.length(model.before_content), 0)
                            + func.coalesce(func.length(model.after_content), 0)
                        ),
                        0,
                    )
                )
            )
            total += int(used or 0)
        witness_bytes = await session.scalar(
            select(func.coalesce(func.sum(func.length(WorkspaceEditAttemptRow.keep_witness)), 0))
        )
        total += int(witness_bytes or 0)
        if total + added > self.max_snapshot_bytes:
            raise ServiceError("workspace_snapshot_limit", "修改私有快照容量已达上限", 429)

    @staticmethod
    def _key(request_key: str) -> None:
        if not request_key or len(request_key) > 128 or any(ord(c) < 32 for c in request_key):
            raise ServiceError("workspace_request_key", "修改幂等键无效", 422)

    @staticmethod
    def _change(row: WorkspaceEditProposalRow) -> StoredWorkspaceChange:
        prepared = decode_prepared(row.prepared, row.before_content, row.after_content)
        return StoredWorkspaceChange(
            WorkspaceChangeView(
                proposal_id=row.proposal_id,
                scope_id=row.scope_id,
                topic_id=row.topic_id,
                workspace_key=row.workspace_key,
                resource_fingerprint=row.resource_fingerprint,
                revision=row.revision,
                status=cast(ChangeStatus, row.status),
                preview=FileChangePreview.model_validate(row.preview),
                active_operation_id=row.active_operation_id,
                expires_at=row.expires_at,
                created_at=row.created_at.replace(tzinfo=UTC),
                updated_at=row.updated_at.replace(tzinfo=UTC),
            ),
            prepared,
        )

    @staticmethod
    def _attempt(row: WorkspaceEditAttemptRow) -> StoredWorkspaceAttempt:
        recovery = None
        if row.recovery is not None:
            value = RecoveryMetadata.model_validate(row.recovery)
            recovery = FileRecovery(
                value.workspace_fingerprint,
                value.path,
                value.stage_name,
                (value.parent_device, value.parent_inode),
            )
        return StoredWorkspaceAttempt(
            WorkspaceAttemptView(
                operation_id=row.operation_id,
                proposal_id=row.proposal_id,
                scope_id=row.scope_id,
                action=cast(Action, row.action),
                expected_revision=row.expected_revision,
                status=cast(AttemptStatus, row.status),
                receipt=FileChangeReceipt.model_validate(row.receipt) if row.receipt else None,
                cleanup_pending=row.cleanup_pending,
                run_id=row.run_id,
                call_id=row.call_id,
                created_at=row.created_at.replace(tzinfo=UTC),
                updated_at=row.updated_at.replace(tzinfo=UTC),
            ),
            decode_prepared(row.prepared, row.before_content, row.after_content),
            row.owner_id,
            row.lease_expires_at,
            recovery,
        )

    async def _proposal(
        self, session: AsyncSession, scope_id: str, proposal_id: str
    ) -> WorkspaceEditProposalRow:
        row = await session.get(WorkspaceEditProposalRow, proposal_id)
        if row is None or row.scope_id != scope_id:
            raise ServiceError("workspace_proposal_not_found", "修改提案不在该作用域中", 404)
        return row

    async def find_preview(
        self, scope_id: str, request_key: str, spec: FileChangeSpec, resource_fingerprint: str
    ) -> StoredWorkspaceChange | None:
        self._key(request_key)
        request_hash = digest(
            {"spec": spec.model_dump(mode="json"), "resource": resource_fingerprint}
        )
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=True)
            row = await session.scalar(
                select(WorkspaceEditProposalRow).where(
                    WorkspaceEditProposalRow.scope_id == scope_id,
                    WorkspaceEditProposalRow.request_key == request_key,
                )
            )
            if row is not None and row.request_hash != request_hash:
                raise ServiceError(
                    "workspace_idempotency_conflict", "提案键已绑定其他请求或资源", 409
                )
            if row is not None:
                await self._resource(session, scope_id, row.workspace_key, resource_fingerprint)
            return self._change(row) if row is not None else None

    async def preview(
        self,
        scope_id: str,
        workspace_key: str,
        resource_fingerprint: str,
        request_key: str,
        prepared: PreparedFileChange,
        *,
        ttl_seconds: float = 3600,
        run_id: str | None = None,
    ) -> tuple[StoredWorkspaceChange, bool]:
        self._key(request_key)
        if not 0 < ttl_seconds <= 86400:
            raise ServiceError("workspace_proposal_ttl", "修改提案有效期无效", 422)
        spec = prepared.spec
        self._validate_prepared(prepared, original_intent=True)
        request_hash = digest(
            {"spec": spec.model_dump(mode="json"), "resource": resource_fingerprint}
        )
        metadata = encode_prepared(prepared)
        before = prepared.before.data if prepared.before is not None else None
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            topic_id = await self._scope(session, scope_id, writing=True, run_id=run_id)
            await self._resource(session, scope_id, workspace_key, resource_fingerprint)
            previous = await session.scalar(
                select(WorkspaceEditProposalRow).where(
                    WorkspaceEditProposalRow.scope_id == scope_id,
                    WorkspaceEditProposalRow.request_key == request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise ServiceError(
                        "workspace_idempotency_conflict", "提案键已绑定其他请求或资源", 409
                    )
                return self._change(previous), False
            count = await session.scalar(select(func.count()).select_from(WorkspaceEditProposalRow))
            if (count or 0) >= self.max_changes:
                raise ServiceError("workspace_proposal_limit", "修改提案历史已达上限", 429)
            await self._bytes_available(session, len(before or b"") + len(prepared.after or b""))
            row = WorkspaceEditProposalRow(
                proposal_id=str(uuid4()),
                scope_id=scope_id,
                topic_id=topic_id,
                request_key=request_key,
                request_hash=request_hash,
                resource_fingerprint=resource_fingerprint,
                workspace_key=workspace_key,
                path=spec.path,
                prepared=metadata,
                preview=prepared.preview().model_dump(mode="json"),
                before_content=before,
                after_content=prepared.after,
                status="proposed",
                revision=0,
                expires_at=time.time() + ttl_seconds,
            )
            session.add(row)
            await session.flush()
            return self._change(row), True

    async def get(self, scope_id: str, proposal_id: str) -> StoredWorkspaceChange:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=False)
            return self._change(await self._proposal(session, scope_id, proposal_id))

    async def list(
        self, scope_id: str, *, offset: int = 0, limit: int = 50
    ) -> list[WorkspaceChangeView]:
        if offset < 0 or not 1 <= limit <= 100:
            raise ServiceError("workspace_pagination", "修改提案分页无效", 422)
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=False)
            rows = await session.scalars(
                select(WorkspaceEditProposalRow)
                .where(WorkspaceEditProposalRow.scope_id == scope_id)
                .order_by(
                    WorkspaceEditProposalRow.created_at.desc(), WorkspaceEditProposalRow.proposal_id
                )
                .offset(offset)
                .limit(limit)
            )
            return [self._change(row).view for row in rows]

    async def find_action(
        self,
        scope_id: str,
        request: WorkspaceChangeAction,
        resource_fingerprint: str,
        *,
        batch_fence: NativeBatchFence | None = None,
        restore_fence: CheckpointRestoreFence | None = None,
    ) -> StoredWorkspaceAttempt | None:
        value = digest(
            {"request": request.model_dump(mode="json"), "resource": resource_fingerprint}
        )
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=True)
            row = await session.scalar(
                select(WorkspaceEditAttemptRow).where(
                    WorkspaceEditAttemptRow.scope_id == scope_id,
                    WorkspaceEditAttemptRow.request_key == request.request_key,
                )
            )
            if row is not None and row.request_hash != value:
                raise ServiceError(
                    "workspace_idempotency_conflict", "动作键已绑定其他请求或资源", 409
                )
            await assert_batch_request(
                session,
                self.owner_id,
                scope_id,
                request,
                resource_fingerprint,
                batch_fence,
                row.operation_id if row is not None else None,
            )
            await assert_restore_request(
                session,
                self.owner_id,
                scope_id,
                request,
                resource_fingerprint,
                restore_fence,
                row.operation_id if row is not None else None,
            )
            if row is not None:
                proposal = await self._proposal(session, scope_id, row.proposal_id)
                await self._resource(
                    session, scope_id, proposal.workspace_key, resource_fingerprint
                )
            return self._attempt(row) if row is not None else None

    @staticmethod
    def _effect_node(
        effect: WorkspaceFileEffectRow,
        attempt: WorkspaceEditAttemptRow,
        proposal: WorkspaceEditProposalRow,
    ) -> _EffectNode | None:
        prepared = decode_prepared(attempt.prepared, attempt.before_content, attempt.after_content)
        if (
            effect.operation_id != attempt.operation_id
            or effect.proposal_id != attempt.proposal_id
            or effect.proposal_id != proposal.proposal_id
            or effect.scope_id != attempt.scope_id
            or effect.scope_id != proposal.scope_id
            or effect.resource_fingerprint != proposal.resource_fingerprint
            or effect.file_key != file_key(prepared)
            or effect.action != attempt.action
            or attempt.status != "done"
            or effect.action not in {"apply", "undo"}
        ):
            raise ServiceError("workspace_effect_corrupt", "文件效果引用校验失败", 409)
        if effect.observed_after is None:
            # 旧低层调用仍写入顺序断点，但无实测属性时不能作为连续性证明。
            return None
        try:
            observed = WorkspaceFileState.model_validate(effect.observed_after)
        except ValidationError:
            raise ServiceError("workspace_effect_corrupt", "文件效果状态校验失败", 409) from None
        if observed.sha256 != prepared.after_sha256:
            raise ServiceError("workspace_effect_corrupt", "文件效果版本校验失败", 409)
        if effect.action == "apply" and prepared != decode_prepared(
            proposal.prepared, proposal.before_content, proposal.after_content
        ):
            raise ServiceError("workspace_effect_corrupt", "文件效果未绑定原提案快照", 409)
        return _EffectNode(effect, attempt, proposal, prepared, observed)

    async def _freeze_keep(
        self, session: AsyncSession, proposal: WorkspaceEditProposalRow, lock_id: str
    ) -> dict[str, JsonValue] | None:
        rows = await session.execute(
            select(WorkspaceFileEffectRow, WorkspaceEditAttemptRow, WorkspaceEditProposalRow)
            .join(
                WorkspaceEditAttemptRow,
                WorkspaceEditAttemptRow.operation_id == WorkspaceFileEffectRow.operation_id,
            )
            .join(
                WorkspaceEditProposalRow,
                WorkspaceEditProposalRow.proposal_id == WorkspaceFileEffectRow.proposal_id,
            )
            .where(WorkspaceFileEffectRow.file_key == lock_id)
            .order_by(WorkspaceFileEffectRow.effect_sequence)
        )
        stack: list[_EffectNode] = []
        for effect, attempt, source in rows:
            node = self._effect_node(effect, attempt, source)
            if node is None:
                stack.clear()
                continue
            # 重放历史事件时不按提案当前undone状态过滤，undo事件才负责弹栈。
            if effect.action == "apply":
                if stack and (
                    not _same_binding(stack[-1], node)
                    or stack[-1].observed != _before_state(node.prepared)
                ):
                    stack.clear()
                stack.append(node)
            elif (
                stack
                and stack[-1].proposal.proposal_id == source.proposal_id
                and _same_binding(stack[-1], node)
                and stack[-1].observed == _before_state(node.prepared)
            ):
                stack.pop()
                # 恢复可能剥离特殊权限位；不能把目标内容相同当作属性也相同。
                if stack and stack[-1].observed != node.observed:
                    stack.clear()
            else:
                stack.clear()
        start = next(
            (
                index
                for index, node in enumerate(stack)
                if node.proposal.proposal_id == proposal.proposal_id
            ),
            None,
        )
        if start is None or start == len(stack) - 1:
            return None
        chain = stack[start:]
        if any(
            node.proposal.scope_id != proposal.scope_id
            or node.proposal.resource_fingerprint != proposal.resource_fingerprint
            or node.proposal.workspace_key != proposal.workspace_key
            or node.proposal.status not in {"applied", "kept"}
            or node.proposal.active_operation_id is not None
            for node in chain
        ):
            return None
        witness = _KeepWitness(
            scope_id=proposal.scope_id,
            resource_fingerprint=proposal.resource_fingerprint,
            file_key=lock_id,
            nodes=[
                _KeepNode(
                    proposal_id=node.proposal.proposal_id,
                    operation_id=node.attempt.operation_id,
                    revision=node.proposal.revision,
                    effect_sequence=node.effect.effect_sequence,
                    snapshot_hash=str(node.attempt.prepared["snapshot_hash"]),
                    observed_after=node.observed,
                )
                for node in chain
            ],
            witness_hash="",
        )
        value = witness.model_dump(mode="json")
        value["witness_hash"] = digest(
            {key: item for key, item in value.items() if key != "witness_hash"}
        )
        return value

    async def _keep_target(
        self, session: AsyncSession, row: WorkspaceEditAttemptRow
    ) -> tuple[PreparedFileChange, WorkspaceFileState | None, _KeepWitness | None]:
        original = self._attempt(row).prepared
        if row.keep_witness is None:
            return original, None, None
        try:
            witness = _KeepWitness.model_validate(row.keep_witness)
        except ValidationError:
            raise ServiceError("workspace_keep_witness_corrupt", "保留见证校验失败", 409) from None
        proposal = await self._proposal(session, row.scope_id, row.proposal_id)
        if (
            row.action != "keep"
            or witness.witness_hash
            != digest(
                {key: item for key, item in row.keep_witness.items() if key != "witness_hash"}
            )
            or witness.scope_id != row.scope_id
            or witness.resource_fingerprint != proposal.resource_fingerprint
            or witness.file_key != file_key(original)
            or witness.nodes[0].proposal_id != row.proposal_id
            or witness.nodes[0].revision != row.expected_revision
        ):
            raise ServiceError("workspace_keep_witness_corrupt", "保留见证绑定校验失败", 409)
        previous: _EffectNode | None = None
        for frozen in witness.nodes:
            effect = await session.get(WorkspaceFileEffectRow, frozen.effect_sequence)
            attempt = await session.get(WorkspaceEditAttemptRow, frozen.operation_id)
            source = await session.get(WorkspaceEditProposalRow, frozen.proposal_id)
            if effect is None or attempt is None or source is None:
                raise ServiceError("workspace_keep_witness_corrupt", "保留见证引用丢失", 409)
            node = self._effect_node(effect, attempt, source)
            if (
                node is None
                or effect.action != "apply"
                or effect.scope_id != row.scope_id
                or effect.file_key != witness.file_key
                or effect.resource_fingerprint != witness.resource_fingerprint
                or source.workspace_key != proposal.workspace_key
                or attempt.prepared["snapshot_hash"] != frozen.snapshot_hash
                or node.observed != frozen.observed_after
                or frozen.revision < attempt.expected_revision + 2
                or (
                    previous is not None
                    and (
                        previous.effect.effect_sequence >= effect.effect_sequence
                        or not _same_binding(previous, node)
                        or previous.observed != _before_state(node.prepared)
                    )
                )
            ):
                raise ServiceError("workspace_keep_witness_corrupt", "保留见证连续性校验失败", 409)
            previous = node
        assert previous is not None
        # 只读冻结引用；不按当前revision/status或新效果重选链尾，历史证明保持原意。
        return previous.prepared, previous.observed, witness

    async def keep_target(
        self, scope_id: str, operation_id: str
    ) -> tuple[PreparedFileChange, WorkspaceFileState | None]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=False)
            row = await session.get(WorkspaceEditAttemptRow, operation_id)
            if row is None or row.scope_id != scope_id:
                raise ServiceError("workspace_operation_not_found", "修改操作不在该作用域中", 404)
            if row.action != "keep":
                raise ServiceError("workspace_change_state", "只有保留动作可查询核实目标", 409)
            prepared, observed, _ = await self._keep_target(session, row)
            return prepared, observed

    async def operation_proof(
        self, scope_id: str, operation_id: str
    ) -> WorkspaceOperationProofView:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=False)
            row = await session.get(WorkspaceEditAttemptRow, operation_id)
            if row is None or row.scope_id != scope_id:
                raise ServiceError("workspace_operation_not_found", "修改操作不在该作用域中", 404)
            tip_id = tip_operation = after = None
            sequence = None
            count = 0
            kind: Literal["not_applicable", "exact_after", "successor_chain"] = "not_applicable"
            if row.action == "keep":
                prepared, _observed, witness = await self._keep_target(session, row)
                kind = "exact_after" if witness is None else "successor_chain"
                tip_id = row.proposal_id
                after = prepared.after_sha256
                if witness is not None:
                    tip = witness.nodes[-1]
                    tip_id, tip_operation = tip.proposal_id, tip.operation_id
                    sequence, count = tip.effect_sequence, len(witness.nodes) - 1
            return WorkspaceOperationProofView(
                operation_id=row.operation_id,
                proposal_id=row.proposal_id,
                scope_id=row.scope_id,
                status=cast(AttemptStatus, row.status),
                kind=kind,
                tip_proposal_id=tip_id,
                tip_operation_id=tip_operation,
                tip_after_sha256=after,
                effect_sequence=sequence,
                successor_count=count,
            )

    async def reserve(
        self,
        scope_id: str,
        request: WorkspaceChangeAction,
        resource_fingerprint: str,
        prepared: PreparedFileChange,
        *,
        run_id: str | None = None,
        call_id: str | None = None,
        batch_fence: NativeBatchFence | None = None,
        restore_fence: CheckpointRestoreFence | None = None,
    ) -> tuple[StoredWorkspaceAttempt, bool]:
        self._key(request.request_key)
        digest_value = digest(
            {"request": request.model_dump(mode="json"), "resource": resource_fingerprint}
        )
        metadata = encode_prepared(prepared)
        self._validate_prepared(prepared, original_intent=False)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            await self._scope(session, scope_id, writing=True, run_id=run_id)
            previous = await session.scalar(
                select(WorkspaceEditAttemptRow).where(
                    WorkspaceEditAttemptRow.scope_id == scope_id,
                    WorkspaceEditAttemptRow.request_key == request.request_key,
                )
            )
            batch_child = await assert_batch_request(
                session,
                self.owner_id,
                scope_id,
                request,
                resource_fingerprint,
                batch_fence,
                previous.operation_id if previous is not None else None,
            )
            restore_child = await assert_restore_request(
                session,
                self.owner_id,
                scope_id,
                request,
                resource_fingerprint,
                restore_fence,
                previous.operation_id if previous is not None else None,
            )
            proposal = await self._proposal(session, scope_id, request.proposal_id)
            original = self._change(proposal).prepared
            command_source = await assert_command_proposal(
                session,
                proposal,
                original,
                applying=request.action == "apply" and previous is None,
            )
            self._validate_prepared(
                prepared,
                original_intent=request.action == "apply"
                and restore_child is None
                and not command_source,
            )
            if previous is not None:
                if previous.request_hash != digest_value:
                    raise ServiceError(
                        "workspace_idempotency_conflict", "动作键已绑定其他请求或资源", 409
                    )
                return self._attempt(previous), False
            await self._resource(session, scope_id, proposal.workspace_key, resource_fingerprint)
            if proposal.resource_fingerprint != resource_fingerprint:
                raise ServiceError("workspace_binding_changed", "修改提案资源版本不匹配", 409)
            if (
                prepared.workspace_fingerprint != original.workspace_fingerprint
                or prepared.spec.path != original.spec.path
                or prepared.parent_identity != original.parent_identity
            ):
                raise ServiceError("workspace_binding_changed", "文件动作不属于原提案", 409)
            if request.action == "apply" and prepared != original:
                raise ServiceError("workspace_proposal_changed", "应用动作必须使用原冻结提案", 409)
            if request.action == "undo" and (
                prepared.after != (original.before.data if original.before else None)
                or (prepared.before.data if prepared.before else None) != original.after
            ):
                raise ServiceError(
                    "workspace_proposal_changed", "撤销须绑定应用后版本与旧内容", 409
                )
            if request.action == "undo" and isinstance(prepared.spec, RestoreFileSpec):
                restore = prepared.spec
                old = original.before
                if (restore.mode, restore.uid, restore.gid) != (
                    (old.mode, old.uid, old.gid) if old is not None else (None, None, None)
                ):
                    raise ServiceError(
                        "workspace_proposal_changed", "撤销属性未绑定原私有快照", 409
                    )
            if request.action == "keep" and prepared != original:
                raise ServiceError("workspace_proposal_changed", "保留动作必须绑定原提案", 409)
            allowed = {"apply": {"proposed"}, "undo": {"applied", "kept"}, "keep": {"applied"}}
            if (
                proposal.status not in allowed[request.action]
                or proposal.active_operation_id is not None
            ):
                raise ServiceError("workspace_change_state", "修改提案当前不允许该动作", 409)
            if request.action == "apply" and proposal.expires_at <= time.time():
                raise ServiceError("workspace_proposal_expired", "修改提案已过期", 410)
            if (
                proposal.revision != request.expected_revision
                or proposal.revision >= 9007199254740989
            ):
                raise ServiceError(
                    "workspace_revision_conflict", "修改提案版本已变化或达到上限", 409
                )
            lock_id = file_key(prepared)
            if await session.get(WorkspaceFileLockRow, lock_id) is not None:
                raise ServiceError("workspace_file_busy", "该文件有执行中或未核实操作", 409)
            count = await session.scalar(select(func.count()).select_from(WorkspaceEditAttemptRow))
            if (count or 0) >= self.max_changes * 8:
                raise ServiceError("workspace_operation_limit", "修改操作历史已达上限", 429)
            keep_witness = (
                await self._freeze_keep(session, proposal, lock_id)
                if request.action == "keep"
                else None
            )
            # 见证只包含ASCII引用/摘要；按SQLAlchemy默认JSON序列化计算持久字节。
            witness_bytes = len(json.dumps(keep_witness)) if keep_witness is not None else 0
            await self._bytes_available(
                session,
                len(prepared.before.data if prepared.before else b"")
                + len(prepared.after or b"")
                + witness_bytes,
            )
            operation_id = str(uuid4())
            next_status = {"apply": "applying", "undo": "undoing", "keep": "keeping"}[
                request.action
            ]
            # ORM会同步下方UPDATE结果；必须在SQL执行前冻结动作来源。
            previous_status = proposal.status
            revision = await session.scalar(
                update(WorkspaceEditProposalRow)
                .where(
                    WorkspaceEditProposalRow.proposal_id == proposal.proposal_id,
                    WorkspaceEditProposalRow.revision == request.expected_revision,
                    WorkspaceEditProposalRow.status == proposal.status,
                    WorkspaceEditProposalRow.active_operation_id.is_(None),
                )
                .values(
                    status=next_status,
                    revision=request.expected_revision + 1,
                    active_operation_id=operation_id,
                    updated_at=utc_now(),
                )
                .returning(WorkspaceEditProposalRow.revision)
            )
            if revision is None:
                raise ServiceError("workspace_revision_conflict", "修改提案版本已变化", 409)
            row = WorkspaceEditAttemptRow(
                operation_id=operation_id,
                proposal_id=proposal.proposal_id,
                scope_id=scope_id,
                request_key=request.request_key,
                request_hash=digest_value,
                action=request.action,
                expected_revision=request.expected_revision,
                previous_status=previous_status,
                status="running",
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                prepared=metadata,
                before_content=prepared.before.data if prepared.before else None,
                after_content=prepared.after,
                keep_witness=keep_witness,
                cleanup_pending=False,
                recovery=RecoveryMetadata(
                    workspace_fingerprint=prepared.workspace_fingerprint,
                    path=prepared.spec.path,
                    stage_name="._krim-tx-" + prepared.token,
                    parent_device=prepared.parent_identity[0],
                    parent_inode=prepared.parent_identity[1],
                ).model_dump(mode="json")
                if request.action != "keep"
                else None,
                run_id=run_id,
                call_id=call_id,
            )
            session.add(row)
            await session.flush()
            if batch_child is not None:
                batch_child.operation_id = operation_id
                batch_child.admission_open = False
            if restore_child is not None:
                restore_child.operation_id = operation_id
                restore_child.admission_open = False
            session.add(WorkspaceFileLockRow(file_key=lock_id, operation_id=operation_id))
            await session.flush()
            return self._attempt(row), True

    async def operation(self, scope_id: str, operation_id: str) -> StoredWorkspaceAttempt:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=False)
            row = await session.get(WorkspaceEditAttemptRow, operation_id)
            if row is None or row.scope_id != scope_id:
                raise ServiceError("workspace_operation_not_found", "修改操作不在该作用域中", 404)
            return self._attempt(row)

    async def assert_execution(self, scope_id: str, operation_id: str) -> None:
        """在文件锁内核对原操作资格；此检查不延长租约、不更改文件或账本。"""
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(WorkspaceEditAttemptRow, operation_id)
            if row is None or row.scope_id != scope_id:
                raise ServiceError("workspace_operation_not_found", "修改操作不在该作用域中", 404)
            await self._scope(session, scope_id, writing=True, run_id=row.run_id)
            if (
                row.status != "running"
                or row.owner_id != self.owner_id
                or (row.lease_expires_at or 0) <= time.time()
            ):
                raise ServiceError("workspace_operation_owner", "修改操作未由当前生产者持有", 409)
            proposal = await self._proposal(session, scope_id, row.proposal_id)
            if proposal.active_operation_id != operation_id:
                raise ServiceError("workspace_operation_conflict", "提案与操作指针不匹配", 409)
            await assert_batch_execution(session, self.owner_id, operation_id)
            await assert_restore_execution(session, self.owner_id, operation_id)
            await assert_command_proposal(
                session, proposal, self._change(proposal).prepared, applying=row.action == "apply"
            )

    async def history(self, scope_id: str, proposal_id: str) -> builtin_list[WorkspaceAttemptView]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=False)
            await self._proposal(session, scope_id, proposal_id)
            rows = await session.scalars(
                select(WorkspaceEditAttemptRow)
                .where(WorkspaceEditAttemptRow.proposal_id == proposal_id)
                .order_by(WorkspaceEditAttemptRow.created_at, WorkspaceEditAttemptRow.operation_id)
            )
            return [self._attempt(row).view for row in rows]

    async def renew(self, operation_id: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            found = await session.scalar(
                update(WorkspaceEditAttemptRow)
                .where(
                    WorkspaceEditAttemptRow.operation_id == operation_id,
                    WorkspaceEditAttemptRow.status == "running",
                    WorkspaceEditAttemptRow.owner_id == self.owner_id,
                    WorkspaceEditAttemptRow.lease_expires_at > time.time(),
                )
                .values(lease_expires_at=time.time() + self.lease_seconds, updated_at=utc_now())
                .returning(WorkspaceEditAttemptRow.operation_id)
            )
            if found is None:
                raise ServiceError("workspace_lease_lost", "文件操作所有权或租约失效", 409)

    async def settle(
        self,
        scope_id: str,
        operation_id: str,
        receipt: FileChangeReceipt,
        recovery: FileRecovery | None = None,
        *,
        resolve: bool = False,
        observed_after: WorkspaceFileState | None = None,
    ) -> StoredWorkspaceAttempt:
        value = FileChangeReceipt.model_validate(receipt.model_dump())
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            row = await session.get(WorkspaceEditAttemptRow, operation_id)
            if row is None or row.scope_id != scope_id:
                raise ServiceError("workspace_operation_not_found", "修改操作不在该作用域中", 404)
            if row.status in {"done", "rejected"}:
                if row.receipt == value.model_dump(mode="json"):
                    return self._attempt(row)
                raise ServiceError("workspace_operation_resolved", "修改操作已有终态回执", 409)
            if (row.status == "uncertain") != resolve:
                raise ServiceError(
                    "workspace_resolution_invalid", "回执不属于当前执行或核实阶段", 409
                )
            if resolve:
                await self._scope(session, scope_id, writing=True)
            elif row.owner_id != self.owner_id or (row.lease_expires_at or 0) <= time.time():
                raise ServiceError("workspace_operation_owner", "修改操作未由当前生产者持有", 409)
            prepared = self._attempt(row).prepared
            if (
                value.path != prepared.spec.path
                or value.before_sha256 != prepared.before_sha256
                or value.after_sha256 != prepared.after_sha256
            ):
                raise ServiceError("workspace_receipt_mismatch", "回执未关联原文件与原版本", 409)
            if recovery is not None and (
                recovery.workspace_fingerprint != prepared.workspace_fingerprint
                or recovery.path != prepared.spec.path
                or recovery.parent_identity != prepared.parent_identity
                or recovery.stage_name != "._krim-tx-" + prepared.token
            ):
                raise ServiceError("workspace_receipt_mismatch", "恢复材料未关联原文件操作", 409)
            proposal = await self._proposal(session, scope_id, row.proposal_id)
            if row.action == "keep" and value.status == "done" and not resolve:
                await self._scope(session, scope_id, writing=True, run_id=row.run_id)
                await self._resource(
                    session, scope_id, proposal.workspace_key, proposal.resource_fingerprint
                )
                await assert_batch_execution(session, self.owner_id, operation_id)
            if proposal.active_operation_id != operation_id:
                raise ServiceError("workspace_operation_conflict", "提案与操作指针不匹配", 409)
            if len(row.receipt_history) >= 16:
                raise ServiceError("workspace_receipt_limit", "核实回执历史已达上限", 409)
            if value.status == "done" and row.action in {"apply", "undo"}:
                observed = (
                    WorkspaceFileState.model_validate(observed_after)
                    if observed_after is not None
                    else None
                )
                if observed is not None and observed.sha256 != prepared.after_sha256:
                    raise ServiceError(
                        "workspace_receipt_mismatch", "实测版本未关联原操作结果", 409
                    )
                session.add(
                    WorkspaceFileEffectRow(
                        operation_id=row.operation_id,
                        proposal_id=row.proposal_id,
                        scope_id=row.scope_id,
                        resource_fingerprint=proposal.resource_fingerprint,
                        file_key=file_key(prepared),
                        action=row.action,
                        observed_after=observed.model_dump(mode="json") if observed else None,
                    )
                )
            row.status = value.status
            row.receipt = value.model_dump(mode="json")
            row.receipt_history = [*row.receipt_history, row.receipt]
            if recovery is not None:
                row.recovery = RecoveryMetadata(
                    workspace_fingerprint=recovery.workspace_fingerprint,
                    path=recovery.path,
                    stage_name=recovery.stage_name,
                    parent_device=recovery.parent_identity[0],
                    parent_inode=recovery.parent_identity[1],
                ).model_dump(mode="json")
            elif value.status in {"done", "rejected"}:
                row.recovery = None
            row.cleanup_pending = value.status in {"done", "rejected"} and row.recovery is not None
            row.owner_id, row.lease_expires_at = None, None
            row.updated_at = utc_now()
            if value.status == "done":
                proposal.status = {"apply": "applied", "undo": "undone", "keep": "kept"}[row.action]
            elif value.status == "uncertain":
                proposal.status = "uncertain"
            else:
                proposal.status = "rejected" if row.action == "apply" else row.previous_status
            proposal.revision += 1
            proposal.updated_at = utc_now()
            if value.status != "uncertain":
                proposal.active_operation_id = None
                await session.execute(
                    delete(WorkspaceFileLockRow).where(
                        WorkspaceFileLockRow.operation_id == operation_id
                    )
                )
            await session.flush()
            return self._attempt(row)

    async def mark_cleaned(self, scope_id: str, operation_id: str) -> StoredWorkspaceAttempt:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            row = await session.get(WorkspaceEditAttemptRow, operation_id)
            if row is None or row.scope_id != scope_id:
                raise ServiceError("workspace_operation_not_found", "修改操作不在该作用域中", 404)
            await self._scope(session, scope_id, writing=True)
            if row.status not in {"done", "rejected"}:
                raise ServiceError("workspace_cleanup_state", "只有已确认操作允许清理标记", 409)
            row.cleanup_pending = False
            row.updated_at = utc_now()
            await session.flush()
            return self._attempt(row)

    async def recover_interrupted(self) -> int:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            rows = list(
                await session.scalars(
                    select(WorkspaceEditAttemptRow).where(
                        WorkspaceEditAttemptRow.status == "running",
                        WorkspaceEditAttemptRow.lease_expires_at <= time.time(),
                    )
                )
            )
            for row in rows:
                prepared = self._attempt(row).prepared
                receipt = FileChangeReceipt(
                    path=prepared.spec.path,
                    status="uncertain",
                    before_sha256=prepared.before_sha256,
                    after_sha256=prepared.after_sha256,
                    changed=row.action != "keep",
                    error_code="workspace_process_interrupted",
                    recovery_required=True,
                ).model_dump(mode="json")
                row.status, row.receipt = "uncertain", receipt
                row.receipt_history = [*row.receipt_history, receipt]
                row.owner_id, row.lease_expires_at = None, None
                row.updated_at = utc_now()
                proposal = await self._proposal(session, row.scope_id, row.proposal_id)
                if proposal.active_operation_id == row.operation_id:
                    proposal.status = "uncertain"
                    proposal.revision += 1
                    proposal.updated_at = utc_now()
            return len(rows)
