"""独立缺失目录恢复账本；冷事实不重获原活观察或再次发布资格。"""

from __future__ import annotations

import os
import re
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import uuid4

from pydantic import JsonValue
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_absence import (
    WorkspaceDirectoryAbsence,
    WorkspaceDirectoryAbsenceReader,
    _Observation,
)
from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointSummary,
    StoredDirectoryCheckpoint,
)
from krim_agent.workspace_directory_restore import (
    DirectoryRestoreDiffEntry,
    DirectoryRestoreNodeView,
    DirectoryRestorePhase,
    DirectoryRestoreStatus,
    DirectoryRestoreTarget,
)
from krim_agent.workspace_directory_restore_writer import (
    DirectoryRestoreStage,
    WorkspaceDirectoryRestoreWriter,
)
from krim_agent.workspace_missing_directory_restore import (
    DirectoryAbsenceSummary,
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestoreFence,
    MissingDirectoryRestoreOperationView,
    MissingDirectoryRestorePreview,
    MissingDirectoryRestoreReceiptView,
    MissingDirectoryRestoreView,
)
from krim_agent.workspace_missing_directory_writer import (
    MissingDirectoryRestoreEffect,
    PreparedMissingDirectoryRestore,
)
from krim_agent.workspace_tree_snapshot import TreeSnapshotEntry, WorkspaceTreeSnapshot

from .database import (
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestoreLockRow,
    WorkspaceMissingDirectoryRestorePlanRow,
)
from .directory_fences import directory_key, entry_key
from .directory_restore_capacity import material_usage, staging_usage
from .directory_restore_codec import decode_stage, encode_stage
from .directory_restore_fences import snapshot_keys
from .directory_restore_sources import DirectoryRestoreSources, source_value, valid_source
from .missing_directory_restore_codec import (
    decode_absence,
    decode_effect,
    decode_prepared,
    encode_absence,
    encode_effect,
    encode_prepared,
)
from .missing_directory_restore_sources import admit, sources
from .tree_move_fences import NamespaceKeys, assert_namespace_keys_available
from .workspace_directory_captures import WorkspaceDirectoryCaptureStore
from .workspace_directory_restores import WorkspaceDirectoryRestoreStore
from .workspace_edits import digest


def _error(kind: str, text: str, status: int = 409) -> ServiceError:
    return ServiceError("workspace_missing_directory_restore_" + kind, text, status)


def _corrupt() -> ServiceError:
    return _error("corrupt", "目录恢复原计划、来源或材料关联损坏")


def _date(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class WorkspaceMissingDirectoryRestoreStore:
    def __init__(
        self,
        captures: WorkspaceDirectoryCaptureStore,
        *,
        max_plans: int = 500,
        max_snapshot_bytes: int = 67_108_864,
        max_staging_bytes: int = 67_108_864,
        max_staging_entries: int = 4096,
        lease_seconds: float = 60,
    ) -> None:
        if type(max_plans) is not int or not 1 <= max_plans <= 5000:
            raise ValueError("目录恢复计划上限无效")
        if type(max_snapshot_bytes) is not int or not 1024 <= max_snapshot_bytes <= 536_870_912:
            raise ValueError("目录恢复私有材料配额无效")
        if type(max_staging_bytes) is not int or not 0 <= max_staging_bytes <= 536_870_912:
            raise ValueError("目录恢复私有暂存字节配额无效")
        if type(max_staging_entries) is not int or not 1 <= max_staging_entries <= 1_000_000:
            raise ValueError("目录恢复私有暂存节点配额无效")
        if type(lease_seconds) not in {int, float} or not 0.2 <= lease_seconds <= 600:
            raise ValueError("目录恢复租约无效")
        self.captures, self.materials, self.edits = captures, captures.materials, captures.edits
        self.database, self.owner_id = self.edits.database, str(uuid4())
        self.max_plans, self.max_snapshot_bytes = max_plans, max_snapshot_bytes
        self.max_staging_bytes, self.max_staging_entries = max_staging_bytes, max_staging_entries
        self.lease_seconds = lease_seconds
        self.observation_owner_id = str(uuid4())
        self._process_id = os.getpid()
        self._live_readers: dict[str, tuple[WorkspaceDirectoryAbsenceReader, _Observation]] = {}
        self.complete = WorkspaceDirectoryRestoreStore(captures)

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        async with self.database.write_lock, self.database.sessions() as sql:
            async with sql.begin():
                await self.edits._writer(sql)
                yield sql

    @staticmethod
    def _request(row: WorkspaceMissingDirectoryRestorePlanRow) -> MissingDirectoryRestorePreview:
        identifier = (
            row.target_checkpoint_id
            if row.target_kind == "checkpoint"
            else row.target_baseline_plan_id
        )
        if identifier is None:
            raise _corrupt()
        return MissingDirectoryRestorePreview(
            target=DirectoryRestoreTarget(
                kind=cast(Literal["checkpoint", "baseline"], row.target_kind), identifier=identifier
            ),
            request_key=row.request_key,
            ttl_seconds=row.ttl_seconds,
        )

    @staticmethod
    def _wanted(
        scope: str,
        request: MissingDirectoryRestorePreview,
        binding_key: str,
        resource: str,
        workspace: str,
        root: str,
        material_id: str,
        summary: dict[str, object],
        stage_token: str,
        model_source: CheckpointModelSource | None,
    ) -> str:
        return digest(
            {
                "kind": "workspace-missing-directory-restore-plan",
                "scope": scope,
                "request": request.model_dump(mode="json"),
                "key": binding_key,
                "resource": resource,
                "workspace": workspace,
                "root": root,
                "material": material_id,
                "summary": summary,
                "stage_token": stage_token,
                "model_source": source_value(model_source),
            }
        )

    @staticmethod
    def _summary(value: object, fingerprint: str | None) -> DirectoryCheckpointSummary:
        result = DirectoryCheckpointSummary.model_validate(value)
        if (
            result.snapshot_fingerprint != fingerprint
            or result.file_count == 0
            and result.byte_size != 0
        ):
            raise _corrupt()
        return result

    @staticmethod
    def _absence_hash(row: WorkspaceMissingDirectoryRestorePlanRow) -> str:
        return digest(
            {
                "plan": row.request_hash,
                "absence": row.absence_metadata,
                "fingerprint": row.absence_fingerprint,
                "owner": row.observation_owner_id,
            }
        )

    @staticmethod
    def _execute_hash(
        row: WorkspaceMissingDirectoryRestorePlanRow,
        request: MissingDirectoryRestoreExecute,
        source: CheckpointModelSource | None,
    ) -> str:
        return digest(
            {
                "plan": row.request_hash,
                "absence": row.absence_hash,
                "request": request.model_dump(mode="json"),
                "source": source_value(source),
            }
        )

    @staticmethod
    def _verification_hash(
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow, source: CheckpointModelSource | None
    ) -> str:
        return digest(
            {
                "execute": attempt.request_hash,
                "count": attempt.verification_count,
                "models": attempt.verification_model_count,
                "source": source_value(source),
            }
        )

    @staticmethod
    def _state(
        row: WorkspaceMissingDirectoryRestorePlanRow,
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow | None,
    ) -> None:
        if (
            re.fullmatch(r"[a-f0-9]{64}", row.token) is None
            or re.fullmatch(r"[a-f0-9]{32}", row.stage_token) is None
            or (row.phase == "preparing")
            != (row.owner_id is not None and row.lease_expires_at is not None)
            or row.phase == "preparing"
            and row.revision != 0
            or (row.prepared_metadata is None) != (row.prepared_payload is None)
            or row.prepared_metadata is not None
            and row.stage_metadata is None
        ):
            raise _corrupt()
        if attempt is None:
            if (
                row.execute_model_bound
                or row.stage_metadata is not None
                or row.prepared_metadata is not None
                or row.cleanup_pending
                or row.staging_reserved_bytes != 0
                or row.staging_reserved_entries != 0
                or row.phase in {"executing", "finished", "uncertain"}
            ):
                raise _corrupt()
            return
        expected = {"running": "executing", "done": "finished", "rejected": "failed"}.get(
            attempt.status, attempt.status
        )
        if (
            re.fullmatch(r"[a-f0-9]{64}", attempt.token) is None
            or row.phase != expected
            or row.error_code != attempt.error_code
            or row.absence_metadata is None
            or attempt.expected_revision < 1
            or row.revision <= attempt.expected_revision
            or (attempt.status == "running")
            != (attempt.owner_id is not None and attempt.lease_expires_at is not None)
            or attempt.verification_count == 0
            and (attempt.verify_model_bound or attempt.verification_model_count != 0)
            or (row.stage_metadata is not None) != bool(row.cleanup_pending)
            or attempt.cancelled != row.cancel_requested
        ):
            raise _corrupt()

    async def _base(
        self, sql: AsyncSession, scope: str, identifier: str
    ) -> tuple[
        WorkspaceMissingDirectoryRestorePlanRow,
        WorkspaceMissingDirectoryRestoreAttemptRow | None,
        DirectoryRestoreSources,
    ]:
        row = await sql.get(WorkspaceMissingDirectoryRestorePlanRow, identifier)
        if row is None or row.scope_id != scope:
            raise _error("not_found", "目录恢复计划不在此作用域", 404)
        topic = await self.edits._scope(sql, scope, writing=False)
        await self.edits._resource(sql, scope, row.workspace_key, row.resource_fingerprint)
        attempt = await sql.scalar(
            select(WorkspaceMissingDirectoryRestoreAttemptRow).where(
                WorkspaceMissingDirectoryRestoreAttemptRow.plan_id == row.plan_id
            )
        )
        origin = await sources(sql, self.edits, row, attempt)
        try:
            target = self._summary(row.target_summary, row.target_fingerprint)
            request = self._request(row)
            checksum = self._wanted(
                scope,
                request,
                row.workspace_key,
                row.resource_fingerprint,
                row.workspace_fingerprint,
                row.root_path,
                row.target_material_id,
                target.model_dump(mode="json"),
                row.stage_token,
                origin.preview,
            )
            if row.topic_id != topic or checksum != row.request_hash:
                raise _corrupt()
            if row.absence_metadata is None:
                if any(
                    value is not None
                    for value in (
                        row.absence_fingerprint,
                        row.absence_hash,
                        row.observation_owner_id,
                    )
                ):
                    raise _corrupt()
            else:
                self._absence(row)
            if row.stage_metadata is not None:
                self._stage(row)
            if row.prepared_metadata is not None:
                self._prepared(row)
            if (row.execute_key is None) != (attempt is None):
                raise _corrupt()
            if attempt is not None and (
                attempt.scope_id != scope or attempt.request_key != row.execute_key
            ):
                raise _corrupt()
            if attempt is not None:
                wanted = MissingDirectoryRestoreExecute(
                    plan_id=row.plan_id,
                    expected_revision=attempt.expected_revision,
                    request_key=attempt.request_key,
                )
                if (
                    attempt.request_hash != row.execute_hash
                    or attempt.request_hash != self._execute_hash(row, wanted, origin.execute)
                ):
                    raise _corrupt()
                if attempt.verification_count == 0:
                    if attempt.verification_hash is not None or origin.verify is not None:
                        raise _corrupt()
                elif attempt.verification_hash != self._verification_hash(attempt, origin.verify):
                    raise _corrupt()
                if attempt.intent_recorded and row.prepared_metadata is None:
                    raise _corrupt()
            self._state(row, attempt)
            return row, attempt, origin
        except (TypeError, ValueError, AttributeError, ServiceError):
            raise _corrupt() from None

    @staticmethod
    def _absence(row: WorkspaceMissingDirectoryRestorePlanRow) -> WorkspaceDirectoryAbsence:
        if row.absence_metadata is None:
            raise _error("baseline_missing", "原目录缺失事实尚未保存")
        value = decode_absence(row.absence_metadata)
        if (
            value.fingerprint != row.absence_fingerprint
            or row.absence_hash != WorkspaceMissingDirectoryRestoreStore._absence_hash(row)
            or value.root_path != row.root_path
            or value.workspace_fingerprint != row.workspace_fingerprint
            or type(row.observation_owner_id) is not str
        ):
            raise _corrupt()
        return value

    @staticmethod
    def _stage(row: WorkspaceMissingDirectoryRestorePlanRow) -> DirectoryRestoreStage:
        value = decode_stage(row.stage_metadata, row.request_hash)
        before = WorkspaceMissingDirectoryRestoreStore._absence(row)
        if (
            value.root_path != row.root_path
            or value.token != row.stage_token
            or value.parent_identity != before.parent.identity
        ):
            raise _corrupt()
        return value

    @staticmethod
    def _absence_keys(value: WorkspaceDirectoryAbsence) -> NamespaceKeys:
        return {
            ("entry", entry_key(value.parent.identity, value.root_path)),
            ("directory", directory_key(value.parent.identity)),
        }

    def _keys(self, row: WorkspaceMissingDirectoryRestorePlanRow) -> NamespaceKeys:
        keys = self._absence_keys(self._absence(row))
        if row.stage_metadata is not None:
            keys.add(("directory", directory_key(self._stage(row).container_identity)))
        if row.prepared_metadata is not None:
            keys |= snapshot_keys(self._prepared(row).staged)
        return keys

    def _observation_owner(self, row: WorkspaceMissingDirectoryRestorePlanRow) -> None:
        if self._process_id != os.getpid() or row.observation_owner_id != self.observation_owner_id:
            raise _error("observation_lost", "原缺失活观察所属实例已改变，须明确重新预览")

    def _live_observation(self, row: WorkspaceMissingDirectoryRestorePlanRow) -> None:
        """只查原实例内存资格；不重新stat、读事件或捕获旧请求。"""
        self._observation_owner(row)
        original = self._live_readers.get(row.plan_id)
        if original is None:
            raise _error("observation_lost", "原活观察已丢失，不能从SQL材料重建")
        reader, witness = original
        if reader._owner_pid != os.getpid():
            raise _error("observation_lost", "不能借用其他进程的活观察")
        expected = (
            decode_absence(row.prepared_metadata["current_absence"])
            if row.prepared_metadata is not None
            else self._absence(row)
        )
        with reader._state_lock:
            actual = reader._observations.get(row.root_path)
            if (
                reader._closed
                or actual is None
                or actual.watch is not witness.watch
                or actual.watch.invalid
                or actual.watch.descriptor < 0
                or actual.value.root_path != row.root_path
                or actual.value.workspace_fingerprint != row.workspace_fingerprint
                or actual.value.parent.identity != witness.value.parent.identity
                or row.stage_metadata is None
                and actual.value != expected
                or row.prepared_metadata is not None
                and actual.value != expected
            ):
                raise _error("observation_lost", "原活观察已关闭、释放或被替换")

    def _bind_observation(
        self,
        row: WorkspaceMissingDirectoryRestorePlanRow,
        binding: WorkspaceBinding,
        reader: WorkspaceDirectoryAbsenceReader,
        absence: WorkspaceDirectoryAbsence,
    ) -> None:
        if (
            type(reader) is not WorkspaceDirectoryAbsenceReader
            or reader.workspace is not binding.workspace
            or reader._owner_pid != os.getpid()
        ):
            raise _error("observation_lost", "缺失事实需要原工作区的活观察实例")
        with reader._state_lock:
            actual = reader._observations.get(row.root_path)
            if (
                reader._closed
                or actual is None
                or actual.value != absence
                or actual.watch.invalid
                or actual.watch.descriptor < 0
            ):
                raise _error("observation_lost", "历史缺失材料不能冒领原活观察")
            original = self._live_readers.get(row.plan_id)
            if original is not None and (
                original[0] is not reader or original[1].watch is not actual.watch
            ):
                raise _error("observation_lost", "原观察器不能由重新捕获或其他实例替换")
            self._live_readers[row.plan_id] = reader, actual

    async def _target(
        self, sql: AsyncSession, scope: str, request: DirectoryRestoreTarget
    ) -> StoredDirectoryCheckpoint:
        return await self.complete._target(sql, scope, request)

    @staticmethod
    def _qualify(binding: WorkspaceBinding, qualify: Callable[[], None]) -> None:
        if not binding.require().writable:
            raise ServiceError("workspace_read_only", "目录恢复需要原可写工作区授权", 403)
        qualify()

    def _live_prepare(
        self, row: WorkspaceMissingDirectoryRestorePlanRow, fence: MissingDirectoryRestoreFence
    ) -> None:
        if (
            type(fence) is not MissingDirectoryRestoreFence
            or fence.kind != "prepare"
            or fence.plan_id != row.plan_id
            or fence.operation_id != row.plan_id
            or type(fence.token) is not str
            or re.fullmatch(r"[a-f0-9]{64}", fence.token) is None
            or not secrets.compare_digest(fence.token, row.token)
            or row.owner_id != self.owner_id
            or self._process_id != os.getpid()
            or row.phase != "preparing"
            or (row.lease_expires_at or 0) <= time.time()
            or row.expires_at <= time.time()
        ):
            raise _error("fenced", "目录恢复原准备资格已失效")
        valid_source(fence.model_source)

    async def _preparing(
        self,
        sql: AsyncSession,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
    ) -> WorkspaceMissingDirectoryRestorePlanRow:
        if type(fence) is not MissingDirectoryRestoreFence or type(binding) is not WorkspaceBinding:
            raise _error("context", "目录恢复需要原私有准备资格", 403)
        row, _attempt, origin = await self._base(sql, scope, fence.plan_id)
        self._live_prepare(row, fence)
        if origin.preview != fence.model_source or (
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
        ) != (row.workspace_key, row.resource_fingerprint, row.workspace_fingerprint):
            raise _error("context", "目录恢复须保留原来源与资源", 403)
        await self.edits._scope(
            sql, scope, writing=True, run_id=origin.preview.run_id if origin.preview else None
        )
        return row

    async def _view(
        self,
        sql: AsyncSession,
        row: WorkspaceMissingDirectoryRestorePlanRow,
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow | None,
    ) -> MissingDirectoryRestoreView:
        receipt = None
        if (
            attempt is not None
            and attempt.effect_metadata is not None
            and attempt.status != "running"
        ):
            if row.prepared_metadata is None or row.prepared_payload is None:
                raise _corrupt()
            prepared = self._prepared(row)
            effect = decode_effect(prepared, attempt.effect_metadata)
            receipt = MissingDirectoryRestoreReceiptView.model_validate(effect.receipt.__dict__)
        try:
            return MissingDirectoryRestoreView(
                plan_id=row.plan_id,
                scope_id=row.scope_id,
                target=self._request(row).target,
                root_path=row.root_path,
                workspace_key=row.workspace_key,
                resource_fingerprint=row.resource_fingerprint,
                target_snapshot=self._summary(row.target_summary, row.target_fingerprint),
                baseline=DirectoryAbsenceSummary(fingerprint=self._absence(row).fingerprint)
                if row.absence_metadata is not None
                else None,
                request_key=row.request_key,
                execute_key=row.execute_key,
                revision=row.revision,
                phase=cast(DirectoryRestorePhase, row.phase),
                status=cast(
                    DirectoryRestoreStatus,
                    {"executing": "running", "finished": "done"}.get(row.phase, row.phase),
                ),
                operation_id=attempt.operation_id if attempt else row.plan_id,
                receipt=receipt,
                cleanup_pending=row.cleanup_pending,
                cancel_requested=row.cancel_requested,
                expires_at=row.expires_at,
                error_code=row.error_code,
                created_at=_date(row.created_at),
                updated_at=_date(row.updated_at),
            )
        except (TypeError, ValueError, AttributeError, ServiceError):
            raise _corrupt() from None

    async def reserve(
        self,
        scope: str,
        binding: WorkspaceBinding,
        request: MissingDirectoryRestorePreview,
        *,
        model_source: CheckpointModelSource | None = None,
        qualify: Callable[[], None],
    ) -> tuple[MissingDirectoryRestoreView, bool]:
        if (
            type(binding) is not WorkspaceBinding
            or type(request) is not MissingDirectoryRestorePreview
        ):
            raise _error("input", "目录恢复请求或绑定类型无效", 422)
        request = MissingDirectoryRestorePreview.model_validate(request.model_dump(mode="json"))
        model_source = valid_source(model_source)
        async with self._transaction() as sql:
            await self.edits._scope(sql, scope, writing=False)
            await self.edits._resource(sql, scope, binding.key, binding.fingerprint)
            old = await sql.scalar(
                select(WorkspaceMissingDirectoryRestorePlanRow).where(
                    WorkspaceMissingDirectoryRestorePlanRow.scope_id == scope,
                    WorkspaceMissingDirectoryRestorePlanRow.request_key == request.request_key,
                )
            )
            if old is not None:
                row, attempt, origin = await self._base(sql, scope, old.plan_id)
                if (
                    self._request(row) != request
                    or origin.preview != model_source
                    or (row.workspace_key, row.resource_fingerprint, row.workspace_fingerprint)
                    != (binding.key, binding.fingerprint, binding.workspace.fingerprint)
                ):
                    raise _error("key_conflict", "原目录恢复键已绑定其他请求、资源或来源")
                await self._expire(sql, row, attempt)
                return await self._view(sql, row, attempt), False
            self._qualify(binding, qualify)
            await self.edits._scope(
                sql, scope, writing=True, run_id=model_source.run_id if model_source else None
            )
            target = await self._target(sql, scope, request.target)
            if (
                target.view.workspace_key,
                target.view.resource_fingerprint,
                target.snapshot.workspace_fingerprint,
            ) != (binding.key, binding.fingerprint, binding.workspace.fingerprint):
                raise _error("target_scope", "目录恢复目标不属于原资源", 403)
            if (
                int(
                    await sql.scalar(
                        select(func.count()).select_from(WorkspaceMissingDirectoryRestorePlanRow)
                    )
                    or 0
                )
                >= self.max_plans
            ):
                raise _error("capacity", "目录恢复计划数量已达上限", 429)
            identifier, stage_token = str(uuid4()), uuid4().hex
            summary = self.captures._summary(target).model_dump(mode="json")
            row = WorkspaceMissingDirectoryRestorePlanRow(
                plan_id=identifier,
                scope_id=scope,
                topic_id=await self.edits._scope(sql, scope, writing=False),
                request_key=request.request_key,
                request_hash=self._wanted(
                    scope,
                    request,
                    binding.key,
                    binding.fingerprint,
                    binding.workspace.fingerprint,
                    target.view.root_path,
                    target.view.checkpoint_id,
                    summary,
                    stage_token,
                    model_source,
                ),
                ttl_seconds=request.ttl_seconds,
                expires_at=time.time() + request.ttl_seconds,
                target_kind=request.target.kind,
                target_checkpoint_id=request.target.identifier
                if request.target.kind == "checkpoint"
                else None,
                target_baseline_plan_id=request.target.identifier
                if request.target.kind == "baseline"
                else None,
                target_material_id=target.view.checkpoint_id,
                target_fingerprint=target.snapshot.fingerprint,
                target_summary=summary,
                root_path=target.view.root_path,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=binding.workspace.fingerprint,
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                token=secrets.token_hex(32),
                stage_token=stage_token,
                preview_model_bound=model_source is not None,
            )
            sql.add(row)
            await sql.flush()
            await admit(sql, self.edits, scope, identifier, "preview", model_source)
            await sql.flush()
            await self._material_capacity(sql)
            result = await self._view(sql, row, None)
            self._qualify(binding, qualify)
            if (row.lease_expires_at or 0) <= time.time() or row.expires_at <= time.time():
                raise _error("fenced", "目录恢复准入已超出原期限")
            return result, True

    async def preparation(self, scope: str, identifier: str) -> MissingDirectoryRestoreFence:
        async with self._transaction() as sql:
            row, _attempt, origin = await self._base(sql, scope, identifier)
            fence = MissingDirectoryRestoreFence(
                row.plan_id, row.plan_id, "prepare", row.token, origin.preview
            )
            self._live_prepare(row, fence)
            return fence

    async def assert_preparation(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> None:
        await self.preparation_view(scope, fence, binding, qualify=qualify)

    async def preparation_view(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
        authorize: Callable[[AsyncSession], Awaitable[None]] | None = None,
    ) -> MissingDirectoryRestoreView:
        """原资格与严格投影同事务；不以另一个冷读取窗口补取原View。"""
        async with self._transaction() as sql:
            if authorize is not None:
                await authorize(sql)
            row = await self._preparing(sql, scope, fence, binding)
            self._qualify(binding, qualify)
            result = await self._view(sql, row, None)
            self._qualify(binding, qualify)
            self._live_prepare(row, fence)
            return result

    async def record_absence(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        absence: WorkspaceDirectoryAbsence,
        *,
        observation_reader: WorkspaceDirectoryAbsenceReader,
        qualify: Callable[[], None],
    ) -> MissingDirectoryRestoreView:
        async with self._transaction() as sql:
            row = await self._preparing(sql, scope, fence, binding)
            self._qualify(binding, qualify)
            metadata = encode_absence(absence)
            if (
                absence.root_path != row.root_path
                or absence.workspace_fingerprint != row.workspace_fingerprint
            ):
                raise _error("baseline", "原缺失事实必须来自原目录与资源")
            self._bind_observation(row, binding, observation_reader, absence)
            if row.absence_metadata is not None:
                self._live_observation(row)
                if self._absence(row) != absence:
                    raise _error("baseline_conflict", "原缺失事实不能替换或重新捕获")
            else:
                await assert_namespace_keys_available(sql, self._absence_keys(absence))
                row.absence_metadata = cast("dict[str, JsonValue]", metadata)
                row.absence_fingerprint = absence.fingerprint
                row.observation_owner_id = self.observation_owner_id
                row.absence_hash = self._absence_hash(row)
                row.updated_at = utc_now()
                await sql.flush()
                await self._material_capacity(sql)
            result = await self._view(sql, row, None)
            self._qualify(binding, qualify)
            self._live_prepare(row, fence)
            self._live_observation(row)
            return result

    async def ready(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> MissingDirectoryRestoreView:
        async with self._transaction() as sql:
            row = await self._preparing(sql, scope, fence, binding)
            self._qualify(binding, qualify)
            self._absence(row)
            self._live_observation(row)
            lease = row.lease_expires_at or 0
            row.phase = "ready"
            row.owner_id, row.lease_expires_at = None, None
            row.revision += 1
            row.updated_at = utc_now()
            await sql.flush()
            result = await self._view(sql, row, None)
            self._qualify(binding, qualify)
            self._live_observation(row)
            if lease <= time.time() or row.expires_at <= time.time():
                raise _error("fenced", "目录恢复准备完成前原资格已过期")
            return result

    async def get(self, scope: str, identifier: str) -> MissingDirectoryRestoreView:
        async with self._transaction() as sql:
            row, attempt, _origin = await self._base(sql, scope, identifier)
            await self._expire(sql, row, attempt)
            return await self._view(sql, row, attempt)

    async def by_key(self, scope: str, request_key: str) -> MissingDirectoryRestoreView | None:
        self.edits._key(request_key)
        async with self._transaction() as sql:
            await self.edits._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(WorkspaceMissingDirectoryRestorePlanRow).where(
                    WorkspaceMissingDirectoryRestorePlanRow.scope_id == scope,
                    WorkspaceMissingDirectoryRestorePlanRow.request_key == request_key,
                )
            )
            if row is None:
                return None
            row, attempt, _origin = await self._base(sql, scope, row.plan_id)
            await self._expire(sql, row, attempt)
            return await self._view(sql, row, attempt)

    @staticmethod
    def _node(entry: TreeSnapshotEntry | None) -> DirectoryRestoreNodeView | None:
        if entry is None:
            return None
        node = entry.file if entry.file is not None else entry.directory
        assert node is not None
        return DirectoryRestoreNodeView(
            kind="file" if entry.file is not None else "directory",
            mode=node.mode,
            uid=node.uid,
            gid=node.gid,
            byte_size=len(entry.file.data) if entry.file is not None else 0,
            sha256=entry.file.sha256 if entry.file is not None else None,
        )

    async def diff(
        self, scope: str, identifier: str, *, offset: int = 0, limit: int = 128
    ) -> list[DirectoryRestoreDiffEntry]:
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 128:
            raise _error("pagination", "缺失恢复差异分页参数无效", 422)
        async with self._transaction() as sql:
            row, attempt, _origin = await self._base(sql, scope, identifier)
            await self._expire(sql, row, attempt)
            if row.absence_metadata is None or row.revision < 1:
                raise _error("not_ready", "缺失差异须等待原缺失基线核实")
            self._absence(row)
            target = (await self._checked_target(sql, row)).snapshot
            return [
                DirectoryRestoreDiffEntry(
                    path=entry.path,
                    operation="create",
                    before=None,
                    after=self._node(entry),
                )
                for entry in sorted(target.entries, key=lambda entry: entry.path)[
                    offset : offset + limit
                ]
            ]

    @staticmethod
    def _bound(row: WorkspaceMissingDirectoryRestorePlanRow, binding: WorkspaceBinding) -> None:
        if type(binding) is not WorkspaceBinding or (
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
        ) != (row.workspace_key, row.resource_fingerprint, row.workspace_fingerprint):
            raise _error("context", "目录恢复必须使用原冻结工作区", 403)

    def _live_operation(
        self,
        row: WorkspaceMissingDirectoryRestorePlanRow,
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow,
        fence: MissingDirectoryRestoreFence,
        origin: DirectoryRestoreSources,
    ) -> None:
        if (
            type(fence) is not MissingDirectoryRestoreFence
            or fence.kind not in {"execute", "verify"}
            or fence.plan_id != row.plan_id
            or fence.operation_id != attempt.operation_id
            or type(fence.token) is not str
            or re.fullmatch(r"[a-f0-9]{64}", fence.token) is None
            or not secrets.compare_digest(fence.token, attempt.token)
            or ("verify" if attempt.verification_count else "execute") != fence.kind
            or attempt.status != "running"
            or attempt.owner_id != self.owner_id
            or self._process_id != os.getpid()
            or (attempt.lease_expires_at or 0) <= time.time()
            or row.phase != "executing"
            or fence.kind == "execute"
            and row.expires_at <= time.time()
            or fence.model_source != (origin.execute if fence.kind == "execute" else origin.verify)
        ):
            raise _error("fenced", "目录恢复原操作资格已失效")
        if fence.kind == "execute":
            self._observation_owner(row)
        valid_source(fence.model_source)

    async def _operating(
        self,
        sql: AsyncSession,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding | None = None,
        *,
        live_scope: bool = True,
    ) -> tuple[
        WorkspaceMissingDirectoryRestorePlanRow,
        WorkspaceMissingDirectoryRestoreAttemptRow,
        DirectoryRestoreSources,
    ]:
        if type(fence) is not MissingDirectoryRestoreFence:
            raise _error("context", "目录恢复需要原私有操作资格", 403)
        row, attempt, origin = await self._base(sql, scope, fence.plan_id)
        if attempt is None:
            raise _error("operation_not_found", "原目录执行操作不存在", 404)
        self._live_operation(row, attempt, fence, origin)
        await self._assert_locks(sql, row, attempt)
        if binding is not None:
            self._bound(row, binding)
        if live_scope:
            await self.edits._scope(
                sql,
                scope,
                writing=True,
                run_id=fence.model_source.run_id if fence.model_source else None,
            )
        return row, attempt, origin

    async def _operation_row(
        self, sql: AsyncSession, scope: str, identifier: str
    ) -> tuple[
        WorkspaceMissingDirectoryRestorePlanRow,
        WorkspaceMissingDirectoryRestoreAttemptRow,
        DirectoryRestoreSources,
    ]:
        attempt = await sql.get(WorkspaceMissingDirectoryRestoreAttemptRow, identifier)
        if attempt is None or attempt.scope_id != scope:
            raise _error("operation_not_found", "目录恢复操作不在此作用域", 404)
        row, actual, origin = await self._base(sql, scope, attempt.plan_id)
        if actual is None or actual.operation_id != identifier:
            raise _corrupt()
        return row, actual, origin

    async def _operation_view(
        self,
        sql: AsyncSession,
        row: WorkspaceMissingDirectoryRestorePlanRow,
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow,
    ) -> MissingDirectoryRestoreOperationView:
        plan = await self._view(sql, row, attempt)
        try:
            return MissingDirectoryRestoreOperationView(
                operation_id=attempt.operation_id,
                plan_id=row.plan_id,
                scope_id=row.scope_id,
                kind="execute",
                request_key=attempt.request_key,
                status=cast(
                    Literal[
                        "running",
                        "done",
                        "rejected",
                        "uncertain",
                        "cancelled",
                        "interrupted",
                        "expired",
                    ],
                    attempt.status,
                ),
                intent_recorded=attempt.intent_recorded,
                receipt=plan.receipt,
                error_code=attempt.error_code,
                created_at=_date(attempt.created_at),
                updated_at=_date(attempt.updated_at),
            )
        except (TypeError, ValueError, ServiceError):
            raise _corrupt() from None

    async def _claim_locks(
        self,
        sql: AsyncSession,
        row: WorkspaceMissingDirectoryRestorePlanRow,
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow,
        keys: NamespaceKeys,
    ) -> None:
        existing = set(
            (item.kind, item.lock_key)
            for item in await sql.scalars(
                select(WorkspaceMissingDirectoryRestoreLockRow).where(
                    WorkspaceMissingDirectoryRestoreLockRow.operation_id == attempt.operation_id
                )
            )
        )
        for kind, key in sorted(keys - existing):
            sql.add(
                WorkspaceMissingDirectoryRestoreLockRow(
                    plan_id=row.plan_id, operation_id=attempt.operation_id, kind=kind, lock_key=key
                )
            )

    async def _assert_locks(
        self,
        sql: AsyncSession,
        row: WorkspaceMissingDirectoryRestorePlanRow,
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow,
    ) -> None:
        expected = self._keys(row)
        actual = set(
            (item.kind, item.lock_key)
            for item in await sql.scalars(
                select(WorkspaceMissingDirectoryRestoreLockRow).where(
                    WorkspaceMissingDirectoryRestoreLockRow.operation_id == attempt.operation_id
                )
            )
        )
        if actual != expected:
            raise _error("lock_lost", "原目录恢复命名空间锁缺失或被替换")

    @staticmethod
    async def _release_locks(
        sql: AsyncSession, attempt: WorkspaceMissingDirectoryRestoreAttemptRow
    ) -> None:
        await sql.execute(
            delete(WorkspaceMissingDirectoryRestoreLockRow).where(
                WorkspaceMissingDirectoryRestoreLockRow.operation_id == attempt.operation_id
            )
        )

    async def _staging_capacity(
        self, sql: AsyncSession, row: WorkspaceMissingDirectoryRestorePlanRow
    ) -> None:
        target = self._summary(row.target_summary, row.target_fingerprint)
        needed_bytes = target.byte_size
        needed_entries = target.entry_count + 1
        used_bytes, used_entries = await staging_usage(sql)
        if (
            int(used_bytes) + needed_bytes > self.max_staging_bytes
            or int(used_entries) + needed_entries > self.max_staging_entries
        ):
            raise _error("capacity", "目录恢复私有暂存配额不足", 429)
        row.staging_reserved_bytes, row.staging_reserved_entries = needed_bytes, needed_entries

    async def _material_capacity(self, sql: AsyncSession) -> None:
        used = await material_usage(sql, sqlite=self.database.engine.dialect.name == "sqlite")
        if used > self.max_snapshot_bytes:
            raise _error("capacity", "目录恢复私有材料和回执预留超过配额", 429)

    async def start(
        self,
        scope: str,
        binding: WorkspaceBinding,
        request: MissingDirectoryRestoreExecute,
        *,
        model_source: CheckpointModelSource | None = None,
        qualify: Callable[[], None],
    ) -> tuple[MissingDirectoryRestoreOperationView, bool]:
        if (
            type(request) is not MissingDirectoryRestoreExecute
            or type(binding) is not WorkspaceBinding
        ):
            raise _error("input", "目录执行请求或绑定无效", 422)
        request = MissingDirectoryRestoreExecute.model_validate(request.model_dump(mode="json"))
        model_source = valid_source(model_source)
        async with self._transaction() as sql:
            row, attempt, origin = await self._base(sql, scope, request.plan_id)
            self._bound(row, binding)
            if attempt is not None:
                if attempt.request_hash != self._execute_hash(row, request, model_source):
                    raise _error("key_conflict", "原计划已绑定其他执行请求或来源")
                await self._expire(sql, row, attempt)
                return await self._operation_view(sql, row, attempt), False
            self._qualify(binding, qualify)
            await self.edits._scope(
                sql, scope, writing=True, run_id=model_source.run_id if model_source else None
            )
            if (
                row.phase != "ready"
                or row.revision != request.expected_revision
                or row.expires_at <= time.time()
                or row.cancel_requested
            ):
                raise _error("revision", "目录恢复不再处于原审阅版本")
            collision = await sql.scalar(
                select(WorkspaceMissingDirectoryRestoreAttemptRow.operation_id).where(
                    WorkspaceMissingDirectoryRestoreAttemptRow.scope_id == scope,
                    WorkspaceMissingDirectoryRestoreAttemptRow.request_key == request.request_key,
                )
            )
            if collision is not None:
                raise _error("key_conflict", "执行键已绑定其他目录计划")
            before = self._absence(row)
            self._live_observation(row)
            await self._checked_target(sql, row)
            keys = self._absence_keys(before)
            await assert_namespace_keys_available(sql, keys)
            await self._staging_capacity(sql, row)
            identifier = str(uuid4())
            checksum = self._execute_hash(row, request, model_source)
            attempt = WorkspaceMissingDirectoryRestoreAttemptRow(
                operation_id=identifier,
                plan_id=row.plan_id,
                scope_id=scope,
                request_key=request.request_key,
                request_hash=checksum,
                expected_revision=request.expected_revision,
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                token=secrets.token_hex(32),
            )
            row.phase, row.execute_key, row.execute_hash = (
                "executing",
                request.request_key,
                checksum,
            )
            row.execute_model_bound, row.error_code = model_source is not None, None
            row.revision += 1
            row.updated_at = utc_now()
            sql.add(attempt)
            await sql.flush()
            await admit(
                sql,
                self.edits,
                scope,
                row.plan_id,
                "execute",
                model_source,
                identifier,
                max_sources=self.max_plans * 4,
            )
            await self._claim_locks(sql, row, attempt, keys)
            await sql.flush()
            result = await self._operation_view(sql, row, attempt)
            self._qualify(binding, qualify)
            self._live_operation(
                row,
                attempt,
                MissingDirectoryRestoreFence(
                    row.plan_id, identifier, "execute", attempt.token, model_source
                ),
                DirectoryRestoreSources(origin.preview, model_source, None),
            )
            return result, True

    async def _checked_target(
        self, sql: AsyncSession, row: WorkspaceMissingDirectoryRestorePlanRow
    ) -> StoredDirectoryCheckpoint:
        target = await self._target(sql, row.scope_id, self._request(row).target)
        if (
            target.view.checkpoint_id != row.target_material_id
            or target.snapshot.fingerprint != row.target_fingerprint
            or target.view.root_path != row.root_path
            or target.snapshot.workspace_fingerprint != row.workspace_fingerprint
        ):
            raise _corrupt()
        return target

    async def execution(self, scope: str, identifier: str) -> MissingDirectoryRestoreFence:
        async with self._transaction() as sql:
            row, attempt, origin = await self._operation_row(sql, scope, identifier)
            fence = MissingDirectoryRestoreFence(
                row.plan_id, attempt.operation_id, "execute", attempt.token, origin.execute
            )
            self._live_operation(row, attempt, fence, origin)
            self._live_observation(row)
            if attempt.intent_recorded:
                raise _error("already_dispatched", "原操作已派发，只能查询或核实")
            return fence

    async def assert_execution(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> None:
        await self.execution_view(scope, fence, binding, qualify=qualify)

    async def execution_view(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
        authorize: Callable[[AsyncSession], Awaitable[None]] | None = None,
    ) -> MissingDirectoryRestoreView:
        """同一原事务保留完整来源/锁/owner核对，投影之后再查原资格。"""
        async with self._transaction() as sql:
            if authorize is not None:
                await authorize(sql)
            row, attempt, origin = await self._operating(sql, scope, fence, binding)
            self._qualify(binding, qualify)
            if fence.kind == "execute":
                self._live_observation(row)
            result = await self._view(sql, row, attempt)
            self._qualify(binding, qualify)
            self._live_operation(row, attempt, fence, origin)
            if fence.kind == "execute":
                self._live_observation(row)
            return result

    async def inputs(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> tuple[WorkspaceTreeSnapshot, WorkspaceDirectoryAbsence, str]:
        async with self._transaction() as sql:
            row, attempt, origin = await self._operating(sql, scope, fence, binding)
            if (
                fence.kind != "execute"
                or attempt.intent_recorded
                or row.stage_metadata is not None
                or row.prepared_metadata is not None
            ):
                raise _error("already_prepared", "原目录操作不得再次准备或重新读取当前树")
            target = (await self._checked_target(sql, row)).snapshot
            before = self._absence(row)
            self._qualify(binding, qualify)
            self._live_operation(row, attempt, fence, origin)
            self._live_observation(row)
            return target, before, row.stage_token

    async def record_stage(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        stage: DirectoryRestoreStage,
        *,
        qualify: Callable[[], None],
    ) -> None:
        async with self._transaction() as sql:
            row, attempt, origin = await self._operating(sql, scope, fence, binding)
            if (
                fence.kind != "execute"
                or attempt.intent_recorded
                or row.prepared_metadata is not None
            ):
                raise _error("already_prepared", "原暂存准入已关闭")
            before = self._absence(row)
            value = encode_stage(stage, row.request_hash)
            if (
                stage.token != row.stage_token
                or stage.root_path != row.root_path
                or stage.parent_identity != before.parent.identity
            ):
                raise _error("stage", "私有暂存未保留原父身份或操作")
            if row.stage_metadata is not None and row.stage_metadata != value:
                raise _error("stage", "原暂存容器不能替换")
            self._qualify(binding, qualify)
            await assert_namespace_keys_available(
                sql,
                {("directory", directory_key(stage.container_identity))},
                exclude_operation_id=attempt.operation_id,
            )
            row.stage_metadata = cast("dict[str, JsonValue]", value)
            await self._claim_locks(
                sql, row, attempt, {("directory", directory_key(stage.container_identity))}
            )
            row.cleanup_pending = True
            row.updated_at = utc_now()
            await sql.flush()
            await self._material_capacity(sql)
            self._qualify(binding, qualify)
            self._live_operation(row, attempt, fence, origin)
            self._live_observation(row)

    async def record_prepared(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        prepared: PreparedMissingDirectoryRestore,
        *,
        qualify: Callable[[], None],
    ) -> None:
        async with self._transaction() as sql:
            row, attempt, origin = await self._operating(sql, scope, fence, binding)
            if fence.kind != "execute" or attempt.intent_recorded or row.stage_metadata is None:
                raise _error("stage", "目录准备须经过原暂存准入")
            metadata, payload = encode_prepared(prepared)
            before = self._absence(row)
            target = (await self._checked_target(sql, row)).snapshot
            if (
                prepared.stage != decode_stage(row.stage_metadata, row.request_hash)
                or prepared.before != before
                or prepared.target_fingerprint != row.target_fingerprint
                or not WorkspaceDirectoryRestoreWriter._same_content(target, prepared.staged)
            ):
                raise _error("prepared", "完整暂存未保留原目标、完整备份或实际身份")
            if row.prepared_metadata is not None and (
                row.prepared_metadata != metadata or row.prepared_payload != payload
            ):
                raise _error("prepared", "原完整准备材料不能替换")
            self._qualify(binding, qualify)
            row.prepared_metadata, row.prepared_payload = (
                cast("dict[str, JsonValue]", metadata),
                payload,
            )
            keys = self._absence_keys(before) | snapshot_keys(prepared.staged)
            await assert_namespace_keys_available(
                sql, keys, exclude_operation_id=attempt.operation_id
            )
            await self._claim_locks(sql, row, attempt, keys)
            row.updated_at = utc_now()
            await sql.flush()
            await self._material_capacity(sql)
            self._qualify(binding, qualify)
            self._live_operation(row, attempt, fence, origin)

            self._live_observation(row)

    def _prepared(
        self, row: WorkspaceMissingDirectoryRestorePlanRow
    ) -> PreparedMissingDirectoryRestore:
        if (
            row.prepared_metadata is None
            or row.prepared_payload is None
            or row.stage_metadata is None
        ):
            raise _error("prepared_missing", "原完整目录准备尚未保存")
        prepared = decode_prepared(row.prepared_metadata, row.prepared_payload)
        if (
            prepared.stage != decode_stage(row.stage_metadata, row.request_hash)
            or prepared.stage.token != row.stage_token
            or prepared.target_fingerprint != row.target_fingerprint
            or prepared.before != self._absence(row)
            or prepared.before.workspace_fingerprint != row.workspace_fingerprint
        ):
            raise _corrupt()
        return prepared

    async def record_intent(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        prepared: PreparedMissingDirectoryRestore,
        *,
        qualify: Callable[[], None],
    ) -> None:
        async with self._transaction() as sql:
            row, attempt, origin = await self._operating(sql, scope, fence, binding)
            if fence.kind != "execute" or attempt.intent_recorded:
                raise _error("already_dispatched", "原目录派发意图只能记录一次")
            if self._prepared(row) != prepared:
                raise _error("prepared", "派发须绑定原完整准备材料")
            self._live_observation(row)
            keys = self._keys(row)
            await assert_namespace_keys_available(
                sql, keys, exclude_operation_id=attempt.operation_id
            )
            await self._claim_locks(sql, row, attempt, keys)
            self._qualify(binding, qualify)
            attempt.intent_recorded, attempt.updated_at = True, utc_now()
            await sql.flush()
            self._qualify(binding, qualify)
            self._live_operation(row, attempt, fence, origin)
            self._live_observation(row)

    async def settle(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        effect: MissingDirectoryRestoreEffect,
        *,
        qualify: Callable[[], None],
    ) -> MissingDirectoryRestoreView:
        async with self._transaction() as sql:
            row, attempt, origin = await self._operating(sql, scope, fence, binding)
            prepared = self._prepared(row)
            metadata = encode_effect(prepared, effect)
            status = effect.receipt.status
            if status != "rejected" and not attempt.intent_recorded:
                raise _error("intent_required", "原目录结果缺少持久派发意图")
            self._qualify(binding, qualify)
            lease = attempt.lease_expires_at or 0
            attempt.effect_metadata = cast("dict[str, JsonValue]", metadata)
            attempt.status, attempt.error_code = status, effect.receipt.error_code
            attempt.owner_id, attempt.lease_expires_at = None, None
            attempt.updated_at = utc_now()
            row.phase = {"done": "finished", "rejected": "failed", "uncertain": "uncertain"}[status]
            row.error_code, row.updated_at = effect.receipt.error_code, utc_now()
            row.revision += 1
            if status != "uncertain":
                await self._release_locks(sql, attempt)
            if status == "done":
                # 发布后仅私有空容器仍保留；原缺失基线无文件字节，不伪造完整树备份。
                row.staging_reserved_bytes, row.staging_reserved_entries = 0, 1
            await sql.flush()
            await self._material_capacity(sql)
            # 完整回执解码/投影也消耗原预算，必须在最后资格检查之前完成。
            result = await self._view(sql, row, attempt)
            self._qualify(binding, qualify)
            if lease <= time.time() or fence.kind == "execute" and row.expires_at <= time.time():
                raise _error("fenced", "回执保存前原操作资格已过期")
            return result

    @staticmethod
    def _end_prepare(
        row: WorkspaceMissingDirectoryRestorePlanRow, phase: str, status: str, code: str
    ) -> None:
        row.phase = phase
        row.error_code, row.owner_id, row.lease_expires_at = code, None, None
        row.updated_at = utc_now()
        row.revision += 1

    async def _end_operation(
        self,
        sql: AsyncSession,
        row: WorkspaceMissingDirectoryRestorePlanRow,
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow,
        phase: str,
        status: str,
        code: str,
    ) -> None:
        if attempt.intent_recorded:
            phase, status = "uncertain", "uncertain"
        attempt.status, attempt.error_code = status, code
        attempt.owner_id, attempt.lease_expires_at = None, None
        attempt.updated_at = utc_now()
        row.phase, row.error_code, row.updated_at = phase, code, utc_now()
        row.cleanup_pending = row.stage_metadata is not None
        if not row.cleanup_pending:
            row.staging_reserved_bytes = row.staging_reserved_entries = 0
        row.revision += 1
        if not attempt.intent_recorded:
            await self._release_locks(sql, attempt)

    async def _expire(
        self,
        sql: AsyncSession,
        row: WorkspaceMissingDirectoryRestorePlanRow,
        attempt: WorkspaceMissingDirectoryRestoreAttemptRow | None,
    ) -> bool:
        now = time.time()
        if row.phase == "preparing" and (
            (row.lease_expires_at or 0) <= now or row.expires_at <= now
        ):
            phase = "expired" if row.expires_at <= now else "interrupted"
            self._end_prepare(row, phase, phase, "workspace_missing_directory_restore_" + phase)
            return True
        observation_lost = False
        if row.phase == "ready":
            try:
                self._live_observation(row)
            except ServiceError as error:
                if error.code != "workspace_missing_directory_restore_observation_lost":
                    raise
                observation_lost = True
        if row.phase == "ready" and (row.expires_at <= now or observation_lost):
            phase = "expired" if row.expires_at <= now else "interrupted"
            row.phase, row.error_code, row.updated_at = (
                phase,
                "workspace_missing_directory_restore_" + phase,
                utc_now(),
            )
            row.revision += 1
            return True
        if (
            row.phase == "executing"
            and attempt is not None
            and attempt.status == "running"
            and (
                (attempt.lease_expires_at or 0) <= now
                or attempt.verification_count == 0
                and row.expires_at <= now
            )
        ):
            phase = (
                "expired"
                if attempt.verification_count == 0 and row.expires_at <= now
                else "interrupted"
            )
            await self._end_operation(
                sql, row, attempt, phase, phase, "workspace_missing_directory_restore_" + phase
            )
            return True
        return False

    async def cancel(self, scope: str, identifier: str) -> MissingDirectoryRestoreView:
        async with self._transaction() as sql:
            row, attempt, _origin = await self._base(sql, scope, identifier)
            await self._expire(sql, row, attempt)
            if row.phase == "uncertain" and row.cancel_requested:
                return await self._view(sql, row, attempt)
            if row.phase in {"preparing", "ready", "executing", "uncertain"}:
                row.cancel_requested = True
                if row.phase == "preparing":
                    self._end_prepare(
                        row,
                        "cancelled",
                        "cancelled",
                        "workspace_missing_directory_restore_cancelled",
                    )
                elif row.phase == "ready":
                    row.phase, row.error_code, row.updated_at = (
                        "cancelled",
                        "workspace_missing_directory_restore_cancelled",
                        utc_now(),
                    )
                    row.revision += 1
                elif attempt is not None:
                    attempt.cancelled = True
                    await self._end_operation(
                        sql,
                        row,
                        attempt,
                        "cancelled",
                        "cancelled",
                        "workspace_missing_directory_restore_cancelled",
                    )
            return await self._view(sql, row, attempt)

    async def fail(
        self, scope: str, fence: MissingDirectoryRestoreFence, code: str
    ) -> MissingDirectoryRestoreView:
        if not isinstance(code, str) or not code or len(code) > 128:
            raise _error("input", "目录恢复失败原因无效", 422)
        if type(fence) is not MissingDirectoryRestoreFence:
            raise _error("context", "目录恢复失败须绑定原私有资格", 403)
        async with self._transaction() as sql:
            row, attempt, origin = await self._base(sql, scope, fence.plan_id)
            if fence.kind == "prepare":
                self._live_prepare(row, fence)
                if fence.model_source != origin.preview:
                    raise _error("fenced", "目录准备失败来源不匹配")
                self._end_prepare(row, "failed", "rejected", code)
            else:
                if attempt is None:
                    raise _error("fenced", "目录执行资格不存在")
                self._live_operation(row, attempt, fence, origin)
                await self._end_operation(sql, row, attempt, "failed", "rejected", code)
            # 仅关闭原准入，不要求已被撤销的binding重新可用。
            return await self._view(sql, row, attempt)

    async def renew(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> None:
        if type(fence) is not MissingDirectoryRestoreFence:
            raise _error("context", "目录恢复续租需要原私有资格", 403)
        async with self._transaction() as sql:
            if fence.kind == "prepare":
                row = await self._preparing(sql, scope, fence, binding)
                previous = row.lease_expires_at or 0
                self._qualify(binding, qualify)
                row.lease_expires_at = time.time() + self.lease_seconds
            else:
                row, attempt, _origin = await self._operating(sql, scope, fence, binding)
                previous = attempt.lease_expires_at or 0
                self._qualify(binding, qualify)
                attempt.lease_expires_at = time.time() + self.lease_seconds
            await sql.flush()
            self._qualify(binding, qualify)
            if previous <= time.time() or fence.kind != "verify" and row.expires_at <= time.time():
                raise _error("fenced", "续租等待越过原截止，拒绝延长已失效资格")

    async def claim_verify(
        self,
        scope: str,
        identifier: str,
        binding: WorkspaceBinding,
        *,
        model_source: CheckpointModelSource | None = None,
        qualify: Callable[[], None],
    ) -> tuple[MissingDirectoryRestoreOperationView, MissingDirectoryRestoreFence | None]:
        model_source = valid_source(model_source)
        async with self._transaction() as sql:
            row, attempt, origin = await self._operation_row(sql, scope, identifier)
            self._bound(row, binding)
            await self._expire(sql, row, attempt)
            if attempt.status in {"done", "rejected", "cancelled", "interrupted", "expired"}:
                return await self._operation_view(sql, row, attempt), None
            if attempt.status != "uncertain" or not attempt.intent_recorded:
                raise _error("busy", "原目录操作尚未收束，不能重复派发或抢占核实")
            if model_source is not None and origin.verify == model_source:
                return await self._operation_view(sql, row, attempt), None
            if attempt.verification_count >= self.max_plans * 4:
                raise _error("capacity", "原目录操作核实次数已达上限", 429)
            self._qualify(binding, qualify)
            await self.edits._scope(
                sql, scope, writing=True, run_id=model_source.run_id if model_source else None
            )
            self._prepared(row)
            await self._assert_locks(sql, row, attempt)
            attempt.status, attempt.error_code = "running", None
            attempt.owner_id, attempt.lease_expires_at, attempt.token = (
                self.owner_id,
                time.time() + self.lease_seconds,
                secrets.token_hex(32),
            )
            attempt.verification_count += 1
            attempt.verification_model_count += int(model_source is not None)
            attempt.verify_model_bound = model_source is not None
            attempt.verification_run_id = model_source.run_id if model_source else None
            attempt.verification_call_id = model_source.call_id if model_source else None
            attempt.verification_hash = self._verification_hash(attempt, model_source)
            row.phase, row.error_code = "executing", None
            row.revision += 1
            row.updated_at = attempt.updated_at = utc_now()
            await admit(
                sql,
                self.edits,
                scope,
                row.plan_id,
                "verify",
                model_source,
                attempt.operation_id,
                max_sources=self.max_plans * 4,
            )
            await sql.flush()
            fence = MissingDirectoryRestoreFence(
                row.plan_id, attempt.operation_id, "verify", attempt.token, model_source
            )
            result = await self._operation_view(sql, row, attempt)
            self._qualify(binding, qualify)
            self._live_operation(
                row,
                attempt,
                fence,
                DirectoryRestoreSources(origin.preview, origin.execute, model_source),
            )
            return result, fence

    async def verification_input(
        self,
        scope: str,
        fence: MissingDirectoryRestoreFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> PreparedMissingDirectoryRestore:
        async with self._transaction() as sql:
            row, attempt, origin = await self._operating(sql, scope, fence, binding)
            if fence.kind != "verify" or not attempt.intent_recorded:
                raise _error("context", "核实只允许原operation的独立verify资格", 403)
            prepared = self._prepared(row)
            self._qualify(binding, qualify)
            self._live_operation(row, attempt, fence, origin)
            return prepared

    async def operation(self, scope: str, identifier: str) -> MissingDirectoryRestoreOperationView:
        async with self._transaction() as sql:
            row, attempt, _origin = await self._operation_row(sql, scope, identifier)
            await self._expire(sql, row, attempt)
            return await self._operation_view(sql, row, attempt)

    async def source(
        self, scope: str, identifier: str, phase: Literal["preview", "execute", "verify"]
    ) -> CheckpointModelSource | None:
        if phase not in {"preview", "execute", "verify"}:
            raise _error("input", "目录恢复来源阶段无效", 422)
        async with self._transaction() as sql:
            _row, _attempt, origin = await self._base(sql, scope, identifier)
            return {"preview": origin.preview, "execute": origin.execute, "verify": origin.verify}[
                phase
            ]

    async def cancel_model_admission(
        self,
        scope: str,
        source: CheckpointModelSource,
        phase: Literal["preview", "execute", "verify"],
        request: MissingDirectoryRestorePreview | MissingDirectoryRestoreExecute | str,
    ) -> MissingDirectoryRestoreView | None:
        """丢准入回程只核实原source及完整请求；同事务只关闭自己仍活跃的阶段。"""
        if valid_source(source) is None:
            raise _error("model_context", "模型收尾需要原父来源", 403)
        async with self._transaction() as sql:
            if phase == "preview" and type(request) is MissingDirectoryRestorePreview:
                selected = await sql.scalar(
                    select(WorkspaceMissingDirectoryRestorePlanRow).where(
                        WorkspaceMissingDirectoryRestorePlanRow.scope_id == scope,
                        WorkspaceMissingDirectoryRestorePlanRow.request_key == request.request_key,
                    )
                )
                identifier = selected.plan_id if selected is not None else None
            elif phase == "execute" and type(request) is MissingDirectoryRestoreExecute:
                selected = await sql.get(WorkspaceMissingDirectoryRestorePlanRow, request.plan_id)
                identifier = (
                    selected.plan_id
                    if selected is not None and selected.scope_id == scope
                    else None
                )
            elif phase == "verify" and type(request) is str:
                operation = await sql.get(WorkspaceMissingDirectoryRestoreAttemptRow, request)
                identifier = (
                    operation.plan_id
                    if operation is not None and operation.scope_id == scope
                    else None
                )
            else:
                raise _error("input", "模型收尾阶段与原请求类型不匹配", 422)
            if identifier is None:
                return None
            row, attempt, origin = await self._base(sql, scope, identifier)
            matched = (
                origin.preview == source and self._request(row) == request
                if phase == "preview"
                else attempt is not None
                and origin.execute == source
                and type(request) is MissingDirectoryRestoreExecute
                and attempt.request_hash == self._execute_hash(row, request, source)
                if phase == "execute"
                else attempt is not None
                and origin.verify == source
                and attempt.operation_id == request
            )
            if not matched:
                return None
            if phase == "preview" and row.phase == "preparing" and row.owner_id == self.owner_id:
                await self._expire(sql, row, attempt)
                if row.phase == "preparing":
                    row.cancel_requested = True
                    self._end_prepare(
                        row,
                        "cancelled",
                        "cancelled",
                        "workspace_missing_directory_restore_cancelled",
                    )
            elif (
                phase != "preview"
                and attempt is not None
                and ("verify" if attempt.verification_count else "execute") == phase
                and row.phase == "executing"
                and attempt.status == "running"
                and attempt.owner_id == self.owner_id
            ):
                await self._expire(sql, row, attempt)
                if attempt.status == "running":
                    row.cancel_requested = True
                    attempt.cancelled = True
                    await self._end_operation(
                        sql,
                        row,
                        attempt,
                        "cancelled",
                        "cancelled",
                        "workspace_missing_directory_restore_cancelled",
                    )
            return await self._view(sql, row, attempt)

    async def operation_by_key(
        self, scope: str, request_key: str
    ) -> MissingDirectoryRestoreOperationView | None:
        self.edits._key(request_key)
        async with self._transaction() as sql:
            await self.edits._scope(sql, scope, writing=False)
            attempt = await sql.scalar(
                select(WorkspaceMissingDirectoryRestoreAttemptRow).where(
                    WorkspaceMissingDirectoryRestoreAttemptRow.scope_id == scope,
                    WorkspaceMissingDirectoryRestoreAttemptRow.request_key == request_key,
                )
            )
            if attempt is None:
                return None
            row, attempt, _origin = await self._operation_row(sql, scope, attempt.operation_id)
            await self._expire(sql, row, attempt)
            return await self._operation_view(sql, row, attempt)

    async def list(
        self, scope: str, *, offset: int = 0, limit: int = 32
    ) -> list[MissingDirectoryRestoreView]:
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 32:
            raise _error("pagination", "目录恢复分页参数无效", 422)
        async with self._transaction() as sql:
            await self.edits._scope(sql, scope, writing=False)
            rows = list(
                await sql.scalars(
                    select(WorkspaceMissingDirectoryRestorePlanRow)
                    .where(WorkspaceMissingDirectoryRestorePlanRow.scope_id == scope)
                    .order_by(
                        WorkspaceMissingDirectoryRestorePlanRow.created_at.desc(),
                        WorkspaceMissingDirectoryRestorePlanRow.plan_id.desc(),
                    )
                    .offset(offset)
                    .limit(limit)
                )
            )
            result = []
            for selected in rows:
                row, attempt, _origin = await self._base(sql, scope, selected.plan_id)
                await self._expire(sql, row, attempt)
                result.append(await self._view(sql, row, attempt))
            return result

    async def recover_interrupted(self) -> int:
        async with self._transaction() as sql:
            rows = list(
                await sql.scalars(
                    select(WorkspaceMissingDirectoryRestorePlanRow).where(
                        WorkspaceMissingDirectoryRestorePlanRow.phase.in_(
                            ("preparing", "ready", "executing")
                        )
                    )
                )
            )
            changed = 0
            for selected in rows:
                row, attempt, _origin = await self._base(sql, selected.scope_id, selected.plan_id)
                changed += int(await self._expire(sql, row, attempt))
            return changed
