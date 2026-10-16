"""原terminal来源的独立回收账本；仅真实私有空容器，读回不重复命名空间操作。"""

from __future__ import annotations

import json
import math
import re
import secrets
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import uuid4

from pydantic import JsonValue
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_container_retention import (
    ContainerRetentionFence,
    ContainerRetentionReceipt,
    ContainerRetentionRequest,
    ContainerRetentionView,
    RetentionStatus,
    StoredContainerRetentionSource,
)
from krim_agent.workspace_directory_restore import IDENTIFIER
from krim_agent.workspace_empty_container_retention import (
    EmptyContainerObservation,
    EmptyContainerReceipt,
)

from . import container_retention_model_sources as model_origins
from .container_retention_sources import ContainerRetentionSources, corrupt, decode_origin, error
from .database import WorkspaceContainerRetentionLockRow as Lock
from .database import WorkspaceContainerRetentionRow as Row
from .database import WorkspaceMissingDirectoryRestorePlanRow as SourcePlan
from .directory_fences import directory_key, entry_key
from .directory_restore_capacity import material_usage
from .tree_move_fences import NamespaceKeys, assert_namespace_keys_available
from .workspace_edits import digest
from .workspace_missing_directory_restores import WorkspaceMissingDirectoryRestoreStore


def date(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class WorkspaceContainerRetentionStore:
    def __init__(
        self,
        restores: WorkspaceMissingDirectoryRestoreStore,
        *,
        owner_id: str | None = None,
        lease_seconds: float = 60,
        max_operations: int = 500,
        max_checks: int = 16,
        max_model_sources: int = 2000,
    ) -> None:
        if (
            type(lease_seconds) not in {int, float}
            or not 0.2 <= lease_seconds <= 600
            or type(max_operations) is not int
            or not 1 <= max_operations <= 5000
            or type(max_checks) is not int
            or not 1 <= max_checks <= 128
            or type(max_model_sources) is not int
            or not 1 <= max_model_sources <= 20000
            or owner_id is not None
            and (type(owner_id) is not str or re.fullmatch(IDENTIFIER, owner_id) is None)
        ):
            raise ValueError("回收租约、Owner或容量配置无效")
        self.restores, self.edits = restores, restores.edits
        self.database = self.edits.database
        self.sources = ContainerRetentionSources(restores)
        self.owner_id = owner_id if owner_id is not None else str(uuid4())
        self.max_model_sources = max_model_sources
        self.lease_seconds, self.max_operations, self.max_checks = (
            lease_seconds,
            max_operations,
            max_checks,
        )

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        async with self.database.write_lock, self.database.sessions() as sql:
            async with sql.begin():
                await self.edits._writer(sql)
                yield sql

    @staticmethod
    def _key(value: str) -> None:
        if (
            type(value) is not str
            or not 1 <= len(value) <= 128
            or any(ord(c) < 32 or ord(c) == 127 for c in value)
        ):
            raise error("parameters", "原回收请求键无效", 422)
        try:
            value.encode("utf-8")
        except UnicodeError:
            raise error("parameters", "原回收请求键无效", 422) from None

    @staticmethod
    def _request_hash(scope: str, request: ContainerRetentionRequest) -> str:
        return digest({"scope": scope, "request": request.model_dump(mode="json")})

    @staticmethod
    def _keys(source: StoredContainerRetentionSource) -> NamespaceKeys:
        stage = source.stage
        return {
            ("entry", entry_key(stage.parent_identity, stage.root_path)),
            ("directory", directory_key(stage.parent_identity)),
            ("directory", directory_key(stage.container_identity)),
        }

    @staticmethod
    def _request(row: Row) -> ContainerRetentionRequest:
        try:
            value = ContainerRetentionRequest.model_validate(row.request_metadata)
            if (
                value.model_dump(mode="json") != row.request_metadata
                or value.request_key != row.request_key
            ):
                raise corrupt()
            return value
        except (TypeError, ValueError, AttributeError):
            raise corrupt() from None

    @staticmethod
    def _receipt(row: Row) -> ContainerRetentionReceipt | None:
        if row.receipt_metadata is None:
            return None
        try:
            if len(json.dumps(row.receipt_metadata).encode()) > 4096:
                raise corrupt()
            value = dict(row.receipt_metadata)
            checksum = value.pop("material_sha256")
            if (
                type(value.get("version")) is not int
                or set(value)
                != {
                    "version",
                    "kind",
                    "operation_id",
                    "scope_id",
                    "source_hash",
                    "request_hash",
                    "epoch",
                    "receipt",
                }
                or checksum != digest(value)
            ):
                raise corrupt()
            if (
                value["version"],
                value["kind"],
                value["operation_id"],
                value["scope_id"],
                value["source_hash"],
                value["request_hash"],
            ) != (
                1,
                "workspace-container-retention-receipt",
                row.operation_id,
                row.scope_id,
                row.source_hash,
                row.request_hash,
            ):
                raise corrupt()
            epoch = value["epoch"]
            if (
                type(epoch) is not int
                or epoch < 0
                or epoch > row.epoch
                or epoch != row.epoch
                and not (row.status == "running" and row.active_kind == "verify")
            ):
                raise corrupt()
            return ContainerRetentionReceipt.model_validate(value["receipt"])
        except (KeyError, TypeError, ValueError, AttributeError):
            raise corrupt() from None

    @staticmethod
    def _encode_receipt(row: Row, receipt: ContainerRetentionReceipt) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "version": 1,
            "kind": "workspace-container-retention-receipt",
            "operation_id": row.operation_id,
            "scope_id": row.scope_id,
            "source_hash": row.source_hash,
            "request_hash": row.request_hash,
            "epoch": row.epoch,
            "receipt": receipt.model_dump(mode="json"),
        }
        value["material_sha256"] = digest(value)
        if len(json.dumps(value).encode()) > 4096:
            raise error("capacity", "回收回执超过4KiB边界", 429)
        return value

    def _view(self, row: Row, source: StoredContainerRetentionSource) -> ContainerRetentionView:
        try:
            self._history(row)
            if (
                re.fullmatch(r"[a-f0-9]{64}", row.token) is None
                or (row.status == "running")
                != (row.owner_id is not None and row.lease_expires_at is not None)
                or row.status == "running"
                and (
                    type(row.owner_id) is not str
                    or re.fullmatch(IDENTIFIER, row.owner_id) is None
                    or type(row.lease_expires_at) not in {int, float}
                    or row.lease_expires_at is None
                    or not math.isfinite(row.lease_expires_at)
                    or row.lease_expires_at <= 0
                )
                or type(row.epoch) is not int
                or not 0 <= row.epoch <= 9007199254740991
                or row.active_kind not in {"remove", "verify"}
                or row.active_kind == "verify"
                and row.verification_count < 1
                or (row.status in {"running", "done", "unknown"})
                != (row.active_source_id == row.source_plan_id)
                or row.active_source_id not in {None, row.source_plan_id}
                or not isinstance(row.verification_metadata, list)
                or len(row.verification_metadata) != row.verification_count
                or len(json.dumps(row.verification_metadata).encode()) > 65536
            ):
                raise corrupt()
            return ContainerRetentionView(
                operation_id=row.operation_id,
                source=source.view,
                request=self._request(row),
                status=cast(RetentionStatus, row.status),
                revision=row.revision,
                intent_recorded=row.intent_recorded,
                cancelled=row.cancel_requested,
                receipt=self._receipt(row),
                verification_count=row.verification_count,
                error_code=row.error_code,
                created_at=date(row.created_at),
                updated_at=date(row.updated_at),
            )
        except (TypeError, ValueError, AttributeError):
            raise corrupt() from None

    async def _base(
        self, sql: AsyncSession, scope: str, identifier: str
    ) -> tuple[Row, StoredContainerRetentionSource, model_origins.ContainerRetentionModelSources]:
        row = await sql.get(Row, identifier)
        if row is None or row.scope_id != scope:
            raise error("not_found", "容器回收操作不在此作用域", 404)
        origin = decode_origin(row.source_metadata, row.source_hash)
        source = await self.sources.read(
            sql,
            scope,
            row.source_plan_id,
            workspace_key=origin.workspace_key,
            resource_fingerprint=origin.resource_fingerprint,
            workspace_fingerprint=origin.workspace_fingerprint,
        )
        request = self._request(row)
        if (
            source.metadata != row.source_metadata
            or source.view.source_hash != row.source_hash
            or source.view.source_operation_id != row.source_operation_id
            or source.view.topic_id != row.topic_id
            or request.source_plan_id != row.source_plan_id
            or request.expected_source_hash != row.source_hash
            or row.request_hash != self._request_hash(scope, request)
        ):
            raise corrupt()
        self._view(row, source)
        origins = await model_origins.sources(sql, self.edits, row, self._history(row))
        return row, source, origins

    @staticmethod
    def _qualify(binding: WorkspaceBinding, qualify: Callable[[], None]) -> None:
        binding.require()
        qualify()
        binding.require()

    async def _live(
        self,
        sql: AsyncSession,
        scope: str,
        fence: ContainerRetentionFence,
        binding: WorkspaceBinding,
        qualify: Callable[[], None],
    ) -> tuple[Row, StoredContainerRetentionSource]:
        if type(fence) is not ContainerRetentionFence or type(fence.epoch) is not int:
            raise error("fenced", "回收须使用原私有Fence", 403)
        row, source, origins = await self._base(sql, scope, fence.operation_id)
        current_source = model_origins.model_source(fence.model_source)
        if current_source != origins.at(row.epoch):
            raise error("model_context", "回收私有围栏丢失或替换当前模型来源", 403)
        self._qualify(binding, qualify)
        await self.edits._scope(
            sql,
            scope,
            writing=True,
            run_id=current_source.run_id if current_source is not None else None,
        )
        if (
            (source.view.workspace_key, source.view.resource_fingerprint)
            != (binding.key, binding.fingerprint)
            or row.status != "running"
            or row.owner_id != self.owner_id
            or row.token != fence.token
            or row.epoch != fence.epoch
            or row.active_kind != fence.kind
            or (row.lease_expires_at or 0) <= time.time()
            or row.cancel_requested
            and fence.kind == "remove"
        ):
            raise error("fenced", "原回收Owner、租约、版本或取消资格失效")
        actual = set(
            (item.kind, item.lock_key)
            for item in await sql.scalars(select(Lock).where(Lock.operation_id == row.operation_id))
        )
        if actual != self._keys(source):
            raise error("lock_lost", "原回收命名空间围栏缺失或替换")
        await assert_namespace_keys_available(sql, actual, exclude_operation_id=row.operation_id)
        return row, source

    async def _last(
        self,
        sql: AsyncSession,
        scope: str,
        fence: ContainerRetentionFence,
        binding: WorkspaceBinding,
        qualify: Callable[[], None],
    ) -> None:
        await sql.flush()
        sql.expire_all()
        await self._live(sql, scope, fence, binding, qualify)

    async def get(self, scope: str, operation_id: str) -> ContainerRetentionView:
        async with self.database.write_lock, self.database.sessions() as sql:
            row, source, _origins = await self._base(sql, scope, operation_id)
            return self._view(row, source)

    async def model_sources(
        self, scope: str, operation_id: str
    ) -> model_origins.ContainerRetentionModelSources:
        async with self.database.write_lock, self.database.sessions() as sql:
            _row, _source, origins = await self._base(sql, scope, operation_id)
            return origins

    async def by_key(self, scope: str, request_key: str) -> ContainerRetentionView | None:
        self._key(request_key)
        async with self.database.write_lock, self.database.sessions() as sql:
            await self.edits._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(Row).where(Row.scope_id == scope, Row.request_key == request_key)
            )
            if row is None:
                return None
            row, source, _origins = await self._base(sql, scope, row.operation_id)
            return self._view(row, source)

    async def reserve(
        self,
        scope: str,
        binding: WorkspaceBinding,
        request: ContainerRetentionRequest,
        *,
        qualify: Callable[[], None],
        model_source: CheckpointModelSource | None = None,
    ) -> tuple[ContainerRetentionView, bool]:
        model_source = model_origins.model_source(model_source)
        try:
            if (
                type(request) is not ContainerRetentionRequest
                or type(binding) is not WorkspaceBinding
            ):
                raise ValueError("请求类型无效")
            request = ContainerRetentionRequest.model_validate(request.model_dump(mode="json"))
        except (TypeError, ValueError, AttributeError):
            raise error("parameters", "回收须使用严格原请求与绑定", 422) from None
        async with self._transaction() as sql:
            previous = await sql.scalar(
                select(Row).where(Row.scope_id == scope, Row.request_key == request.request_key)
            )
            if previous is not None:
                previous, source, origins = await self._base(sql, scope, previous.operation_id)
                if previous.request_hash != self._request_hash(scope, request):
                    raise error("key_conflict", "原回收键已绑定不同请求")
                if model_source is not None and origins.remove != model_source:
                    raise error("model_context", "模型不能冒认其他调用的原回收准入", 403)
                return self._view(previous, source), False
            self._qualify(binding, qualify)
            if model_source is not None and not scope.startswith("session:"):
                raise error("model_context", "模型回收需要原session", 403)
            await self.edits._scope(
                sql,
                scope,
                writing=True,
                run_id=model_source.run_id if model_source is not None else None,
            )
            source = await self.sources.read(
                sql,
                scope,
                request.source_plan_id,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=binding.workspace.fingerprint,
            )
            if source.view.source_hash != request.expected_source_hash:
                raise error("source_changed", "原审阅来源版本已改变")
            if (
                await sql.scalar(
                    select(Row.operation_id).where(Row.active_source_id == request.source_plan_id)
                )
                is not None
            ):
                raise error("source_busy", "原来源已有在途、未知或已完成回收，请读取原操作")
            count = int(await sql.scalar(select(func.count()).select_from(Row)) or 0)
            if count >= self.max_operations:
                raise error("capacity", "回收历史数量已满", 429)
            keys = self._keys(source)
            await assert_namespace_keys_available(sql, keys)
            row = Row(
                operation_id=str(uuid4()),
                source_plan_id=request.source_plan_id,
                source_operation_id=source.view.source_operation_id,
                active_source_id=request.source_plan_id,
                scope_id=scope,
                topic_id=source.view.topic_id,
                source_hash=source.view.source_hash,
                source_metadata=cast(dict[str, JsonValue], source.metadata),
                request_key=request.request_key,
                request_hash=self._request_hash(scope, request),
                request_metadata=request.model_dump(mode="json"),
                status="running",
                revision=0,
                epoch=0,
                active_kind="remove",
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                token=secrets.token_hex(32),
                intent_recorded=False,
                cancel_requested=False,
                receipt_metadata=None,
                verification_count=0,
                verification_metadata=[],
                error_code=None,
                created_at=utc_now(),
                updated_at=utc_now(),
            )
            sql.add(row)
            # ORM未声明relationship；先flush父记录，再写同事务围栏的外键。
            await sql.flush()
            for kind, key in sorted(keys):
                sql.add(Lock(operation_id=row.operation_id, kind=kind, lock_key=key))
            await sql.flush()
            await model_origins.admit(
                sql, self.edits, row, model_source, max_sources=self.max_model_sources
            )
            await sql.flush()
            if (
                await material_usage(sql, sqlite=self.database.engine.dialect.name == "sqlite")
                > self.restores.max_snapshot_bytes
            ):
                raise error("capacity", "回收与原恢复材料总容量已满", 429)
            result = self._view(row, source)
            fence = ContainerRetentionFence(
                row.operation_id, row.epoch, row.token, "remove", model_source
            )
            await self._last(sql, scope, fence, binding, qualify)
            return result, True

    async def execution(self, scope: str, operation_id: str) -> ContainerRetentionFence:
        async with self.database.write_lock, self.database.sessions() as sql:
            row, _source, _origins = await self._base(sql, scope, operation_id)
            if (
                row.status != "running"
                or row.owner_id != self.owner_id
                or (row.lease_expires_at or 0) <= time.time()
                or row.cancel_requested
            ):
                raise error("fenced", "原回收生产资格已失效")
            return ContainerRetentionFence(
                row.operation_id,
                row.epoch,
                row.token,
                cast(Literal["remove", "verify"], row.active_kind),
                _origins.at(row.epoch),
            )

    async def inputs(
        self,
        scope: str,
        fence: ContainerRetentionFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> StoredContainerRetentionSource:
        async with self._transaction() as sql:
            _row, source = await self._live(sql, scope, fence, binding, qualify)
            await self._last(sql, scope, fence, binding, qualify)
            return source

    async def assert_execution(
        self,
        scope: str,
        fence: ContainerRetentionFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> None:
        async with self.database.write_lock, self.database.sessions() as sql:
            await self._live(sql, scope, fence, binding, qualify)

    async def record_intent(
        self,
        scope: str,
        fence: ContainerRetentionFence,
        binding: WorkspaceBinding,
        observed: EmptyContainerObservation,
        *,
        qualify: Callable[[], None],
    ) -> None:
        async with self._transaction() as sql:
            row, source = await self._live(sql, scope, fence, binding, qualify)
            request = self._request(row)
            if (
                fence.kind != "remove"
                or row.intent_recorded
                or type(observed) is not EmptyContainerObservation
                or observed.stage != source.stage
                or observed.status != "empty"
                or observed.fingerprint != request.expected_container_fingerprint
                or observed.error_code is not None
            ):
                raise error("intent", "回收派发须绑定原审阅空容器和唯一意图")
            row.intent_recorded = True
            row.revision += 1
            row.updated_at = utc_now()
            await self._last(sql, scope, fence, binding, qualify)

    async def settle(
        self,
        scope: str,
        fence: ContainerRetentionFence,
        binding: WorkspaceBinding,
        effect: EmptyContainerReceipt,
        *,
        qualify: Callable[[], None],
    ) -> ContainerRetentionView:
        async with self._transaction() as sql:
            row, source = await self._live(sql, scope, fence, binding, qualify)
            request = self._request(row)
            if (
                fence.kind != "remove"
                or type(effect) is not EmptyContainerReceipt
                or effect.stage != source.stage
                or effect.expected_fingerprint != request.expected_container_fingerprint
            ):
                raise error("effect", "回收回执未绑定原Stage与审阅版本")
            receipt = ContainerRetentionReceipt(
                outcome=effect.status,
                expected_fingerprint=effect.expected_fingerprint,
                observed_fingerprint=effect.observed_fingerprint,
                changed=effect.changed,
                recovery_required=effect.recovery_required,
                error_code=effect.error_code,
            )
            if receipt.outcome in {"removed", "unknown"} and not row.intent_recorded:
                raise error("intent", "物理回收缺少原持久派发意图")
            lease = row.lease_expires_at or 0
            row.receipt_metadata = self._encode_receipt(row, receipt)
            row.status = (
                "done" if receipt.outcome in {"removed", "already_absent"} else receipt.outcome
            )
            row.error_code = receipt.error_code
            row.owner_id = row.lease_expires_at = None
            row.revision += 1
            row.updated_at = utc_now()
            if row.status == "rejected":
                row.active_source_id = None
            if row.status != "unknown":
                await sql.execute(delete(Lock).where(Lock.operation_id == row.operation_id))
            if row.status == "done":
                original = await sql.get(SourcePlan, row.source_plan_id)
                if (
                    original is None
                    or original.staging_reserved_bytes != 0
                    or original.staging_reserved_entries not in {0, 1}
                ):
                    raise corrupt()
                original.staging_reserved_entries = 0
            await sql.flush()
            result = self._view(row, source)
            self._qualify(binding, qualify)
            current_source = model_origins.model_source(fence.model_source)
            await self.edits._scope(
                sql,
                scope,
                writing=True,
                run_id=current_source.run_id if current_source is not None else None,
            )
            if lease <= time.time():
                raise error("fenced", "保存回收结果前原租约已失效")
            return result

    @staticmethod
    def _check_hash(row: Row, value: dict[str, JsonValue]) -> str:
        return digest(
            {
                "operation_id": row.operation_id,
                "scope_id": row.scope_id,
                "source_hash": row.source_hash,
                "check": value,
            }
        )

    def _history(self, row: Row) -> list[dict[str, JsonValue]]:
        try:
            if (
                type(row.verification_metadata) is not list
                or len(row.verification_metadata) != row.verification_count
                or not 0 <= len(row.verification_metadata) <= 128
                or row.epoch != row.verification_count
                or row.active_kind != ("verify" if row.verification_count else "remove")
            ):
                raise corrupt()
            if len(json.dumps(row.verification_metadata).encode()) > 65536:
                raise corrupt()
            result = []
            keys: set[str] = set()
            for index, stored in enumerate(row.verification_metadata, 1):
                value = dict(stored)
                checksum = value.pop("material_sha256")
                if set(value) != {
                    "request_key",
                    "request_hash",
                    "expected_revision",
                    "epoch",
                    "status",
                    "receipt",
                    "error_code",
                    "created_at",
                    "updated_at",
                } or checksum != self._check_hash(row, value):
                    raise corrupt()
                key = value["request_key"]
                if type(key) is not str or key in keys:
                    raise corrupt()
                self._key(key)
                keys.add(key)
                revision = value["expected_revision"]
                if (
                    type(revision) is not int
                    or not 0 <= revision <= 9007199254740991
                    or type(value["epoch"]) is not int
                    or value["epoch"] != index
                ):
                    raise corrupt()
                if value["request_hash"] != digest(
                    {
                        "operation_id": row.operation_id,
                        "expected_revision": revision,
                        "request_key": key,
                    }
                ):
                    raise corrupt()
                if type(value["created_at"]) is not str or type(value["updated_at"]) is not str:
                    raise corrupt()
                start, finish = (
                    datetime.fromisoformat(value["created_at"]),
                    datetime.fromisoformat(value["updated_at"]),
                )
                if start.utcoffset() is None or finish.utcoffset() is None or finish < start:
                    raise corrupt()
                if value["status"] == "running":
                    if (
                        index != row.verification_count
                        or row.status != "running"
                        or value["receipt"] is not None
                        or value["error_code"] is not None
                    ):
                        raise corrupt()
                else:
                    receipt = ContainerRetentionReceipt.model_validate(value["receipt"])
                    if (
                        value["status"] not in {"done", "unknown", "rejected"}
                        or receipt.error_code != value["error_code"]
                        or receipt.expected_fingerprint
                        != self._request(row).expected_container_fingerprint
                    ):
                        raise corrupt()
                    if (
                        value["status"] == "done"
                        and receipt.outcome != "absence_verified"
                        or value["status"] == "unknown"
                        and receipt.outcome != "unknown"
                        or value["status"] == "rejected"
                        and receipt.outcome != "rejected"
                    ):
                        raise corrupt()
                if (
                    index == row.verification_count
                    and row.status != "running"
                    and (
                        value["status"] != row.status
                        or ContainerRetentionReceipt.model_validate(value["receipt"])
                        != self._receipt(row)
                    )
                ):
                    raise corrupt()
                result.append(dict(stored))
            return result
        except (KeyError, ValueError, TypeError, AttributeError):
            raise corrupt() from None

    def _finish_check(self, row: Row, receipt: ContainerRetentionReceipt) -> None:
        if row.active_kind != "verify":
            return
        history = [dict(item) for item in row.verification_metadata]
        if not history or history[-1]["epoch"] != row.epoch or history[-1]["status"] != "running":
            raise corrupt()
        value = dict(history[-1])
        value.pop("material_sha256")
        value.update(
            status=row.status,
            receipt=receipt.model_dump(mode="json"),
            error_code=row.error_code,
            updated_at=date(row.updated_at).isoformat(),
        )
        value["material_sha256"] = self._check_hash(row, value)
        history[-1] = value
        row.verification_metadata = history

    async def _end(self, sql: AsyncSession, row: Row, code: str) -> None:
        if row.intent_recorded:
            row.status = "unknown"
            receipt = ContainerRetentionReceipt(
                outcome="unknown",
                expected_fingerprint=self._request(row).expected_container_fingerprint,
                observed_fingerprint=None,
                changed=True,
                recovery_required=True,
                error_code=code,
            )
            row.receipt_metadata = self._encode_receipt(row, receipt)
        else:
            row.status = "cancelled" if row.cancel_requested else "interrupted"
            row.active_source_id = None
            receipt = None
        row.error_code, row.owner_id, row.lease_expires_at = code, None, None
        row.revision += 1
        row.updated_at = utc_now()
        if not row.intent_recorded:
            await sql.execute(delete(Lock).where(Lock.operation_id == row.operation_id))
        if receipt is not None:
            self._finish_check(row, receipt)

    async def cancel(
        self,
        scope: str,
        operation_id: str,
        binding: WorkspaceBinding,
        *,
        expected_revision: int,
        qualify: Callable[[], None],
    ) -> ContainerRetentionView:
        if type(expected_revision) is not int or not 0 <= expected_revision <= 9007199254740989:
            raise error("parameters", "回收取消须使用实际原版本", 422)
        async with self._transaction() as sql:
            row, source, _origins = await self._base(sql, scope, operation_id)
            if row.status not in {"running", "unknown"} or row.cancel_requested:
                return self._view(row, source)
            self._qualify(binding, qualify)
            await self.edits._scope(sql, scope, writing=True)
            if (source.view.workspace_key, source.view.resource_fingerprint) != (
                binding.key,
                binding.fingerprint,
            ):
                raise error("source", "取消原回收的资源不符", 403)
            if row.revision != expected_revision:
                raise error("revision", "回收取消版本已改变")
            row.cancel_requested = True
            if row.status == "running":
                await self._end(sql, row, "workspace_container_retention_cancelled")
            else:
                row.revision += 1
                row.updated_at = utc_now()
            await sql.flush()
            result = self._view(row, source)
            self._qualify(binding, qualify)
            await self.edits._scope(sql, scope, writing=True)
            return result

    async def fail(
        self, scope: str, fence: ContainerRetentionFence, code: str
    ) -> ContainerRetentionView:
        if (
            type(fence) is not ContainerRetentionFence
            or type(code) is not str
            or not 1 <= len(code) <= 128
        ):
            raise error("parameters", "回收收束须使用原Fence和原因", 422)
        async with self._transaction() as sql:
            row, source, _origins = await self._base(sql, scope, fence.operation_id)
            if row.status != "running":
                return self._view(row, source)
            if (
                model_origins.model_source(fence.model_source) != _origins.at(row.epoch)
                or row.owner_id != self.owner_id
                or row.token != fence.token
                or row.epoch != fence.epoch
                or row.active_kind != fence.kind
            ):
                raise error("fenced", "旧回收生产者不能收束新操作")
            await self._end(sql, row, code)
            await sql.flush()
            return self._view(row, source)

    async def recover(self) -> int:
        async with self._transaction() as sql:
            identifiers = list(
                await sql.scalars(
                    select(Row.operation_id)
                    .where(Row.status == "running", Row.lease_expires_at <= time.time())
                    .order_by(Row.operation_id)
                    .limit(self.max_operations)
                )
            )
            for identifier in identifiers:
                row = await sql.get(Row, identifier)
                if row is None:
                    raise corrupt()
                row, _source, _origins = await self._base(sql, row.scope_id, row.operation_id)
                await self._end(sql, row, "workspace_container_retention_interrupted")
            await sql.flush()
            return len(identifiers)

    async def renew(
        self,
        scope: str,
        fence: ContainerRetentionFence,
        binding: WorkspaceBinding,
        *,
        qualify: Callable[[], None],
    ) -> None:
        async with self._transaction() as sql:
            row, _source = await self._live(sql, scope, fence, binding, qualify)
            row.lease_expires_at = time.time() + self.lease_seconds
            await self._last(sql, scope, fence, binding, qualify)

    async def claim_verify(
        self,
        scope: str,
        operation_id: str,
        binding: WorkspaceBinding,
        *,
        expected_revision: int,
        request_key: str,
        qualify: Callable[[], None],
        model_source: CheckpointModelSource | None = None,
    ) -> tuple[ContainerRetentionView, ContainerRetentionFence | None]:
        model_source = model_origins.model_source(model_source)
        self._key(request_key)
        if type(expected_revision) is not int or not 0 <= expected_revision <= 9007199254740989:
            raise error("parameters", "核实回收须使用实际原版本", 422)
        checksum = digest(
            {
                "operation_id": operation_id,
                "expected_revision": expected_revision,
                "request_key": request_key,
            }
        )
        async with self._transaction() as sql:
            row, source, _origins = await self._base(sql, scope, operation_id)
            history = self._history(row)
            previous = next((item for item in history if item["request_key"] == request_key), None)
            if previous is not None:
                if previous["request_hash"] != checksum:
                    raise error("key_conflict", "原核实键已绑定不同请求")
                epoch = previous["epoch"]
                assert type(epoch) is int
                if model_source is not None and _origins.at(epoch) != model_source:
                    raise error("model_context", "模型不能冒认原核实调用", 403)
                return self._view(row, source), None
            if row.status in {"done", "rejected", "cancelled", "interrupted"}:
                return self._view(row, source), None
            if row.status != "unknown":
                raise error("busy", "原回收仍在执行，不能重复核实")
            if row.revision != expected_revision:
                raise error("revision", "原回收核实版本已改变")
            if row.verification_count >= self.max_checks:
                raise error("capacity", "回收核实次数已满", 429)
            self._qualify(binding, qualify)
            if model_source is not None and not scope.startswith("session:"):
                raise error("model_context", "模型核实须保留原session", 403)
            await self.edits._scope(
                sql,
                scope,
                writing=True,
                run_id=model_source.run_id if model_source is not None else None,
            )
            if (source.view.workspace_key, source.view.resource_fingerprint) != (
                binding.key,
                binding.fingerprint,
            ):
                raise error("source", "原核实资源不符", 403)
            actual = set(
                (item.kind, item.lock_key)
                for item in await sql.scalars(
                    select(Lock).where(Lock.operation_id == row.operation_id)
                )
            )
            if actual != self._keys(source):
                raise error("lock_lost", "原未知回收命名空间围栏缺失")
            await assert_namespace_keys_available(
                sql, actual, exclude_operation_id=row.operation_id
            )
            row.epoch += 1
            row.revision += 1
            row.verification_count += 1
            row.active_kind = "verify"
            row.status, row.error_code = "running", None
            row.owner_id, row.lease_expires_at, row.token = (
                self.owner_id,
                time.time() + self.lease_seconds,
                secrets.token_hex(32),
            )
            row.updated_at = utc_now()
            item: dict[str, JsonValue] = {
                "request_key": request_key,
                "request_hash": checksum,
                "expected_revision": expected_revision,
                "epoch": row.epoch,
                "status": "running",
                "receipt": None,
                "error_code": None,
                "created_at": date(row.updated_at).isoformat(),
                "updated_at": date(row.updated_at).isoformat(),
            }
            item["material_sha256"] = self._check_hash(row, item)
            row.verification_metadata = [*history, item]
            await sql.flush()
            await model_origins.admit(
                sql, self.edits, row, model_source, max_sources=self.max_model_sources
            )
            await sql.flush()
            if (
                await material_usage(sql, sqlite=self.database.engine.dialect.name == "sqlite")
                > self.restores.max_snapshot_bytes
            ):
                raise error("capacity", "模型核实与原材料总容量已满", 429)
            if len(json.dumps(row.verification_metadata).encode()) > 65536:
                raise error("capacity", "回收核实历史JSON容量已满", 429)
            result = self._view(row, source)
            fence = ContainerRetentionFence(
                row.operation_id, row.epoch, row.token, "verify", model_source
            )
            await self._last(sql, scope, fence, binding, qualify)
            return result, fence

    async def finish_verify(
        self,
        scope: str,
        fence: ContainerRetentionFence,
        binding: WorkspaceBinding,
        observed: EmptyContainerObservation,
        *,
        qualify: Callable[[], None],
    ) -> ContainerRetentionView:
        async with self._transaction() as sql:
            row, source = await self._live(sql, scope, fence, binding, qualify)
            if (
                fence.kind != "verify"
                or type(observed) is not EmptyContainerObservation
                or observed.stage != source.stage
            ):
                raise error("effect", "核实须使用原Stage的只读观察")
            expected = self._request(row).expected_container_fingerprint
            if (
                observed.status == "absent"
                and observed.fingerprint is not None
                and observed.error_code is None
            ):
                row.status = "done"
                receipt = ContainerRetentionReceipt(
                    outcome="absence_verified",
                    expected_fingerprint=expected,
                    observed_fingerprint=observed.fingerprint,
                    changed=False,
                    recovery_required=False,
                    error_code=None,
                )
            elif (
                observed.status == "empty"
                and observed.fingerprint == expected
                and observed.error_code is None
            ):
                row.status = "rejected"
                receipt = ContainerRetentionReceipt(
                    outcome="rejected",
                    expected_fingerprint=expected,
                    observed_fingerprint=observed.fingerprint,
                    changed=False,
                    recovery_required=False,
                    error_code="workspace_container_retention_not_removed",
                )
                row.active_source_id = None
            else:
                row.status = "unknown"
                receipt = ContainerRetentionReceipt(
                    outcome="unknown",
                    expected_fingerprint=expected,
                    observed_fingerprint=observed.fingerprint,
                    changed=True,
                    recovery_required=True,
                    error_code=observed.error_code or "workspace_container_retention_changed",
                )
            lease = row.lease_expires_at or 0
            row.receipt_metadata = self._encode_receipt(row, receipt)
            row.error_code = receipt.error_code
            row.owner_id = row.lease_expires_at = None
            row.revision += 1
            row.updated_at = utc_now()
            self._finish_check(row, receipt)
            if row.status != "unknown":
                await sql.execute(delete(Lock).where(Lock.operation_id == row.operation_id))
            if row.status == "done":
                original = await sql.get(SourcePlan, row.source_plan_id)
                if (
                    original is None
                    or original.staging_reserved_bytes != 0
                    or original.staging_reserved_entries not in {0, 1}
                ):
                    raise corrupt()
                original.staging_reserved_entries = 0
            await sql.flush()
            result = self._view(row, source)
            self._qualify(binding, qualify)
            current_source = model_origins.model_source(fence.model_source)
            await self.edits._scope(
                sql,
                scope,
                writing=True,
                run_id=current_source.run_id if current_source is not None else None,
            )
            if lease <= time.time():
                raise error("fenced", "保存核实结果前原租约已失效")
            return result
