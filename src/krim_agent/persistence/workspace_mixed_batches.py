"""混合提案批次的冻结、原事务围栏及原文件/目录账本投影。"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import uuid4

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace_change import WorkspaceAttemptView, WorkspaceChangeAction
from krim_agent.workspace_directory import DirectoryAttemptView, DirectoryChangeAction
from krim_agent.workspace_mixed_batch import (
    MixedBatchAction,
    MixedBatchDispatch,
    MixedBatchExecution,
    MixedBatchFence,
    MixedBatchHistoryPage,
    MixedBatchItemStatus,
    MixedBatchItemView,
    MixedBatchModelSource,
    MixedBatchRequest,
    MixedBatchStatus,
    MixedBatchSummary,
    MixedBatchTarget,
    MixedBatchView,
)

from .database import (
    MixedWorkspaceBatchChildRow,
    MixedWorkspaceBatchModelSourceRow,
    MixedWorkspaceBatchRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
)
from .directory_fences import entry_key
from .mixed_batch_fences import MIXED_REQUEST_PREFIX, assert_live_execution
from .workspace_batches import (
    _MAX_HISTORY,
    _batch_status,
    _decode_history_cursor,
    _history_cursor,
)
from .workspace_directories import WorkspaceDirectoryStore
from .workspace_edits import WorkspaceEditStore, digest

_MIXED_HISTORY_PREFIX = "krim-mixed-history:"


def _mixed_history_error() -> ServiceError:
    return ServiceError(
        "workspace_mixed_batch_history_cursor_invalid", "混合批次游标无效或作用域不符", 422
    )


@dataclass(frozen=True)
class _Frozen:
    source: Literal["file", "directory"]
    proposal_id: str
    revision: int
    path: str
    target_path: str | None
    physical_keys: tuple[str, ...]
    snapshot_hash: str


class MixedBatchStore:
    def __init__(
        self,
        files: WorkspaceEditStore,
        directories: WorkspaceDirectoryStore,
        *,
        lease_seconds: float = 60,
        max_batches: int = 500,
    ) -> None:
        if files.database is not directories.database or files.owner_id != directories.owner_id:
            raise ValueError("混合批次须共用同一Database与owner")
        if not 0.2 <= lease_seconds <= 600 or not 1 <= max_batches <= 5000:
            raise ValueError("混合批次租约或容量无效")
        self.files, self.directories = files, directories
        self.database, self.owner_id = files.database, files.owner_id
        self.lease_seconds, self.max_batches = lease_seconds, max_batches

    async def _freeze(
        self,
        sql: AsyncSession,
        scope: str,
        key: str,
        resource: str,
        action: MixedBatchAction,
        target: MixedBatchTarget,
    ) -> _Frozen:
        keys: tuple[str, ...]
        if target.source == "file":
            row = await self.files._proposal(sql, scope, target.proposal_id)
            file = self.files._change(row)
            from .command_proposals import assert_command_proposal

            await assert_command_proposal(sql, row, file.prepared, applying=action == "apply")
            path = file.prepared.spec.path
            if file.view.preview.path != path:
                raise ServiceError(
                    "workspace_mixed_batch_corrupt", "文件展示路径与原私有规格不符", 409
                )
            current_key, current_resource = row.workspace_key, row.resource_fingerprint
            revision, status = row.revision, row.status
            expires, active = row.expires_at, row.active_operation_id
            snapshot_hash = str(row.prepared["snapshot_hash"])
            target_path = None
            keys = (entry_key(file.prepared.parent_identity, path),)
        else:
            directory_row = await sql.get(WorkspaceDirectoryProposalRow, target.proposal_id)
            if directory_row is None or directory_row.scope_id != scope:
                raise ServiceError("workspace_directory_not_found", "目录提案不在该作用域", 404)
            directory = self.directories._view(directory_row)
            from .command_directory_proposals import assert_directory_source

            await assert_directory_source(sql, directory_row)
            path, target_path = directory.view.path, directory.view.target_path
            current_key, current_resource = (
                directory_row.workspace_key,
                directory_row.resource_fingerprint,
            )
            revision, status = directory_row.revision, directory_row.status
            expires, active = directory_row.expires_at, directory_row.active_operation_id
            snapshot_hash = digest(directory_row.prepared)
            keys = (entry_key(directory.prepared.parent_identity, path),)
            if target_path is not None:
                assert directory.prepared.target_parent_identity is not None
                keys += (entry_key(directory.prepared.target_parent_identity, target_path),)
        if current_key != key or current_resource != resource:
            raise ServiceError("workspace_binding_changed", "批次目标不属于原资源版本", 403)
        if revision != target.expected_revision or revision >= 9007199254740989:
            raise ServiceError("workspace_revision_conflict", "批次目标修订号已变化或达到上限", 409)
        allowed = {"apply": {"proposed"}, "keep": {"applied"}, "undo": {"applied", "kept"}}
        if active is not None or status not in allowed[action]:
            raise ServiceError("workspace_mixed_batch_state", "批次目标不允许该动作", 409)
        if action == "apply" and expires <= time.time():
            raise ServiceError("workspace_mixed_batch_state", "批次提案已过期", 409)
        return _Frozen(
            target.source, target.proposal_id, revision, path, target_path, keys, snapshot_hash
        )

    @staticmethod
    def _independent(items: list[_Frozen]) -> None:
        seen: set[str] = set()
        for item in items:
            if seen.intersection(item.physical_keys):
                raise ServiceError("workspace_mixed_batch_dependency", "批次物理entry重叠", 409)
            seen.update(item.physical_keys)
        for parent in items:
            if parent.source != "directory":
                continue
            for path in (parent.path, parent.target_path):
                if path is None:
                    continue
                for child in items:
                    if parent is child:
                        continue
                    if any(
                        value is not None and (value == path or value.startswith(path + "/"))
                        for value in (child.path, child.target_path)
                    ):
                        raise ServiceError(
                            "workspace_mixed_batch_dependency", "目录后代须使用依赖计划", 409
                        )

    async def reserve(
        self,
        scope: str,
        workspace_key: str,
        resource: str,
        request: MixedBatchRequest,
        *,
        model_source: MixedBatchModelSource | None = None,
    ) -> tuple[MixedBatchView, bool]:
        if model_source is not None:
            if not model_source.run_id or model_source.run_id == "local":
                raise ServiceError(
                    "workspace_mixed_batch_run_mismatch", "模型批次必须绑定原Run", 403
                )
            self.files._key(model_source.call_id)
        request = MixedBatchRequest.model_validate(request.model_dump())
        wanted = digest(
            {"request": request.model_dump(mode="json"), "key": workspace_key, "resource": resource}
        )
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=True)
            await self.files._resource(sql, scope, workspace_key, resource)
            previous = await sql.scalar(
                select(MixedWorkspaceBatchRow).where(
                    MixedWorkspaceBatchRow.scope_id == scope,
                    MixedWorkspaceBatchRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != wanted:
                    raise ServiceError(
                        "workspace_mixed_batch_idempotency_conflict", "批次键已绑定其他参数", 409
                    )
                if model_source is not None:
                    source = await sql.get(MixedWorkspaceBatchModelSourceRow, previous.batch_id)
                    if source is None or (source.run_id, source.call_id) != (
                        model_source.run_id,
                        model_source.call_id,
                    ):
                        raise ServiceError(
                            "workspace_mixed_batch_run_conflict", "原请求键已绑定其他Run/call", 409
                        )
                await self._expire(sql, previous)
                return await self._view(sql, previous), False
            if model_source is not None:
                await self.files._scope(sql, scope, writing=True, run_id=model_source.run_id)
                previous_call = await sql.scalar(
                    select(MixedWorkspaceBatchModelSourceRow).where(
                        MixedWorkspaceBatchModelSourceRow.run_id == model_source.run_id,
                        MixedWorkspaceBatchModelSourceRow.call_id == model_source.call_id,
                    )
                )
                if previous_call is not None:
                    raise ServiceError(
                        "workspace_mixed_batch_call_conflict", "同一模型调用不能换键重新接受", 409
                    )
            active = await sql.scalar(
                select(MixedWorkspaceBatchRow)
                .where(
                    MixedWorkspaceBatchRow.scope_id == scope,
                    MixedWorkspaceBatchRow.producer_status == "running",
                )
                .limit(1)
            )
            if active is not None:
                await self._expire(sql, active)
                if active.producer_status == "running":
                    raise ServiceError("workspace_mixed_batch_busy", "作用域已有运行中的批次", 409)
            count = await sql.scalar(select(func.count()).select_from(MixedWorkspaceBatchRow))
            if (count or 0) >= self.max_batches:
                raise ServiceError("workspace_mixed_batch_limit", "混合批次历史已达上限", 429)
            frozen = [
                await self._freeze(sql, scope, workspace_key, resource, request.action, item)
                for item in request.targets
            ]
            self._independent(frozen)
            snapshot = digest(
                {
                    "key": workspace_key,
                    "resource": resource,
                    "targets": [
                        (
                            x.source,
                            x.proposal_id,
                            x.revision,
                            x.path,
                            x.target_path,
                            x.physical_keys,
                            x.snapshot_hash,
                        )
                        for x in frozen
                    ],
                }
            )
            if request.snapshot_revision is not None and request.snapshot_revision != snapshot:
                raise ServiceError("workspace_mixed_batch_snapshot_conflict", "批次快照不符", 409)
            identifier = str(uuid4())
            row = MixedWorkspaceBatchRow(
                batch_id=identifier,
                scope_id=scope,
                workspace_key=workspace_key,
                resource_fingerprint=resource,
                request_key=request.request_key,
                request_hash=wanted,
                snapshot_revision=snapshot,
                action=request.action,
                producer_status="running",
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                token=secrets.token_hex(32),
                stop_reason=None,
                created_at=utc_now(),
                updated_at=utc_now(),
            )
            sql.add(row)
            await sql.flush()
            if model_source is not None:
                sql.add(
                    MixedWorkspaceBatchModelSourceRow(
                        batch_id=identifier,
                        run_id=model_source.run_id,
                        call_id=model_source.call_id,
                    )
                )
                await sql.flush()
            for position, item in enumerate(frozen):
                sql.add(
                    MixedWorkspaceBatchChildRow(
                        batch_id=identifier,
                        position=position,
                        source=item.source,
                        proposal_id=item.proposal_id,
                        file_proposal_id=item.proposal_id if item.source == "file" else None,
                        directory_proposal_id=item.proposal_id
                        if item.source == "directory"
                        else None,
                        expected_revision=item.revision,
                        path=item.path,
                        target_path=item.target_path,
                        physical_keys=list(item.physical_keys),
                        request_key=f"{MIXED_REQUEST_PREFIX}{identifier}:{position}",
                        file_operation_id=None,
                        directory_operation_id=None,
                        admission_open=True,
                        status="pending",
                        error_code=None,
                    )
                )
            await sql.flush()
            return await self._view(sql, row), True

    @staticmethod
    async def _children(sql: AsyncSession, identifier: str) -> list[MixedWorkspaceBatchChildRow]:
        return list(
            await sql.scalars(
                select(MixedWorkspaceBatchChildRow)
                .where(
                    MixedWorkspaceBatchChildRow.batch_id == identifier,
                )
                .order_by(MixedWorkspaceBatchChildRow.position)
            )
        )

    async def _item(
        self, sql: AsyncSession, parent: MixedWorkspaceBatchRow, child: MixedWorkspaceBatchChildRow
    ) -> MixedBatchItemView:
        operation: WorkspaceAttemptView | DirectoryAttemptView | None = None
        proposal: WorkspaceEditProposalRow | WorkspaceDirectoryProposalRow
        attempt: WorkspaceEditAttemptRow | WorkspaceDirectoryAttemptRow | None
        keys: tuple[str, ...]
        status = cast(MixedBatchItemStatus, child.status)
        error_code = child.error_code
        identifier = (
            child.file_operation_id if child.source == "file" else child.directory_operation_id
        )
        if child.source == "file":
            proposal = await self.files._proposal(sql, parent.scope_id, child.proposal_id)
            file_original = self.files._change(proposal).prepared
            keys = (entry_key(file_original.parent_identity, file_original.spec.path),)
            path, target_path = file_original.spec.path, None
            attempt = await sql.get(WorkspaceEditAttemptRow, identifier) if identifier else None
            if attempt is not None:
                operation = self.files._attempt(cast(WorkspaceEditAttemptRow, attempt)).view
        else:
            proposal = await self.directories._proposal(sql, parent.scope_id, child.proposal_id)
            directory = self.directories._view(proposal)
            directory_original = directory.prepared
            path, target_path = directory.view.path, directory.view.target_path
            keys = (entry_key(directory_original.parent_identity, path),)
            if target_path is not None:
                assert directory_original.target_parent_identity is not None
                keys += (entry_key(directory_original.target_parent_identity, target_path),)
            attempt = (
                await sql.get(WorkspaceDirectoryAttemptRow, identifier) if identifier else None
            )
            if attempt is not None:
                operation = self.directories._attempt(
                    cast(WorkspaceDirectoryAttemptRow, attempt)
                ).view
        if (
            child.source not in {"file", "directory"}
            or proposal.workspace_key != parent.workspace_key
            or proposal.resource_fingerprint != parent.resource_fingerprint
            or path != child.path
            or target_path != child.target_path
            or list(keys) != child.physical_keys
            or (
                identifier is not None
                and (
                    attempt is None
                    or attempt.scope_id != parent.scope_id
                    or attempt.proposal_id != child.proposal_id
                    or attempt.action != parent.action
                    or attempt.expected_revision != child.expected_revision
                    or attempt.request_key != child.request_key
                )
            )
        ):
            raise ServiceError(
                "workspace_mixed_batch_corrupt", "混合子项与原提案/操作关联不符", 409
            )
        if operation is not None:
            origin = await sql.get(MixedWorkspaceBatchModelSourceRow, parent.batch_id)
            if origin is not None and (
                attempt is None
                or (attempt.run_id, attempt.call_id) != (origin.run_id, origin.call_id)
            ):
                raise ServiceError(
                    "workspace_mixed_batch_corrupt", "混合原操作缺少原模型Run/call", 409
                )
            status = operation.status
            error_code = operation.receipt.error_code if operation.receipt else None
            if origin is not None:
                # 来源call只用于持久完整性和审计关联；混合模型回程不泄露原Provider调用标识。
                operation = operation.model_copy(update={"run_id": None, "call_id": None})
        return MixedBatchItemView(
            source=cast(Literal["file", "directory"], child.source),
            position=child.position,
            proposal_id=child.proposal_id,
            expected_revision=child.expected_revision,
            path=child.path,
            target_path=child.target_path,
            status=status,
            operation=operation,
            error_code=error_code,
        )

    async def _view(self, sql: AsyncSession, row: MixedWorkspaceBatchRow) -> MixedBatchView:
        items = [
            await self._item(sql, row, child) for child in await self._children(sql, row.batch_id)
        ]
        done = sum(item.status == "done" for item in items)
        status: MixedBatchStatus = (
            "running"
            if row.producer_status == "running"
            else "uncertain"
            if any(item.status in {"running", "uncertain"} for item in items)
            else "done"
            if done == len(items)
            else cast(Literal["cancelled", "interrupted"], row.producer_status)
            if row.producer_status in {"cancelled", "interrupted"}
            else "partial"
            if done
            else "failed"
        )
        return MixedBatchView(
            batch_id=row.batch_id,
            scope_id=row.scope_id,
            request_key=row.request_key,
            snapshot_revision=row.snapshot_revision,
            action=cast(MixedBatchAction, row.action),
            status=status,
            producer_status=cast(
                Literal["running", "finished", "cancelled", "interrupted"], row.producer_status
            ),
            items=items,
            stop_reason=row.stop_reason,
            created_at=row.created_at.replace(tzinfo=UTC),
            updated_at=max(
                [row.updated_at.replace(tzinfo=UTC)]
                + [
                    datetime.fromtimestamp(item.operation.updated_at, UTC)
                    if isinstance(item.operation, DirectoryAttemptView)
                    else item.operation.updated_at
                    for item in items
                    if item.operation is not None
                ]
            ),
        )

    async def _close(
        self,
        sql: AsyncSession,
        row: MixedWorkspaceBatchRow,
        status: Literal["finished", "cancelled", "interrupted"],
        reason: str | None,
    ) -> None:
        row.producer_status, row.stop_reason = status, reason
        row.owner_id, row.lease_expires_at = None, None
        row.updated_at = utc_now()
        for child in await self._children(sql, row.batch_id):
            child.admission_open = False
            if (
                child.file_operation_id is None
                and child.directory_operation_id is None
                and child.status == "pending"
            ):
                child.status, child.error_code = (
                    "skipped",
                    reason or "workspace_mixed_batch_not_started",
                )
        await sql.flush()

    @staticmethod
    async def _row(sql: AsyncSession, scope: str, identifier: str) -> MixedWorkspaceBatchRow:
        row = await sql.get(MixedWorkspaceBatchRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError("workspace_mixed_batch_not_found", "批次不在该作用域", 404)
        return row

    async def execution(self, scope: str, identifier: str) -> MixedBatchExecution:
        async with self.database.write_lock, self.database.sessions() as sql:
            await self.files._scope(sql, scope, writing=True)
            row = await self._row(sql, scope, identifier)
            await assert_live_execution(sql, row, self.owner_id)
            await self.files._resource(sql, scope, row.workspace_key, row.resource_fingerprint)
            children = tuple(
                MixedBatchDispatch(
                    fence=MixedBatchFence(
                        row.batch_id,
                        child.position,
                        row.token,
                        cast(Literal["file", "directory"], child.source),
                    ),
                    request=(
                        WorkspaceChangeAction if child.source == "file" else DirectoryChangeAction
                    )(
                        proposal_id=child.proposal_id,
                        action=cast(MixedBatchAction, row.action),
                        expected_revision=child.expected_revision,
                        request_key=child.request_key,
                    ),
                )
                for child in await self._children(sql, identifier)
                if child.admission_open
                and child.status == "pending"
                and child.file_operation_id is None
                and child.directory_operation_id is None
            )
            source = await sql.get(MixedWorkspaceBatchModelSourceRow, row.batch_id)
            return MixedBatchExecution(
                await self._view(sql, row),
                row.token,
                children,
                source.run_id if source is not None else None,
                source.call_id if source is not None else None,
                MixedBatchModelSource(source.run_id, source.call_id)
                if source is not None
                else None,
            )

    async def renew(self, identifier: str, token: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await sql.get(MixedWorkspaceBatchRow, identifier)
            if row is None or not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_mixed_batch_fence_mismatch", "混合执行凭据不符", 403)
            await assert_live_execution(sql, row, self.owner_id)
            await self.files._scope(sql, row.scope_id, writing=True)
            await self.files._resource(
                sql, row.scope_id, row.workspace_key, row.resource_fingerprint
            )
            row.lease_expires_at = time.time() + self.lease_seconds
            row.updated_at = utc_now()

    async def finish_child(
        self,
        scope: str,
        fence: MixedBatchFence,
        *,
        error_code: str | None = None,
        skipped: bool = False,
    ) -> MixedBatchItemView:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await self._row(sql, scope, fence.batch_id)
            child = await sql.get(MixedWorkspaceBatchChildRow, (fence.batch_id, fence.position))
            if (
                child is None
                or child.source != fence.source
                or not secrets.compare_digest(row.token, fence.token)
            ):
                raise ServiceError("workspace_mixed_batch_fence_mismatch", "混合子项凭据不符", 403)
            await assert_live_execution(sql, row, self.owner_id)
            child.admission_open = False
            if (
                child.file_operation_id is None
                and child.directory_operation_id is None
                and child.status == "pending"
            ):
                child.status = "skipped" if skipped else "rejected"
                child.error_code = error_code or "workspace_mixed_batch_not_executed"
            row.updated_at = utc_now()
            await sql.flush()
            return await self._item(sql, row, child)

    async def finish(self, scope: str, identifier: str, token: str) -> MixedBatchView:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await self._row(sql, scope, identifier)
            if not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_mixed_batch_fence_mismatch", "混合执行凭据不符", 403)
            await assert_live_execution(sql, row, self.owner_id)
            await self._close(sql, row, "finished", None)
            return await self._view(sql, row)

    async def recover_interrupted(self) -> int:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            rows = list(
                await sql.scalars(
                    select(MixedWorkspaceBatchRow).where(
                        MixedWorkspaceBatchRow.producer_status == "running"
                    )
                )
            )
            recovered = 0
            for row in rows:
                await self._expire(sql, row)
                recovered += row.producer_status != "running"
            return recovered

    async def _expire(self, sql: AsyncSession, row: MixedWorkspaceBatchRow) -> None:
        if row.producer_status == "running" and (row.lease_expires_at or 0) <= time.time():
            await self._close(sql, row, "interrupted", "workspace_mixed_batch_lease_expired")

    async def get(self, scope: str, identifier: str) -> MixedBatchView:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await sql.get(MixedWorkspaceBatchRow, identifier)
            if row is None or row.scope_id != scope:
                raise ServiceError("workspace_mixed_batch_not_found", "批次不在该作用域", 404)
            await self._expire(sql, row)
            return await self._view(sql, row)

    async def by_key(self, scope: str, key: str) -> MixedBatchView | None:
        self.files._key(key)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(MixedWorkspaceBatchRow).where(
                    MixedWorkspaceBatchRow.scope_id == scope,
                    MixedWorkspaceBatchRow.request_key == key,
                )
            )
            if row is None:
                return None
            await self._expire(sql, row)
            return await self._view(sql, row)

    @staticmethod
    def _history_cursor(
        scope: str, upper: tuple[datetime, str], before: tuple[datetime, str]
    ) -> str:
        return _MIXED_HISTORY_PREFIX + _history_cursor(scope, upper, before)

    @staticmethod
    def _decode_history_cursor(
        cursor: str, scope: str
    ) -> tuple[tuple[datetime, str], tuple[datetime, str]]:
        try:
            if (
                not isinstance(cursor, str)
                or not 1 <= len(cursor) <= 1024
                or not cursor.startswith(_MIXED_HISTORY_PREFIX)
            ):
                raise _mixed_history_error()
            return _decode_history_cursor(cursor[len(_MIXED_HISTORY_PREFIX) :], scope)
        except ServiceError:
            raise _mixed_history_error() from None

    async def history(
        self, scope: str, *, cursor: str | None = None, limit: int = 32
    ) -> MixedBatchHistoryPage:
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceError(
                "workspace_mixed_batch_history_limit_invalid", "混合批次历史每页须为1至32项", 422
            )
        decoded = self._decode_history_cursor(cursor, scope) if cursor is not None else None
        batch = MixedWorkspaceBatchRow
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._scope(sql, scope, writing=False)
            bounded = (
                select(batch.batch_id)
                .where(batch.scope_id == scope)
                .limit(_MAX_HISTORY + 1)
                .subquery()
            )
            total = await sql.scalar(select(func.count()).select_from(bounded)) or 0
            if total > _MAX_HISTORY:
                raise ServiceError(
                    "workspace_mixed_batch_history_too_large", "混合批次历史超过5000项读取上限", 409
                )
            conditions = [batch.scope_id == scope]
            if decoded is not None:
                upper, before = decoded
                for created, identifier in (upper, before):
                    if (
                        await sql.scalar(
                            select(batch.batch_id).where(
                                batch.scope_id == scope,
                                batch.created_at == created,
                                batch.batch_id == identifier,
                            )
                        )
                        is None
                    ):
                        raise _mixed_history_error()
                conditions.append(
                    (batch.created_at < upper[0])
                    | ((batch.created_at == upper[0]) & (batch.batch_id <= upper[1]))
                )
                total = (
                    await sql.scalar(select(func.count()).select_from(batch).where(*conditions))
                    or 0
                )
                conditions.append(
                    (batch.created_at < before[0])
                    | ((batch.created_at == before[0]) & (batch.batch_id < before[1]))
                )
            expired = and_(
                batch.producer_status == "running",
                or_(batch.lease_expires_at.is_(None), batch.lease_expires_at <= time.time()),
            )
            rows = (
                (
                    await sql.execute(
                        select(
                            batch.batch_id,
                            batch.request_key,
                            batch.action,
                            case((expired, "interrupted"), else_=batch.producer_status).label(
                                "producer_status"
                            ),
                            batch.snapshot_revision,
                            case(
                                (expired, "workspace_mixed_batch_lease_expired"),
                                else_=batch.stop_reason,
                            ).label("stop_reason"),
                            batch.created_at,
                            batch.updated_at,
                        )
                        .where(*conditions)
                        .order_by(batch.created_at.desc(), batch.batch_id.desc())
                        .limit(limit + 1)
                    )
                )
                .mappings()
                .all()
            )
            if not rows:
                if decoded is not None:
                    raise _mixed_history_error()
                return MixedBatchHistoryPage(scope_id=scope, total=0, items=[], next_cursor=None)
            upper = (
                decoded[0] if decoded is not None else (rows[0]["created_at"], rows[0]["batch_id"])
            )
            selected = rows[:limit]
            child = MixedWorkspaceBatchChildRow
            file = WorkspaceEditAttemptRow
            directory = WorkspaceDirectoryAttemptRow
            origin = MixedWorkspaceBatchModelSourceRow
            effective = func.coalesce(file.status, directory.status, child.status)
            invalid = or_(
                and_(
                    child.file_operation_id.is_not(None),
                    or_(
                        file.operation_id.is_(None),
                        file.scope_id != scope,
                        file.proposal_id != child.proposal_id,
                        file.action != batch.action,
                        file.expected_revision != child.expected_revision,
                        file.request_key != child.request_key,
                        and_(
                            origin.batch_id.is_not(None),
                            file.run_id.is_distinct_from(origin.run_id),
                        ),
                        and_(
                            origin.batch_id.is_not(None),
                            file.call_id.is_distinct_from(origin.call_id),
                        ),
                    ),
                ),
                and_(
                    child.directory_operation_id.is_not(None),
                    or_(
                        directory.operation_id.is_(None),
                        directory.scope_id != scope,
                        directory.proposal_id != child.proposal_id,
                        directory.action != batch.action,
                        directory.expected_revision != child.expected_revision,
                        directory.request_key != child.request_key,
                        and_(
                            origin.batch_id.is_not(None),
                            directory.run_id.is_distinct_from(origin.run_id),
                        ),
                        and_(
                            origin.batch_id.is_not(None),
                            directory.call_id.is_distinct_from(origin.call_id),
                        ),
                    ),
                ),
            )
            counts = {
                count.batch_id: count
                for count in await sql.execute(
                    select(
                        child.batch_id,
                        func.count().label("item_count"),
                        func.sum(case((effective == "done", 1), else_=0)).label("done_count"),
                        func.sum(case((effective.in_(["running", "uncertain"]), 1), else_=0)).label(
                            "unresolved_count"
                        ),
                        func.sum(case((invalid, 1), else_=0)).label("invalid_count"),
                        func.max(func.coalesce(file.updated_at, directory.updated_at)).label(
                            "operation_updated_at"
                        ),
                    )
                    .join(batch, batch.batch_id == child.batch_id)
                    .outerjoin(origin, origin.batch_id == child.batch_id)
                    .outerjoin(file, file.operation_id == child.file_operation_id)
                    .outerjoin(directory, directory.operation_id == child.directory_operation_id)
                    .where(child.batch_id.in_([row["batch_id"] for row in selected]))
                    .group_by(child.batch_id)
                )
            }
            summaries: list[MixedBatchSummary] = []
            for row in selected:
                count = counts.get(row["batch_id"])
                if count is not None and count.invalid_count:
                    raise ServiceError(
                        "workspace_mixed_batch_corrupt", "混合历史原操作关联不完整", 409
                    )
                total_items = count.item_count if count is not None else 0
                done = count.done_count if count is not None else 0
                unresolved = count.unresolved_count if count is not None else 0
                updated = row["updated_at"]
                if count is not None and count.operation_updated_at is not None:
                    updated = max(updated, count.operation_updated_at)
                status = _batch_status(row["producer_status"], total_items, done, unresolved)
                summaries.append(
                    MixedBatchSummary.model_validate(
                        {
                            **dict(row),
                            "status": status,
                            "item_count": total_items,
                            "done_count": done,
                            "created_at": row["created_at"].replace(tzinfo=UTC),
                            "updated_at": updated.replace(tzinfo=UTC),
                        }
                    )
                )
            last = selected[-1]
            return MixedBatchHistoryPage(
                scope_id=scope,
                total=total,
                items=summaries,
                next_cursor=(
                    self._history_cursor(scope, upper, (last["created_at"], last["batch_id"]))
                    if len(rows) > limit
                    else None
                ),
            )

    async def cancel(self, scope: str, identifier: str) -> MixedBatchView:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await sql.get(MixedWorkspaceBatchRow, identifier)
            if row is None or row.scope_id != scope:
                raise ServiceError("workspace_mixed_batch_not_found", "批次不在该作用域", 404)
            await self._expire(sql, row)
            if row.producer_status == "running":
                await self._close(sql, row, "cancelled", "workspace_mixed_batch_cancelled")
            return await self._view(sql, row)
