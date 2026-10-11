"""混合提案批次的冻结、原事务围栏及原文件/目录账本投影。"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import uuid4

from sqlalchemy import func, select
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
    MixedBatchItemStatus,
    MixedBatchItemView,
    MixedBatchRequest,
    MixedBatchStatus,
    MixedBatchTarget,
    MixedBatchView,
)

from .database import (
    MixedWorkspaceBatchChildRow,
    MixedWorkspaceBatchRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
)
from .directory_fences import entry_key
from .mixed_batch_fences import MIXED_REQUEST_PREFIX, assert_live
from .workspace_directories import WorkspaceDirectoryStore
from .workspace_edits import WorkspaceEditStore, digest


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
    ) -> tuple[MixedBatchView, bool]:
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
                await self._expire(sql, previous)
                return await self._view(sql, previous), False
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
            status = operation.status
            error_code = operation.receipt.error_code if operation.receipt else None
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
            assert_live(row, self.owner_id)
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
            return MixedBatchExecution(await self._view(sql, row), row.token, children)

    async def renew(self, identifier: str, token: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await sql.get(MixedWorkspaceBatchRow, identifier)
            if row is None or not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_mixed_batch_fence_mismatch", "混合执行凭据不符", 403)
            assert_live(row, self.owner_id)
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
            assert_live(row, self.owner_id)
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
            assert_live(row, self.owner_id)
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
