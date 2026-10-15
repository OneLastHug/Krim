"""原生目录完整材料的可信存储；冷读不访问工作区或生产者。"""

from __future__ import annotations

import json
from datetime import UTC
from typing import cast
from uuid import uuid4

from pydantic import JsonValue
from sqlalchemy import LargeBinary, func, select
from sqlalchemy import cast as sql_cast
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointMaterialCreate,
    DirectoryCheckpointMaterialView,
    StoredDirectoryCheckpoint,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot

from .database import WorkspaceDirectoryCheckpointRow
from .tree_snapshot_codec import decode_snapshot, encode_snapshot
from .workspace_edits import WorkspaceEditStore, digest


def _corrupt() -> ServiceError:
    return ServiceError("workspace_directory_checkpoint_corrupt", "原生目录检查点完整材料损坏", 409)


class WorkspaceDirectoryCheckpointStore:
    def __init__(
        self,
        edits: WorkspaceEditStore,
        *,
        max_checkpoints: int = 500,
        max_snapshot_bytes: int = 67108864,
    ) -> None:
        if type(max_checkpoints) is not int or not 1 <= max_checkpoints <= 5000:
            raise ValueError("目录检查点材料数量无效")
        if type(max_snapshot_bytes) is not int or not 1024 <= max_snapshot_bytes <= 536870912:
            raise ValueError("目录检查点材料容量无效")
        self.edits, self.database = edits, edits.database
        self.max_checkpoints, self.max_snapshot_bytes = max_checkpoints, max_snapshot_bytes

    @staticmethod
    def _bounds(snapshot: WorkspaceTreeSnapshot, max_file_bytes: int = 8388608) -> None:
        base_depth = len(snapshot.root_path.split("/"))
        if len(snapshot.entries) > 1024 or snapshot.total_bytes > 33554432:
            raise ServiceError("workspace_directory_checkpoint_limit", "完整目录材料超过限额", 413)
        for entry in snapshot.entries:
            if len(entry.path.split("/")) - base_depth + 1 > 64:
                raise ServiceError("workspace_directory_checkpoint_limit", "目录层级超过限额", 413)
            if entry.file is not None and len(entry.file.data) > min(max_file_bytes, 8388608):
                raise ServiceError("workspace_directory_checkpoint_limit", "文件超过读取限额", 413)

    async def _stored(
        self,
        sql: AsyncSession,
        scope: str,
        row: WorkspaceDirectoryCheckpointRow,
        workspace_key: str,
        resource_fingerprint: str,
    ) -> StoredDirectoryCheckpoint:
        if (
            row.scope_id != scope
            or row.workspace_key != workspace_key
            or row.resource_fingerprint != resource_fingerprint
        ):
            raise ServiceError(
                "workspace_directory_checkpoint_not_found", "目录材料不在此作用域或资源中", 404
            )
        topic = await self.edits._scope(sql, scope, writing=False)
        try:
            request = DirectoryCheckpointMaterialCreate(
                title=row.title, root_path=row.root_path, request_key=row.request_key
            )
            snapshot = decode_snapshot(row.metadata_value, row.payload)
            self._bounds(snapshot)
            if (
                row.topic_id != topic
                or row.request_hash != digest(request.model_dump(mode="json"))
                or row.root_path != snapshot.root_path
                or row.workspace_fingerprint != snapshot.workspace_fingerprint
                or row.snapshot_fingerprint != snapshot.fingerprint
            ):
                raise _corrupt()
            view = DirectoryCheckpointMaterialView(
                checkpoint_id=row.checkpoint_id,
                scope_id=scope,
                request_key=row.request_key,
                title=row.title,
                root_path=row.root_path,
                workspace_key=row.workspace_key,
                resource_fingerprint=row.resource_fingerprint,
                snapshot_fingerprint=row.snapshot_fingerprint,
                entry_count=len(snapshot.entries),
                file_count=sum(entry.file is not None for entry in snapshot.entries),
                directory_count=sum(entry.directory is not None for entry in snapshot.entries),
                byte_size=snapshot.total_bytes,
                created_at=(
                    row.created_at.replace(tzinfo=UTC)
                    if row.created_at.tzinfo is None
                    else row.created_at.astimezone(UTC)
                ),
            )
        except (ValueError, TypeError, AttributeError, ServiceError):
            raise _corrupt() from None
        return StoredDirectoryCheckpoint(view, snapshot)

    async def get_material(
        self, scope: str, identifier: str, *, workspace_key: str, resource_fingerprint: str
    ) -> StoredDirectoryCheckpoint:
        async with self.database.write_lock, self.database.sessions() as sql:
            await self.edits._scope(sql, scope, writing=False)
            await self.edits._resource(sql, scope, workspace_key, resource_fingerprint)
            row = await sql.get(WorkspaceDirectoryCheckpointRow, identifier)
            if row is None:
                raise ServiceError(
                    "workspace_directory_checkpoint_not_found", "目录材料不存在", 404
                )
            return await self._stored(sql, scope, row, workspace_key, resource_fingerprint)

    async def by_key(
        self, scope: str, request_key: str, *, workspace_key: str, resource_fingerprint: str
    ) -> StoredDirectoryCheckpoint | None:
        self.edits._key(request_key)
        async with self.database.write_lock, self.database.sessions() as sql:
            await self.edits._scope(sql, scope, writing=False)
            await self.edits._resource(sql, scope, workspace_key, resource_fingerprint)
            row = await sql.scalar(
                select(WorkspaceDirectoryCheckpointRow).where(
                    WorkspaceDirectoryCheckpointRow.scope_id == scope,
                    WorkspaceDirectoryCheckpointRow.request_key == request_key,
                )
            )
            return (
                await self._stored(sql, scope, row, workspace_key, resource_fingerprint)
                if row is not None
                else None
            )

    async def save_captured(
        self,
        scope: str,
        binding: WorkspaceBinding,
        request: DirectoryCheckpointMaterialCreate,
        snapshot: WorkspaceTreeSnapshot | None = None,
    ) -> tuple[StoredDirectoryCheckpoint, bool]:
        if (
            type(request) is not DirectoryCheckpointMaterialCreate
            or type(binding) is not WorkspaceBinding
        ):
            raise ServiceError(
                "workspace_directory_checkpoint_input", "目录材料请求或绑定类型无效", 422
            )
        request = DirectoryCheckpointMaterialCreate.model_validate(request.model_dump(mode="json"))
        checksum = digest(request.model_dump(mode="json"))
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            await self.edits._scope(sql, scope, writing=False)
            await self.edits._resource(sql, scope, binding.key, binding.fingerprint)
            previous = await sql.scalar(
                select(WorkspaceDirectoryCheckpointRow).where(
                    WorkspaceDirectoryCheckpointRow.scope_id == scope,
                    WorkspaceDirectoryCheckpointRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != checksum:
                    raise ServiceError(
                        "workspace_directory_checkpoint_key_conflict",
                        "原目录材料键已绑定其他请求",
                        409,
                    )
                return (
                    await self._stored(sql, scope, previous, binding.key, binding.fingerprint),
                    False,
                )
            topic = await self.edits._scope(sql, scope, writing=True)
            workspace = binding.require()
            if type(snapshot) is not WorkspaceTreeSnapshot:
                raise ServiceError(
                    "workspace_directory_checkpoint_material", "须提供完整私有目录快照", 422
                )
            try:
                self._bounds(snapshot, workspace.max_file_bytes)
                if (
                    snapshot.root_path != request.root_path
                    or snapshot.workspace_fingerprint != workspace.fingerprint
                ):
                    raise ServiceError(
                        "workspace_directory_checkpoint_binding",
                        "目录快照与原请求或授权根不符",
                        409,
                    )
                metadata, payload = encode_snapshot(snapshot)
            except (ValueError, TypeError, AttributeError):
                raise ServiceError(
                    "workspace_directory_checkpoint_material", "完整私有目录快照无效", 422
                ) from None
            metadata_bytes = len(json.dumps(metadata, ensure_ascii=True).encode("utf-8"))
            if metadata_bytes > 2097152:
                raise ServiceError(
                    "workspace_directory_checkpoint_limit", "目录材料元数据超过限额", 413
                )
            count = int(
                await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryCheckpointRow))
                or 0
            )
            used = int(
                await sql.scalar(
                    select(
                        func.coalesce(
                            func.sum(
                                func.length(WorkspaceDirectoryCheckpointRow.payload)
                                + func.length(
                                    sql_cast(
                                        WorkspaceDirectoryCheckpointRow.metadata_value, LargeBinary
                                    )
                                )
                            ),
                            0,
                        )
                    )
                )
                or 0
            )
            added = len(payload) + metadata_bytes
            if count >= self.max_checkpoints or used + added > self.max_snapshot_bytes:
                raise ServiceError(
                    "workspace_directory_checkpoint_capacity", "目录材料历史或总容量已满", 429
                )
            binding.require()
            await self.edits._scope(sql, scope, writing=True)
            await self.edits._resource(sql, scope, binding.key, binding.fingerprint)
            row = WorkspaceDirectoryCheckpointRow(
                checkpoint_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                request_key=request.request_key,
                request_hash=checksum,
                title=request.title,
                root_path=request.root_path,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=snapshot.workspace_fingerprint,
                snapshot_fingerprint=snapshot.fingerprint,
                metadata_value=cast(dict[str, JsonValue], metadata),
                payload=payload,
                created_at=utc_now(),
            )
            sql.add(row)
            await sql.flush()
            return await self._stored(sql, scope, row, binding.key, binding.fingerprint), True
