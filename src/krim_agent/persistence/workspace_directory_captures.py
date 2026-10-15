"""目录捕获持久请求/租约；完整材料与原任务同事务关联，历史零重放。"""

from __future__ import annotations

import re
import secrets
import time
from builtins import list as builtin_list
from collections.abc import Callable
from datetime import UTC
from typing import cast
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointCaptureFence,
    DirectoryCheckpointCaptureView,
    DirectoryCheckpointMaterialCreate,
    DirectoryCheckpointStatus,
    DirectoryCheckpointSummary,
    DirectoryCheckpointView,
    StoredDirectoryCheckpoint,
    directory_checkpoint_model_source,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot

from .database import (
    NativeCheckpointModelSourceRow,
    WorkspaceDirectoryCaptureModelSourceRow,
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryCheckpointRow,
    WorkspaceRestoreModelSourceRow,
)
from .workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from .workspace_edits import digest


def _error(suffix: str, message: str, status: int = 409) -> ServiceError:
    return ServiceError(f"workspace_directory_capture_{suffix}", message, status)


class WorkspaceDirectoryCaptureStore:
    def __init__(
        self,
        materials: WorkspaceDirectoryCheckpointStore,
        *,
        lease_seconds: float = 60,
        max_captures: int = 500,
    ) -> None:
        if type(lease_seconds) not in {int, float} or not 0.2 <= lease_seconds <= 600:
            raise ValueError("目录捕获租约无效")
        if type(max_captures) is not int or not 1 <= max_captures <= 5000:
            raise ValueError("目录捕获任务数量上限无效")
        self.materials, self.edits = materials, materials.edits
        self.database, self.owner_id = self.edits.database, self.edits.owner_id
        self.lease_seconds, self.max_captures = lease_seconds, max_captures

    @staticmethod
    def _request(row: WorkspaceDirectoryCaptureRow) -> DirectoryCheckpointMaterialCreate:
        return DirectoryCheckpointMaterialCreate(
            title=row.title, root_path=row.root_path, request_key=row.request_key
        )

    @staticmethod
    def _hash(
        request: DirectoryCheckpointMaterialCreate,
        binding: WorkspaceBinding,
        source: CheckpointModelSource | None = None,
    ) -> str:
        return WorkspaceDirectoryCaptureStore._checksum(
            request, binding.key, binding.fingerprint, binding.workspace.fingerprint, source
        )

    @staticmethod
    def _checksum(
        request: DirectoryCheckpointMaterialCreate,
        key: str,
        resource: str,
        workspace: str,
        source: CheckpointModelSource | None,
    ) -> str:
        value = {
            "request": request.model_dump(mode="json"),
            "key": key,
            "resource": resource,
            "workspace": workspace,
        }
        if source is not None:
            value["model_source"] = {"run_id": source.run_id, "call_id": source.call_id}
        return digest(value)

    async def _source(
        self, sql: AsyncSession, row: WorkspaceDirectoryCaptureRow, *, live: bool = False
    ) -> CheckpointModelSource | None:
        saved = await sql.get(WorkspaceDirectoryCaptureModelSourceRow, row.checkpoint_id)
        if bool(row.model_bound) != (saved is not None):
            raise _error("corrupt", "目录捕获模型标记或原来源缺失")
        if saved is None:
            return None
        try:
            source = directory_checkpoint_model_source(
                CheckpointModelSource(saved.run_id, saved.call_id)
            )
        except ServiceError:
            raise _error("corrupt", "目录捕获持久来源损坏") from None
        assert source is not None
        if not row.scope_id.startswith("session:"):
            raise _error("corrupt", "目录模型捕获缺少原Agent会话")
        await self.edits._scope(sql, row.scope_id, writing=live, run_id=source.run_id)
        return source

    async def source(self, scope: str, identifier: str) -> CheckpointModelSource | None:
        async with self.database.write_lock, self.database.sessions() as sql:
            row = await self._row(sql, scope, identifier)
            return await self._source(sql, row)

    async def _row(
        self, sql: AsyncSession, scope: str, identifier: str, *, resource: bool = True
    ) -> WorkspaceDirectoryCaptureRow:
        row = await sql.get(WorkspaceDirectoryCaptureRow, identifier)
        if row is None or row.scope_id != scope:
            raise _error("not_found", "目录捕获不在此作用域中", 404)
        source = await self._source(sql, row)
        try:
            request = self._request(row)
            checksum = self._checksum(
                request,
                row.workspace_key,
                row.resource_fingerprint,
                row.workspace_fingerprint,
                source,
            )
            if row.request_hash != checksum:
                raise ValueError("原目录捕获请求摘要损坏")
        except (ValueError, TypeError, AttributeError):
            raise _error("corrupt", "目录捕获原请求损坏") from None
        topic = await self.edits._scope(sql, scope, writing=False)
        if row.topic_id != topic:
            raise _error("corrupt", "目录捕获不属于原话题")
        if resource:
            await self.edits._resource(sql, scope, row.workspace_key, row.resource_fingerprint)
        return row

    @staticmethod
    def _end(
        row: WorkspaceDirectoryCaptureRow, status: DirectoryCheckpointStatus, code: str | None
    ) -> None:
        row.status, row.error_code = status, code
        row.owner_id, row.lease_expires_at = None, None
        row.updated_at = utc_now()

    def _expire(self, row: WorkspaceDirectoryCaptureRow) -> bool:
        if row.status == "capturing" and (row.lease_expires_at or 0) <= time.time():
            self._end(row, "interrupted", "workspace_directory_capture_interrupted")
            return True
        return False

    async def _material(
        self, sql: AsyncSession, row: WorkspaceDirectoryCaptureRow
    ) -> StoredDirectoryCheckpoint | None:
        if row.material_id is None:
            return None
        material = await sql.get(WorkspaceDirectoryCheckpointRow, row.material_id)
        if material is None:
            raise _error("corrupt", "目录捕获缺少关联完整材料")
        stored = await self.materials._stored(
            sql, row.scope_id, material, row.workspace_key, row.resource_fingerprint
        )
        if (
            material.topic_id != row.topic_id
            or material.request_key != f"capture:{row.checkpoint_id}"
            or material.root_path != row.root_path
            or material.title != row.title
            or material.workspace_fingerprint != row.workspace_fingerprint
        ):
            raise _error("corrupt", "目录捕获与原完整材料关联不匹配")
        return stored

    @staticmethod
    def _summary(stored: StoredDirectoryCheckpoint) -> DirectoryCheckpointSummary:
        return DirectoryCheckpointSummary.model_validate(
            stored.view.model_dump(
                include={
                    "snapshot_fingerprint",
                    "entry_count",
                    "file_count",
                    "directory_count",
                    "byte_size",
                }
            )
        )

    async def _view(
        self, sql: AsyncSession, row: WorkspaceDirectoryCaptureRow
    ) -> DirectoryCheckpointView:
        stored = await self._material(sql, row)
        try:
            return DirectoryCheckpointView(
                checkpoint_id=row.checkpoint_id,
                scope_id=row.scope_id,
                title=row.title,
                root_path=row.root_path,
                request_key=row.request_key,
                workspace_key=row.workspace_key,
                resource_fingerprint=row.resource_fingerprint,
                status=cast(DirectoryCheckpointStatus, row.status),
                snapshot=self._summary(stored) if stored is not None else None,
                error_code=row.error_code,
                created_at=row.created_at.replace(tzinfo=UTC),
                updated_at=row.updated_at.replace(tzinfo=UTC),
            )
        except (ValueError, TypeError, AttributeError):
            raise _error("corrupt", "目录捕获状态或摘要损坏") from None

    async def reserve(
        self,
        scope: str,
        binding: WorkspaceBinding,
        request: DirectoryCheckpointMaterialCreate,
        *,
        model_source: CheckpointModelSource | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> tuple[DirectoryCheckpointView, bool]:
        if (
            type(binding) is not WorkspaceBinding
            or type(request) is not DirectoryCheckpointMaterialCreate
        ):
            raise _error("input", "目录捕获请求或绑定类型无效", 422)
        request = DirectoryCheckpointMaterialCreate.model_validate(request.model_dump(mode="json"))
        model_source = directory_checkpoint_model_source(model_source)
        checksum = self._hash(request, binding, model_source)
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            if authorize is not None:
                authorize()
            topic = await self.edits._scope(sql, scope, writing=False)
            await self.edits._resource(sql, scope, binding.key, binding.fingerprint)
            previous = await sql.scalar(
                select(WorkspaceDirectoryCaptureRow).where(
                    WorkspaceDirectoryCaptureRow.scope_id == scope,
                    WorkspaceDirectoryCaptureRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                previous = await self._row(sql, scope, previous.checkpoint_id)
                if await self._source(sql, previous) != model_source:
                    raise _error("model_conflict", "原目录捕获绑定其他父调用来源")
                if previous.request_hash != checksum:
                    raise _error("key_conflict", "原目录捕获键已绑定其他请求或资源")
                self._expire(previous)
                return await self._view(sql, previous), False
            if model_source is not None:
                if not scope.startswith("session:"):
                    raise _error("model_context", "目录模型捕获须使用原Agent会话", 403)
                await self.edits._scope(sql, scope, writing=True, run_id=model_source.run_id)
                for table in (
                    WorkspaceDirectoryCaptureModelSourceRow,
                    NativeCheckpointModelSourceRow,
                    WorkspaceRestoreModelSourceRow,
                ):
                    if (
                        await sql.scalar(
                            select(table).where(
                                table.run_id == model_source.run_id,
                                table.call_id == model_source.call_id,
                            )
                        )
                        is not None
                    ):
                        raise _error("model_call_conflict", "原Run/call已绑定其他检查点操作")
            else:
                await self.edits._scope(sql, scope, writing=True)
            if not binding.require().writable:
                raise ServiceError("workspace_read_only", "原工作区未授写能力", 403)
            count = int(
                await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryCaptureRow))
                or 0
            )
            if count >= self.max_captures:
                raise _error("capacity", "目录捕获任务数量已达上限", 429)
            row = WorkspaceDirectoryCaptureRow(
                checkpoint_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                request_key=request.request_key,
                request_hash=checksum,
                title=request.title,
                root_path=request.root_path,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=binding.workspace.fingerprint,
                status="capturing",
                model_bound=model_source is not None,
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                token=secrets.token_hex(32),
            )
            sql.add(row)
            await sql.flush()
            if model_source is not None:
                sql.add(
                    WorkspaceDirectoryCaptureModelSourceRow(
                        checkpoint_id=row.checkpoint_id,
                        run_id=model_source.run_id,
                        call_id=model_source.call_id,
                    )
                )
                await sql.flush()
            await self.edits._scope(
                sql,
                scope,
                writing=True,
                run_id=model_source.run_id if model_source is not None else None,
            )
            binding.require()
            if authorize is not None:
                authorize()
            return await self._view(sql, row), True

    @staticmethod
    def _valid_fence(fence: DirectoryCheckpointCaptureFence) -> None:
        if (
            type(fence) is not DirectoryCheckpointCaptureFence
            or type(fence.checkpoint_id) is not str
            or not 1 <= len(fence.checkpoint_id) <= 36
            or type(fence.token) is not str
            or re.fullmatch(r"[0-9a-f]{64}", fence.token) is None
        ):
            raise _error("context", "目录捕获内部围栏无效", 403)
        directory_checkpoint_model_source(fence.model_source)

    def _assert_live(
        self, row: WorkspaceDirectoryCaptureRow, fence: DirectoryCheckpointCaptureFence
    ) -> None:
        if (
            row.status != "capturing"
            or row.owner_id != self.owner_id
            or (row.lease_expires_at or 0) <= time.time()
            or not secrets.compare_digest(row.token, fence.token)
        ):
            raise _error("fenced", "目录捕获原owner、租约或token已失效")

    async def _live(
        self, sql: AsyncSession, scope: str, fence: DirectoryCheckpointCaptureFence
    ) -> WorkspaceDirectoryCaptureRow:
        self._valid_fence(fence)
        row = await self._row(sql, scope, fence.checkpoint_id)
        self._assert_live(row, fence)
        await self.edits._scope(sql, scope, writing=True)
        if await self._source(sql, row, live=True) != fence.model_source:
            raise _error("model_context", "目录围栏未保留原父调用来源", 403)
        self._assert_live(row, fence)
        return row

    async def execution(self, scope: str, identifier: str) -> DirectoryCheckpointCaptureFence:
        async with self.database.write_lock, self.database.sessions() as sql:
            await self.edits._scope(sql, scope, writing=True)
            row = await self._row(sql, scope, identifier)
            fence = DirectoryCheckpointCaptureFence(
                identifier, row.token, await self._source(sql, row, live=True)
            )
            self._assert_live(row, fence)
            return fence

    async def assert_capture(
        self, scope: str, fence: DirectoryCheckpointCaptureFence, root_path: str
    ) -> None:
        async with self.database.write_lock, self.database.sessions() as sql:
            row = await self._live(sql, scope, fence)
            if row.root_path != root_path:
                raise _error("target", "目录捕获未保留原冻结根", 403)

    async def renew(
        self,
        scope: str,
        fence: DirectoryCheckpointCaptureFence,
        *,
        authorize: Callable[[], None] | None = None,
    ) -> None:
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            row = await self._live(sql, scope, fence)
            previous_lease = row.lease_expires_at
            row.lease_expires_at = time.time() + self.lease_seconds
            await sql.flush()
            if (previous_lease or 0) <= time.time():
                raise _error("fenced", "续租提交前原目录租约已失效")
            if authorize is not None:
                authorize()

    @staticmethod
    def _qualify(binding: WorkspaceBinding, qualify: Callable[[], None]) -> None:
        if not binding.require().writable:
            raise ServiceError("workspace_read_only", "原工作区未授写能力", 403)
        qualify()

    @classmethod
    def _capture_view(
        cls, row: WorkspaceDirectoryCaptureRow, stored: StoredDirectoryCheckpoint
    ) -> DirectoryCheckpointCaptureView:
        return DirectoryCheckpointCaptureView(
            checkpoint_id=row.checkpoint_id,
            root_path=row.root_path,
            **cls._summary(stored).model_dump(),
        )

    async def record(
        self,
        scope: str,
        fence: DirectoryCheckpointCaptureFence,
        binding: WorkspaceBinding,
        snapshot: WorkspaceTreeSnapshot,
        *,
        qualify: Callable[[], None],
    ) -> DirectoryCheckpointCaptureView:
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            row = await self._live(sql, scope, fence)
            if (binding.key, binding.fingerprint, binding.workspace.fingerprint) != (
                row.workspace_key,
                row.resource_fingerprint,
                row.workspace_fingerprint,
            ):
                raise _error("target", "目录捕获资源与原绑定不匹配", 403)
            self._qualify(binding, qualify)
            if row.material_id is not None:
                raise _error("already_captured", "原目录任务已保存材料，禁止重新读取或覆盖")
            request = DirectoryCheckpointMaterialCreate(
                title=row.title, root_path=row.root_path, request_key=f"capture:{row.checkpoint_id}"
            )
            stored, created = await self.materials._save_captured(
                sql, scope, binding, request, snapshot
            )
            if not created:
                raise _error("corrupt", "目录生产者不能引用先前无关联材料")
            row.material_id = stored.view.checkpoint_id
            row.updated_at = utc_now()
            await sql.flush()
            self._assert_live(row, fence)
            self._qualify(binding, qualify)
            return self._capture_view(row, stored)

    async def finish(
        self,
        scope: str,
        fence: DirectoryCheckpointCaptureFence,
        binding: WorkspaceBinding,
        value: DirectoryCheckpointCaptureView,
        *,
        qualify: Callable[[], None],
    ) -> None:
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            row = await self._live(sql, scope, fence)
            stored = await self._material(sql, row)
            if stored is None or self._capture_view(row, stored) != value:
                raise _error("unverified", "目录捕获回程与原私有完整材料不匹配")
            if (binding.key, binding.fingerprint, binding.workspace.fingerprint) != (
                row.workspace_key,
                row.resource_fingerprint,
                row.workspace_fingerprint,
            ):
                raise _error("target", "目录完成资格与原资源不匹配", 403)
            self._assert_live(row, fence)
            lease = row.lease_expires_at
            self._qualify(binding, qualify)
            self._end(row, "available", None)
            await sql.flush()
            # flush也可能等待；原deadline/租约和绑定必须在最后一个await之后仍有效。
            self._qualify(binding, qualify)
            if (lease or 0) <= time.time():
                raise _error("fenced", "目录完成前原捕获租约已过期")

    async def fail(self, scope: str, fence: DirectoryCheckpointCaptureFence, code: str) -> None:
        self._valid_fence(fence)
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            # 撤权不能阻止原token关闭生产准入；此路径只收束原任务，绝不发布材料。
            row = await self._row(sql, scope, fence.checkpoint_id, resource=False)
            if await self._source(sql, row) != fence.model_source:
                raise _error("model_context", "迟到失败未保留原目录父来源", 403)
            if row.owner_id not in {None, self.owner_id} or not secrets.compare_digest(
                row.token, fence.token
            ):
                raise _error("fenced", "迟到目录失败不能改写其他owner任务")
            self._expire(row)
            if row.status == "capturing":
                self._end(row, "failed", code)

    async def get(self, scope: str, identifier: str) -> DirectoryCheckpointView:
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            await self.edits._scope(sql, scope, writing=False)
            row = await self._row(sql, scope, identifier)
            self._expire(row)
            return await self._view(sql, row)

    async def by_key(self, scope: str, request_key: str) -> DirectoryCheckpointView | None:
        self.edits._key(request_key)
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            await self.edits._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(WorkspaceDirectoryCaptureRow).where(
                    WorkspaceDirectoryCaptureRow.scope_id == scope,
                    WorkspaceDirectoryCaptureRow.request_key == request_key,
                )
            )
            if row is None:
                return None
            row = await self._row(sql, scope, row.checkpoint_id)
            self._expire(row)
            return await self._view(sql, row)

    async def cancel(self, scope: str, identifier: str) -> DirectoryCheckpointView:
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            await self.edits._scope(sql, scope, writing=False)
            row = await self._row(sql, scope, identifier)
            self._expire(row)
            if row.status == "capturing":
                self._end(row, "cancelled", "workspace_directory_capture_cancelled")
            return await self._view(sql, row)

    async def list(
        self, scope: str, *, offset: int = 0, limit: int = 32
    ) -> builtin_list[DirectoryCheckpointView]:
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise _error("page", "目录捕获分页无效", 422)
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            await self.edits._scope(sql, scope, writing=False)
            rows = await sql.scalars(
                select(WorkspaceDirectoryCaptureRow)
                .where(WorkspaceDirectoryCaptureRow.scope_id == scope)
                .order_by(
                    WorkspaceDirectoryCaptureRow.created_at.desc(),
                    WorkspaceDirectoryCaptureRow.checkpoint_id.desc(),
                )
                .offset(offset)
                .limit(limit)
            )
            result = []
            for row in rows:
                row = await self._row(sql, scope, row.checkpoint_id)
                self._expire(row)
                result.append(await self._view(sql, row))
            return result

    async def recover_interrupted(self) -> int:
        async with self.database.write_lock, self.database.sessions() as sql, sql.begin():
            await self.edits._writer(sql)
            rows = await sql.scalars(
                select(WorkspaceDirectoryCaptureRow).where(
                    WorkspaceDirectoryCaptureRow.status == "capturing",
                    WorkspaceDirectoryCaptureRow.lease_expires_at <= time.time(),
                )
            )
            return sum(self._expire(row) for row in rows)
