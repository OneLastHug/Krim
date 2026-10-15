"""原生检查点捕获账本；文件内容只在可信执行层和私有快照间传递。"""

from __future__ import annotations

import hashlib
import secrets
import time
from builtins import list as builtin_list
from collections.abc import Callable
from datetime import UTC
from typing import Literal, cast
from uuid import uuid4

from pydantic import JsonValue, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace_checkpoint import (
    CapturedFileMetadata,
    CheckpointCaptureFence,
    CheckpointCaptureFormat,
    CheckpointModelSource,
    WorkspaceCheckpointCreate,
    WorkspaceCheckpointFileView,
    WorkspaceCheckpointView,
    checkpoint_content_kind,
)
from krim_agent.workspace_writer import CapturedFile, FileSnapshot

from .database import (
    NativeCheckpointModelSourceRow,
    NativeWorkspaceCheckpointFileRow,
    NativeWorkspaceCheckpointRow,
    WorkspaceFileLockRow,
)
from .workspace_edits import WorkspaceEditStore, digest, snapshot_metadata


def encode_capture(
    capture: CapturedFile, *, allow_binary: bool = False, native_bytes: bool = False
) -> dict[str, JsonValue]:
    if native_bytes and not allow_binary:
        raise ServiceError("workspace_checkpoint_format", "原生字节格式须允许私有bytes", 422)
    snapshot = capture.snapshot
    if snapshot is not None:
        if len(snapshot.data) > 8388608 or (not allow_binary and b"\0" in snapshot.data):
            raise ServiceError("workspace_checkpoint_content", "检查点只保存有界文本内容", 422)
        if not allow_binary:
            try:
                snapshot.data.decode("utf-8")
            except UnicodeDecodeError:
                raise ServiceError("workspace_encoding", "检查点内容须为UTF-8", 422) from None
    value = CapturedFileMetadata(
        path=capture.path,
        workspace_fingerprint=capture.workspace_fingerprint,
        parent_device=capture.parent_identity[0],
        parent_inode=capture.parent_identity[1],
        snapshot=snapshot_metadata(snapshot),
        content_sha256=snapshot.sha256 if snapshot is not None else None,
        byte_size=len(snapshot.data) if snapshot is not None else 0,
        snapshot_hash="0" * 64,
        content_kind=checkpoint_content_kind(snapshot.data if snapshot else None)
        if native_bytes
        else None,
    ).model_dump(mode="json")
    value["snapshot_hash"] = digest(
        {key: item for key, item in value.items() if key != "snapshot_hash"}
    )
    return value


def capture_metadata(value: dict[str, JsonValue] | None) -> CapturedFileMetadata:
    try:
        metadata = CapturedFileMetadata.model_validate(value)
    except ValidationError:
        raise ServiceError("workspace_checkpoint_corrupt", "检查点私有元数据损坏", 409) from None
    assert value is not None
    if metadata.snapshot_hash != digest(
        {key: item for key, item in value.items() if key != "snapshot_hash"}
    ):
        raise ServiceError("workspace_checkpoint_corrupt", "检查点私有元数据摘要不匹配", 409)
    return metadata


def decode_capture(value: dict[str, JsonValue] | None, content: bytes | None) -> CapturedFile:
    metadata = capture_metadata(value)
    checksum = hashlib.sha256(content).hexdigest() if content is not None else None
    if (
        checksum != metadata.content_sha256
        or (content is None) != (metadata.snapshot is None)
        or len(content or b"") != metadata.byte_size
        or metadata.content_kind is not None
        and metadata.content_kind != checkpoint_content_kind(content)
    ):
        raise ServiceError("workspace_checkpoint_corrupt", "检查点私有内容与身份不匹配", 409)
    snapshot = None
    if metadata.snapshot is not None:
        assert content is not None
        data = metadata.snapshot
        snapshot = FileSnapshot(
            content,
            data.mode,
            data.device,
            data.inode,
            data.mtime_ns,
            data.ctime_ns,
            data.uid,
            data.gid,
        )
    return CapturedFile(
        metadata.path,
        metadata.workspace_fingerprint,
        (metadata.parent_device, metadata.parent_inode),
        snapshot,
    )


class WorkspaceCheckpointStore:
    def __init__(
        self,
        edits: WorkspaceEditStore,
        *,
        lease_seconds: float = 60,
        max_checkpoints: int = 500,
        max_snapshot_bytes: int = 67108864,
    ) -> None:
        if not 0.2 <= lease_seconds <= 600 or not 1 <= max_checkpoints <= 5000:
            raise ValueError("检查点租约或数量上限无效")
        if not 1024 <= max_snapshot_bytes <= 536870912:
            raise ValueError("检查点快照容量无效")
        self.edits, self.database, self.owner_id = edits, edits.database, edits.owner_id
        self.lease_seconds = lease_seconds
        self.max_checkpoints, self.max_snapshot_bytes = max_checkpoints, max_snapshot_bytes

    @staticmethod
    async def _row(
        session: AsyncSession, scope: str, identifier: str
    ) -> NativeWorkspaceCheckpointRow:
        row = await session.get(NativeWorkspaceCheckpointRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError("workspace_checkpoint_not_found", "检查点不在该作用域中", 404)
        if row.capture_format not in {"text", "bytes"}:
            raise ServiceError("workspace_checkpoint_corrupt", "检查点捕获格式损坏", 409)
        return row

    @staticmethod
    def _metadata(
        row: NativeWorkspaceCheckpointRow, value: dict[str, JsonValue] | None
    ) -> CapturedFileMetadata:
        metadata = capture_metadata(value)
        if (metadata.content_kind is not None) != (row.capture_format == "bytes"):
            raise ServiceError("workspace_checkpoint_corrupt", "检查点种类与冻结格式不匹配", 409)
        return metadata

    @classmethod
    def _decode(
        cls, row: NativeWorkspaceCheckpointRow, entry: NativeWorkspaceCheckpointFileRow
    ) -> CapturedFile:
        cls._metadata(row, entry.capture_metadata)
        return decode_capture(entry.capture_metadata, entry.content)

    @staticmethod
    def _end(
        row: NativeWorkspaceCheckpointRow,
        status: Literal["available", "failed", "cancelled", "interrupted"],
        code: str | None,
    ) -> None:
        row.status, row.error_code = status, code
        row.owner_id, row.lease_expires_at = None, None
        row.updated_at = utc_now()

    def _expire(self, row: NativeWorkspaceCheckpointRow) -> None:
        if row.status == "capturing" and (row.lease_expires_at or 0) <= time.time():
            self._end(row, "interrupted", "workspace_checkpoint_interrupted")

    async def _view(
        self, session: AsyncSession, row: NativeWorkspaceCheckpointRow
    ) -> WorkspaceCheckpointView:
        await self._source(session, row)
        entries = await session.execute(
            select(
                NativeWorkspaceCheckpointFileRow.path,
                NativeWorkspaceCheckpointFileRow.captured,
                NativeWorkspaceCheckpointFileRow.capture_metadata,
            )
            .where(NativeWorkspaceCheckpointFileRow.checkpoint_id == row.checkpoint_id)
            .order_by(NativeWorkspaceCheckpointFileRow.position)
        )
        files = []
        for path, captured, raw in entries:
            metadata = self._metadata(row, raw) if captured else None
            files.append(
                WorkspaceCheckpointFileView(
                    path=path,
                    captured=captured,
                    exists=(metadata.snapshot is not None) if metadata else None,
                    sha256=metadata.content_sha256 if metadata else None,
                    byte_size=metadata.byte_size if metadata else None,
                    mode=metadata.snapshot.mode if metadata and metadata.snapshot else None,
                    content_kind=metadata.content_kind if metadata else None,
                )
            )
        return WorkspaceCheckpointView(
            checkpoint_id=row.checkpoint_id,
            scope_id=row.scope_id,
            title=row.title,
            request_key=row.request_key,
            workspace_key=row.workspace_key,
            resource_fingerprint=row.resource_fingerprint,
            capture_format=cast(CheckpointCaptureFormat, row.capture_format),
            status=cast(
                Literal["capturing", "available", "failed", "cancelled", "interrupted"], row.status
            ),
            files=files,
            error_code=row.error_code,
            created_at=row.created_at.replace(tzinfo=UTC),
            updated_at=row.updated_at.replace(tzinfo=UTC),
        )

    async def reserve(
        self,
        scope: str,
        request: WorkspaceCheckpointCreate,
        workspace_key: str,
        resource_fingerprint: str,
        workspace_fingerprint: str,
        *,
        capture_format: CheckpointCaptureFormat | None = None,
        model_source: CheckpointModelSource | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> tuple[WorkspaceCheckpointView, bool]:
        request = WorkspaceCheckpointCreate.model_validate(request.model_dump())
        if capture_format is None:
            capture_format = request.capture_format
        elif request.capture_format != "text" and request.capture_format != capture_format:
            raise ServiceError("workspace_checkpoint_format", "公开与可信捕获格式不一致", 422)
        if capture_format not in {"text", "bytes"}:
            raise ServiceError("workspace_checkpoint_format", "检查点捕获格式无效", 422)
        original_request = request.model_dump(mode="json")
        original_request.pop("capture_format", None)
        original: dict[str, JsonValue] = {
            "request": original_request,
            "key": workspace_key,
            "resource": resource_fingerprint,
            "workspace": workspace_fingerprint,
        }
        if capture_format == "bytes":
            original["capture_format"] = "bytes"
        fingerprint = digest(original)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            if authorize is not None:
                authorize()
            topic = await self.edits._scope(session, scope, writing=False)
            previous = await session.scalar(
                select(NativeWorkspaceCheckpointRow).where(
                    NativeWorkspaceCheckpointRow.scope_id == scope,
                    NativeWorkspaceCheckpointRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != fingerprint:
                    raise ServiceError(
                        "workspace_checkpoint_key_conflict", "检查点键已绑定其他请求", 409
                    )
                if await self._source(session, previous) != model_source:
                    raise ServiceError(
                        "workspace_checkpoint_model_conflict", "原检查点绑定其他父调用来源", 409
                    )
                self._expire(previous)
                await session.flush()
                return await self._view(session, previous), False
            await self.edits._scope(session, scope, writing=True)
            await self.edits._resource(session, scope, workspace_key, resource_fingerprint)
            if model_source is not None:
                self._valid_source(model_source)
                if not scope.startswith("session:"):
                    raise ServiceError(
                        "workspace_checkpoint_model_context", "模型检查点需要原Agent会话", 403
                    )
                await self.edits._scope(session, scope, writing=True, run_id=model_source.run_id)
                if (
                    await session.scalar(
                        select(NativeCheckpointModelSourceRow).where(
                            NativeCheckpointModelSourceRow.run_id == model_source.run_id,
                            NativeCheckpointModelSourceRow.call_id == model_source.call_id,
                        )
                    )
                    is not None
                ):
                    raise ServiceError(
                        "workspace_checkpoint_model_call_conflict",
                        "原模型调用已绑定其他检查点",
                        409,
                    )
            count = await session.scalar(
                select(func.count()).select_from(NativeWorkspaceCheckpointRow)
            )
            if (count or 0) >= self.max_checkpoints:
                raise ServiceError("workspace_checkpoint_limit", "检查点历史数量已达上限", 429)
            if authorize is not None:
                authorize()
            row = NativeWorkspaceCheckpointRow(
                checkpoint_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                workspace_key=workspace_key,
                resource_fingerprint=resource_fingerprint,
                workspace_fingerprint=workspace_fingerprint,
                request_key=request.request_key,
                request_hash=fingerprint,
                title=request.title,
                capture_format=capture_format,
                model_bound=model_source is not None,
                status="capturing",
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                token=secrets.token_hex(32),
            )
            session.add(row)
            await session.flush()
            if model_source is not None:
                session.add(
                    NativeCheckpointModelSourceRow(
                        checkpoint_id=row.checkpoint_id,
                        run_id=model_source.run_id,
                        call_id=model_source.call_id,
                    )
                )
            session.add_all(
                [
                    NativeWorkspaceCheckpointFileRow(
                        checkpoint_id=row.checkpoint_id,
                        position=position,
                        path=path,
                        captured=False,
                    )
                    for position, path in enumerate(request.paths)
                ]
            )
            await session.flush()
            return await self._view(session, row), True

    @staticmethod
    def _valid_source(source: CheckpointModelSource) -> None:
        if (
            not isinstance(source, CheckpointModelSource)
            or not isinstance(source.run_id, str)
            or not 1 <= len(source.run_id) <= 36
            or source.run_id == "local"
            or not isinstance(source.call_id, str)
            or not 1 <= len(source.call_id) <= 128
            or any(ord(char) < 32 or ord(char) == 127 for char in source.call_id)
        ):
            raise ServiceError(
                "workspace_checkpoint_model_context", "模型检查点需要原Run/call", 403
            )

    async def _source(
        self, session: AsyncSession, row: NativeWorkspaceCheckpointRow, *, live: bool = False
    ) -> CheckpointModelSource | None:
        saved = await session.get(NativeCheckpointModelSourceRow, row.checkpoint_id)
        if bool(row.model_bound) != (saved is not None):
            raise ServiceError("workspace_checkpoint_corrupt", "原模型来源标记或材料缺失", 409)
        if saved is None:
            return None
        source = CheckpointModelSource(saved.run_id, saved.call_id)
        self._valid_source(source)
        if not row.scope_id.startswith("session:"):
            raise ServiceError("workspace_checkpoint_corrupt", "模型检查点原会话损坏", 409)
        await self.edits._scope(session, row.scope_id, writing=live, run_id=source.run_id)
        return source

    async def source(self, scope: str, identifier: str) -> CheckpointModelSource | None:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope, writing=False)
            return await self._source(session, await self._row(session, scope, identifier))

    async def _fence(
        self,
        session: AsyncSession,
        row: NativeWorkspaceCheckpointRow,
        fence: CheckpointCaptureFence,
    ) -> None:
        if await self._source(session, row) != fence.model_source:
            raise ServiceError(
                "workspace_checkpoint_model_context", "捕获围栏未保留原模型父调用", 403
            )

    async def _live(
        self,
        session: AsyncSession,
        scope: str,
        identifier: str,
        token: str | None = None,
        capture_format: CheckpointCaptureFormat | None = None,
    ) -> NativeWorkspaceCheckpointRow:
        row = await self._row(session, scope, identifier)
        if (
            row.status != "capturing"
            or row.owner_id != self.owner_id
            or (row.lease_expires_at or 0) <= time.time()
            or (token is not None and not secrets.compare_digest(row.token, token))
            or (capture_format is not None and capture_format != row.capture_format)
        ):
            raise ServiceError("workspace_checkpoint_fenced", "检查点捕获资格已失效", 409)
        await self.edits._scope(session, scope, writing=True)
        await self.edits._resource(session, scope, row.workspace_key, row.resource_fingerprint)
        await self._source(session, row, live=True)
        return row

    async def execution(self, scope: str, identifier: str) -> tuple[CheckpointCaptureFence, ...]:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await self._live(session, scope, identifier)
            source = await self._source(session, row)
            positions = await session.scalars(
                select(NativeWorkspaceCheckpointFileRow.position)
                .where(NativeWorkspaceCheckpointFileRow.checkpoint_id == identifier)
                .order_by(NativeWorkspaceCheckpointFileRow.position)
            )
            return tuple(
                CheckpointCaptureFence(
                    identifier,
                    position,
                    row.token,
                    cast(CheckpointCaptureFormat, row.capture_format),
                    source,
                )
                for position in positions
            )

    async def assert_capture(
        self, scope: str, fence: CheckpointCaptureFence, path: str | None = None
    ) -> None:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await self._live(
                session, scope, fence.checkpoint_id, fence.token, fence.capture_format
            )
            await self._fence(session, row, fence)
            entry = await session.get(
                NativeWorkspaceCheckpointFileRow, (fence.checkpoint_id, fence.position)
            )
            if entry is None or (path is not None and entry.path != path):
                raise ServiceError("workspace_checkpoint_target", "捕获路径未匹配冻结目标", 403)

    @staticmethod
    async def _idle_file(session: AsyncSession, capture: CapturedFile) -> None:
        key = digest({"parent": list(capture.parent_identity), "name": capture.path.split("/")[-1]})
        if await session.get(WorkspaceFileLockRow, key) is not None:
            raise ServiceError("workspace_file_busy", "检查点文件有未收束的修改操作", 409)

    async def record(
        self,
        scope: str,
        fence: CheckpointCaptureFence,
        capture: CapturedFile,
        *,
        authorize: Callable[[], None] | None = None,
    ) -> None:
        metadata = encode_capture(
            capture,
            allow_binary=fence.capture_format == "bytes",
            native_bytes=fence.capture_format == "bytes",
        )
        content = capture.snapshot.data if capture.snapshot else None
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._live(
                session, scope, fence.checkpoint_id, fence.token, fence.capture_format
            )
            await self._fence(session, row, fence)
            if authorize is not None:
                authorize()
            entry = await session.get(
                NativeWorkspaceCheckpointFileRow, (fence.checkpoint_id, fence.position)
            )
            if (
                entry is None
                or entry.path != capture.path
                or row.workspace_fingerprint != capture.workspace_fingerprint
            ):
                raise ServiceError("workspace_checkpoint_target", "捕获内容未绑定原路径和资源", 403)
            await self._idle_file(session, capture)
            if entry.captured:
                if entry.capture_metadata != metadata or entry.content != content:
                    raise ServiceError("workspace_checkpoint_changed", "捕获目标已有其他版本", 409)
                return
            used = await session.scalar(
                select(
                    func.coalesce(
                        func.sum(func.length(NativeWorkspaceCheckpointFileRow.content)), 0
                    )
                )
            )
            if int(used or 0) + len(content or b"") > self.max_snapshot_bytes:
                raise ServiceError("workspace_checkpoint_bytes", "检查点私有内容容量已达上限", 429)
            if authorize is not None:
                authorize()
            entry.content, entry.capture_metadata, entry.captured = content, metadata, True
            row.updated_at = utc_now()

    async def captures(self, scope: str, identifier: str) -> tuple[CapturedFile, ...]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            await self._source(session, row)
            entries = list(
                await session.scalars(
                    select(NativeWorkspaceCheckpointFileRow)
                    .where(NativeWorkspaceCheckpointFileRow.checkpoint_id == identifier)
                    .order_by(NativeWorkspaceCheckpointFileRow.position)
                )
            )
            if not entries or any(not item.captured for item in entries):
                raise ServiceError("workspace_checkpoint_incomplete", "检查点文件尚未全部捕获", 409)
            captures = tuple(self._decode(row, item) for item in entries)
            if any(
                value.path != item.path or value.workspace_fingerprint != row.workspace_fingerprint
                for value, item in zip(captures, entries, strict=True)
            ):
                raise ServiceError(
                    "workspace_checkpoint_corrupt", "检查点私有路径或资源不匹配", 409
                )
            return captures

    async def captured(self, scope: str, fence: CheckpointCaptureFence) -> CapturedFile | None:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope, writing=False)
            row = await self._row(session, scope, fence.checkpoint_id)
            await self._fence(session, row, fence)
            entry = await session.get(
                NativeWorkspaceCheckpointFileRow, (fence.checkpoint_id, fence.position)
            )
            if (
                entry is None
                or not secrets.compare_digest(row.token, fence.token)
                or row.capture_format != fence.capture_format
            ):
                raise ServiceError("workspace_checkpoint_fenced", "检查点文件凭据不匹配", 403)
            if not entry.captured:
                return None
            value = self._decode(row, entry)
            if value.path != entry.path or value.workspace_fingerprint != row.workspace_fingerprint:
                raise ServiceError("workspace_checkpoint_corrupt", "检查点路径或资源损坏", 409)
            return value

    async def renew(self, scope: str, fence: CheckpointCaptureFence) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._live(
                session, scope, fence.checkpoint_id, fence.token, fence.capture_format
            )
            await self._fence(session, row, fence)
            row.lease_expires_at = time.time() + self.lease_seconds

    async def finish(
        self,
        scope: str,
        fence: CheckpointCaptureFence,
        *,
        authorize: Callable[[], None] | None = None,
    ) -> WorkspaceCheckpointView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._live(
                session, scope, fence.checkpoint_id, fence.token, fence.capture_format
            )
            await self._fence(session, row, fence)
            entries = list(
                await session.scalars(
                    select(NativeWorkspaceCheckpointFileRow).where(
                        NativeWorkspaceCheckpointFileRow.checkpoint_id == row.checkpoint_id
                    )
                )
            )
            if not entries or any(not entry.captured for entry in entries):
                raise ServiceError("workspace_checkpoint_incomplete", "检查点捕获未完成", 409)
            for entry in entries:
                await self._idle_file(session, self._decode(row, entry))
            if authorize is not None:
                authorize()
            self._end(row, "available", None)
            await session.flush()
            return await self._view(session, row)

    async def fail(self, scope: str, fence: CheckpointCaptureFence, code: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await self._row(session, scope, fence.checkpoint_id)
            await self._fence(session, row, fence)
            if (
                not secrets.compare_digest(row.token, fence.token)
                or row.capture_format != fence.capture_format
            ):
                raise ServiceError("workspace_checkpoint_fenced", "检查点捕获凭据不匹配", 403)
            if row.status == "capturing":
                self._expire(row)
                if row.status == "capturing":
                    self._end(row, "failed", code)

    async def get(self, scope: str, identifier: str) -> WorkspaceCheckpointView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            self._expire(row)
            await session.flush()
            return await self._view(session, row)

    async def by_key(self, scope: str, key: str) -> WorkspaceCheckpointView | None:
        self.edits._key(key)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope, writing=False)
            row = await session.scalar(
                select(NativeWorkspaceCheckpointRow).where(
                    NativeWorkspaceCheckpointRow.scope_id == scope,
                    NativeWorkspaceCheckpointRow.request_key == key,
                )
            )
            if row is None:
                return None
            self._expire(row)
            await session.flush()
            return await self._view(session, row)

    async def cancel(self, scope: str, identifier: str) -> WorkspaceCheckpointView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            self._expire(row)
            if row.status == "capturing":
                self._end(row, "cancelled", "workspace_checkpoint_cancelled")
            await session.flush()
            return await self._view(session, row)

    async def list(
        self, scope: str, *, offset: int = 0, limit: int = 32
    ) -> builtin_list[WorkspaceCheckpointView]:
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ServiceError("workspace_checkpoint_page", "检查点分页无效", 422)
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope, writing=False)
            rows = await session.scalars(
                select(NativeWorkspaceCheckpointRow)
                .where(NativeWorkspaceCheckpointRow.scope_id == scope)
                .order_by(
                    NativeWorkspaceCheckpointRow.created_at.desc(),
                    NativeWorkspaceCheckpointRow.checkpoint_id.desc(),
                )
                .offset(offset)
                .limit(limit)
            )
            return [await self._view(session, row) for row in rows]
