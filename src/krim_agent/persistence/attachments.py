"""SQLite Blob 附件存储；话题隔离、哈希校验和显式 attachment_id 绑定。"""

from __future__ import annotations

import base64
import hashlib
from datetime import UTC
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer

from krim_agent.attachment import (
    MAX_TOPIC_ATTACHMENT_BYTES,
    MAX_TOPIC_ATTACHMENTS,
    AttachmentContent,
    AttachmentUpload,
    AttachmentView,
    attachment_hash,
    attachment_id,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now

from .database import AttachmentRow, Database, TopicRow


class AttachmentRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    @staticmethod
    def _identifier(value: str) -> str:
        try:
            return attachment_id(value)
        except ValueError:
            raise ServiceError("attachment_id_invalid", "附件 ID 无效", 422) from None

    @staticmethod
    async def _topic(session: AsyncSession, topic_id: str) -> TopicRow:
        topic = await session.get(TopicRow, topic_id)
        if topic is None:
            raise ServiceError("topic_not_found", "话题不存在", 404)
        return topic

    @staticmethod
    def _view(row: AttachmentRow) -> AttachmentView:
        return AttachmentView(
            id=row.id,
            topic_id=row.topic_id,
            filename=row.filename,
            media_type=row.media_type,
            byte_size=row.byte_size,
            sha256=row.sha256,
            created_at=row.created_at.replace(tzinfo=UTC),
        )

    async def upload(self, topic_id: str, request: AttachmentUpload) -> AttachmentView:
        request = AttachmentUpload.model_validate(request.model_dump(mode="json"))
        raw = request.decode()
        digest = attachment_hash(raw)
        request_hash = hashlib.sha256(request.model_dump_json().encode()).hexdigest()
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._topic(session, topic_id)
            previous = await session.scalar(
                select(AttachmentRow).where(
                    AttachmentRow.topic_id == topic_id,
                    AttachmentRow.idempotency_key == request.idempotency_key,
                )
            )
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise ServiceError("idempotency_conflict", "附件幂等键已绑定其他内容", 409)
                return self._view(previous)
            result = await session.execute(
                select(
                    func.count(AttachmentRow.id),
                    func.coalesce(func.sum(AttachmentRow.byte_size), 0),
                ).where(AttachmentRow.topic_id == topic_id)
            )
            used_count, used_bytes = result.one()
            if used_count >= MAX_TOPIC_ATTACHMENTS:
                raise ServiceError("attachment_count_limit", "话题附件数量超过上限", 429)
            if int(used_bytes) + len(raw) > MAX_TOPIC_ATTACHMENT_BYTES:
                raise ServiceError("attachment_topic_limit", "话题附件总量超过上限", 429)
            row = AttachmentRow(
                id=str(uuid4()),
                topic_id=topic_id,
                filename=request.filename,
                media_type=request.media_type.lower(),
                byte_size=len(raw),
                sha256=digest,
                content=raw,
                idempotency_key=request.idempotency_key,
                request_hash=request_hash,
                created_at=utc_now(),
            )
            session.add(row)
            await session.flush()
            return self._view(row)

    async def list(self, topic_id: str) -> list[AttachmentView]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._topic(session, topic_id)
            rows = await session.scalars(
                select(AttachmentRow)
                .options(defer(AttachmentRow.content))
                .where(AttachmentRow.topic_id == topic_id)
                .order_by(AttachmentRow.created_at, AttachmentRow.id)
            )
            return [self._view(row) for row in rows]

    async def get(self, topic_id: str, attachment_identifier: str) -> AttachmentView:
        identifier = self._identifier(attachment_identifier)
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(AttachmentRow, identifier)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("attachment_not_found", "附件不在该话题中", 404)
            return self._view(row)

    async def content(self, topic_id: str, attachment_identifier: str) -> AttachmentContent:
        identifier = self._identifier(attachment_identifier)
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(AttachmentRow, identifier)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("attachment_not_found", "附件不在该话题中", 404)
            view = self._view(row)
            if hashlib.sha256(row.content).hexdigest() != row.sha256:
                raise ServiceError("attachment_corrupt", "附件校验和不一致", 500)
            return AttachmentContent(
                attachment=view,
                content_base64=base64.b64encode(row.content).decode("ascii"),
            )

    async def delete(self, topic_id: str, attachment_identifier: str) -> None:
        identifier = self._identifier(attachment_identifier)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.get(AttachmentRow, identifier)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("attachment_not_found", "附件不在该话题中", 404)
            await session.delete(row)
