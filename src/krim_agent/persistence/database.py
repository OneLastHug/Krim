"""SQLAlchemy 2 异步 SQLite；SRC-OSS-006，M1 建表而非迁移框架。"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from pydantic import JsonValue
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    select,
    update,
)
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from krim_agent.protocol.domain import (
    TERMINAL_STATUSES,
    ChatRequest,
    MessageView,
    RunStatus,
    RunView,
    ServiceError,
    TopicView,
)
from krim_agent.protocol.models import utc_now
from krim_agent.tools.core import ToolAudit


class Base(DeclarativeBase):
    pass


class TopicRow(Base):
    __tablename__ = "topics"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    title: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class RunRow(Base):
    __tablename__ = "runs"
    __table_args__ = (UniqueConstraint("topic_id", "idempotency_key"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    input_message_id: Mapped[str] = mapped_column(String(36))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    input_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default="queued")
    stop_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class MessageRow(Base):
    __tablename__ = "messages"
    __table_args__ = (UniqueConstraint("topic_id", "sequence"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"), index=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    role: Mapped[str] = mapped_column(String(16))
    content: Mapped[str] = mapped_column(Text)
    details: Mapped[dict[str, JsonValue]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class ToolAuditRow(Base):
    __tablename__ = "tool_audits"
    call_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), primary_key=True, index=True)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class Database:
    def __init__(self, url: str) -> None:
        parsed = make_url(url)
        if parsed.drivername != "sqlite+aiosqlite":
            raise ValueError("M1 只支持 sqlite+aiosqlite")
        if parsed.database and parsed.database != ":memory:":
            Path(parsed.database).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_async_engine(url, hide_parameters=True)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.write_lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            await connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            await connection.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self.engine.dispose()


class KrimRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create_topic(self, title: str) -> TopicView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = TopicRow(id=str(uuid4()), title=title)
            session.add(row)
            await session.flush()
            return TopicView.model_validate(row)

    async def get_topic(self, topic_id: str) -> TopicView:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(TopicRow, topic_id)
            if row is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            return TopicView.model_validate(row)

    async def list_topics(self) -> list[TopicView]:
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(TopicRow).order_by(TopicRow.created_at, TopicRow.id)
            )
            return [TopicView.model_validate(row) for row in rows]

    async def begin_run(
        self, topic_id: str, request: ChatRequest, *, allow_new: bool = True
    ) -> tuple[RunView, bool]:
        digest = hashlib.sha256(request.text.encode()).hexdigest()
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            existing = await session.scalar(
                select(RunRow).where(
                    RunRow.topic_id == topic_id, RunRow.idempotency_key == request.idempotency_key
                )
            )
            if existing is not None:
                if existing.input_hash != digest:
                    raise ServiceError("idempotency_conflict", "同一幂等键不能用于不同输入", 409)
                return RunView.model_validate(existing), False
            if not allow_new:
                raise ServiceError("run_limit", "同时运行数量超过限制", 429)
            active = await session.scalar(
                select(RunRow.id).where(
                    RunRow.topic_id == topic_id, RunRow.status.in_(("queued", "running"))
                )
            )
            if active:
                raise ServiceError("topic_busy", "话题已有活跃运行", 409)
            run = RunRow(
                id=str(uuid4()),
                topic_id=topic_id,
                input_message_id=str(uuid4()),
                idempotency_key=request.idempotency_key,
                input_hash=digest,
            )
            session.add(run)
            await session.flush()
            highest = await session.scalar(
                select(func.max(MessageRow.sequence)).where(MessageRow.topic_id == topic_id)
            )
            session.add(
                MessageRow(
                    id=run.input_message_id,
                    topic_id=topic_id,
                    run_id=run.id,
                    sequence=(highest or 0) + 1,
                    role="user",
                    content=request.text,
                    details={},
                )
            )
            topic.updated_at = utc_now()
            return RunView.model_validate(run), True

    async def list_messages(self, topic_id: str) -> list[MessageView]:
        await self.get_topic(topic_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(MessageRow)
                .where(MessageRow.topic_id == topic_id)
                .order_by(MessageRow.sequence)
            )
            return [MessageView.model_validate(row) for row in rows]

    async def add_message(
        self, run: RunView, role: str, content: str, details: dict[str, JsonValue] | None = None
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            highest = await session.scalar(
                select(func.max(MessageRow.sequence)).where(MessageRow.topic_id == run.topic_id)
            )
            session.add(
                MessageRow(
                    id=str(uuid4()),
                    topic_id=run.topic_id,
                    run_id=run.id,
                    sequence=(highest or 0) + 1,
                    role=role,
                    content=content,
                    details=details or {},
                )
            )

    async def get_run(self, run_id: str) -> RunView:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(RunRow, run_id)
            if row is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            return RunView.model_validate(row)

    async def set_running(self, run_id: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(RunRow)
                .where(RunRow.id == run_id)
                .values(status="running", updated_at=utc_now())
            )

    async def request_stop(self, run_id: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(RunRow)
                .where(RunRow.id == run_id, RunRow.status.not_in(TERMINAL_STATUSES))
                .values(stop_requested=True, updated_at=utc_now())
            )

    async def finish(
        self, run_id: str, status: RunStatus, error: str | None, audits: list[ToolAudit]
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            for audit in audits:
                session.add(
                    ToolAuditRow(
                        call_id=audit.call_id, run_id=run_id, payload=audit.model_dump(mode="json")
                    )
                )
            await session.execute(
                update(RunRow)
                .where(RunRow.id == run_id)
                .values(status=status, error=error, finished_at=utc_now(), updated_at=utc_now())
            )

    async def audits(self, run_id: str) -> list[ToolAudit]:
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(select(ToolAuditRow).where(ToolAuditRow.run_id == run_id))
            return [ToolAudit.model_validate(row.payload) for row in rows]

    async def recover_interrupted(self) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(RunRow)
                .where(RunRow.status.in_(("queued", "running")))
                .values(
                    status="interrupted",
                    error="服务重启；未自动重放工具",
                    finished_at=utc_now(),
                    updated_at=utc_now(),
                )
            )
