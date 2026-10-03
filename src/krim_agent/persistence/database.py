"""SQLAlchemy 2 异步 SQLite 与消息树；迁移和分支边界见 ADR-0005。"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from pydantic import JsonValue
from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    event,
    func,
    select,
    update,
)
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from krim_agent.protocol.domain import (
    TERMINAL_STATUSES,
    ChatRequest,
    EditRequest,
    MessageTreeView,
    MessageView,
    RegenerateRequest,
    RunStatus,
    RunView,
    ServiceError,
    StreamEventView,
    TopicView,
)
from krim_agent.protocol.models import StreamFrame, utc_now
from krim_agent.tools.core import ToolAudit

from .migration import upgrade_database
from .tree import ROOT, MessageTree


class Base(DeclarativeBase):
    pass


class TopicRow(Base):
    __tablename__ = "topics"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    title: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    active_branches: Mapped[dict[str, str | None]] = mapped_column(JSON, default=dict)


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
    action: Mapped[str] = mapped_column(String(16), default="send")
    source_message_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)


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
    parent_id: Mapped[str | None] = mapped_column(ForeignKey("messages.id"), index=True)


class ToolAuditRow(Base):
    __tablename__ = "tool_audits"
    call_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), primary_key=True, index=True)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class RunEventRow(Base):
    __tablename__ = "run_events"
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), primary_key=True, index=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[Literal["start", "chunk", "end", "error"]] = mapped_column(String(16))
    data: Mapped[JsonValue] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class OAuthTokenRow(Base):
    __tablename__ = "oauth_tokens"
    __table_args__ = (
        CheckConstraint("revision >= 0", name="ck_oauth_revision"),
        CheckConstraint(
            "(lease_owner IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_oauth_lease",
        ),
    )
    scope_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    subject: Mapped[str] = mapped_column(String(256), primary_key=True)
    server: Mapped[str] = mapped_column(String(32))
    ciphertext: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class Database:
    def __init__(self, url: str) -> None:
        parsed = make_url(url)
        if parsed.drivername != "sqlite+aiosqlite":
            raise ValueError("M1 只支持 sqlite+aiosqlite")
        self.is_persistent = bool(
            parsed.database
            and parsed.database != ":memory:"
            and parsed.query.get("mode") != "memory"
        )
        if parsed.database and parsed.database != ":memory:":
            Path(parsed.database).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_async_engine(url, hide_parameters=True)

        @event.listens_for(self.engine.sync_engine, "connect")
        def foreign_keys(connection: Any, _record: Any) -> None:
            # 明确由 SQLAlchemy 开始事务，使 SQLite DDL 也能完整回滚。
            connection.isolation_level = None
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        @event.listens_for(self.engine.sync_engine, "begin")
        def begin(connection: Connection) -> None:
            connection.exec_driver_sql("BEGIN")

        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.write_lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(upgrade_database)

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
        self,
        topic_id: str,
        request: ChatRequest | EditRequest | RegenerateRequest,
        *,
        allow_new: bool = True,
        action: Literal["send", "edit", "regenerate"] = "send",
        source_message_id: str | None = None,
    ) -> tuple[RunView, bool]:
        text = "" if isinstance(request, RegenerateRequest) else request.text
        explicit_parent = request.parent_message_id if isinstance(request, ChatRequest) else None
        # 保留普通发送的 M1 幂等摘要；分支操作将动作与目标纳入摘要。
        fingerprint = (
            text
            if action == "send" and explicit_parent is None
            else json.dumps(
                {
                    "action": action,
                    "source": source_message_id,
                    "parent": explicit_parent,
                    "text": text,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        digest = hashlib.sha256(fingerprint.encode()).hexdigest()
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
            await self._require_idle(session, topic_id)
            messages = await self._messages(session, topic_id)
            tree = MessageTree(messages)
            path = tree.active_path(topic.active_branches)
            choices = dict(topic.active_branches)
            parent_id = path[-1].id if path else None
            input_id = str(uuid4())
            if action == "send" and explicit_parent is None:
                tree.require_complete_tools(path)
            if action == "edit":
                source = tree.nodes.get(source_message_id or "")
                if source is None:
                    raise ServiceError("message_not_found", "消息不在该话题中", 404)
                if source.role != "user":
                    raise ServiceError("message_not_editable", "只能编辑用户消息", 409)
                parent_id = source.parent_id
                choices = tree.select(source.id, choices)
            elif action == "regenerate":
                source = tree.user_ancestor(source_message_id or "")
                input_id = source.id
                choices = tree.select(source.id, choices)
                choices[source.id] = None
            elif explicit_parent is not None:
                source = tree.nodes.get(explicit_parent)
                await self._require_boundary(session, source)
                choices = tree.select(explicit_parent, choices)
                parent_id = explicit_parent
            run = RunRow(
                id=str(uuid4()),
                topic_id=topic_id,
                input_message_id=input_id,
                idempotency_key=request.idempotency_key,
                input_hash=digest,
                action=action,
                source_message_id=source_message_id,
            )
            session.add(run)
            await session.flush()
            if action != "regenerate":
                highest = max((message.sequence for message in messages), default=0)
                session.add(
                    MessageRow(
                        id=run.input_message_id,
                        topic_id=topic_id,
                        run_id=run.id,
                        sequence=highest + 1,
                        role="user",
                        content=text,
                        details={"edited_from": source_message_id} if action == "edit" else {},
                        parent_id=parent_id,
                    )
                )
                choices[parent_id or ROOT] = input_id
            topic.active_branches = choices
            topic.updated_at = utc_now()
            return RunView.model_validate(run), True

    async def _messages(self, session: AsyncSession, topic_id: str) -> list[MessageView]:
        rows = await session.scalars(
            select(MessageRow).where(MessageRow.topic_id == topic_id).order_by(MessageRow.sequence)
        )
        return [MessageView.model_validate(row) for row in rows]

    async def _require_idle(self, session: AsyncSession, topic_id: str) -> None:
        active = await session.scalar(
            select(RunRow.id).where(
                RunRow.topic_id == topic_id, RunRow.status.in_(("queued", "running"))
            )
        )
        if active:
            raise ServiceError("topic_busy", "话题已有活跃运行", 409)

    async def _require_boundary(self, session: AsyncSession, message: MessageView | None) -> None:
        if message is None:
            raise ServiceError("message_not_found", "消息不在该话题中", 404)
        if message.role == "tool" or message.details.get("tool_calls"):
            raise ServiceError("incomplete_tool_group", "不能从工具组中间切换或发送", 409)
        run = await session.get(RunRow, message.run_id)
        if run is None or run.status not in TERMINAL_STATUSES:
            raise ServiceError("message_not_ready", "消息所属运行尚未完成", 409)

    async def message_tree(self, topic_id: str) -> MessageTreeView:
        async with self.database.write_lock, self.database.sessions() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            messages = await self._messages(session, topic_id)
            path = MessageTree(messages).active_path(topic.active_branches)
            return MessageTreeView(
                topic=TopicView.model_validate(topic),
                messages=messages,
                active_path=[message.id for message in path],
            )

    async def list_messages(self, topic_id: str) -> list[MessageView]:
        tree = await self.message_tree(topic_id)
        by_id = {message.id: message for message in tree.messages}
        return [by_id[message_id] for message_id in tree.active_path]

    async def select_branch(self, topic_id: str, message_id: str) -> TopicView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            await self._require_idle(session, topic_id)
            tree = MessageTree(await self._messages(session, topic_id))
            await self._require_boundary(session, tree.nodes.get(message_id))
            choices = tree.select(message_id, topic.active_branches)
            tree.active_path(choices)
            topic.active_branches = choices
            topic.updated_at = utc_now()
            return TopicView.model_validate(topic)

    async def add_message(
        self, run: RunView, role: str, content: str, details: dict[str, JsonValue] | None = None
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            topic = await session.get(TopicRow, run.topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "话题不存在", 404)
            highest = await session.scalar(
                select(func.max(MessageRow.sequence)).where(MessageRow.topic_id == run.topic_id)
            )
            parent_id = (
                await session.scalar(
                    select(MessageRow.id)
                    .where(MessageRow.run_id == run.id)
                    .order_by(MessageRow.sequence.desc())
                    .limit(1)
                )
                or run.input_message_id
            )
            message_id = str(uuid4())
            payload = dict(details or {})
            if run.action == "regenerate":
                payload["regenerated_from"] = run.source_message_id
            session.add(
                MessageRow(
                    id=message_id,
                    topic_id=run.topic_id,
                    run_id=run.id,
                    sequence=(highest or 0) + 1,
                    role=role,
                    content=content,
                    details=payload,
                    parent_id=parent_id,
                )
            )
            topic.active_branches = {**topic.active_branches, parent_id: message_id}
            topic.updated_at = utc_now()

    async def get_run(self, run_id: str) -> RunView:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(RunRow, run_id)
            if row is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            return RunView.model_validate(row)

    async def run_messages(self, run_id: str) -> list[MessageView]:
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(MessageRow).where(MessageRow.run_id == run_id).order_by(MessageRow.sequence)
            )
            return [MessageView.model_validate(row) for row in rows]

    async def append_event(self, frame: StreamFrame) -> StreamFrame:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            run = await session.get(RunRow, frame.stream_id)
            if run is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            existing = await session.get(RunEventRow, (frame.stream_id, frame.seq))
            if existing is not None:
                current = StreamFrame(
                    stream_id=existing.run_id,
                    seq=existing.seq,
                    kind=existing.kind,
                    data=existing.data,
                    created_at=existing.created_at,
                )
                same = current.model_dump(mode="json") == frame.model_dump(mode="json")
                if frame.kind == "end":
                    current_data = current.data if isinstance(current.data, dict) else {}
                    frame_data = frame.data if isinstance(frame.data, dict) else {}
                    same = current.kind == frame.kind and {
                        key: value for key, value in current_data.items() if value is not None
                    } == {key: value for key, value in frame_data.items() if value is not None}
                if not same:
                    raise ServiceError("event_conflict", "流事件序号已经写入不同内容", 409)
                return current
            highest = await session.scalar(
                select(func.max(RunEventRow.seq)).where(RunEventRow.run_id == frame.stream_id)
            )
            if highest is not None and frame.seq != highest + 1:
                raise ServiceError("event_gap", "流事件序号不连续", 409)
            if highest is None and frame.seq != 0:
                raise ServiceError("event_gap", "流事件必须从序号 0 开始", 409)
            last_kind = await session.scalar(
                select(RunEventRow.kind)
                .where(RunEventRow.run_id == frame.stream_id)
                .order_by(RunEventRow.seq.desc())
                .limit(1)
            )
            if last_kind in {"end", "error"}:
                raise ServiceError("event_after_terminal", "终态帧之后不能追加流事件", 409)
            if frame.kind in {"end", "error"} and run.status not in TERMINAL_STATUSES:
                raise ServiceError("run_not_terminal", "必须先保存运行终态再发布终帧", 409)
            session.add(
                RunEventRow(
                    run_id=frame.stream_id,
                    seq=frame.seq,
                    kind=frame.kind,
                    data=frame.data,
                    created_at=frame.created_at,
                )
            )
            return frame

    async def list_events(self, run_id: str) -> list[StreamFrame]:
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(RunEventRow).where(RunEventRow.run_id == run_id).order_by(RunEventRow.seq)
            )
            return [
                StreamFrame(
                    stream_id=row.run_id,
                    seq=row.seq,
                    kind=row.kind,
                    data=row.data,
                    created_at=row.created_at,
                )
                for row in rows
            ]

    async def event_summary(self, run_id: str) -> StreamEventView:
        events = await self.list_events(run_id)
        if not events:
            return StreamEventView(
                run_id=run_id,
                first_seq=0,
                last_seq=-1,
                count=0,
                durable=self.database.is_persistent,
            )
        return StreamEventView(
            run_id=run_id,
            first_seq=events[0].seq,
            last_seq=events[-1].seq,
            count=len(events),
            terminal=events[-1].kind in {"end", "error"},
            durable=self.database.is_persistent,
        )

    async def ensure_terminal_event(self, run_id: str) -> list[StreamFrame]:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            run = await session.get(RunRow, run_id)
            if run is None:
                raise ServiceError("run_not_found", "运行不存在", 404)
            rows = await session.scalars(
                select(RunEventRow).where(RunEventRow.run_id == run_id).order_by(RunEventRow.seq)
            )
            events = [
                StreamFrame(
                    stream_id=row.run_id,
                    seq=row.seq,
                    kind=row.kind,
                    data=row.data,
                    created_at=row.created_at,
                )
                for row in rows
            ]
            if (
                not events
                or events[-1].kind in {"end", "error"}
                or run.status not in TERMINAL_STATUSES
            ):
                return events
            frame = StreamFrame(
                stream_id=run.id,
                seq=events[-1].seq + 1,
                kind="end",
                data={"status": run.status, "error": run.error, "code": run.error_code},
            )
            session.add(
                RunEventRow(
                    run_id=run.id,
                    seq=frame.seq,
                    kind=frame.kind,
                    data=frame.data,
                    created_at=frame.created_at,
                )
            )
            return [*events, frame]

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
        self,
        run_id: str,
        status: RunStatus,
        error: str | None,
        audits: list[ToolAudit],
        *,
        error_code: str | None = None,
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
                .values(
                    status=status,
                    error=error,
                    error_code=error_code,
                    finished_at=utc_now(),
                    updated_at=utc_now(),
                )
            )

    async def audits(self, run_id: str) -> list[ToolAudit]:
        await self.get_run(run_id)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(select(ToolAuditRow).where(ToolAuditRow.run_id == run_id))
            return [ToolAudit.model_validate(row.payload) for row in rows]

    async def recover_interrupted(self) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            rows = list(
                await session.scalars(
                    select(RunRow).where(RunRow.status.in_(("queued", "running")))
                )
            )
            for row in rows:
                row.status = "interrupted"
                row.error = "服务重启；未自动重放工具"
                row.error_code = "interrupted"
                row.finished_at = utc_now()
                row.updated_at = utc_now()
                highest = await session.scalar(
                    select(func.max(RunEventRow.seq)).where(RunEventRow.run_id == row.id)
                )
                last_kind = await session.scalar(
                    select(RunEventRow.kind)
                    .where(RunEventRow.run_id == row.id)
                    .order_by(RunEventRow.seq.desc())
                    .limit(1)
                )
                if last_kind == "end":
                    continue
                session.add(
                    RunEventRow(
                        run_id=row.id,
                        seq=(highest + 1) if highest is not None else 0,
                        kind="end",
                        data={"status": "interrupted", "error": row.error, "code": "interrupted"},
                    )
                )
