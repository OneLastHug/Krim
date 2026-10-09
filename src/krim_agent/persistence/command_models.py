"""0030命令记录、私有字节与不可变生命周期事件。"""

from datetime import datetime

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
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class CommandGuardRow(Base):
    __tablename__ = "command_ledger_guard"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    revision: Mapped[int] = mapped_column(Integer)


class CommandRow(Base):
    __tablename__ = "command_executions"
    __table_args__ = (
        UniqueConstraint("scope_id", "request_key"),
        UniqueConstraint("run_id", "call_id"),
        CheckConstraint(
            "phase IN ('preparing','dispatched','running','finished','interrupted')",
            name="ck_command_phase",
        ),
        CheckConstraint("revision >= 0 AND reserved_bytes >= 0", name="ck_command_counts"),
        CheckConstraint(
            "(phase IN ('preparing','dispatched','running') AND owner_id IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR (phase IN ('finished','interrupted') "
            "AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_command_owner",
        ),
        CheckConstraint(
            "(run_id IS NULL AND call_id IS NULL) OR (run_id IS NOT NULL AND call_id IS NOT NULL)",
            name="ck_command_call",
        ),
    )
    command_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    scope_id: Mapped[str] = mapped_column(String(80), index=True)
    topic_id: Mapped[str] = mapped_column(ForeignKey("topics.id"))
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    snapshot: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)
    call_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    phase: Mapped[str] = mapped_column(String(16))
    revision: Mapped[int] = mapped_column(Integer)
    token: Mapped[str] = mapped_column(String(64))
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    sandbox_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean)
    dispatched: Mapped[bool] = mapped_column(Boolean)
    started: Mapped[bool] = mapped_column(Boolean)
    stdout: Mapped[bytes] = mapped_column(LargeBinary)
    stderr: Mapped[bytes] = mapped_column(LargeBinary)
    result: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reserved_bytes: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime)
    updated_at: Mapped[datetime] = mapped_column(DateTime)
    interrupted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class CommandFileRow(Base):
    __tablename__ = "command_files"
    command_id: Mapped[str] = mapped_column(
        ForeignKey("command_executions.command_id"), primary_key=True
    )
    kind: Mapped[str] = mapped_column(String(8), primary_key=True)
    path: Mapped[str] = mapped_column(String(512), primary_key=True)
    mode: Mapped[int] = mapped_column(Integer)
    sha256: Mapped[str] = mapped_column(String(64))
    content: Mapped[bytes] = mapped_column(LargeBinary)


class CommandEventRow(Base):
    __tablename__ = "command_events"
    command_id: Mapped[str] = mapped_column(
        ForeignKey("command_executions.command_id"), primary_key=True
    )
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[str] = mapped_column(String(24))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime)
