"""评估任务事务模型；与业务 Run 通过已校验的绑定关联。"""

from datetime import datetime

from pydantic import JsonValue
from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from krim_agent.protocol.models import utc_now

from .database import Base


class EvalJobRow(Base):
    __tablename__ = "eval_jobs"
    __table_args__ = (
        UniqueConstraint("scope", "request_key"),
        CheckConstraint(
            "epoch >= 0 AND revision >= 0 AND event_count >= 0", name="ck_eval_job_counts"
        ),
        CheckConstraint(
            "(owner_id IS NULL AND lease_expires_at IS NULL) OR "
            "(owner_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_eval_job_lease",
        ),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    scope: Mapped[str] = mapped_column(String(64), index=True)
    request_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    snapshot: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), index=True)
    control: Mapped[str | None] = mapped_column(String(16), nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    epoch: Mapped[int] = mapped_column(Integer, default=0)
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    report: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class EvalItemRow(Base):
    __tablename__ = "eval_items"
    job_id: Mapped[str] = mapped_column(ForeignKey("eval_jobs.id"), primary_key=True)
    ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    case_id: Mapped[str] = mapped_column(String(128))
    repetition: Mapped[int] = mapped_column(Integer)
    generation: Mapped[int] = mapped_column(Integer, default=1)
    phase: Mapped[str] = mapped_column(String(16), index=True)
    bindings: Mapped[dict[str, JsonValue]] = mapped_column(JSON, default=dict)
    output: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    scores: Mapped[list[JsonValue]] = mapped_column(JSON, default=list)
    result: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)
    history: Mapped[list[JsonValue]] = mapped_column(JSON, default=list)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    duration_ms: Mapped[float] = mapped_column(Float, default=0)
    scoring_ms: Mapped[float] = mapped_column(Float, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)


class EvalEventRow(Base):
    __tablename__ = "eval_events"
    job_id: Mapped[str] = mapped_column(ForeignKey("eval_jobs.id"), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[str] = mapped_column(String(32))
    data: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now)
