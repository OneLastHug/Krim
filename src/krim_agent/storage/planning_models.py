"""Additive session planning tables; migration registration belongs to the host."""

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
)

from .models import metadata

session_plans = Table(
    "session_plans",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("workspace_id", String(64), nullable=False),
    Column("owner_id", String(64), nullable=False),
    Column("revision", Integer, nullable=False),
    CheckConstraint("revision >= 1"),
)

session_plan_versions = Table(
    "session_plan_versions",
    metadata,
    Column("session_id", String(36), ForeignKey("session_plans.session_id"), primary_key=True),
    Column("revision", Integer, primary_key=True),
    Column("content_hash", String(64), nullable=False),
    Column("counts", JSON, nullable=False),
    Column("total", Integer, nullable=False),
    Column("updated_at", Float, nullable=False),
    CheckConstraint("revision >= 1"),
    CheckConstraint("total >= 0"),
)

session_plan_steps = Table(
    "session_plan_steps",
    metadata,
    Column("session_id", String(36), primary_key=True),
    Column("revision", Integer, primary_key=True),
    Column("position", Integer, primary_key=True),
    Column("step_id", String(128), nullable=False),
    Column("text", String, nullable=False),
    Column("status", String(16), nullable=False),
    Column("content_hash", String(64), nullable=False),
    ForeignKeyConstraint(
        ["session_id", "revision"],
        ["session_plan_versions.session_id", "session_plan_versions.revision"],
    ),
    UniqueConstraint("session_id", "revision", "step_id"),
    CheckConstraint("position >= 0"),
    CheckConstraint("status IN ('pending', 'in_progress', 'completed', 'blocked')"),
)
Index(
    "ix_session_plan_steps_status",
    session_plan_steps.c.session_id,
    session_plan_steps.c.revision,
    session_plan_steps.c.status,
    session_plan_steps.c.position,
)

session_plan_requests = Table(
    "session_plan_requests",
    metadata,
    Column("session_id", String(36), primary_key=True),
    Column("actor_id", String(64), primary_key=True),
    Column("request_key", String(128), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
    ForeignKeyConstraint(
        ["session_id", "revision"],
        ["session_plan_versions.session_id", "session_plan_versions.revision"],
    ),
)

PLANNING_TABLES = (
    session_plans,
    session_plan_versions,
    session_plan_steps,
    session_plan_requests,
)
PLANNING_TABLE_NAMES = frozenset(table.name for table in PLANNING_TABLES)
# Short alias for migration collectors that use a generic module-level name.
TABLE_NAMES = PLANNING_TABLE_NAMES
