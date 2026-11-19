"""目标持续执行的独立表；迁移登记与共享 schema 由宿主统一处理。

设计约束（与中央 ``SQLiteStore._validate_schema`` 兼容）：

* 不使用 partial unique index；“每会话单一当前目标”由 ``session_goals`` 以
  ``session_id`` 为主键（单 head 行）结构性保证，不依赖只有部分索引才能表达的约束。
* 所有索引均为主键或普通 ``UniqueConstraint``，和既有表的验签规则一致。
* ``goal_versions`` 自带 origin/created_at，使请求键幂等回放不依赖当前 head 行。

- ``session_goals``：每会话一行当前目标 head（同会话单 active goal 的存储基础）。
- ``goal_versions``：不可变目标版本，CAS/幂等回放的唯一事实来源。
- ``goal_requests``：会话内 request_key -> 已提交的 goal_id/版本，幂等重放。
- ``goal_continuations``：内部续跑请求与幂等 continue key（宿主排队协议的事实）。
"""

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
)

from .models import metadata

GOAL_STATUS_SQL = "'active', 'paused', 'blocked', 'completed', 'cancelled'"
CONTINUATION_STATUS_SQL = "'queued', 'consumed', 'superseded', 'failed'"

session_goals = Table(
    "session_goals",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("workspace_id", String(64), nullable=False),
    Column("owner_id", String(64), nullable=False),
    Column("goal_id", String(36), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("objective", String, nullable=False),
    Column("completion_criteria", JSON, nullable=False),
    Column("evidence_refs", JSON, nullable=False),
    Column("status_note", String),
    Column("blocked_reason", String),
    Column("origin_turn_id", String(36), nullable=False),
    Column("origin_run_id", String(36), nullable=False),
    Column("updated_turn_id", String(36)),
    Column("updated_run_id", String(36)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    CheckConstraint("revision >= 1"),
    CheckConstraint(f"status IN ({GOAL_STATUS_SQL})"),
)

goal_versions = Table(
    "goal_versions",
    metadata,
    Column("goal_id", String(36), primary_key=True),
    Column("revision", Integer, primary_key=True),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("workspace_id", String(64), nullable=False),
    Column("owner_id", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("objective", String, nullable=False),
    Column("completion_criteria", JSON, nullable=False),
    Column("evidence_refs", JSON, nullable=False),
    Column("status_note", String),
    Column("blocked_reason", String),
    Column("origin_turn_id", String(36), nullable=False),
    Column("origin_run_id", String(36), nullable=False),
    Column("updated_turn_id", String(36)),
    Column("updated_run_id", String(36)),
    Column("content_hash", String(64), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    CheckConstraint("revision >= 1"),
    CheckConstraint(f"status IN ({GOAL_STATUS_SQL})"),
)

goal_requests = Table(
    "goal_requests",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("request_key", String(128), primary_key=True),
    Column("actor_id", String(64), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("goal_id", String(36), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
    CheckConstraint("revision >= 1"),
)

goal_continuations = Table(
    "goal_continuations",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    # The continue key is also the host turn request key: primary key gives
    # session-scoped uniqueness without a partial index.
    Column("request_key", String(128), primary_key=True),
    Column("workspace_id", String(64), nullable=False),
    Column("owner_id", String(64), nullable=False),
    Column("goal_id", String(36), nullable=False),
    Column("goal_revision_base", Integer, nullable=False),
    Column("source_turn_id", String(36)),
    Column("source_run_id", String(36)),
    Column("origin_turn_id", String(36), nullable=False),
    Column("origin_run_id", String(36), nullable=False),
    Column("status", String(16), nullable=False),
    Column("internal", Boolean, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("turn_id", String(36)),
    Column("outcome", String(16)),
    Column("code", String(64)),
    Column("goal_revision", Integer),
    Column("progressed", Boolean),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    CheckConstraint("internal = 1"),
    CheckConstraint(f"status IN ({CONTINUATION_STATUS_SQL})"),
)

GOAL_TABLES = (session_goals, goal_versions, goal_requests, goal_continuations)
GOAL_TABLE_NAMES = frozenset(table.name for table in GOAL_TABLES)
# Short alias for migration collectors that use a generic module-level name.
TABLE_NAMES = GOAL_TABLE_NAMES
