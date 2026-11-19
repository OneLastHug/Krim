"""Additive durable compaction-request tables; migration registration belongs to the host.

Only manual compaction scheduling state lives here. Summaries, prepared
projections and the frozen ledger stay in the existing ``compactions`` /
``work_turns`` tables owned by the compaction algorithm; snapshot content is
projected by the injected snapshot provider, never stored by this table.
"""

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
)

from .models import metadata

# One row per (session, request_key): a durable manual compaction request that
# survives restarts and is consumed only by a successful model-boundary pass.
context_requests = Table(
    "context_requests",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("request_key", String(128), primary_key=True),
    Column("actor_id", String(64), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("reason", String(2048)),
    Column("status", String(16), nullable=False),
    Column("run_id", String(36)),
    Column("result_hash", String(64)),
    Column("metadata", JSON),
    Column("created_at", Float, nullable=False),
    Column("fulfilled_at", Float),
    CheckConstraint("revision >= 1"),
    CheckConstraint("status IN ('pending', 'fulfilled')"),
)

CONTEXT_TABLES = (context_requests,)
CONTEXT_TABLE_NAMES = frozenset(table.name for table in CONTEXT_TABLES)
# Short alias for migration collectors that use a generic module-level name.
TABLE_NAMES = CONTEXT_TABLE_NAMES
