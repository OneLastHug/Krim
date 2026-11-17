"""Tables registered separately for the central schema migration."""

from sqlalchemy import JSON, Column, Float, ForeignKey, Integer, String, Table, UniqueConstraint

from .models import metadata

interaction_modes = Table(
    "interaction_modes",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("mode", String(16), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("updated_at", Float, nullable=False),
)
interaction_requests = Table(
    "interaction_requests",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("workspace_id", String(64), nullable=False),
    Column("owner_id", String(64), nullable=False),
    Column("turn_id", String(36), nullable=False),
    Column("run_id", String(36), nullable=False),
    Column("request_key", String(128), nullable=False),
    Column("kind", String(32), nullable=False),
    Column("status", String(16), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("mode_revision", Integer, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("response", JSON),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    UniqueConstraint("session_id", "request_key"),
)
interaction_mutations = Table(
    "interaction_mutations",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("request_key", String(128), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    Column("result", JSON, nullable=False),
)

INTERACTION_TABLES = (interaction_modes, interaction_requests, interaction_mutations)
INTERACTION_TABLE_NAMES = frozenset(table.name for table in INTERACTION_TABLES)
TABLE_NAMES = INTERACTION_TABLE_NAMES
