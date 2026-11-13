"""Reversible session archive and trash metadata, separate from transcripts."""

from sqlalchemy import JSON, Column, Float, ForeignKey, Integer, String, Table

from .models import metadata

session_lifecycle = Table(
    "session_lifecycle",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("archived_at", Float),
    Column("deleted_at", Float),
    Column("updated_at", Float, nullable=False),
    Column("revision", Integer, nullable=False),
)
session_lifecycle_requests = Table(
    "session_lifecycle_requests",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("request_key", String(128), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("result", JSON, nullable=False),
    Column("created_at", Float, nullable=False),
)
SESSION_LIFECYCLE_TABLES = (session_lifecycle, session_lifecycle_requests)
SESSION_LIFECYCLE_TABLE_NAMES = frozenset(table.name for table in SESSION_LIFECYCLE_TABLES)
