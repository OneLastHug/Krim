"""Skills and private memory: additive schema, no changes to existing tables."""

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.ext.asyncio import AsyncConnection

from .models import metadata

skills = Table(
    "skills",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("principal_id", String(64), nullable=False),
    Column("name", String(64), nullable=False),
    Column("enabled", Boolean, nullable=False),
    Column("revision", Integer, nullable=False),
    UniqueConstraint("principal_id", "name"),
)
skill_versions = Table(
    "skill_versions",
    metadata,
    Column("skill_id", String(36), ForeignKey("skills.id"), primary_key=True),
    Column("revision", Integer, primary_key=True),
    Column("document", JSON, nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("created_at", Float, nullable=False),
)
memories = Table(
    "memories",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("principal_id", String(64), nullable=False),
    Column("body", String, nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("source_ref", String(256)),
    Column("revision", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    # Append in the same order as ALTER TABLE for exact historical schema validation.
    # NULL/NULL preserves the historical account-private scope.
    Column("workspace_id", String(128)),
    Column("project_id", String(36)),
    Column("source_kind", String(16), nullable=False, server_default="user"),
    Column("memory_kind", String(16), nullable=False, server_default="note"),
    Column("authorization_turn_id", String(36)),
)
memory_preferences = Table(
    "memory_preferences",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("enabled", Boolean, nullable=False),
    Column("revision", Integer, nullable=False),
)
session_extensions = Table(
    "session_extensions",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("skills", JSON, nullable=False),
    Column("memory_enabled", Boolean, nullable=False),
    Column("revision", Integer, nullable=False),
)
turn_extensions = Table(
    "turn_extensions",
    metadata,
    Column("turn_id", String(36), ForeignKey("turns.id"), primary_key=True),
    Column("snapshot", JSON, nullable=False),
    Column("fingerprint", String(64), nullable=False),
)
extension_requests = Table(
    "extension_requests",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("request_key", String(128), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    # Receipts contain resource IDs/revisions, never memory bodies or Skill content.
    Column("result", JSON, nullable=False),
)
EXTENSION_TABLES = (
    skills,
    skill_versions,
    memories,
    memory_preferences,
    session_extensions,
    turn_extensions,
    extension_requests,
)
EXTENSION_TABLE_NAMES = frozenset(table.name for table in EXTENSION_TABLES)

memory_scope_index = Index(
    "ix_memories_owner_scope_updated",
    memories.c.principal_id,
    memories.c.workspace_id,
    memories.c.project_id,
    memories.c.updated_at,
    memories.c.id,
)


async def migrate_memory_scopes(conn: AsyncConnection) -> None:
    """Called by the central migration transaction; does not change schema_version.

    Existing records, receipts, preference switches and frozen snapshots are untouched.
    Safe to call again after an interrupted migration or on a fresh database.
    """
    from sqlalchemy import inspect

    columns = await conn.run_sync(
        lambda sync: {column["name"] for column in inspect(sync).get_columns("memories")}
    )
    additions = {
        "workspace_id": "VARCHAR(128)",
        "project_id": "VARCHAR(36)",
        "source_kind": "VARCHAR(16) NOT NULL DEFAULT 'user'",
        "memory_kind": "VARCHAR(16) NOT NULL DEFAULT 'note'",
        "authorization_turn_id": "VARCHAR(36)",
    }
    for name, declaration in additions.items():
        if name not in columns:
            await conn.exec_driver_sql(f"ALTER TABLE memories ADD COLUMN {name} {declaration}")
    await conn.run_sync(lambda sync: memory_scope_index.create(sync, checkfirst=True))
