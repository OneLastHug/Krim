"""Per-account MCP endpoints and immutable session/turn selections."""

from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Table,
    UniqueConstraint,
)

from .models import metadata

mcp_connections = Table(
    "mcp_connections",
    metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "principal_id",
        String(64),
        ForeignKey("accounts.principal_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("name", String(32), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("credential_revision", Integer, nullable=False),
    Column("config", JSON, nullable=False),
    Column("credential_envelope", String(16384)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    UniqueConstraint("principal_id", "name"),
)

session_mcp = Table(
    "session_mcp",
    metadata,
    Column(
        "session_id",
        String(36),
        ForeignKey("sessions.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("connections", JSON, nullable=False),
    Column("revision", Integer, nullable=False),
)

turn_mcp = Table(
    "turn_mcp",
    metadata,
    Column(
        "turn_id",
        String(36),
        ForeignKey("turns.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("snapshot", JSON, nullable=False),
    Column("fingerprint", String(64), nullable=False),
)

mcp_requests = Table(
    "mcp_requests",
    metadata,
    Column(
        "principal_id",
        String(64),
        ForeignKey("accounts.principal_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("request_key", String(128), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    Column("result", JSON, nullable=False),
    Column("secret_connection_id", String(36)),
    Column("secret_revision", Integer),
    Column("secret_envelope", String(16384)),
)

MCP_TABLES = (mcp_connections, session_mcp, turn_mcp, mcp_requests)
MCP_TABLE_NAMES = frozenset(table.name for table in MCP_TABLES)
