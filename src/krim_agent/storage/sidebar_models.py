"""Schema 17: per-principal session sidebar presentation layout."""

from sqlalchemy import JSON, Column, Float, Integer, String, Table

from .models import metadata

session_sidebars = Table(
    "session_sidebars",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("workspace_id", String(64), primary_key=True),
    Column("revision", Integer, nullable=False),
    Column("groups", JSON, nullable=False),
    Column("sessions", JSON, nullable=False),
    Column("updated_at", Float, nullable=False),
)

SIDEBAR_TABLES = (session_sidebars,)
SIDEBAR_TABLE_NAMES = frozenset(table.name for table in SIDEBAR_TABLES)
