"""Schema 16: immutable preview bindings and hashed browser credentials."""

from sqlalchemy import JSON, Column, Float, ForeignKey, Integer, String, Table

from .models import metadata

preview_records = Table(
    "development_previews", metadata,
    Column("preview_id", String(128), primary_key=True),
    Column("host_key", String(40), nullable=False, unique=True),
    Column("owner_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("project_id", String(36), ForeignKey("sandbox_projects.project_id"), nullable=False),
    Column("environment_id", String(128),
           ForeignKey("development_environments.environment_id"), nullable=False),
    Column("generation", Integer, nullable=False),
    Column("process_id", String(128),
           ForeignKey("development_processes.process_id"), nullable=False),
    Column("port", Integer, nullable=False),
    Column("state", String(16), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("route_epoch", Integer, nullable=False),
    Column("document", JSON, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
)

preview_credentials = Table(
    "development_preview_credentials", metadata,
    Column("digest", String(64), primary_key=True),
    Column("kind", String(16), nullable=False),
    Column("preview_id", String(128),
           ForeignKey("development_previews.preview_id"), nullable=False),
    Column("principal", JSON, nullable=False),
    Column("expires_at", Float, nullable=False),
    Column("consumed_at", Float),
    Column("created_at", Float, nullable=False),
)

PREVIEW_TABLES = (preview_records, preview_credentials)
PREVIEW_TABLE_NAMES = frozenset(table.name for table in PREVIEW_TABLES)
