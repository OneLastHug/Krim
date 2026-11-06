"""账号管理六表；复用核心 metadata，旧schema表集合由排除本模块表计算。"""

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)

from .models import metadata

accounts = Table(
    "accounts",
    metadata,
    Column("principal_id", String(64), primary_key=True),
    Column("display_name", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("platform_role", String(16), nullable=False),
    Column("authorization_generation", Integer, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("legacy", Boolean, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
)
access_keys = Table(
    "access_keys",
    metadata,
    Column("key_id", String(32), primary_key=True),
    Column("principal_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("token_digest", String(64), nullable=False),
    Column("credential_version", Integer, nullable=False),
    Column("masked", String(96), nullable=False),
    Column("weak", Boolean, nullable=False),
    Column("status", String(16), nullable=False),
    Column("issued_at", Float, nullable=False),
    Column("revoked_at", Float),
    Column("last_used_at", Float),
    Column("request_key", String(128), nullable=False),
    UniqueConstraint("token_digest"),
    UniqueConstraint("principal_id", "credential_version"),
)
account_workspaces = Table(
    "account_workspaces",
    metadata,
    Column("principal_id", String(64), ForeignKey("accounts.principal_id"), primary_key=True),
    Column("workspace_id", String(64), primary_key=True),
    Column("role", String(16), nullable=False),
    Column("approve", Boolean, nullable=False),
)
workspace_profiles = Table(
    "workspace_profiles",
    metadata,
    Column("workspace_id", String(64), primary_key=True),
    Column("display_name", String(64), nullable=False),
    Column("owner_id", String(64), ForeignKey("accounts.principal_id")),
)
account_events = Table(
    "account_events",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("request_key", String(128), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("action", String(32), nullable=False),
    Column("actor", String(64), nullable=False),
    Column("target", String(64)),
    Column("result", JSON, nullable=False),
    Column("at", Float, nullable=False),
    UniqueConstraint("actor", "request_key"),
)
account_meta = Table(
    "account_meta",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("body", JSON, nullable=False),
)

ACCOUNT_TABLES = (
    accounts,
    access_keys,
    account_workspaces,
    workspace_profiles,
    account_events,
    account_meta,
)
ACCOUNT_TABLE_NAMES = frozenset(table.name for table in ACCOUNT_TABLES)
IDENTITY_META_ID = "identity"
