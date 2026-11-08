"""沙箱数据表：账号环境、持久项目、操作意图与会话绑定；复用核心metadata。"""

from sqlalchemy import (
    JSON,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)

from .models import metadata

sandbox_environments = Table(
    "sandbox_environments",
    metadata,
    Column("environment_id", String(36), primary_key=True),
    Column("principal_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("profile_name", String(64)),
    Column("quota_override", JSON, nullable=False, default=dict),
    Column("revision", Integer, nullable=False, default=1),
    Column("status", String(16), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    UniqueConstraint("principal_id"),
)
sandbox_projects = Table(
    "sandbox_projects",
    metadata,
    Column("project_id", String(36), primary_key=True),
    Column(
        "environment_id",
        String(36),
        ForeignKey("sandbox_environments.environment_id"),
        nullable=False,
    ),
    Column("principal_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("workspace_id", String(128), nullable=False),
    Column("title", String(128), nullable=False),
    Column("generation", Integer, nullable=False, default=0),
    Column("status", String(16), nullable=False),
    Column("lease_id", String(128)),
    Column("lease_expires_at", Float),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
)
sandbox_operations = Table(
    "sandbox_operations",
    metadata,
    Column("operation_id", String(36), primary_key=True),
    Column("principal_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("request_key", String(128), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("project_id", String(36), ForeignKey("sandbox_projects.project_id")),
    Column("kind", String(64), nullable=False),
    Column("status", String(64), nullable=False),
    Column("result", JSON, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    UniqueConstraint("principal_id", "request_key"),
)

SANDBOX_V8_TABLES = (sandbox_environments, sandbox_projects, sandbox_operations)

sandbox_session_bindings = Table(
    "sandbox_session_bindings",
    metadata,
    Column("session_id", String(36), ForeignKey("sessions.id"), primary_key=True),
    Column("principal_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("project_id", String(36), ForeignKey("sandbox_projects.project_id")),
    Column("revision", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
)
SANDBOX_TABLES = (*SANDBOX_V8_TABLES, sandbox_session_bindings)
SANDBOX_TABLE_NAMES = frozenset(table.name for table in SANDBOX_TABLES)

sandbox_quota_state = Table(
    "sandbox_quota_state",
    metadata,
    Column("principal_id", String(64), ForeignKey("accounts.principal_id"), primary_key=True),
    Column(
        "environment_id",
        String(36),
        ForeignKey("sandbox_environments.environment_id"),
        nullable=False,
        unique=True,
    ),
    Column("desired", JSON, nullable=False),
    Column("old_desired", JSON, nullable=False),
    Column("desired_revision", Integer, nullable=False),
    Column("applied_revision", Integer, nullable=False),
    Column("operation_id", String(36), nullable=False),
    Column("state", String(32), nullable=False),
    Column("updated_at", Float, nullable=False),
)
SANDBOX_TABLES = (*SANDBOX_TABLES, sandbox_quota_state)
SANDBOX_TABLE_NAMES = frozenset(table.name for table in SANDBOX_TABLES)
