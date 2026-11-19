"""Schema 15: project development bindings and durable command admission.

One operation ledger serves SDK side effects and process reservations. Runtime
process control remains outside SQLite; unknown outcomes cannot be redispatched.
"""

from sqlalchemy import JSON, Column, Float, ForeignKey, Integer, String, Table, UniqueConstraint

from .models import metadata

development_environments = Table(
    "development_environments",
    metadata,
    Column("environment_id", String(128), primary_key=True),
    Column("project_id", String(36), ForeignKey("sandbox_projects.project_id"), nullable=False),
    Column("owner_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("generation", Integer, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("record", JSON, nullable=False),
    Column("previous_record", JSON),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    UniqueConstraint("project_id"),
)

development_operations = Table(
    "development_operations",
    metadata,
    Column("operation_id", String(128), primary_key=True),
    Column("environment_id", String(128),
           ForeignKey("development_environments.environment_id"), nullable=False),
    Column("owner_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("project_id", String(36), ForeignKey("sandbox_projects.project_id"), nullable=False),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("run_id", String(36), ForeignKey("runs.id"), nullable=False),
    Column("identity", JSON, nullable=False),
    Column("receipt", JSON, nullable=False),
    Column("command_digest", String(64)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
)

development_processes = Table(
    "development_processes",
    metadata,
    Column("process_id", String(128), primary_key=True),
    Column("operation_id", String(128),
           ForeignKey("development_operations.operation_id"), nullable=False, unique=True),
    Column("environment_id", String(128),
           ForeignKey("development_environments.environment_id"), nullable=False),
    Column("owner_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("project_id", String(36), ForeignKey("sandbox_projects.project_id"), nullable=False),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("run_id", String(36), ForeignKey("runs.id"), nullable=False),
    Column("lifecycle", String(32), nullable=False),
    Column("state", String(32), nullable=False),
    Column("request_digest", String(64), nullable=False),
    Column("request", JSON, nullable=False),
    Column("handle", JSON, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("execution_id", String(128)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
)

project_access_records = Table(
    "project_access_records",
    metadata,
    Column("project_id", String(36), ForeignKey("sandbox_projects.project_id"), primary_key=True),
    Column("kind", String(16), primary_key=True),
    Column("resource_id", String(128), primary_key=True),
    Column("owner_id", String(64), ForeignKey("accounts.principal_id"), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("document", JSON, nullable=False),
    Column("updated_at", Float, nullable=False),
)

DEVELOPMENT_TABLES = (
    development_environments, development_operations, development_processes, project_access_records,
)
DEVELOPMENT_TABLE_NAMES = frozenset(table.name for table in DEVELOPMENT_TABLES)
