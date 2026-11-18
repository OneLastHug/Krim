"""Project capability ceilings, independent of session confirmation modes."""

from sqlalchemy import Column, Float, ForeignKey, Integer, String, Table

from .models import metadata

project_memberships = Table(
    "project_memberships", metadata,
    Column("project_id", String, ForeignKey("sandbox_projects.project_id"), primary_key=True),
    Column("principal_id", String, primary_key=True),
    Column("access", String, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("updated_at", Float, nullable=False),
)

project_defaults = Table(
    "project_defaults", metadata,
    Column("principal_id", String, primary_key=True),
    Column("workspace_id", String, primary_key=True),
    Column("project_id", String, ForeignKey("sandbox_projects.project_id"), nullable=False),
)

PROJECT_MEMBERSHIP_TABLES = (project_memberships, project_defaults)
PROJECT_MEMBERSHIP_TABLE_NAMES = {table.name for table in PROJECT_MEMBERSHIP_TABLES}
