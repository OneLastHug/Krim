"""Project ACL: capability ceilings never inherit the workspace admin exception.

These functions borrow the caller's connection so create/bind/authorize can share
one short transaction. Session modes and approval records do not belong here.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal, cast

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.security import PrincipalContext
from krim_agent.storage.project_membership_models import project_memberships
from krim_agent.storage.sandbox_models import sandbox_projects

type ProjectAccess = Literal["none", "read", "write"]


@dataclass(frozen=True)
class ProjectAccessSnapshot:
    access: ProjectAccess
    revision: int
    project_id: str
    owner_id: str
    workspace_id: str

    def require(self, *, write: bool = False, manage: bool = False,
                principal_id: str | None = None) -> None:
        if self.access == "none":
            raise ServiceError("not_found", 404)
        if write and self.access != "write":
            raise ServiceError("project_readonly", 403)
        if manage and principal_id != self.owner_id:
            raise ServiceError("permission_denied", 403)


async def resolve_project_access(
    conn: AsyncConnection, principal: PrincipalContext, project_id: str,
) -> ProjectAccessSnapshot:
    row = (await conn.execute(select(sandbox_projects).where(
        sandbox_projects.c.project_id == project_id,
    ))).mappings().first()
    if row is None:
        raise ServiceError("not_found", 404)
    principal.member(row["workspace_id"])
    membership = (await conn.execute(select(project_memberships).where(
        project_memberships.c.project_id == project_id,
        project_memberships.c.principal_id == principal.principal_id,
    ))).mappings().first()
    # Missing records are denied, including platform/workspace administrators.
    access = membership["access"] if membership is not None else "none"
    revision = membership["revision"] if membership is not None else 0
    if access not in {"none", "read", "write"} or type(revision) is not int or revision < 0:
        raise ServiceError("facts_corrupt")
    return ProjectAccessSnapshot(
        cast(ProjectAccess, access), revision, project_id, row["principal_id"], row["workspace_id"],
    )


async def initialize_project_access(
    conn: AsyncConnection, project_id: str, owner_id: str,
) -> None:
    existing = await conn.scalar(select(project_memberships.c.revision).where(
        project_memberships.c.project_id == project_id,
        project_memberships.c.principal_id == owner_id,
    ))
    if existing is None:
        await conn.execute(insert(project_memberships).values(
            project_id=project_id, principal_id=owner_id, access="write",
            revision=1, updated_at=time.time(),
        ))


async def migrate_project_access(conn: AsyncConnection) -> None:
    rows = (await conn.execute(select(sandbox_projects.c.project_id,
                                      sandbox_projects.c.principal_id))).all()
    for project_id, owner_id in rows:
        await initialize_project_access(conn, project_id, owner_id)


async def set_project_access(
    conn: AsyncConnection, principal: PrincipalContext, project_id: str,
    subject_id: str, access: ProjectAccess, *, expected_revision: int,
) -> ProjectAccessSnapshot:
    """Owner-only CAS seam for future per-project membership management.

    Callers must coordinate revocation with runtime cancellation before exposing
    this internal seam as a public management endpoint.
    """
    if (access not in {"none", "read", "write"} or not subject_id
            or type(expected_revision) is not int or expected_revision < 0):
        raise ServiceError("invalid_input", 422)
    current = await resolve_project_access(conn, principal, project_id)
    current.require(manage=True, principal_id=principal.principal_id)
    row = (await conn.execute(select(project_memberships).where(
        project_memberships.c.project_id == project_id,
        project_memberships.c.principal_id == subject_id,
    ))).mappings().first()
    if (row["revision"] if row is not None else 0) != expected_revision:
        raise ServiceError("revision_conflict", 409)
    values = dict(access=access, revision=expected_revision + 1, updated_at=time.time())
    if row is None:
        await conn.execute(insert(project_memberships).values(
            project_id=project_id, principal_id=subject_id, **values,
        ))
    else:
        await conn.execute(update(project_memberships).where(
            project_memberships.c.project_id == project_id,
            project_memberships.c.principal_id == subject_id,
            project_memberships.c.revision == expected_revision,
        ).values(**values))
    return ProjectAccessSnapshot(access, expected_revision + 1, project_id,
                                 current.owner_id, current.workspace_id)
