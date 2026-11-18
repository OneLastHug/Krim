"""Atomic project selection/binding; defaults are identities, never name matches."""

from __future__ import annotations

import time
import uuid
from typing import Literal

from sqlalchemy import insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.elements import ColumnElement

from krim_agent.contracts import ServiceError
from krim_agent.project_access.policy import initialize_project_access, resolve_project_access
from krim_agent.security import PrincipalContext

from .account_models import account_workspaces, accounts
from .models import sessions, turns
from .project_membership_models import project_defaults, project_memberships
from .sandbox_models import sandbox_environments, sandbox_projects, sandbox_session_bindings


def visible_sessions(principal_id: str) -> ColumnElement[bool]:
    """A correlated SQL predicate, applied before pagination and trace reads."""
    bound = select(sandbox_session_bindings.c.session_id).where(
        sandbox_session_bindings.c.session_id == sessions.c.id,
        sandbox_session_bindings.c.project_id.is_not(None),
    ).exists()
    permitted = select(sandbox_session_bindings.c.session_id).join(
        project_memberships,
        project_memberships.c.project_id == sandbox_session_bindings.c.project_id,
    ).where(
        sandbox_session_bindings.c.session_id == sessions.c.id,
        project_memberships.c.principal_id == principal_id,
        project_memberships.c.access.in_(["read", "write"]),
    ).exists()
    return or_(~bound, permitted)


async def default_project(conn: AsyncConnection, owner: str, workspace: str) -> str | None:
    """Create a logical default only for an existing, identified account.

    Legacy local/offline fixtures have no managed account and are not claimed by
    a new administrator. No filesystem/environment preparation happens here.
    """
    project_id = await conn.scalar(select(project_defaults.c.project_id).where(
        project_defaults.c.principal_id == owner,
        project_defaults.c.workspace_id == workspace,
    ))
    if project_id is not None:
        return str(project_id)
    account = await conn.scalar(select(accounts.c.principal_id).where(
        accounts.c.principal_id == owner,
    ))
    member = await conn.scalar(select(account_workspaces.c.workspace_id).where(
        account_workspaces.c.principal_id == owner,
        account_workspaces.c.workspace_id == workspace,
    ))
    if account is None or member is None:
        return None
    now = time.time()
    environment_id = await conn.scalar(select(sandbox_environments.c.environment_id).where(
        sandbox_environments.c.principal_id == owner,
    ))
    if environment_id is None:
        environment_id = str(uuid.uuid4())
        await conn.execute(insert(sandbox_environments).values(
            environment_id=environment_id, principal_id=owner, profile_name=None,
            quota_override={}, revision=1, status="unprovisioned", created_at=now, updated_at=now,
        ))
    project_id = str(uuid.uuid4())
    await conn.execute(insert(sandbox_projects).values(
        project_id=project_id, environment_id=environment_id, principal_id=owner,
        workspace_id=workspace, title="默认项目", generation=0, status="unprovisioned",
        created_at=now, updated_at=now,
    ))
    await initialize_project_access(conn, project_id, owner)
    await conn.execute(insert(project_defaults).values(
        principal_id=owner, workspace_id=workspace, project_id=project_id,
    ))
    return project_id


async def bind_new_session(
    conn: AsyncConnection, principal: PrincipalContext, workspace: str,
    session_id: str, requested_project: str | None,
) -> Literal["manual", "auto", "bypass"]:
    project_id = requested_project or await default_project(conn, principal.principal_id, workspace)
    if project_id is None:
        return "manual"
    access = await resolve_project_access(conn, principal, project_id)
    if access.workspace_id != workspace:
        raise ServiceError("not_found", 404)
    access.require()
    now = time.time()
    await conn.execute(insert(sandbox_session_bindings).values(
        session_id=session_id, principal_id=principal.principal_id, project_id=project_id,
        revision=1, created_at=now, updated_at=now,
    ))
    creator_write = access.access == "write" and access.owner_id == principal.principal_id
    return "bypass" if creator_write else "manual"


async def migrate_session_projects(conn: AsyncConnection) -> dict[str, int]:
    """Idempotent logical migration, retaining real bindings and busy sessions.

    The returned counts are a rehearsal/report seam; unknown owners and active
    turns are left untouched rather than reassigned or replayed.
    """
    report = {"bound": 0, "busy": 0, "unknown_owner": 0}
    rows = (await conn.execute(select(sessions.c.id, sessions.c.owner_id,
                                      sessions.c.workspace_id))).mappings().all()
    for row in rows:
        binding = (await conn.execute(select(sandbox_session_bindings).where(
            sandbox_session_bindings.c.session_id == row["id"],
        ))).mappings().first()
        if binding is not None and binding["project_id"] is not None:
            continue
        busy = await conn.scalar(select(turns.c.id).where(
            turns.c.session_id == row["id"], turns.c.status.in_(["queued", "running"]),
        ).limit(1))
        if busy is not None:
            report["busy"] += 1
            continue
        project_id = await default_project(conn, row["owner_id"], row["workspace_id"])
        if project_id is None:
            report["unknown_owner"] += 1
            continue
        now = time.time()
        if binding is None:
            await conn.execute(insert(sandbox_session_bindings).values(
                session_id=row["id"], principal_id=row["owner_id"], project_id=project_id,
                revision=1, created_at=now, updated_at=now,
            ))
        else:
            if binding["principal_id"] != row["owner_id"]:
                raise ServiceError("facts_corrupt")
            await conn.execute(update(sandbox_session_bindings).where(
                sandbox_session_bindings.c.session_id == row["id"],
            ).values(project_id=project_id, revision=binding["revision"] + 1, updated_at=now))
        report["bound"] += 1
    return report
