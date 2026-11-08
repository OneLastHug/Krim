"""会话项目绑定：空闲时版本化变更，任务入队与绑定共享SQLite写端。"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.models import BindProjectBody
from krim_agent.security import PrincipalContext
from krim_agent.storage.models import session_controls, sessions, turns
from krim_agent.storage.sandbox_models import sandbox_projects as projects
from krim_agent.storage.sandbox_models import sandbox_session_bindings as bindings

if TYPE_CHECKING:
    from krim_agent.sandbox.service import SandboxService


async def session_row(conn: AsyncConnection, actor: PrincipalContext, session_id: str) -> Any:
    row = (
        (
            await conn.execute(
                select(sessions).where(
                    sessions.c.id == session_id, sessions.c.owner_id == actor.principal_id
                )
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    actor.member(row["workspace_id"])
    return row


async def binding_row(conn: AsyncConnection, session_id: str) -> Any:
    return (
        (await conn.execute(select(bindings).where(bindings.c.session_id == session_id)))
        .mappings()
        .first()
    )


def view(session: Any, binding: Any) -> dict[str, Any]:
    return {
        "session_id": session["id"],
        "project_id": binding["project_id"] if binding else None,
        "revision": binding["revision"] if binding else 0,
        "session_revision": session["revision"],
        "workspace_id": session["workspace_id"],
    }


async def read_binding(
    service: SandboxService, actor: PrincipalContext, session_id: str
) -> dict[str, Any]:
    if service.store.schema_version < 9:
        raise ServiceError("schema_upgrade_required")

    async def read(conn: AsyncConnection) -> dict[str, Any]:
        session = await session_row(conn, actor, session_id)
        binding = await binding_row(conn, session_id)
        if binding is not None and binding["principal_id"] != actor.principal_id:
            raise ServiceError("facts_corrupt")
        return view(session, binding)

    return await service._read(actor, read)


async def bind_project(
    service: SandboxService,
    actor: PrincipalContext,
    session_id: str,
    body: object,
    *,
    request_key: str,
) -> dict[str, Any]:
    if service.store.schema_version < 9:
        raise ServiceError("schema_upgrade_required")
    values = service._parse(BindProjectBody, body)

    async def perform(conn: AsyncConnection) -> dict[str, Any]:
        session = await session_row(conn, actor, session_id)
        actor.member(session["workspace_id"], write=True)
        if session["status"] != "open":
            raise ServiceError("session_closed")
        if session["target_key"] == "target-workspace":
            raise ServiceError("workspace_contract_required")
        binding = await binding_row(conn, session_id)
        if binding is not None and binding["principal_id"] != actor.principal_id:
            raise ServiceError("facts_corrupt")
        if (binding["revision"] if binding else 0) != values.expected_revision or session[
            "revision"
        ] != values.expected_session_revision:
            raise ServiceError("revision_conflict")
        busy = await conn.scalar(
            select(turns.c.id)
            .where(turns.c.session_id == session_id, turns.c.status.in_(["queued", "running"]))
            .limit(1)
        )
        control = await conn.scalar(
            select(session_controls.c.id)
            .where(
                session_controls.c.session_id == session_id, session_controls.c.status == "pending"
            )
            .limit(1)
        )
        if busy is not None or control is not None:
            raise ServiceError("workspace_busy")
        for selected in {values.project_id, binding["project_id"] if binding else None} - {None}:
            project = (
                (
                    await conn.execute(
                        select(projects).where(
                            projects.c.project_id == selected,
                            projects.c.principal_id == actor.principal_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if project is None or project["workspace_id"] != session["workspace_id"]:
                raise ServiceError("not_found", 404)
            if project["lease_id"] is not None or project["status"] not in {
                "ready",
                "unprovisioned",
            }:
                raise ServiceError("workspace_busy")
        if binding is not None and values.project_id == binding["project_id"]:
            return view(session, binding)
        revision = (binding["revision"] if binding else 0) + 1
        now = time.time()
        saved = {
            "session_id": session_id,
            "principal_id": actor.principal_id,
            "project_id": values.project_id,
            "revision": revision,
            "updated_at": now,
        }
        if binding is None:
            await conn.execute(insert(bindings).values(**saved, created_at=now))
        else:
            await conn.execute(
                update(bindings).where(bindings.c.session_id == session_id).values(**saved)
            )
        # sql.submit核对同一session revision，不能用改绑前的页面状态入队。
        await conn.execute(
            update(sessions)
            .where(sessions.c.id == session_id)
            .values(revision=session["revision"] + 1)
        )
        return view({**session, "revision": session["revision"] + 1}, saved)

    return await service._mutation(
        actor,
        request_key,
        {
            "action": "bind_project",
            "session_id": session_id,
            "body": values.model_dump(mode="json"),
        },
        perform,
    )
