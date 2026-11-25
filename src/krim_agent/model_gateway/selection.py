"""Explicit, idle-session model changes with the existing control receipt ledger."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from pydantic import Field, JsonValue
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, StrictModel, fingerprint, parse_json
from krim_agent.security import PrincipalContext
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.models import session_controls, sessions, turns

from .contracts import GatewaySelection
from .runtime import frozen_gateway_marker

if TYPE_CHECKING:
    from krim_agent.composition import Application


class SessionModelUpdate(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    expected_revision: int = Field(ge=0)
    model_selection: GatewaySelection | None = None


def _view(row: Any) -> dict[str, JsonValue]:
    target = row["target"]
    marker = frozen_gateway_marker(target)
    provider = target.get("provider", {})
    config = provider.get("config", {}) if isinstance(provider, dict) else {}
    return {
        "session_id": row["id"],
        "revision": row["revision"],
        "model_selection": marker.get("selection") if marker is not None else None,
        "configuration_revision": marker.get("revision") if marker is not None else None,
        "model": config.get("model"),
        "reasoning_effort": config.get("reasoning_effort"),
        "thinking": config.get("thinking"),
    }


async def get_selection(
    app: Application,
    actor: PrincipalContext,
    workspace: str,
    session_id: str,
) -> dict[str, JsonValue]:
    if app.sessions is None:
        raise ServiceError("provider_not_configured", 503)
    await app.sessions.get(actor, workspace, session_id)

    async def read(conn: AsyncConnection) -> dict[str, JsonValue]:
        return _view(await session_sql.session_row(conn, session_id))

    return await app.store.work_read(read)


async def set_selection(
    app: Application,
    actor: PrincipalContext,
    workspace: str,
    session_id: str,
    body: SessionModelUpdate,
) -> dict[str, JsonValue]:
    if app.sessions is None or app.service.model_gateway is None:
        raise ServiceError("provider_not_configured", 503)
    accepted = await app.sessions.get(actor, workspace, session_id, write=True)
    if accepted.owner_id != actor.principal_id:
        raise ServiceError("permission_denied", 403)
    digest = fingerprint({"actor": actor.principal_id, **body.model_dump(mode="json")})

    async def receipt(conn: AsyncConnection) -> dict[str, JsonValue] | None:
        old = (
            (
                await conn.execute(
                    select(session_controls).where(
                        session_controls.c.session_id == session_id,
                        session_controls.c.request_key == body.request_key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if old is None:
            return None
        if old["request_hash"] != digest or old["kind"] != "model_selection":
            raise ServiceError("key_conflict")
        return cast(dict[str, JsonValue], old["result"])

    prior = await app.store.work_read(receipt)
    if prior is not None:
        return prior
    if body.model_selection is None:
        binding = app.service.bindings.get(accepted.target_key)
        if binding is None:
            raise ServiceError("provider_not_configured", 503)
    else:
        binding = await app.service.model_gateway.for_create(
            actor.principal_id,
            accepted.target_key,
            body.model_selection,
        )
    binding.assert_current()
    target = cast(dict[str, JsonValue], parse_json(binding.descriptor_json))

    async def operation(conn: AsyncConnection) -> dict[str, JsonValue]:
        if app.identities is not None and app.identities.managed:
            app.identities.validate(actor)
        row = await session_sql.session_row(conn, session_id)
        if row["owner_id"] != actor.principal_id or row["workspace_id"] != workspace:
            raise ServiceError("permission_denied", 403)
        old = await receipt(conn)
        if old is not None:
            return old
        if row["revision"] != body.expected_revision:
            raise ServiceError("revision_conflict")
        if row["status"] != "open" or await session_sql.closing(conn, session_id):
            raise ServiceError("session_closed")
        busy = await conn.scalar(
            select(turns.c.id)
            .where(
                turns.c.session_id == session_id,
                turns.c.status.in_(("queued", "running")),
            )
            .limit(1)
        )
        if busy is not None:
            raise ServiceError("session_busy", 409)
        binding.assert_current()
        revision = row["revision"] + 1
        await conn.execute(
            update(sessions)
            .where(sessions.c.id == session_id)
            .values(
                target=target,
                revision=revision,
            )
        )
        result = _view({**dict(row), "target": target, "revision": revision})
        now = time.time()
        await conn.execute(
            insert(session_controls).values(
                id=str(uuid.uuid4()),
                session_id=session_id,
                request_key=body.request_key,
                request_hash=digest,
                kind="model_selection",
                actor_id=actor.principal_id,
                status="completed",
                target_turn_id=None,
                expected_revision=body.expected_revision,
                result=result,
                created_at=now,
                finished_at=now,
            )
        )
        return result

    return await app.store._write(operation)
