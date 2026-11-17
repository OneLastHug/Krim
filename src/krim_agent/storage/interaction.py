"""Small SQL operations; callers own transactions and authorization."""

from __future__ import annotations

import time
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError

from .interaction_models import interaction_modes, interaction_requests


async def mode(conn: AsyncConnection, session_id: str) -> dict[str, Any]:
    row = (
        await conn.execute(
            select(interaction_modes).where(interaction_modes.c.session_id == session_id)
        )
    ).mappings().first()
    return (
        {str(key): value for key, value in row.items()} if row is not None
        else {"session_id": session_id, "mode": "execute", "revision": 0, "updated_at": None}
    )


async def set_mode(conn: AsyncConnection, session_id: str, value: str) -> dict[str, Any]:
    previous = await mode(conn, session_id)
    values = {"mode": value, "revision": previous["revision"] + 1, "updated_at": time.time()}
    if previous["revision"] == 0:
        await conn.execute(insert(interaction_modes).values(session_id=session_id, **values))
    else:
        await conn.execute(
            update(interaction_modes)
            .where(interaction_modes.c.session_id == session_id)
            .values(**values)
        )
    return {"session_id": session_id, **values}


async def request(conn: AsyncConnection, request_id: str, session_id: str) -> dict[str, Any] | None:
    row = (
        await conn.execute(
            select(interaction_requests).where(
                interaction_requests.c.id == request_id,
                interaction_requests.c.session_id == session_id,
            )
        )
    ).mappings().first()
    return None if row is None else {str(key): value for key, value in row.items()}


async def pending(conn: AsyncConnection, session_id: str) -> list[dict[str, Any]]:
    rows = (
        await conn.execute(
            select(interaction_requests).where(
                interaction_requests.c.session_id == session_id,
                interaction_requests.c.status == "pending",
            ).order_by(interaction_requests.c.created_at, interaction_requests.c.id)
        )
    ).mappings().all()
    return [{str(key): value for key, value in row.items()} for row in rows]


async def settle(
    conn: AsyncConnection,
    item: dict[str, Any],
    status: str,
    response: dict[str, Any],
) -> dict[str, Any]:
    values = {
        "status": status,
        "response": response,
        "revision": item["revision"] + 1,
        "updated_at": time.time(),
    }
    changed = await conn.execute(
        update(interaction_requests).where(
            interaction_requests.c.id == item["id"],
            interaction_requests.c.revision == item["revision"],
            interaction_requests.c.status == "pending",
        ).values(**values)
    )
    if changed.rowcount != 1:
        raise ServiceError("input_revision_conflict", 409)
    return {**item, **values}
