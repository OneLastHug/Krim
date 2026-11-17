"""Transaction-local goal storage; authorization is provided by GoalService."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError

from .goal_models import goal_continuations, goal_requests, goal_versions, session_goals


async def current(conn: AsyncConnection, session_id: str) -> dict[str, Any] | None:
    row = (
        (await conn.execute(select(session_goals).where(session_goals.c.session_id == session_id)))
        .mappings()
        .first()
    )
    return None if row is None else dict(row)


async def version(conn: AsyncConnection, goal_id: str, revision: int) -> dict[str, Any] | None:
    row = (
        (
            await conn.execute(
                select(goal_versions).where(
                    goal_versions.c.goal_id == goal_id,
                    goal_versions.c.revision == revision,
                )
            )
        )
        .mappings()
        .first()
    )
    return None if row is None else dict(row)


async def known_request(
    conn: AsyncConnection, session_id: str, request_key: str
) -> dict[str, Any] | None:
    row = (
        (
            await conn.execute(
                select(goal_requests).where(
                    goal_requests.c.session_id == session_id,
                    goal_requests.c.request_key == request_key,
                )
            )
        )
        .mappings()
        .first()
    )
    return None if row is None else dict(row)


async def record_request(
    conn: AsyncConnection,
    session_id: str,
    request_key: str,
    actor_id: str,
    request_hash: str,
    goal_id: str,
    revision: int,
    created_at: float,
) -> None:
    result = await conn.execute(
        sqlite_insert(goal_requests)
        .values(
            session_id=session_id,
            request_key=request_key,
            actor_id=actor_id,
            request_hash=request_hash,
            goal_id=goal_id,
            revision=revision,
            created_at=created_at,
        )
        .on_conflict_do_nothing(index_elements=["session_id", "request_key"])
    )
    if result.rowcount != 1:
        row = await known_request(conn, session_id, request_key)
        if row is None or row["request_hash"] != request_hash:
            raise ServiceError("key_conflict", 409)


async def insert_head(conn: AsyncConnection, values: dict[str, Any]) -> bool:
    result = await conn.execute(
        sqlite_insert(session_goals)
        .values(**values)
        .on_conflict_do_nothing(index_elements=["session_id"])
    )
    return result.rowcount == 1


async def update_head(
    conn: AsyncConnection, session_id: str, expected_revision: int, values: dict[str, Any]
) -> bool:
    result = await conn.execute(
        update(session_goals)
        .where(
            session_goals.c.session_id == session_id,
            session_goals.c.revision == expected_revision,
        )
        .values(**values)
    )
    return result.rowcount == 1


async def insert_version(conn: AsyncConnection, values: dict[str, Any]) -> None:
    await conn.execute(sqlite_insert(goal_versions).values(**values))


async def latest_continuation(conn: AsyncConnection, session_id: str) -> dict[str, Any] | None:
    row = (
        (
            await conn.execute(
                select(goal_continuations)
                .where(goal_continuations.c.session_id == session_id)
                .order_by(
                    goal_continuations.c.created_at.desc(),
                    goal_continuations.c.request_key.desc(),
                )
                .limit(1)
            )
        )
        .mappings()
        .first()
    )
    return None if row is None else dict(row)


async def continuation(
    conn: AsyncConnection, session_id: str, request_key: str
) -> dict[str, Any] | None:
    row = (
        (
            await conn.execute(
                select(goal_continuations).where(
                    goal_continuations.c.session_id == session_id,
                    goal_continuations.c.request_key == request_key,
                )
            )
        )
        .mappings()
        .first()
    )
    return None if row is None else dict(row)


async def insert_continuation(conn: AsyncConnection, values: dict[str, Any]) -> bool:
    result = await conn.execute(
        sqlite_insert(goal_continuations)
        .values(**values)
        .on_conflict_do_nothing(index_elements=["session_id", "request_key"])
    )
    return result.rowcount == 1


async def update_continuation(
    conn: AsyncConnection, session_id: str, request_key: str, values: dict[str, Any]
) -> None:
    await conn.execute(
        update(goal_continuations)
        .where(
            goal_continuations.c.session_id == session_id,
            goal_continuations.c.request_key == request_key,
        )
        .values(**values)
    )


async def continuations_with_status(
    conn: AsyncConnection, session_id: str, statuses: tuple[str, ...]
) -> list[dict[str, Any]]:
    rows = (
        (
            await conn.execute(
                select(goal_continuations)
                .where(
                    goal_continuations.c.session_id == session_id,
                    goal_continuations.c.status.in_(statuses),
                )
                .order_by(goal_continuations.c.created_at, goal_continuations.c.request_key)
            )
        )
        .mappings()
        .all()
    )
    return [dict(row) for row in rows]


async def set_continuations_status(
    conn: AsyncConnection,
    session_id: str,
    from_statuses: tuple[str, ...],
    status: str,
    updated_at: float,
    *,
    keep_request_key: str | None = None,
) -> list[dict[str, Any]]:
    """Idempotent bulk transition; ``keep_request_key`` spares the in-flight turn's own row."""
    rows = [
        row
        for row in await continuations_with_status(conn, session_id, from_statuses)
        if row["request_key"] != keep_request_key
    ]
    if not rows:
        return []
    statement = update(goal_continuations).where(
        goal_continuations.c.session_id == session_id,
        goal_continuations.c.status.in_(from_statuses),
    )
    if keep_request_key is not None:
        statement = statement.where(goal_continuations.c.request_key != keep_request_key)
    await conn.execute(statement.values(status=status, updated_at=updated_at))
    return rows


async def consumed_failures(conn: AsyncConnection, goal_id: str) -> list[dict[str, Any]]:
    rows = (
        (
            await conn.execute(
                select(goal_continuations)
                .where(
                    goal_continuations.c.goal_id == goal_id,
                    goal_continuations.c.status == "consumed",
                    goal_continuations.c.outcome == "failed",
                )
                .order_by(
                    goal_continuations.c.updated_at.desc(),
                    goal_continuations.c.request_key.desc(),
                )
                .limit(16)
            )
        )
        .mappings()
        .all()
    )
    return [dict(row) for row in rows]


async def turn_row(conn: AsyncConnection, turn_id: str) -> dict[str, Any] | None:
    from .models import turns

    row = (await conn.execute(select(turns).where(turns.c.id == turn_id))).mappings().first()
    return None if row is None else dict(row)


async def latest_user_turn(conn: AsyncConnection, session_id: str) -> dict[str, Any] | None:
    """Latest real user turn: excludes host-internal continuation turns."""
    from .models import turns

    prefixes = ("goal-continue:%", "goal-resume:%")
    row = (
        (
            await conn.execute(
                select(turns)
                .where(
                    turns.c.session_id == session_id,
                    turns.c.request_key.notlike(prefixes[0]),
                    turns.c.request_key.notlike(prefixes[1]),
                )
                .order_by(turns.c.ordinal.desc())
                .limit(1)
            )
        )
        .mappings()
        .first()
    )
    return None if row is None else dict(row)


async def newer_user_turns(
    conn: AsyncConnection, session_id: str, after_ordinal: int
) -> list[dict[str, Any]]:
    """Queued/running real user turns after this one; they always win over continuation."""
    from .models import turns

    rows = (
        (
            await conn.execute(
                select(turns)
                .where(
                    turns.c.session_id == session_id,
                    turns.c.ordinal > after_ordinal,
                    turns.c.status.in_(("queued", "running")),
                    turns.c.request_key.notlike("goal-continue:%"),
                    turns.c.request_key.notlike("goal-resume:%"),
                )
                .order_by(turns.c.ordinal)
            )
        )
        .mappings()
        .all()
    )
    return [dict(row) for row in rows]


async def lifecycle_row(conn: AsyncConnection, session_id: str) -> dict[str, Any] | None:
    from .session_lifecycle_models import session_lifecycle

    row = (
        (
            await conn.execute(
                select(session_lifecycle).where(session_lifecycle.c.session_id == session_id)
            )
        )
        .mappings()
        .first()
    )
    return None if row is None else dict(row)
