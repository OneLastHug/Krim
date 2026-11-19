"""Transaction-local plan storage. Authorization is provided by PlanningService."""

from __future__ import annotations

import time
from collections import Counter
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.planning.contracts import (
    PlanCounts,
    PlanRead,
    PlanSnapshot,
    PlanStep,
    PlanUpdate,
    plan_digest,
)

from .planning_models import (
    session_plan_requests,
    session_plan_steps,
    session_plan_versions,
    session_plans,
)


async def current_revision(
    conn: AsyncConnection, workspace_id: str, session_id: str, owner_id: str
) -> int:
    row = (
        (await conn.execute(select(session_plans).where(session_plans.c.session_id == session_id)))
        .mappings()
        .first()
    )
    if row is None:
        return 0
    if row["workspace_id"] != workspace_id or row["owner_id"] != owner_id:
        raise ServiceError("facts_corrupt")
    return int(row["revision"])


async def snapshot(
    conn: AsyncConnection, session_id: str, revision: int, query: PlanRead
) -> PlanSnapshot:
    if revision == 0:
        return PlanSnapshot(
            session_id=session_id,
            revision=0,
            content_hash=plan_digest([]),
            updated_at=None,
            counts=PlanCounts(),
            total=0,
            matched_total=0,
            steps=(),
            offset=query.offset,
            limit=query.limit,
            next_offset=None,
        )
    row = (
        (
            await conn.execute(
                select(session_plan_versions).where(
                    session_plan_versions.c.session_id == session_id,
                    session_plan_versions.c.revision == revision,
                )
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    try:
        counts = PlanCounts.model_validate(row["counts"])
        if sum(counts.model_dump().values()) != row["total"]:
            raise ValueError("Invalid count")
    except ValueError:
        raise ServiceError("facts_corrupt") from None
    statement = select(session_plan_steps).where(
        session_plan_steps.c.session_id == session_id,
        session_plan_steps.c.revision == revision,
    )
    matched_total = row["total"]
    if query.statuses:
        statement = statement.where(session_plan_steps.c.status.in_(query.statuses))
        matched_total = sum(getattr(counts, status) for status in query.statuses)
    rows = (
        (
            await conn.execute(
                statement.order_by(session_plan_steps.c.position)
                .offset(query.offset)
                .limit(query.limit)
            )
        )
        .mappings()
        .all()
    )
    steps: list[PlanStep] = []
    for item in rows:
        try:
            step = PlanStep(id=item["step_id"], text=item["text"], status=item["status"])
            if item["content_hash"] != plan_digest(
                {"position": item["position"], "step": step.model_dump(mode="json")}
            ):
                raise ValueError("Invalid step hash")
        except ValueError:
            raise ServiceError("facts_corrupt") from None
        steps.append(step)
    if len(steps) != min(query.limit, max(0, matched_total - query.offset)):
        raise ServiceError("facts_corrupt")
    end = query.offset + len(steps)
    return PlanSnapshot(
        session_id=session_id,
        revision=revision,
        content_hash=row["content_hash"],
        updated_at=row["updated_at"],
        counts=counts,
        total=row["total"],
        matched_total=matched_total,
        steps=tuple(steps),
        offset=query.offset,
        limit=query.limit,
        next_offset=end if end < matched_total else None,
    )


async def known_request(
    conn: AsyncConnection, session_id: str, actor_id: str, request_key: str, digest: str
) -> PlanSnapshot | None:
    row = (
        (
            await conn.execute(
                select(session_plan_requests).where(
                    session_plan_requests.c.session_id == session_id,
                    session_plan_requests.c.actor_id == actor_id,
                    session_plan_requests.c.request_key == request_key,
                )
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    if row["request_hash"] != digest:
        raise ServiceError("key_conflict", 409)
    return await snapshot(conn, session_id, row["revision"], PlanRead())


async def replace(
    conn: AsyncConnection,
    workspace_id: str,
    session_id: str,
    owner_id: str,
    actor_id: str,
    body: PlanUpdate,
    digest: str,
) -> PlanSnapshot:
    revision = await current_revision(conn, workspace_id, session_id, owner_id)
    if revision != body.expected_revision:
        raise ServiceError("revision_conflict", 409)
    next_revision = revision + 1
    now = time.time()
    if revision == 0:
        result = await conn.execute(
            sqlite_insert(session_plans)
            .values(
                session_id=session_id,
                workspace_id=workspace_id,
                owner_id=owner_id,
                revision=next_revision,
            )
            .on_conflict_do_nothing(index_elements=["session_id"])
        )
    else:
        result = await conn.execute(
            update(session_plans)
            .where(
                session_plans.c.session_id == session_id,
                session_plans.c.revision == body.expected_revision,
            )
            .values(revision=next_revision)
        )
    if result.rowcount != 1:
        raise ServiceError("revision_conflict", 409)
    steps = [step.model_dump(mode="json") for step in body.steps]
    counts = PlanCounts.model_validate(dict(Counter(step.status for step in body.steps)))
    await conn.execute(
        insert(session_plan_versions).values(
            session_id=session_id,
            revision=next_revision,
            content_hash=plan_digest(steps),
            counts=counts.model_dump(mode="json"),
            total=len(steps),
            updated_at=now,
        )
    )
    # Bounded SQL batches are transport pagination, not a plan-size quota.
    batch: list[dict[str, Any]] = []
    for position, step in enumerate(steps):
        batch.append(
            {
                "session_id": session_id,
                "revision": next_revision,
                "position": position,
                "step_id": step["id"],
                "text": step["text"],
                "status": step["status"],
                "content_hash": plan_digest({"position": position, "step": step}),
            }
        )
        if len(batch) == 200:
            await conn.execute(insert(session_plan_steps), batch)
            batch = []
    if batch:
        await conn.execute(insert(session_plan_steps), batch)
    await conn.execute(
        insert(session_plan_requests).values(
            session_id=session_id,
            actor_id=actor_id,
            request_key=body.request_key,
            request_hash=digest,
            revision=next_revision,
            created_at=now,
        )
    )
    return await snapshot(conn, session_id, next_revision, PlanRead())
