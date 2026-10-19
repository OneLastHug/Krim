"""Session SQL接缝；借用唯一Store/连接，不创建Engine或资源owner。"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

from pydantic import JsonValue
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.config import Settings
from krim_agent.context import builder
from krim_agent.context.types import ContextPolicy, ContextSnapshot, HistorySource, SessionContext
from krim_agent.contracts import Message, RunRequest, RunView, ServiceError, encoded, fingerprint
from krim_agent.runtime.contracts import RunSnapshot, settled
from krim_agent.sessions.types import ControlView, SessionCreate, SessionView, TurnRequest, TurnView

from .models import session_controls, sessions, turns

if TYPE_CHECKING:
    from .sqlite import SQLiteStore


async def read[T](store: SQLiteStore, operation: Callable[[AsyncConnection], Awaitable[T]]) -> T:
    if store.closed:
        raise ServiceError("closed")

    async def owned() -> T:
        async with store.lock, store.engine.begin() as conn:
            return await operation(conn)

    try:
        return await settled(owned())
    except ServiceError:
        raise
    except Exception:
        raise ServiceError("storage_unavailable", 503) from None


def require(store: SQLiteStore) -> None:
    if store.schema_version != 3:
        raise ServiceError("schema_upgrade_required")


async def session_row(conn: AsyncConnection, session_id: str) -> Any:
    row = (
        (await conn.execute(select(sessions).where(sessions.c.id == session_id))).mappings().first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    return row


async def turn_row(conn: AsyncConnection, turn_id: str) -> Any:
    row = (await conn.execute(select(turns).where(turns.c.id == turn_id))).mappings().first()
    if row is None:
        raise ServiceError("not_found", 404)
    return row


def session_view(row: Any) -> SessionView:
    values = {k: row[k] for k in SessionView.model_fields if k != "target_fingerprint"}
    values["target_fingerprint"] = fingerprint(row["target"])
    return SessionView.model_validate_json(encoded(values))


def turn_view(row: Any) -> TurnView:
    return TurnView.model_validate_json(encoded({k: row[k] for k in TurnView.model_fields}))


async def get_session(store: SQLiteStore, session_id: str) -> SessionView:
    require(store)

    async def operation(conn: AsyncConnection) -> SessionView:
        return session_view(await session_row(conn, session_id))

    return await read(store, operation)


async def by_key(store: SQLiteStore, workspace: str, owner: str, key: str) -> SessionView:
    require(store)

    async def operation(conn: AsyncConnection) -> SessionView:
        row = (
            (
                await conn.execute(
                    select(sessions).where(
                        sessions.c.workspace_id == workspace,
                        sessions.c.owner_id == owner,
                        sessions.c.create_key == key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        return session_view(row)

    return await read(store, operation)


async def list_sessions(
    store: SQLiteStore, workspace: str, owner: str, admin: bool, offset: int, limit: int
) -> list[SessionView]:
    require(store)

    async def operation(conn: AsyncConnection) -> list[SessionView]:
        query = select(sessions).where(sessions.c.workspace_id == workspace)
        if not admin:
            query = query.where(sessions.c.owner_id == owner)
        rows = (
            (
                await conn.execute(
                    query.order_by(sessions.c.created_at.desc(), sessions.c.id)
                    .offset(offset)
                    .limit(limit)
                )
            )
            .mappings()
            .all()
        )
        return [session_view(row) for row in rows]

    return await read(store, operation)


async def create_session(
    store: SQLiteStore,
    workspace: str,
    owner: str,
    body: SessionCreate,
    target: dict[str, JsonValue],
    policy: ContextPolicy,
    settings: Settings,
) -> SessionView:
    require(store)
    digest = fingerprint(body.model_dump(mode="json"))

    async def operation(conn: AsyncConnection) -> SessionView:
        old = (
            (
                await conn.execute(
                    select(sessions).where(
                        sessions.c.workspace_id == workspace,
                        sessions.c.owner_id == owner,
                        sessions.c.create_key == body.request_key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if old is not None:
            if old["request_hash"] != digest:
                raise ServiceError("key_conflict")
            return session_view(old)
        count = await conn.scalar(
            select(func.count())
            .select_from(sessions)
            .where(sessions.c.owner_id == owner, sessions.c.status == "open")
        )
        if (count or 0) >= settings.session_max_open:
            raise ServiceError("capacity", 429)
        row = dict(
            id=str(uuid.uuid4()),
            workspace_id=workspace,
            owner_id=owner,
            create_key=body.request_key,
            request_hash=digest,
            title=body.title,
            target_key=body.target_key,
            target=target,
            context_policy=policy.model_dump(mode="json"),
            revision=0,
            status="open",
            dispatch_paused=False,
            created_at=time.time(),
            closed_at=None,
        )
        await conn.execute(insert(sessions).values(**row))
        return session_view(row)

    async def confirm() -> SessionView | None:
        found = await by_key(store, workspace, owner, body.request_key)

        async def check(conn: AsyncConnection) -> bool:
            return bool((await session_row(conn, found.id))["request_hash"] == digest)

        return found if await read(store, check) else None

    return await store._eval_write(operation, confirm)


async def get_turn(store: SQLiteStore, turn_id: str) -> TurnView:
    require(store)

    async def operation(conn: AsyncConnection) -> TurnView:
        return turn_view(await turn_row(conn, turn_id))

    return await read(store, operation)


async def turn_by_key(store: SQLiteStore, session_id: str, key: str) -> TurnView:
    require(store)

    async def operation(conn: AsyncConnection) -> TurnView:
        row = (
            (
                await conn.execute(
                    select(turns).where(
                        turns.c.session_id == session_id, turns.c.request_key == key
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        return turn_view(row)

    return await read(store, operation)


async def list_turns(
    store: SQLiteStore, session_id: str, offset: int, limit: int
) -> list[TurnView]:
    require(store)

    async def operation(conn: AsyncConnection) -> list[TurnView]:
        rows = (
            (
                await conn.execute(
                    select(turns)
                    .where(turns.c.session_id == session_id)
                    .order_by(turns.c.ordinal)
                    .offset(offset)
                    .limit(limit)
                )
            )
            .mappings()
            .all()
        )
        return [turn_view(row) for row in rows]

    return await read(store, operation)


async def submit(
    store: SQLiteStore, session_id: str, body: TurnRequest, settings: Settings
) -> TurnView:
    require(store)
    digest = fingerprint(body.model_dump(mode="json"))

    async def operation(conn: AsyncConnection) -> TurnView:
        session = await session_row(conn, session_id)
        rows = (
            (
                await conn.execute(
                    select(turns).where(turns.c.session_id == session_id).order_by(turns.c.ordinal)
                )
            )
            .mappings()
            .all()
        )
        old = next((row for row in rows if row["request_key"] == body.request_key), None)
        if old is not None:
            if old["request_hash"] != digest:
                raise ServiceError("key_conflict")
            return turn_view(old)
        if session["status"] != "open":
            raise ServiceError("session_closed")
        if session["revision"] != body.expected_revision:
            raise ServiceError("revision_conflict")
        if (
            len(rows) >= settings.session_max_turns
            or sum(len(row["text"].encode()) for row in rows) + len(body.text.encode())
            > settings.session_max_history_bytes
        ):
            raise ServiceError("input_limit", 422)
        if sum(row["status"] == "queued" for row in rows) >= settings.session_max_queued:
            raise ServiceError("capacity", 429)
        row = dict(
            id=str(uuid.uuid4()),
            session_id=session_id,
            workspace_id=session["workspace_id"],
            owner_id=session["owner_id"],
            request_key=body.request_key,
            request_hash=digest,
            text=body.text,
            status="queued",
            ordinal=len(rows) + 1,
            accepted_revision=body.expected_revision,
            run_id=None,
            context_snapshot=None,
            context_fingerprint=None,
            transcript=None,
            projection_version=builder.VERSION,
            code=None,
            created_at=time.time(),
            started_at=None,
            finished_at=None,
        )
        await conn.execute(insert(turns).values(**row))
        await conn.execute(
            update(sessions)
            .where(sessions.c.id == session_id)
            .values(revision=session["revision"] + 1)
        )
        return turn_view(row)

    async def confirm() -> TurnView | None:
        found = await turn_by_key(store, session_id, body.request_key)
        return (
            found
            if found.text == body.text and found.accepted_revision == body.expected_revision
            else None
        )

    return await store._eval_write(operation, confirm)


async def turn_request(store: SQLiteStore, turn_id: str) -> tuple[RunRequest, str]:
    async def operation(conn: AsyncConnection) -> tuple[RunRequest, str]:
        turn = await turn_row(conn, turn_id)
        session = await session_row(conn, turn["session_id"])
        return RunRequest(
            text=turn["text"], request_key="ac-turn." + turn_id, target=session["target_key"]
        ), fingerprint(session["target"])

    return await read(store, operation)


async def begin_run(
    store: SQLiteStore,
    turn_id: str,
    request: RunRequest,
    target: dict[str, JsonValue],
    settings: Settings,
) -> tuple[RunView, bool]:
    require(store)
    run_id = str(uuid.uuid4())

    async def operation(conn: AsyncConnection) -> tuple[RunView, bool]:
        turn = await turn_row(conn, turn_id)
        session = await session_row(conn, turn["session_id"])
        if turn["run_id"] is not None:
            return store._view(await store._row(conn, turn["run_id"])), False
        if turn["status"] != "queued" or session["status"] != "open":
            raise ServiceError("session_closed")
        if session["dispatch_paused"]:
            raise ServiceError("capacity", 429)
        if fingerprint(session["target"]) != fingerprint(target):
            raise ServiceError("target_binding_changed")
        previous = (
            (
                await conn.execute(
                    select(turns)
                    .where(turns.c.session_id == session["id"], turns.c.ordinal < turn["ordinal"])
                    .order_by(turns.c.ordinal)
                )
            )
            .mappings()
            .all()
        )
        if any(row["status"] in {"queued", "running"} for row in previous):
            raise ServiceError("capacity", 429)
        if time.time() - turn["created_at"] >= settings.session_queue_timeout:
            raise ServiceError("queue_timeout")
        history: list[Message] = []
        sources = []
        for row in previous:
            if row["transcript"] is None:
                messages = [
                    Message(role="user", content=row["text"]),
                    Message(
                        role="user",
                        content="[不可信历史状态数据] "
                        + row["status"]
                        + ": "
                        + (row["code"] or "unknown"),
                    ),
                ]
            else:
                messages = [Message.model_validate_json(encoded(m)) for m in row["transcript"]]
            if any(m.role == "system" for m in messages):
                raise ServiceError("facts_corrupt")
            history.extend(messages)
            seq = -1
            if row["run_id"]:
                seq = len((await store._snapshot(conn, row["run_id"])).events) - 1
            sources.append(
                HistorySource(
                    turn_id=row["id"],
                    run_id=row["run_id"],
                    last_seq=seq,
                    fingerprint=fingerprint([m.model_dump(mode="json") for m in messages]),
                )
            )
        policy = ContextPolicy.model_validate_json(encoded(session["context_policy"]))
        context = builder.prepare(
            settings.system_prompt,
            history,
            sources,
            turn["text"],
            cast(list[dict[str, JsonValue]], target["tools"]),
            policy,
            settings.max_request_bytes,
        )
        effective = target | {
            "context": {
                "schema_version": 1,
                "policy": policy.model_dump(mode="json"),
                "builder": context.builder_fingerprint,
            }
        }
        digest = fingerprint({"request": request.model_dump(mode="json"), "target": effective})
        run = await store._insert_run(conn, request, effective, run_id, digest)
        raw = context.model_dump(mode="json")
        await conn.execute(
            update(turns)
            .where(turns.c.id == turn_id)
            .values(
                run_id=run.id,
                status="running",
                context_snapshot=raw,
                context_fingerprint=fingerprint(raw),
                started_at=time.time(),
            )
        )
        return run, True

    async def confirm() -> tuple[RunView, bool] | None:
        found = await get_turn(store, turn_id)
        if found.run_id is None:
            return None
        snap = await store.snapshot(found.run_id)
        if snap.request != request or snap.session_context is None:
            raise ServiceError("facts_corrupt")
        return snap.run, found.run_id == run_id

    return await store._eval_write(operation, confirm)


async def source(
    conn: AsyncConnection, run_id: str, request_key: str, version: int
) -> SessionContext | None:
    if version != 3:
        return None
    row = (await conn.execute(select(turns).where(turns.c.run_id == run_id))).mappings().first()
    if row is None:
        if request_key.startswith("ac-turn."):
            raise ServiceError("facts_corrupt")
        return None
    try:
        session = await session_row(conn, row["session_id"])
        if (
            row["context_snapshot"] is None
            or fingerprint(row["context_snapshot"]) != row["context_fingerprint"]
            or row["workspace_id"] != session["workspace_id"]
            or row["owner_id"] != session["owner_id"]
            or request_key != "ac-turn." + row["id"]
        ):
            raise ValueError("关联损坏")
        context = ContextSnapshot.model_validate_json(encoded(row["context_snapshot"]))
        return SessionContext(
            workspace_id=row["workspace_id"],
            session_id=row["session_id"],
            turn_id=row["id"],
            context=context,
        )
    except ValueError:
        raise ServiceError("facts_corrupt") from None


async def finish_turn(conn: AsyncConnection, snapshot: RunSnapshot) -> None:
    if snapshot.session_context is None:
        return
    status = "cancelled" if snapshot.run.status == "aborted" else snapshot.run.status
    await conn.execute(
        update(turns)
        .where(turns.c.id == snapshot.session_context.turn_id)
        .values(
            status=status,
            code=snapshot.run.code,
            finished_at=snapshot.run.finished_at,
            transcript=[m.model_dump(mode="json") for m in builder.transcript(snapshot)],
        )
    )


async def queued(store: SQLiteStore) -> list[str]:
    async def operation(conn: AsyncConnection) -> list[str]:
        rows = (
            await conn.execute(
                select(turns.c.id, turns.c.session_id)
                .join(sessions, turns.c.session_id == sessions.c.id)
                .where(
                    turns.c.status == "queued",
                    sessions.c.status == "open",
                    sessions.c.dispatch_paused.is_(False),
                )
                .order_by(turns.c.created_at, turns.c.ordinal)
                .limit(512)
            )
        ).all()
        seen = set()
        result = []
        for row in rows:
            if row.session_id not in seen:
                seen.add(row.session_id)
                result.append(row.id)
        return result

    return await read(store, operation)


async def fail_queued(
    store: SQLiteStore, turn_id: str, code: str, *, status: str = "failed"
) -> None:
    async def operation(conn: AsyncConnection) -> bool:
        await conn.execute(
            update(turns)
            .where(turns.c.id == turn_id, turns.c.status == "queued")
            .values(status=status, code=code, finished_at=time.time())
        )
        return True

    async def confirm() -> bool | None:
        return (
            True if (await get_turn(store, turn_id)).status not in {"queued", "running"} else None
        )

    await store._eval_write(operation, confirm)


def control_view(row: Any) -> ControlView:
    return ControlView.model_validate_json(encoded({k: row[k] for k in ControlView.model_fields}))


async def control_by_key(store: SQLiteStore, session_id: str, key: str) -> ControlView:
    async def operation(conn: AsyncConnection) -> ControlView:
        row = (
            (
                await conn.execute(
                    select(session_controls).where(
                        session_controls.c.session_id == session_id,
                        session_controls.c.request_key == key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        return control_view(row)

    return await read(store, operation)


async def begin_control(
    store: SQLiteStore,
    session_id: str,
    key: str,
    kind: str,
    actor: str,
    revision: int,
    payload: dict[str, JsonValue],
    turn_id: str | None = None,
) -> ControlView:
    digest = fingerprint(
        {
            "kind": kind,
            "actor": actor,
            "revision": 0 if kind == "approval" else revision,
            "payload": payload,
            "turn_id": turn_id,
        }
    )

    async def operation(conn: AsyncConnection) -> ControlView:
        session = await session_row(conn, session_id)
        old = (
            (
                await conn.execute(
                    select(session_controls).where(
                        session_controls.c.session_id == session_id,
                        session_controls.c.request_key == key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if old is not None:
            if old["request_hash"] != digest:
                raise ServiceError("key_conflict")
            return control_view(old)
        if revision != session["revision"]:
            raise ServiceError("revision_conflict")
        if turn_id is not None and (await turn_row(conn, turn_id))["session_id"] != session_id:
            raise ServiceError("not_found", 404)
        if session["status"] == "closed" and kind != "close":
            raise ServiceError("session_closed")
        row = dict(
            id=str(uuid.uuid4()),
            session_id=session_id,
            request_key=key,
            request_hash=digest,
            kind=kind,
            actor_id=actor,
            status="pending",
            target_turn_id=turn_id,
            expected_revision=revision,
            result=None,
            created_at=time.time(),
            finished_at=None,
        )
        await conn.execute(insert(session_controls).values(**row))
        values: dict[str, Any] = {"revision": session["revision"] + 1}
        if kind == "rename":
            values["title"] = payload["title"]
        if kind == "resume":
            values["dispatch_paused"] = False
        if kind == "close":
            values["dispatch_paused"] = True
            await conn.execute(
                update(turns)
                .where(turns.c.session_id == session_id, turns.c.status == "queued")
                .values(status="cancelled", code="cancelled", finished_at=time.time())
            )
        if kind == "cancel_turn":
            await conn.execute(
                update(turns)
                .where(turns.c.id == turn_id, turns.c.status == "queued")
                .values(status="cancelled", code="cancelled", finished_at=time.time())
            )
        await conn.execute(update(sessions).where(sessions.c.id == session_id).values(**values))
        return control_view(row)

    async def confirm() -> ControlView | None:
        found = await control_by_key(store, session_id, key)

        async def check(conn: AsyncConnection) -> bool:
            stored = (
                await conn.execute(
                    select(session_controls.c.request_hash).where(session_controls.c.id == found.id)
                )
            ).scalar_one()
            return bool(stored == digest)

        return found if await read(store, check) else None

    return await store._eval_write(operation, confirm)


async def complete_control(
    store: SQLiteStore, control: ControlView, result: dict[str, JsonValue]
) -> ControlView:
    async def operation(conn: AsyncConnection) -> ControlView:
        committed = result
        if control.kind == "close":
            current = await session_row(conn, control.session_id)
            if current["status"] != "closed":
                await conn.execute(
                    update(sessions)
                    .where(sessions.c.id == control.session_id)
                    .values(status="closed", closed_at=time.time(), dispatch_paused=True)
                )
            committed = cast(
                dict[str, JsonValue],
                session_view(await session_row(conn, control.session_id)).model_dump(mode="json"),
            )
        await conn.execute(
            update(session_controls)
            .where(session_controls.c.id == control.id, session_controls.c.status == "pending")
            .values(status="completed", result=committed, finished_at=time.time())
        )
        row = (
            (
                await conn.execute(
                    select(session_controls).where(session_controls.c.id == control.id)
                )
            )
            .mappings()
            .one()
        )
        return control_view(row)

    async def confirm() -> ControlView | None:
        found = await control_by_key(store, control.session_id, control.request_key)
        return found if found.status == "completed" else None

    return await store._eval_write(operation, confirm)


async def pause_queues(conn: AsyncConnection) -> None:
    ids = select(turns.c.session_id).where(turns.c.status == "queued")
    await conn.execute(update(sessions).where(sessions.c.id.in_(ids)).values(dispatch_paused=True))


async def recover_controls(conn: AsyncConnection) -> None:
    rows = (
        (await conn.execute(select(session_controls).where(session_controls.c.status == "pending")))
        .mappings()
        .all()
    )
    for row in rows:
        if row["kind"] == "close":
            await conn.execute(
                update(sessions)
                .where(sessions.c.id == row["session_id"])
                .values(status="closed", dispatch_paused=True, closed_at=time.time())
            )
        await conn.execute(
            update(session_controls)
            .where(session_controls.c.id == row["id"])
            .values(
                status="completed",
                result={"status": "interrupted", "code": "interrupted"},
                finished_at=time.time(),
            )
        )
