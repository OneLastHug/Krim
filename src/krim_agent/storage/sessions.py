"""Session SQL接缝；借用唯一Store/连接，不创建Engine或资源owner。"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

from pydantic import JsonValue
from sqlalchemy import and_, func, insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.config import Settings
from krim_agent.context import builder
from krim_agent.context.types import (
    ContextPolicy,
    ContextSnapshot,
    HistorySource,
    SessionContext,
    WorkspaceSessionContext,
)
from krim_agent.contracts import Message, RunRequest, RunView, ServiceError, encoded, fingerprint
from krim_agent.account_mcp.service import AccountMCPService
from krim_agent.extensions import ExtensionService, extension_messages, load_turn_extensions
from krim_agent.storage.mcp_models import turn_mcp
from krim_agent.runtime.contracts import RunSnapshot, settled
from krim_agent.security import PrincipalContext
from krim_agent.sessions.types import ControlView, SessionCreate, SessionView, TurnRequest, TurnView
from krim_agent.workspace.types import ExecutionAccess, WorkspaceTurnRequest

from . import work as work_sql
from .models import runs, session_controls, sessions, turns, work_turns

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
    if store.schema_version not in {3, 4, 5, 6, 7, 8, 9, 10, 11, 12}:
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
        if body.target_key == "target-workspace":
            work_sql.require(store)
            await work_sql.initialize(conn, row, target)
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


async def list_trace_index(
    store: SQLiteStore,
    workspace: str,
    owner: str,
    admin: bool,
    limit: int,
    *,
    status: str | None = None,
    session_id: str | None = None,
    turn_id: str | None = None,
    before: tuple[float, str] | None = None,
) -> list[dict[str, Any]]:
    """单条受限查询；不装配Turn或Run快照，也不读取正文。"""
    require(store)
    if type(limit) is not int or not 1 <= limit <= 50:
        raise ServiceError("invalid_input", 422)

    async def operation(conn: AsyncConnection) -> list[dict[str, Any]]:
        query = (
            select(
                runs.c.id.label("run_id"),
                runs.c.trace_id,
                sessions.c.id.label("session_id"),
                turns.c.id.label("turn_id"),
                sessions.c.title.label("session_title"),
                sessions.c.target_key,
                runs.c.status,
                runs.c.created_at,
                runs.c.finished_at,
            )
            .select_from(
                turns.join(sessions, turns.c.session_id == sessions.c.id).join(
                    runs, turns.c.run_id == runs.c.id
                )
            )
            .where(
                sessions.c.workspace_id == workspace,
                turns.c.workspace_id == workspace,
                turns.c.owner_id == sessions.c.owner_id,
            )
        )
        if not admin:
            query = query.where(
                sessions.c.owner_id == owner,
                turns.c.owner_id == owner,
            )
        if status is not None:
            query = query.where(runs.c.status == status)
        if session_id is not None:
            query = query.where(sessions.c.id == session_id)
        if turn_id is not None:
            query = query.where(turns.c.id == turn_id)
        if before is not None:
            created_at, run_id = before
            query = query.where(
                or_(
                    runs.c.created_at < created_at,
                    and_(runs.c.created_at == created_at, runs.c.id < run_id),
                )
            )
        rows = (
            (
                await conn.execute(
                    query.order_by(runs.c.created_at.desc(), runs.c.id.desc()).limit(limit + 1)
                )
            )
            .mappings()
            .all()
        )
        return [dict(row) for row in rows]

    return await read(store, operation)


async def closing(conn: AsyncConnection, session_id: str) -> bool:
    # 同一写事务读取持久控制事实；resume不能解除这个关闭栅栏。
    pending = await conn.scalar(
        select(session_controls.c.id)
        .where(
            session_controls.c.session_id == session_id,
            session_controls.c.kind == "close",
            session_controls.c.status == "pending",
        )
        .limit(1)
    )
    return pending is not None


async def submit(
    store: SQLiteStore,
    session_id: str,
    body: TurnRequest | WorkspaceTurnRequest,
    settings: Settings,
    *,
    access: ExecutionAccess | None = None,
    actor: PrincipalContext | None = None,
    extension_service: ExtensionService | None = None,
    account_mcp_service: AccountMCPService | None = None,
) -> TurnView:
    require(store)
    if isinstance(body, WorkspaceTurnRequest):
        # frozen字段仍可包含可变JSON；首个await前验证并脱离调用者的完整嵌套对象。
        body = WorkspaceTurnRequest.model_validate_json(body.model_dump_json())
    digest = fingerprint(body.model_dump(mode="json"))

    async def operation(conn: AsyncConnection) -> TurnView:
        session = await session_row(conn, session_id)
        if actor is not None and extension_service is not None:
            extension_service.actor(actor)
        if session["target_key"] == "target-workspace":
            if access is None or not isinstance(body, WorkspaceTurnRequest):
                raise ServiceError("workspace_contract_required")
            await work_sql.row(conn, session_id, access, write=True)
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
            if actor is not None and extension_service is not None:
                extension_service.actor(actor)
            return turn_view(old)
        if session["target_key"] == "target-workspace":
            work_sql.require(store)
            if not isinstance(body, WorkspaceTurnRequest) or access is None:
                raise ServiceError("workspace_contract_required")
            await work_sql.row(conn, session_id, access, write=True)
        elif isinstance(body, WorkspaceTurnRequest):
            raise ServiceError("workspace_contract_required")
        if session["status"] != "open" or await closing(conn, session_id):
            raise ServiceError("session_closed")
        if actor is not None and account_mcp_service is not None:
            account_mcp_service.actor(actor)
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
        if actor is not None and extension_service is not None:
            await extension_service.freeze(
                conn, session, row["id"], row["text"], actor, turn_admitted=True
            )
        if actor is not None and account_mcp_service is not None:
            await account_mcp_service.freeze(conn, session, row["id"], actor)
        if session["target_key"] == "target-workspace":
            assert isinstance(body, WorkspaceTurnRequest) and access is not None
            await work_sql.enqueue(conn, row, body, access)
        await conn.execute(
            update(sessions)
            .where(sessions.c.id == session_id)
            .values(revision=session["revision"] + 1)
        )
        return turn_view(row)

    async def confirm() -> TurnView | None:
        found = await turn_by_key(store, session_id, body.request_key)
        if isinstance(body, WorkspaceTurnRequest):

            async def full_match(conn: AsyncConnection) -> bool:
                assert access is not None
                await work_sql.row(conn, session_id, access, write=True)
                stored = (
                    await conn.execute(
                        select(work_turns.c.request_hash).where(work_turns.c.turn_id == found.id)
                    )
                ).scalar_one_or_none()
                return stored == digest

            return found if await read(store, full_match) else None
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
        if (
            turn["status"] != "queued"
            or session["status"] != "open"
            or await closing(conn, session["id"])
        ):
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
        if session["target_key"] == "target-workspace":
            work_sql.require(store)
            from .models import run_events

            frozen = []
            for row in previous:
                last = (
                    await conn.scalar(
                        select(func.max(run_events.c.seq)).where(
                            run_events.c.run_id == row["run_id"]
                        )
                    )
                    if row["run_id"]
                    else None
                )
                frozen.append(dict(row) | {"terminal_seq": last if last is not None else -1})
            frame = await work_sql.bind(
                conn,
                turn,
                run_id,
                target,
                [dict(row) | {"terminal_seq": row["terminal_seq"]} for row in frozen],
            )
            effective = target | {"context": frame}
            digest = fingerprint({"request": request.model_dump(mode="json"), "target": effective})
            run = await store._insert_run(conn, request, effective, run_id, digest)
            await conn.execute(
                update(work_turns).where(work_turns.c.turn_id == turn_id).values(run_id=run.id)
            )
            await conn.execute(
                update(turns)
                .where(turns.c.id == turn_id)
                .values(run_id=run.id, status="running", started_at=time.time())
            )
            return run, True
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
        ext = (
            await load_turn_extensions(conn, turn["id"])
            if store.schema_version >= 11
            else {"version": "extensions/2", "revision": 0, "items": []}
        )
        extension_snapshot = cast(dict[str, JsonValue], ext)
        account_mcp = await load_turn_mcp(conn, turn["id"]) if store.schema_version >= 12 else None
        context = builder.prepare(
            settings.system_prompt,
            history,
            sources,
            turn["text"],
            cast(list[dict[str, JsonValue]], target["tools"]),
            policy,
            settings.max_request_bytes,
            extension_messages(ext),
            extension_snapshot,
        )
        context_descriptor: dict[str, JsonValue] = {
            "schema_version": 1,
            "policy": policy.model_dump(mode="json"),
            "builder": context.builder_fingerprint,
        }
        context_descriptor["extensions"] = fingerprint(extension_snapshot)
        if account_mcp is not None and account_mcp["snapshot"]["connections"]:
            context_descriptor["account_mcp"] = account_mcp["fingerprint"]
        effective = target | {"context": context_descriptor}
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


async def load_turn_mcp(conn: AsyncConnection, turn_id: str) -> dict[str, Any] | None:
    row = (await conn.execute(select(turn_mcp).where(turn_mcp.c.turn_id == turn_id)))
    found = row.mappings().first()
    if found is None:
        return None
    snapshot = cast(dict[str, Any], found["snapshot"])
    if fingerprint(snapshot) != found["fingerprint"]:
        raise ServiceError("facts_corrupt")
    return {"snapshot": snapshot, "fingerprint": found["fingerprint"]}


async def source(
    conn: AsyncConnection,
    run_id: str,
    request_key: str,
    version: int,
    target: dict[str, JsonValue],
) -> SessionContext | WorkspaceSessionContext | None:
    if version not in {3, 4, 5, 6, 7, 8, 9, 10, 11, 12}:
        return None
    row = (await conn.execute(select(turns).where(turns.c.run_id == run_id))).mappings().first()
    if row is None:
        # context只由内部Session准入事务写入；旧合法请求前缀不能声明来源。
        if "context" in target:
            raise ServiceError("facts_corrupt")
        return None
    try:
        session = await session_row(conn, row["session_id"])
        if session["target_key"] == "target-workspace":
            frame = await work_sql.load(conn, run_id)
            if (
                frame is None
                or request_key != "ac-turn." + row["id"]
                or target.get("context")
                != {
                    "schema_version": 2,
                    "effective_fingerprint": frame.effective_fingerprint,
                    "source_fingerprint": fingerprint(list(frame.source_refs)),
                }
            ):
                raise ServiceError("facts_corrupt")
            prepared = await conn.scalar(
                select(work_turns.c.prepared_context).where(work_turns.c.run_id == run_id)
            )
            if prepared is not None:
                from krim_agent.context.types import PreparedContext

                context2 = PreparedContext.model_validate_json(encoded(prepared))
                if context2.source_fingerprint != fingerprint(list(frame.source_refs)):
                    raise ServiceError("facts_corrupt")
            return WorkspaceSessionContext(
                workspace_id=row["workspace_id"],
                session_id=row["session_id"],
                turn_id=row["id"],
                effective_fingerprint=frame.effective_fingerprint,
                source_fingerprint=fingerprint(list(frame.source_refs)),
            )
        if (
            row["context_snapshot"] is None
            or fingerprint(row["context_snapshot"]) != row["context_fingerprint"]
            or row["workspace_id"] != session["workspace_id"]
            or row["owner_id"] != session["owner_id"]
            or request_key != "ac-turn." + row["id"]
        ):
            raise ValueError("关联损坏")
        context = ContextSnapshot.model_validate_json(encoded(row["context_snapshot"]))
        expected = {
            "schema_version": 1,
            "policy": context.policy.model_dump(mode="json"),
            "builder": context.builder_fingerprint,
        }
        target_context = target.get("context")
        if context.extensions:
            if not isinstance(target_context, dict) or "extensions" not in target_context:
                raise ValueError("扩展上下文身份缺失")
            expected["extensions"] = fingerprint(context.extensions)
        elif isinstance(target_context, dict) and "extensions" in target_context:
            expected["extensions"] = fingerprint(context.extensions)
        mcp = await load_turn_mcp(conn, row["id"]) if version >= 12 else None
        if (
            mcp is not None
            and mcp["snapshot"]["connections"]
            and isinstance(target_context, dict)
        ):
            expected["account_mcp"] = mcp["fingerprint"]
        elif isinstance(target_context, dict) and "account_mcp" in target_context:
            raise ValueError("账号MCP快照缺失")
        elif mcp is not None and mcp["snapshot"]["connections"]:
            raise ValueError("账号MCP上下文标记缺失")
        if fingerprint(target.get("context")) != fingerprint(expected):
            raise ValueError("内部上下文身份损坏")
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
    if isinstance(snapshot.session_context, WorkspaceSessionContext):
        await work_sql.finish(
            conn, snapshot.run.id, interrupted=snapshot.run.status == "interrupted"
        )

        def task_transcript() -> list[Message]:
            from dataclasses import replace

            summary = {
                e.data.get("step_index")
                for e in snapshot.events
                if e.kind == "model_start" and e.data.get("purpose") == "summary"
            }
            events = tuple(
                e
                for e in snapshot.events
                if e.kind != "summary_text"
                and not (
                    e.kind in {"model_start", "model_end", "usage"}
                    and e.data.get("step_index") in summary
                )
            )
            return builder.transcript(replace(snapshot, events=events))

        from .models import compactions

        await conn.execute(
            update(compactions)
            .where(compactions.c.run_id == snapshot.run.id, compactions.c.status == "reserved")
            .values(
                status="interrupted"
                if snapshot.run.status in {"aborted", "interrupted"}
                else "failed",
                finished_at=time.time(),
            )
        )
        await conn.execute(
            update(turns)
            .where(turns.c.id == snapshot.session_context.turn_id)
            .values(
                status="cancelled" if snapshot.run.status == "aborted" else snapshot.run.status,
                code=snapshot.run.code,
                finished_at=snapshot.run.finished_at,
                transcript=[m.model_dump(mode="json") for m in task_transcript()],
            )
        )
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
        if kind == "resume" and await closing(conn, session_id):
            raise ServiceError("session_closed")
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
            outstanding = await conn.scalar(
                select(func.count())
                .select_from(turns)
                .where(
                    turns.c.session_id == control.session_id,
                    turns.c.status.in_(["queued", "running"]),
                )
            )
            if outstanding:
                raise ServiceError("not_terminal")
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


async def workspace_history(
    conn: AsyncConnection,
    run_id: str,
    offset: int,
    limit: int,
    access: ExecutionAccess,
    *,
    source_anchor: str,
) -> list[dict[str, Any]]:
    """同连接分页，核实完整冻结引用；不持锁请求模型。"""
    frame = await work_sql.load(conn, run_id)
    if frame is None or access.session_id != frame.session_id or not 1 <= limit <= 16:
        raise ServiceError("facts_corrupt")
    if fingerprint(list(frame.source_refs)) != source_anchor:
        raise ServiceError("facts_corrupt")
    from .models import runs

    target = await conn.scalar(select(runs.c.target).where(runs.c.id == run_id))
    if (
        not isinstance(target, dict)
        or not isinstance(target.get("context"), dict)
        or target["context"].get("source_fingerprint") != source_anchor
    ):
        raise ServiceError("facts_corrupt")
    await work_sql.row(conn, frame.session_id, access)
    selected = frame.source_refs[offset : offset + limit]
    result = []
    for ref in selected:
        row = await turn_row(conn, cast(str, ref["turn_id"]))
        if row["status"] in {"queued", "running"}:
            raise ServiceError("facts_corrupt")
        messages = (
            [Message.model_validate_json(encoded(m)) for m in row["transcript"]]
            if row["transcript"] is not None
            else [
                Message(role="user", content=row["text"]),
                Message(
                    role="user",
                    content="[不可信历史状态数据] "
                    + row["status"]
                    + ": "
                    + (row["code"] or "unknown"),
                ),
            ]
        )
        builder.validate_groups(messages)
        result.append({"ref": dict(ref), "messages": messages})
    return result
