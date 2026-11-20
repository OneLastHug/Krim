"""Session SQL接缝；借用唯一Store/连接，不创建Engine或资源owner。"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import JsonValue
from sqlalchemy import and_, func, insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.account_mcp.service import AccountMCPService
from krim_agent.config import Settings
from krim_agent.context import builder
from krim_agent.context.serialization import fingerprint as unlimited_fingerprint
from krim_agent.context.types import (
    CompactionPolicy,
    ContextPolicy,
    ContextSnapshot,
    DevelopmentContextState,
    DevelopmentSessionContext,
    HistorySource,
    OrdinaryContextState,
    OrdinarySessionContext,
    SessionContext,
    WorkspaceSessionContext,
    upgrade_to_token_window,
)
from krim_agent.contracts import (
    RETIRED_TARGETS,
    TERMINAL,
    Message,
    RunRequest,
    RunView,
    ServiceError,
    encoded,
    fingerprint,
)
from krim_agent.extensions import ExtensionService, extension_messages, load_turn_extensions
from krim_agent.runtime.contracts import RunSnapshot, settled
from krim_agent.security import PrincipalContext
from krim_agent.sessions.types import ControlView, SessionCreate, SessionView, TurnRequest, TurnView
from krim_agent.workspace.types import ExecutionAccess, WorkspaceTurnRequest

from . import work as work_sql
from .account_models import accounts
from .mcp_models import turn_mcp
from .models import runs, session_controls, sessions, turns, work_turns
from .sandbox_models import sandbox_projects, sandbox_session_bindings, sandbox_turn_contracts

if TYPE_CHECKING:
    from .sqlite import SQLiteStore


def request_limit(
    settings: Settings,
    policy: ContextPolicy,
    target: dict[str, JsonValue] | None = None,
) -> int | None:
    """Resolve only explicit transport/policy limits, never a byte fallback quota."""
    capacities: list[int] = []
    if settings.max_request_bytes is not None:
        capacities.append(settings.max_request_bytes)
    limits = target.get("limits") if target is not None else None
    if isinstance(limits, dict):
        declarations = [limits]
        http = limits.get("http")
        if isinstance(http, dict):
            declarations.append(http)
        for declaration in declarations:
            value = declaration.get("max_request_bytes")
            if value is not None:
                if type(value) is not int or value < 256:
                    raise ServiceError("facts_corrupt")
                capacities.append(value)
    if policy.max_bytes is not None:
        capacities.append(policy.max_bytes)
    return min(capacities) if capacities else None


async def current_context_policy(conn: AsyncConnection, session: Any) -> ContextPolicy:
    previous = ContextPolicy.model_validate_json(encoded(session["context_policy"]))
    policy = upgrade_to_token_window(previous)
    if policy != previous:
        await conn.execute(update(sessions).where(sessions.c.id == session["id"]).values(
            context_policy=policy.model_dump(mode="json"),
        ))
    return policy


def frozen_target_key(raw: dict[str, JsonValue]) -> str | None:
    """读取冻结锚点声明的目标键；结构异常时返回None而不是抛出。"""
    frozen = raw.get("frozen")
    value = frozen.get("target_key") if isinstance(frozen, dict) else None
    return value if isinstance(value, str) else None


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
    if store.schema_version not in {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}:
        raise ServiceError("schema_upgrade_required")


async def session_row(conn: AsyncConnection, session_id: str) -> Any:
    row = (
        (await conn.execute(select(sessions).where(sessions.c.id == session_id))).mappings().first()
    )
    if row is None:
        raise ServiceError("not_found", 404)
    return row


async def session_lifecycle_row(conn: AsyncConnection, session_id: str) -> Any | None:
    from .session_lifecycle_models import session_lifecycle

    return (
        (
            await conn.execute(
                select(session_lifecycle).where(session_lifecycle.c.session_id == session_id)
            )
        )
        .mappings()
        .first()
    )


async def turn_row(conn: AsyncConnection, turn_id: str) -> Any:
    row = (await conn.execute(select(turns).where(turns.c.id == turn_id))).mappings().first()
    if row is None:
        raise ServiceError("not_found", 404)
    return row


def session_view(row: Any) -> SessionView:
    lifecycle_fields = {"archived_at", "deleted_at", "lifecycle_revision"}
    values = {
        key: row[key]
        for key in SessionView.model_fields
        if key != "target_fingerprint" and key not in lifecycle_fields
    }
    values["target_fingerprint"] = fingerprint(row["target"])
    values.update(archived_at=None, deleted_at=None, lifecycle_revision=0)
    return SessionView.model_validate_json(encoded(values))


async def lifecycle_view(
    conn: AsyncConnection, view: SessionView, *, enabled: bool = True
) -> SessionView:
    if not enabled:
        return view
    row = await session_lifecycle_row(conn, view.id)
    return view.model_copy(
        update={
            "archived_at": row["archived_at"] if row is not None else None,
            "deleted_at": row["deleted_at"] if row is not None else None,
            "lifecycle_revision": row["revision"] if row is not None else 0,
        }
    )


def turn_view(row: Any) -> TurnView:
    return TurnView.model_validate_json(encoded({k: row[k] for k in TurnView.model_fields}))


async def get_session(store: SQLiteStore, session_id: str) -> SessionView:
    require(store)

    async def operation(conn: AsyncConnection) -> SessionView:
        return await lifecycle_view(
            conn,
            session_view(await session_row(conn, session_id)),
            enabled=store.schema_version >= 14,
        )

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
        return await lifecycle_view(
            conn,
            session_view(row),
            enabled=store.schema_version >= 14,
        )

    return await read(store, operation)


async def list_sessions(
    store: SQLiteStore,
    workspace: str,
    owner: str,
    admin: bool,
    offset: int,
    limit: int,
    *,
    view: str = "active",
) -> list[SessionView]:
    require(store)
    if view not in {"active", "archived", "trash"}:
        raise ServiceError("invalid_input", 422)
    if store.schema_version < 14 and view != "active":
        raise ServiceError("schema_upgrade_required")
    if store.schema_version >= 14:
        from .session_lifecycle_models import session_lifecycle

    async def operation(conn: AsyncConnection) -> list[SessionView]:
        if store.schema_version < 14:
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

        lifecycle_join = sessions.outerjoin(
            session_lifecycle, session_lifecycle.c.session_id == sessions.c.id
        )
        query = (
            select(
                sessions,
                session_lifecycle.c.archived_at.label("lifecycle_archived_at"),
                session_lifecycle.c.deleted_at.label("lifecycle_deleted_at"),
                session_lifecycle.c.revision.label("lifecycle_revision"),
            )
            .select_from(lifecycle_join)
            .where(sessions.c.workspace_id == workspace)
        )
        if not admin or store.schema_version >= 19:
            query = query.where(sessions.c.owner_id == owner)
        if store.schema_version >= 19:
            from .project_sessions import visible_sessions

            query = query.where(visible_sessions(owner))
        if view == "active":
            query = query.where(
                session_lifecycle.c.deleted_at.is_(None),
                session_lifecycle.c.archived_at.is_(None),
            )
        elif view == "archived":
            query = query.where(
                session_lifecycle.c.deleted_at.is_(None),
                session_lifecycle.c.archived_at.is_not(None),
            )
        else:
            query = query.where(session_lifecycle.c.deleted_at.is_not(None))
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
        results = []
        for row in rows:
            value = session_view(row)
            results.append(
                value.model_copy(
                    update={
                        "archived_at": row["lifecycle_archived_at"],
                        "deleted_at": row["lifecycle_deleted_at"],
                        "lifecycle_revision": row["lifecycle_revision"] or 0,
                    }
                )
            )
        return results

    return await read(store, operation)


async def create_session(
    store: SQLiteStore,
    workspace: str,
    owner: str,
    body: SessionCreate,
    target: dict[str, JsonValue],
    policy: ContextPolicy,
    settings: Settings,
    *, principal: PrincipalContext | None = None,
) -> SessionView:
    require(store)
    # Keep old create-key replays valid when no new project field was supplied.
    document = body.model_dump(mode="json")
    if body.project_id is None:
        document.pop("project_id", None)
    digest = fingerprint(document)

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
            if store.schema_version >= 14:
                lifecycle = await session_lifecycle_row(conn, old["id"])
                if lifecycle is not None and lifecycle["deleted_at"] is not None:
                    raise ServiceError("session_in_trash", 409)
            if principal is not None and store.schema_version >= 19:
                from krim_agent.project_access.policy import resolve_project_access

                bound_project = await conn.scalar(
                    select(sandbox_session_bindings.c.project_id).where(
                        sandbox_session_bindings.c.session_id == old["id"],
                    )
                )
                if bound_project is not None:
                    access = await resolve_project_access(conn, principal, bound_project)
                    access.require()
            return await lifecycle_view(
                conn,
                session_view(old),
                enabled=store.schema_version >= 14,
            )
        count = await conn.scalar(
            select(func.count())
            .select_from(sessions)
            .where(sessions.c.owner_id == owner, sessions.c.status == "open")
        )
        if settings.session_max_open is not None and (count or 0) >= settings.session_max_open:
            raise ServiceError("capacity", 429)
        session_id = str(uuid.uuid4())
        row = dict(
            id=session_id,
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
        if store.schema_version >= 19:
            from .execution_policy import initialize
            from .project_sessions import bind_new_session

            mode: Literal["manual", "auto", "bypass"] = "manual"
            if principal is not None:
                if principal.principal_id != owner:
                    raise ServiceError("permission_denied", 403)
                mode = await bind_new_session(
                    conn, principal, workspace, session_id, body.project_id,
                )
            elif body.project_id is not None:
                raise ServiceError("permission_denied", 403)
            await initialize(conn, session_id, mode)
        if body.target_key == "target-workspace":
            work_sql.require(store)
            await work_sql.initialize(conn, row, target)
        return await lifecycle_view(
            conn,
            session_view(row),
            enabled=store.schema_version >= 14,
        )

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
        if not admin or store.schema_version >= 19:
            query = query.where(
                sessions.c.owner_id == owner,
                turns.c.owner_id == owner,
            )
        if store.schema_version >= 19:
            from .project_sessions import visible_sessions

            query = query.where(visible_sessions(owner))
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
    elif isinstance(body, TurnRequest) and body.completion is not None:
        body = TurnRequest.model_validate_json(body.model_dump_json())
    digest = fingerprint(body.model_dump(mode="json"))

    async def operation(conn: AsyncConnection) -> TurnView:
        session = await session_row(conn, session_id)
        if actor is not None and extension_service is not None:
            extension_service.actor(actor)
        if session["target_key"] == "target-workspace":
            if access is None or not isinstance(body, WorkspaceTurnRequest):
                raise ServiceError("workspace_contract_required")
            await work_sql.row(conn, session_id, access, write=True)
        elif isinstance(body, TurnRequest) and body.completion is not None:
            if session["target_key"] != "target-http" or session["target"].get("sandbox") is None:
                raise ServiceError("workspace_contract_required")
        old = (
            (
                await conn.execute(
                    select(turns).where(
                        turns.c.session_id == session_id,
                        turns.c.request_key == body.request_key,
                    )
                )
            )
            .mappings()
            .first()
        )
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
        # Aggregate scheduling facts without loading all historical transcripts.
        count, ordinal, queued_count = (
            await conn.execute(
                select(
                    func.count(turns.c.id),
                    func.max(turns.c.ordinal),
                    func.count(turns.c.id).filter(turns.c.status == "queued"),
                ).where(turns.c.session_id == session_id)
            )
        ).one()
        if settings.session_max_turns is not None and count >= settings.session_max_turns:
            raise ServiceError("input_limit", 422)
        if settings.session_max_history_bytes is not None:
            from sqlalchemy import LargeBinary
            from sqlalchemy import cast as sql_cast

            history_bytes = await conn.scalar(
                select(
                    func.sum(func.length(sql_cast(turns.c.text, LargeBinary))),
                ).where(turns.c.session_id == session_id)
            )
            if (history_bytes or 0) + len(body.text.encode()) > settings.session_max_history_bytes:
                raise ServiceError("input_limit", 422)
        if settings.session_max_queued is not None and queued_count >= settings.session_max_queued:
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
            ordinal=(ordinal or 0) + 1,
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
        if isinstance(body, TurnRequest) and body.completion is not None:
            if session["target_key"] != "target-http" or session["target"].get("sandbox") is None:
                raise ServiceError("workspace_contract_required")
        await conn.execute(insert(turns).values(**row))
        if isinstance(body, TurnRequest) and body.completion is not None:
            completion = body.completion.model_dump(mode="json")
            await conn.execute(
                insert(sandbox_turn_contracts).values(
                    turn_id=row["id"],
                    completion=completion,
                    fingerprint=fingerprint(completion),
                    created_at=time.time(),
                )
            )
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
        ordinary_compaction = session["target_key"] not in {
            "target-development",
            "target-workspace",
        } and getattr(settings, "history_compaction", True)
        paged_development = (
            session["target_key"] == "target-development"
            and settings.development_history_compaction
        )
        if (ordinary_compaction or paged_development) and await conn.scalar(
            select(turns.c.id)
            .where(
                turns.c.session_id == session["id"],
                turns.c.ordinal < turn["ordinal"],
                turns.c.status.in_(("queued", "running")),
            )
            .limit(1)
        ) is not None:
            raise ServiceError("capacity", 429)
        previous: Sequence[Any] = []
        if not (ordinary_compaction or paged_development):
            previous = (
                (
                    await conn.execute(
                        select(turns)
                        .where(
                            turns.c.session_id == session["id"],
                            turns.c.ordinal < turn["ordinal"],
                        )
                        .order_by(turns.c.ordinal)
                    )
                )
                .mappings()
                .all()
            )
            if any(row["status"] in {"queued", "running"} for row in previous):
                raise ServiceError("capacity", 429)
        if (
            settings.session_queue_timeout is not None
            and time.time() - turn["created_at"] >= settings.session_queue_timeout
        ):
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
        if paged_development:
            from .development_context import _source_rows, make_frozen

            binding = (
                (
                    await conn.execute(
                        select(sandbox_session_bindings).where(
                            sandbox_session_bindings.c.session_id == session["id"]
                        )
                    )
                )
                .mappings()
                .first()
            )
            if binding is None or binding["project_id"] is None:
                raise ServiceError("workspace_contract_required")
            if binding["principal_id"] != session["owner_id"]:
                raise ServiceError("facts_corrupt")
            project = (
                (
                    await conn.execute(
                        select(sandbox_projects).where(
                            sandbox_projects.c.project_id == binding["project_id"]
                        )
                    )
                )
                .mappings()
                .first()
            )
            account = (
                (
                    await conn.execute(
                        select(accounts.c.status, accounts.c.authorization_generation).where(
                            accounts.c.principal_id == session["owner_id"]
                        )
                    )
                )
                .mappings()
                .first()
            )
            if (
                project is None
                or project["principal_id"] != session["owner_id"]
                or project["workspace_id"] != session["workspace_id"]
                or account is None
                or account["status"] != "active"
            ):
                raise ServiceError("permission_denied")
            refs = await _source_rows(conn, session["id"], turn_id)
            frozen_sources = tuple(
                HistorySource(
                    turn_id=cast(str, ref["turn_id"]),
                    run_id=cast(str | None, ref["run_id"]),
                    last_seq=cast(int, ref["terminal_seq"]),
                    fingerprint=cast(str, ref["transcript_fingerprint"]),
                )
                for ref in refs
            )
            policy = await current_context_policy(conn, session)
            ext = (
                await load_turn_extensions(conn, turn["id"])
                if store.schema_version >= 11
                else {"version": "extensions/2", "revision": 0, "items": []}
            )
            extension_snapshot = cast(dict[str, JsonValue], ext)
            tools = cast(list[dict[str, JsonValue]], target["tools"])
            builder.check_budget(
                [
                    Message(role="system", content=settings.system_prompt),
                    *extension_messages(extension_snapshot),
                    Message(role="user", content=turn["text"]),
                ],
                tools,
                policy,
                request_limit(settings, policy, target),
            )
            development_frozen = make_frozen(
                owner_id=session["owner_id"],
                workspace_id=session["workspace_id"],
                project_id=binding["project_id"],
                session_id=session["id"],
                turn_id=turn["id"],
                run_id=run_id,
                authorization_generation=account["authorization_generation"],
                sources=frozen_sources,
                current=Message(role="user", content=turn["text"]),
                system_prompt=settings.system_prompt,
                extensions=extension_snapshot,
                policy=policy,
                compaction=CompactionPolicy(),
                tool_fingerprint=fingerprint(tools),
            )
            raw = DevelopmentContextState(frozen=development_frozen).model_dump(mode="json")
            raw_hash = unlimited_fingerprint(raw["frozen"])
            development_descriptor: dict[str, JsonValue] = {
                "schema_version": 3,
                "source_fingerprint": development_frozen.source_fingerprint,
                "frozen_fingerprint": raw_hash,
            }
            account_mcp = (
                await load_turn_mcp(conn, turn["id"]) if store.schema_version >= 12 else None
            )
            if account_mcp is not None and account_mcp["snapshot"]["connections"]:
                development_descriptor["account_mcp"] = account_mcp["fingerprint"]
            effective = target | {"context": development_descriptor}
            digest = fingerprint({"request": request.model_dump(mode="json"), "target": effective})
            run = await store._insert_run(conn, request, effective, run_id, digest)
            await conn.execute(
                update(turns)
                .where(turns.c.id == turn_id)
                .values(
                    run_id=run.id,
                    status="running",
                    context_snapshot=raw,
                    context_fingerprint=raw_hash,
                    started_at=time.time(),
                )
            )
            return run, True
        if ordinary_compaction:
            from .session_context import freeze_sources
            from .session_context import make_frozen as make_ordinary_frozen

            ordinary_sources = await freeze_sources(conn, session["id"], turn_id)
            policy = await current_context_policy(conn, session)
            ext = (
                await load_turn_extensions(conn, turn["id"])
                if store.schema_version >= 11
                else {"version": "extensions/2", "revision": 0, "items": []}
            )
            extension_snapshot = cast(dict[str, JsonValue], ext)
            tools = cast(list[dict[str, JsonValue]], target["tools"])
            # canonical活跃身份冻结其generation（无论legacy标记）；只有实际无托管
            # identity的宿主static路径才为None。None不构成授权：runtime/source验证
            # 必须显式持有宿主legacy actor才会放行。
            from krim_agent.storage.project_access import SQLiteAccessRepository

            canonical = (
                await SQLiteAccessRepository._actor(conn, session["owner_id"])
                if store.schema_version >= 7
                else None
            )
            if canonical is not None:
                canonical.owns(session["workspace_id"], session["owner_id"], write=True)
            else:
                account_status = (
                    await conn.scalar(
                        select(accounts.c.status).where(
                            accounts.c.principal_id == session["owner_id"],
                        )
                    )
                    if store.schema_version >= 7
                    else None
                )
                # Existing managed accounts with missing/revoked keys cannot
                # silently fall back to static host authentication.
                if account_status is not None:
                    raise ServiceError("permission_denied", 403)
            builder.check_budget(
                [
                    Message(role="system", content=settings.system_prompt),
                    *extension_messages(extension_snapshot),
                    Message(role="user", content=turn["text"]),
                ],
                tools,
                policy,
                request_limit(settings, policy, target),
            )
            ordinary_frozen = make_ordinary_frozen(
                owner_id=session["owner_id"],
                workspace_id=session["workspace_id"],
                session_id=session["id"],
                turn_id=turn_id,
                run_id=run_id,
                target_key=session["target_key"],
                sources=ordinary_sources,
                current=Message(role="user", content=turn["text"]),
                system_prompt=settings.system_prompt,
                extensions=extension_snapshot,
                policy=policy,
                compaction=CompactionPolicy(),
                tool_fingerprint=unlimited_fingerprint(tools),
                authorization_generation=canonical.generation if canonical is not None else None,
            )
            raw = OrdinaryContextState(frozen=ordinary_frozen).model_dump(mode="json")
            raw_hash = unlimited_fingerprint(raw["frozen"])
            ordinary_descriptor: dict[str, JsonValue] = {
                "schema_version": 4,
                "source_fingerprint": ordinary_frozen.source_fingerprint,
                "frozen_fingerprint": raw_hash,
            }
            account_mcp = (
                await load_turn_mcp(conn, turn["id"]) if store.schema_version >= 12 else None
            )
            if account_mcp is not None and account_mcp["snapshot"]["connections"]:
                ordinary_descriptor["account_mcp"] = account_mcp["fingerprint"]
            contract = (
                (
                    await conn.execute(
                        select(sandbox_turn_contracts).where(
                            sandbox_turn_contracts.c.turn_id == turn_id
                        )
                    )
                )
                .mappings()
                .first()
            )
            if contract is not None and session["target_key"] == "target-http":
                if fingerprint(contract["completion"]) != contract["fingerprint"]:
                    raise ServiceError("facts_corrupt")
                ordinary_descriptor["sandbox_completion_fingerprint"] = contract["fingerprint"]
            effective = target | {"context": ordinary_descriptor}
            digest = fingerprint({"request": request.model_dump(mode="json"), "target": effective})
            run = await store._insert_run(conn, request, effective, run_id, digest)
            await conn.execute(
                update(turns)
                .where(turns.c.id == turn_id)
                .values(
                    run_id=run.id,
                    status="running",
                    context_snapshot=raw,
                    context_fingerprint=raw_hash,
                    started_at=time.time(),
                )
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
        policy = await current_context_policy(conn, session)
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
            request_limit(settings, policy, target),
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
        contract = (
            (
                await conn.execute(
                    select(sandbox_turn_contracts).where(
                        sandbox_turn_contracts.c.turn_id == turn_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if contract is not None and session["target_key"] == "target-http":
            if fingerprint(contract["completion"]) != contract["fingerprint"]:
                raise ServiceError("facts_corrupt")
            context_descriptor["sandbox_completion_fingerprint"] = contract["fingerprint"]
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
    found = (
        (await conn.execute(select(turn_mcp).where(turn_mcp.c.turn_id == turn_id)))
        .mappings()
        .first()
    )
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
) -> (
    SessionContext
    | DevelopmentSessionContext
    | OrdinarySessionContext
    | WorkspaceSessionContext
    | None
):
    if version not in {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}:
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
        target_context = target.get("context")
        if (
            session["target_key"] == "target-development"
            and isinstance(target_context, dict)
            and target_context.get("schema_version") == 3
        ):
            from .development_context import DevelopmentCompactionSource, state_from_raw

            if (
                not isinstance(row["context_snapshot"], dict)
                or unlimited_fingerprint(row["context_snapshot"].get("frozen"))
                != row["context_fingerprint"]
                or row["workspace_id"] != session["workspace_id"]
                or row["owner_id"] != session["owner_id"]
                or request_key != "ac-turn." + row["id"]
            ):
                raise ServiceError("facts_corrupt")
            state = state_from_raw(row["context_snapshot"], row["context_fingerprint"])
            frozen = state.frozen
            expected_context: dict[str, JsonValue] = {
                "schema_version": 3,
                "source_fingerprint": frozen.source_fingerprint,
                "frozen_fingerprint": row["context_fingerprint"],
            }
            mcp = await load_turn_mcp(conn, row["id"]) if version >= 12 else None
            if mcp is not None and mcp["snapshot"]["connections"]:
                expected_context["account_mcp"] = mcp["fingerprint"]
            elif "account_mcp" in target_context:
                raise ServiceError("facts_corrupt")
            if fingerprint(target_context) != fingerprint(expected_context):
                raise ServiceError("facts_corrupt")
            source_view = DevelopmentCompactionSource(frozen)
            await source_view.verify_frozen(conn, run_id)
            return DevelopmentSessionContext(
                owner_id=frozen.owner_id,
                workspace_id=frozen.workspace_id,
                project_id=frozen.project_id,
                session_id=frozen.session_id,
                turn_id=frozen.turn_id,
                authorization_generation=frozen.authorization_generation,
                frozen=frozen,
                prepared=state.prepared,
            )
        if (
            session["target_key"] not in {"target-development", "target-workspace"}
            and isinstance(target_context, dict)
            and target_context.get("schema_version") == 4
        ):
            # 只有显式version4描述符进入ordinary解析；旧schema1快照不会重解释为4。
            from .session_context import OrdinaryCompactionSource
            from .session_context import state_from_raw as ordinary_state_from_raw

            if (
                not isinstance(row["context_snapshot"], dict)
                or unlimited_fingerprint(row["context_snapshot"].get("frozen"))
                != row["context_fingerprint"]
                or row["workspace_id"] != session["workspace_id"]
                or row["owner_id"] != session["owner_id"]
                or request_key != "ac-turn." + row["id"]
                or frozen_target_key(row["context_snapshot"]) != session["target_key"]
            ):
                raise ServiceError("facts_corrupt")
            ordinary_state = ordinary_state_from_raw(
                row["context_snapshot"], row["context_fingerprint"]
            )
            ordinary_anchor = ordinary_state.frozen
            ordinary_expected: dict[str, JsonValue] = {
                "schema_version": 4,
                "source_fingerprint": ordinary_anchor.source_fingerprint,
                "frozen_fingerprint": row["context_fingerprint"],
            }
            mcp = await load_turn_mcp(conn, row["id"]) if version >= 12 else None
            if mcp is not None and mcp["snapshot"]["connections"]:
                ordinary_expected["account_mcp"] = mcp["fingerprint"]
            elif "account_mcp" in target_context:
                raise ServiceError("facts_corrupt")
            if session["target_key"] == "target-http" and version >= 13:
                contract = (
                    (
                        await conn.execute(
                            select(sandbox_turn_contracts).where(
                                sandbox_turn_contracts.c.turn_id == row["id"],
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if contract is not None:
                    if fingerprint(contract["completion"]) != contract["fingerprint"]:
                        raise ServiceError("facts_corrupt")
                    ordinary_expected["sandbox_completion_fingerprint"] = contract["fingerprint"]
            elif "sandbox_completion_fingerprint" in target_context:
                raise ServiceError("facts_corrupt")
            if unlimited_fingerprint(target_context) != unlimited_fingerprint(ordinary_expected):
                raise ServiceError("facts_corrupt")
            ordinary_source = OrdinaryCompactionSource(ordinary_anchor)
            await ordinary_source.verify_frozen(conn, run_id)
            return OrdinarySessionContext(
                owner_id=ordinary_anchor.owner_id,
                workspace_id=ordinary_anchor.workspace_id,
                session_id=ordinary_anchor.session_id,
                turn_id=ordinary_anchor.turn_id,
                frozen=ordinary_anchor,
                prepared=ordinary_state.prepared,
            )
        if isinstance(row["context_snapshot"], dict) and row["context_snapshot"].get(
            "schema_version"
        ) in {3, 4}:
            raise ServiceError("facts_corrupt")
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
        if mcp is not None and mcp["snapshot"]["connections"] and isinstance(target_context, dict):
            expected["account_mcp"] = mcp["fingerprint"]
        elif isinstance(target_context, dict) and "account_mcp" in target_context:
            raise ValueError("账号MCP快照缺失")
        elif mcp is not None and mcp["snapshot"]["connections"]:
            raise ValueError("账号MCP上下文标记缺失")
        if session["target_key"] == "target-http" and isinstance(target_context, dict):
            if target_context.get("sandbox_completion_fingerprint"):
                store_version = target_context
                contract = (
                    (
                        await conn.execute(
                            select(sandbox_turn_contracts).where(
                                sandbox_turn_contracts.c.turn_id == row["id"]
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if (
                    contract is None
                    or contract["fingerprint"] != store_version["sandbox_completion_fingerprint"]
                    or fingerprint(contract["completion"]) != contract["fingerprint"]
                ):
                    raise ValueError("沙箱完成契约关联损坏")
                expected["sandbox_completion_fingerprint"] = contract["fingerprint"]
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


def task_transcript(snapshot: RunSnapshot) -> list[Message]:
    """Freeze a complete, event-backed projection for the run being finalized.

    Existing terminal turns are not read through this write path, so repairing
    future projections never rewrites their frozen transcript snapshots.
    """
    from dataclasses import replace

    summary_steps = {
        event.data.get("step_index")
        for event in snapshot.events
        if event.kind == "summary_text" or event.data.get("purpose") == "summary"
    }
    events = tuple(
        event
        for event in snapshot.events
        if event.kind != "summary_text"
        and event.data.get("purpose") != "summary"
        and not (
            event.kind in {"model_start", "model_end", "text", "usage"}
            and event.data.get("step_index") in summary_steps
        )
    )
    projected = builder.transcript(replace(snapshot, events=events))
    builder.validate_groups(projected)
    return projected


async def finish_turn(conn: AsyncConnection, snapshot: RunSnapshot) -> None:
    if snapshot.session_context is None:
        return
    if isinstance(snapshot.session_context, WorkspaceSessionContext):
        await work_sql.finish(
            conn, snapshot.run.id, interrupted=snapshot.run.status == "interrupted"
        )

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
            .where(
                turns.c.id == snapshot.session_context.turn_id,
                turns.c.run_id == snapshot.run.id,
            )
            .values(
                status="cancelled" if snapshot.run.status == "aborted" else snapshot.run.status,
                code=snapshot.run.code,
                finished_at=snapshot.run.finished_at,
                transcript=[m.model_dump(mode="json") for m in task_transcript(snapshot)],
            )
        )
        return
    if isinstance(snapshot.session_context, (DevelopmentSessionContext, OrdinarySessionContext)):
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
            .where(
                turns.c.id == snapshot.session_context.turn_id,
                turns.c.run_id == snapshot.run.id,
            )
            .values(
                status="cancelled" if snapshot.run.status == "aborted" else snapshot.run.status,
                code=snapshot.run.code,
                finished_at=snapshot.run.finished_at,
                transcript=[
                    message.model_dump(mode="json") for message in task_transcript(snapshot)
                ],
            )
        )
        return
    status = "cancelled" if snapshot.run.status == "aborted" else snapshot.run.status
    await conn.execute(
        update(turns)
        .where(
                turns.c.id == snapshot.session_context.turn_id,
                turns.c.run_id == snapshot.run.id,
            )
        .values(
            status=status,
            code=snapshot.run.code,
            finished_at=snapshot.run.finished_at,
            transcript=[m.model_dump(mode="json") for m in task_transcript(snapshot)],
        )
    )


async def owned_active_runs(store: SQLiteStore, principal_id: str) -> list[str]:
    """Include parked runs even when their producer has already ended."""

    async def operation(conn: AsyncConnection) -> list[str]:
        return list(
            await conn.scalars(
                select(runs.c.id)
                .join(turns, turns.c.run_id == runs.c.id)
                .join(sessions, sessions.c.id == turns.c.session_id)
                .where(sessions.c.owner_id == principal_id, runs.c.status.not_in(TERMINAL))
                .order_by(runs.c.created_at, runs.c.id)
            )
        )

    return await read(store, operation)


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


async def retire_queued_targets(
    store: SQLiteStore,
    targets: frozenset[str] = RETIRED_TARGETS,
) -> int:
    """Interrupt old-target turns that have no run and cannot be dispatched.

    Request bodies, accepted revisions, and frozen history are left untouched.
    The status and control updates share one transaction so the session dispatcher
    cannot see a retired queued turn after startup recovery.
    """
    selected = frozenset(targets)
    if not selected:
        return 0
    planned_sessions: list[str] = []
    planned_turns: list[str] = []
    planned_resumes: list[str] = []
    selection_complete = False
    now = time.time()
    result_value = {"status": "interrupted", "code": "target_retired"}

    async def operation(conn: AsyncConnection) -> int:
        nonlocal selection_complete
        planned_sessions[:] = list(
            await conn.scalars(
                select(sessions.c.id).where(sessions.c.target_key.in_(selected))
            )
        )
        selection_complete = True
        if not planned_sessions:
            return 0
        queued_rows = (
            await conn.execute(
                select(turns.c.id)
                .where(
                    turns.c.session_id.in_(planned_sessions),
                    turns.c.status == "queued",
                    turns.c.run_id.is_(None),
                )
                .order_by(turns.c.created_at, turns.c.ordinal)
            )
        ).all()
        planned_turns[:] = [cast(str, row.id) for row in queued_rows]
        if planned_turns:
            work_rows = (
                await conn.execute(
                    select(work_turns.c.turn_id, work_turns.c.run_id).where(
                        work_turns.c.turn_id.in_(planned_turns)
                    )
                )
            ).all()
            if any(row.run_id is not None for row in work_rows):
                raise ServiceError("facts_corrupt")
            await conn.execute(
                update(turns)
                .where(
                    turns.c.id.in_(planned_turns),
                    turns.c.status == "queued",
                    turns.c.run_id.is_(None),
                )
                .values(status="interrupted", code="target_retired", finished_at=now)
            )
            await conn.execute(
                update(work_turns)
                .where(
                    work_turns.c.turn_id.in_(planned_turns),
                    work_turns.c.run_id.is_(None),
                )
                .values(phase="interrupted", finished_at=now)
            )
        resume_rows = (
            await conn.execute(
                select(session_controls.c.id).where(
                    session_controls.c.session_id.in_(planned_sessions),
                    session_controls.c.kind == "resume",
                    session_controls.c.status == "pending",
                )
            )
        ).all()
        planned_resumes[:] = [cast(str, row.id) for row in resume_rows]
        if planned_resumes:
            await conn.execute(
                update(session_controls)
                .where(
                    session_controls.c.id.in_(planned_resumes),
                    session_controls.c.status == "pending",
                )
                .values(status="completed", result=result_value, finished_at=now)
            )
        await conn.execute(
            update(sessions)
            .where(sessions.c.id.in_(planned_sessions))
            .values(dispatch_paused=True)
        )
        return len(planned_turns)

    async def confirm() -> int | None:
        async def verify(conn: AsyncConnection) -> int | None:
            if not selection_complete:
                return None
            if not planned_sessions:
                return 0
            if planned_turns:
                turns_ok = (
                    await conn.scalar(
                        select(func.count()).select_from(turns).where(
                            turns.c.id.in_(planned_turns),
                            turns.c.status == "interrupted",
                            turns.c.code == "target_retired",
                            turns.c.run_id.is_(None),
                        )
                    )
                    or 0
                )
                if turns_ok != len(planned_turns):
                    return None
                work_rows = (
                    await conn.execute(
                        select(work_turns.c.phase, work_turns.c.finished_at).where(
                            work_turns.c.turn_id.in_(planned_turns),
                            work_turns.c.run_id.is_(None),
                        )
                    )
                ).all()
                if any(row.phase != "interrupted" or row.finished_at is None for row in work_rows):
                    return None
            if planned_resumes:
                controls_ok = (
                    await conn.scalar(
                        select(func.count()).select_from(session_controls).where(
                            session_controls.c.id.in_(planned_resumes),
                            session_controls.c.status == "completed",
                            session_controls.c.result == result_value,
                        )
                    )
                    or 0
                )
                if controls_ok != len(planned_resumes):
                    return None
            paused = (
                await conn.scalar(
                    select(func.count()).select_from(sessions).where(
                        sessions.c.id.in_(planned_sessions),
                        sessions.c.dispatch_paused.is_(True),
                    )
                )
                or 0
            )
            if paused != len(planned_sessions):
                return None
            remaining = (
                await conn.scalar(
                    select(func.count())
                    .select_from(turns.join(sessions, sessions.c.id == turns.c.session_id))
                    .where(
                        sessions.c.id.in_(planned_sessions),
                        sessions.c.target_key.in_(selected),
                        turns.c.status == "queued",
                        turns.c.run_id.is_(None),
                    )
                )
                or 0
            )
            if remaining:
                return None
            return len(planned_turns)

        return await read(store, verify)

    return await store._eval_write(operation, confirm)


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
