"""Idempotent, reversible archive and trash state for sessions.

This module never removes session, turn, run, tool, Skill, memory, or MCP history.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Literal

from pydantic import JsonValue
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, fingerprint, parse_json
from krim_agent.sessions.types import SessionView

from .models import session_controls, turns
from .session_lifecycle_models import session_lifecycle, session_lifecycle_requests
from .sessions import read, require, session_lifecycle_row, session_row, session_view

if TYPE_CHECKING:
    from .sqlite import SQLiteStore

LifecycleAction = Literal["archive", "unarchive", "trash", "restore"]


def _with_state(view: SessionView, row: Any | None) -> SessionView:
    return view.model_copy(
        update={
            "archived_at": row["archived_at"] if row is not None else None,
            "deleted_at": row["deleted_at"] if row is not None else None,
            "lifecycle_revision": row["revision"] if row is not None else 0,
        }
    )


async def decorate(conn: AsyncConnection, view: SessionView) -> SessionView:
    return _with_state(view, await session_lifecycle_row(conn, view.id))


async def request_by_key(
    store: SQLiteStore,
    principal_id: str,
    workspace: str,
    session_id: str,
    request_key: str,
) -> SessionView:
    require(store)

    async def operation(conn: AsyncConnection) -> SessionView:
        session = await session_row(conn, session_id)
        if session["workspace_id"] != workspace or session["owner_id"] != principal_id:
            raise ServiceError("not_found", 404)
        row = (
            await conn.execute(
                select(session_lifecycle_requests).where(
                    session_lifecycle_requests.c.principal_id == principal_id,
                    session_lifecycle_requests.c.request_key == request_key,
                    session_lifecycle_requests.c.session_id == session_id,
                )
            )
        ).mappings().first()
        if row is None:
            raise ServiceError("not_found", 404)
        try:
            return SessionView.model_validate(row["result"])
        except ValueError:
            raise ServiceError("facts_corrupt") from None

    return await read(store, operation)


async def apply(
    store: SQLiteStore,
    *,
    principal_id: str,
    workspace: str,
    session_id: str,
    request_key: str,
    expected_revision: int,
    action: LifecycleAction,
) -> SessionView:
    require(store)
    payload: dict[str, JsonValue] = {
        "principal_id": principal_id,
        "workspace_id": workspace,
        "session_id": session_id,
        "action": action,
        "expected_revision": expected_revision,
    }
    digest = fingerprint(payload)

    async def operation(conn: AsyncConnection) -> SessionView:
        old = (
            await conn.execute(
                select(session_lifecycle_requests).where(
                    session_lifecycle_requests.c.principal_id == principal_id,
                    session_lifecycle_requests.c.request_key == request_key,
                    session_lifecycle_requests.c.session_id == session_id,
                )
            )
        ).mappings().first()
        if old is not None:
            if old["request_hash"] != digest:
                raise ServiceError("key_conflict")
            try:
                return SessionView.model_validate(old["result"])
            except ValueError:
                raise ServiceError("facts_corrupt") from None

        session = await session_row(conn, session_id)
        if session["workspace_id"] != workspace or session["owner_id"] != principal_id:
            raise ServiceError("not_found", 404)
        state = await session_lifecycle_row(conn, session_id)
        current = dict(
            session_id=session_id,
            archived_at=state["archived_at"] if state else None,
            deleted_at=state["deleted_at"] if state else None,
            updated_at=state["updated_at"] if state else 0.0,
            revision=state["revision"] if state else 0,
        )
        if current["revision"] != expected_revision:
            raise ServiceError("revision_conflict", 409)
        if action in {"archive", "unarchive"} and current["deleted_at"] is not None:
            raise ServiceError("session_in_trash", 409)
        if action in {"archive", "unarchive", "trash", "restore"}:
            if session["status"] != "closed":
                raise ServiceError("session_must_be_closed", 409)
            pending_close = await conn.scalar(
                select(session_controls.c.id)
                .where(
                    session_controls.c.session_id == session_id,
                    session_controls.c.kind == "close",
                    session_controls.c.status == "pending",
                )
                .limit(1)
            )
            if pending_close is not None:
                raise ServiceError("session_busy", 409)
        if action == "archive":
            if current["archived_at"] is not None:
                raise ServiceError("revision_conflict", 409)
            current["archived_at"] = time.time()
        elif action == "unarchive":
            if current["archived_at"] is None:
                raise ServiceError("revision_conflict", 409)
            current["archived_at"] = None
        elif action == "trash":
            if session["status"] != "closed":
                raise ServiceError("session_must_be_closed", 409)
            outstanding = await conn.scalar(
                select(func.count())
                .select_from(turns)
                .where(
                    turns.c.session_id == session_id,
                    turns.c.status.in_(["queued", "running"]),
                )
            )
            if outstanding:
                raise ServiceError("session_busy", 409)
            if current["deleted_at"] is not None:
                raise ServiceError("revision_conflict", 409)
            current["deleted_at"] = time.time()
        elif action == "restore":
            if current["deleted_at"] is None and current["archived_at"] is None:
                raise ServiceError("revision_conflict", 409)
            current["deleted_at"] = None
            current["archived_at"] = None
        else:
            raise ServiceError("invalid_input", 422)
        current["revision"] += 1
        current["updated_at"] = time.time()
        if state is None:
            await conn.execute(insert(session_lifecycle).values(**current))
        else:
            await conn.execute(
                update(session_lifecycle)
                .where(session_lifecycle.c.session_id == session_id)
                .values(
                    archived_at=current["archived_at"],
                    deleted_at=current["deleted_at"],
                    updated_at=current["updated_at"],
                    revision=current["revision"],
                )
            )
        result = await decorate(conn, session_view(session))
        await conn.execute(
            insert(session_lifecycle_requests).values(
                principal_id=principal_id,
                request_key=request_key,
                request_hash=digest,
                session_id=session_id,
                result=parse_json(result.model_dump_json()),
                created_at=time.time(),
            )
        )
        return result

    async def confirm() -> SessionView | None:
        try:
            return await request_by_key(
                store, principal_id, workspace, session_id, request_key
            )
        except ServiceError as error:
            if error.code == "not_found":
                return None
            raise

    return await store._eval_write(operation, confirm)


async def lifecycle_row(conn: AsyncConnection, session_id: str) -> dict[str, Any]:
    row = await session_lifecycle_row(conn, session_id)
    return {
        "session_id": session_id,
        "archived_at": row["archived_at"] if row else None,
        "deleted_at": row["deleted_at"] if row else None,
        "updated_at": row["updated_at"] if row else 0.0,
        "revision": row["revision"] if row else 0,
    }
