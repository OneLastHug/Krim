"""Frozen development history with separate integrity and live-access checks."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import JsonValue
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.context import builder
from krim_agent.context.serialization import encoded, fingerprint
from krim_agent.context.types import (
    CompactionPolicy,
    ContextPolicy,
    DevelopmentContextFrozen,
    DevelopmentContextState,
    HistorySource,
    PreparedContext,
)
from krim_agent.contracts import TERMINAL, Message, ServiceError
from krim_agent.project_access.types import ProjectGrant
from krim_agent.storage.development_models import project_access_records
from krim_agent.storage.models import run_events, runs, sessions, turns
from krim_agent.storage.sandbox_models import sandbox_projects, sandbox_session_bindings

DOMAIN = "development-session-history/3"
SOURCE_PAGE_SIZE = 128


def scope_of(frozen: DevelopmentContextFrozen) -> dict[str, JsonValue]:
    return {
        key: getattr(frozen, key)
        for key in ("owner_id", "workspace_id", "project_id", "session_id")
    }


def source_fingerprint(
    scope: dict[str, JsonValue],
    sources: tuple[HistorySource, ...],
    extensions: dict[str, JsonValue],
) -> str:
    return fingerprint(
        {
            "version": "session-history/3",
            "scope": scope,
            "projection": builder.VERSION,
            "sources": [item.model_dump(mode="json") for item in sources],
            "extensions": extensions,
        }
    )


def state_from_raw(raw: Any, context_hash: str | None) -> DevelopmentContextState:
    try:
        state = DevelopmentContextState.model_validate_json(encoded(raw))
        frozen = state.frozen
        if (
            context_hash != fingerprint(frozen.model_dump(mode="json"))
            or frozen.projection_version != builder.VERSION
            or frozen.current.role != "user"
            or frozen.current.tool_calls
            or frozen.current.call_id is not None
            or frozen.source_fingerprint
            != source_fingerprint(scope_of(frozen), frozen.sources, frozen.extensions)
            or (state.prepared is None) != (state.prepared_hash is None)
        ):
            raise ValueError("context binding drift")
        if state.prepared is not None:
            prepared = state.prepared
            if (
                state.prepared_hash != fingerprint(prepared.model_dump(mode="json"))
                or prepared.source_fingerprint != frozen.source_fingerprint
                or prepared.sources != frozen.sources
                or prepared.policy != frozen.compaction
                or not prepared.messages
                or prepared.messages[0].role != "system"
                or prepared.messages[0].content != frozen.system_prompt
            ):
                raise ValueError("prepared context drift")
            builder.validate_groups(list(prepared.messages[1:]))
        return state
    except (TypeError, ValueError):
        raise ServiceError("facts_corrupt") from None


def _history_messages(row: Any) -> list[Message]:
    if row["transcript"] is None:
        messages = [
            Message(role="user", content=row["text"]),
            Message(
                role="user",
                content="[不可信历史状态数据] " + row["status"] + ": " + (row["code"] or "unknown"),
            ),
        ]
    else:
        messages = [Message.model_validate_json(encoded(item)) for item in row["transcript"]]
    builder.validate_groups(messages)
    return messages


async def _previous_rows(
    conn: AsyncConnection,
    session_id: str,
    turn_id: str,
    *,
    full: bool,
) -> Sequence[Mapping[str, Any]]:
    current = (
        (
            await conn.execute(
                select(turns.c.ordinal, turns.c.owner_id, turns.c.workspace_id).where(
                    turns.c.id == turn_id,
                    turns.c.session_id == session_id,
                )
            )
        )
        .mappings()
        .first()
    )
    if current is None:
        raise ServiceError("facts_corrupt")
    columns = (
        (turns,)
        if full
        else (
            turns.c.id,
            turns.c.ordinal,
            turns.c.status,
            turns.c.run_id,
            turns.c.owner_id,
            turns.c.workspace_id,
        )
    )
    rows: list[Mapping[str, Any]] = []
    after = -1
    while True:
        page = (
            (
                await conn.execute(
                    select(*columns)
                    .where(
                        turns.c.session_id == session_id,
                        turns.c.ordinal < current["ordinal"],
                        turns.c.ordinal > after,
                    )
                    .order_by(turns.c.ordinal)
                    .limit(SOURCE_PAGE_SIZE)
                )
            )
            .mappings()
            .all()
        )
        if not page:
            break
        next_ordinal = page[-1]["ordinal"]
        if next_ordinal <= after:
            raise ServiceError("facts_corrupt")
        rows.extend(dict(row) for row in page)
        after = next_ordinal
    if any(
        row["owner_id"] != current["owner_id"] or row["workspace_id"] != current["workspace_id"]
        for row in rows
    ):
        raise ServiceError("facts_corrupt")
    return [dict(row) for row in rows]


async def _terminal_sequences(
    conn: AsyncConnection,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    ids = [row["run_id"] for row in rows if row["run_id"] is not None]
    if not ids:
        if any(row["status"] in {"queued", "running"} for row in rows):
            raise ServiceError("capacity", 429)
        return {}
    if len(rows) > SOURCE_PAGE_SIZE:
        result: dict[str, int] = {}
        for offset in range(0, len(rows), SOURCE_PAGE_SIZE):
            result.update(await _terminal_sequences(conn, rows[offset : offset + SOURCE_PAGE_SIZE]))
        return result
    latest = (
        select(run_events.c.run_id, func.max(run_events.c.seq).label("seq"))
        .where(run_events.c.run_id.in_(ids))
        .group_by(run_events.c.run_id)
        .subquery()
    )
    records = (
        (
            await conn.execute(
                select(
                    runs.c.id, runs.c.status, runs.c.request_key, latest.c.seq, run_events.c.kind
                )
                .join(latest, latest.c.run_id == runs.c.id)
                .join(
                    run_events,
                    (run_events.c.run_id == runs.c.id) & (run_events.c.seq == latest.c.seq),
                )
                .where(runs.c.id.in_(ids))
            )
        )
        .mappings()
        .all()
    )
    found = {row["id"]: row for row in records}
    sequences = {}
    for row in rows:
        if row["status"] in {"queued", "running"}:
            raise ServiceError("capacity", 429)
        if row["run_id"] is None:
            continue
        record = found.get(row["run_id"])
        if (
            record is None
            or record["status"] not in TERMINAL
            or record["kind"] != "terminal"
            or record["request_key"] != "ac-turn." + row["id"]
        ):
            raise ServiceError("facts_corrupt")
        sequences[row["run_id"]] = record["seq"]
    return sequences


async def _source_rows(
    conn: AsyncConnection,
    session_id: str,
    turn_id: str,
) -> list[dict[str, JsonValue]]:
    rows = await _previous_rows(conn, session_id, turn_id, full=False)
    sequences = await _terminal_sequences(conn, rows)
    refs: list[dict[str, JsonValue]] = []
    for offset in range(0, len(rows), 16):
        ids = [row["id"] for row in rows[offset : offset + 16]]
        page = (
            (
                await conn.execute(
                    select(turns)
                    .where(
                        turns.c.id.in_(ids),
                        turns.c.session_id == session_id,
                    )
                    .order_by(turns.c.ordinal)
                )
            )
            .mappings()
            .all()
        )
        if [row["id"] for row in page] != ids:
            raise ServiceError("facts_corrupt")
        refs.extend(
            {
                "turn_id": row["id"],
                "run_id": row["run_id"],
                "terminal_seq": sequences.get(row["run_id"], -1),
                "transcript_fingerprint": fingerprint(
                    [message.model_dump(mode="json") for message in _history_messages(row)]
                ),
            }
            for row in page
        )
    return refs


async def _verify_frozen_refs(conn: AsyncConnection, frozen: DevelopmentContextFrozen) -> None:
    rows = await _previous_rows(conn, frozen.session_id, frozen.turn_id, full=False)
    if len(rows) != len(frozen.sources):
        raise ServiceError("facts_corrupt")
    sequences = await _terminal_sequences(conn, rows)
    for row, source in zip(rows, frozen.sources, strict=True):
        if (
            row["id"] != source.turn_id
            or row["run_id"] != source.run_id
            or (sequences.get(source.run_id, -1) if source.run_id is not None else -1)
            != source.last_seq
        ):
            raise ServiceError("facts_corrupt")


class DevelopmentCompactionSource:
    domain = DOMAIN

    def __init__(self, frozen: DevelopmentContextFrozen):
        self.frozen = DevelopmentContextFrozen.model_validate_json(frozen.model_dump_json())
        self.session_id = frozen.session_id
        self.source_refs: tuple[dict[str, JsonValue], ...] = tuple(
            {
                "turn_id": source.turn_id,
                "run_id": source.run_id,
                "terminal_seq": source.last_seq,
                "transcript_fingerprint": source.fingerprint,
            }
            for source in frozen.sources
        )
        self.source_fingerprint = frozen.source_fingerprint
        self.context_policy = frozen.policy
        self.compaction_policy = frozen.compaction
        self.extensions = frozen.extensions
        self.policy_binding: dict[str, JsonValue] = {
            "scope": scope_of(frozen),
            "authorization_generation": frozen.authorization_generation,
            "projection": frozen.projection_version,
            "tool_fingerprint": frozen.tool_fingerprint,
            "system_fingerprint": fingerprint(frozen.system_prompt),
        }

    async def verify_frozen(self, conn: AsyncConnection, run_id: str) -> None:
        await self._verify_frozen(conn, run_id)

    async def _verify_frozen(self, conn: AsyncConnection, run_id: str) -> Any:
        frozen = self.frozen
        canonical = (
            (
                await conn.execute(
                    select(
                        turns,
                        sessions.c.target_key,
                        sessions.c.owner_id.label("session_owner"),
                        sessions.c.workspace_id.label("session_workspace"),
                        runs.c.request,
                        runs.c.target,
                        runs.c.request_key.label("run_request_key"),
                        runs.c.status.label("run_status"),
                        runs.c.stop_requested,
                    )
                    .join(sessions, sessions.c.id == turns.c.session_id)
                    .join(runs, runs.c.id == turns.c.run_id)
                    .where(
                        turns.c.id == frozen.turn_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if (
            run_id != frozen.run_id
            or canonical is None
            or canonical["session_id"] != frozen.session_id
            or canonical["owner_id"] != frozen.owner_id
            or canonical["workspace_id"] != frozen.workspace_id
            or canonical["session_owner"] != frozen.owner_id
            or canonical["session_workspace"] != frozen.workspace_id
            or canonical["target_key"] != "target-development"
            or canonical["run_id"] != run_id
            or canonical["text"] != frozen.current.content
            or canonical["projection_version"] != frozen.projection_version
        ):
            raise ServiceError("facts_corrupt")
        state = state_from_raw(canonical["context_snapshot"], canonical["context_fingerprint"])
        if state.frozen != frozen:
            raise ServiceError("facts_corrupt")
        run = canonical
        if (
            run is None
            or run["run_request_key"] != "ac-turn." + frozen.turn_id
            or run["request"].get("text") != frozen.current.content
            or not isinstance(run["target"], dict)
            or not isinstance(run["target"].get("context"), dict)
            or run["target"]["context"].get("schema_version") != 3
            or run["target"]["context"].get("source_fingerprint") != frozen.source_fingerprint
            or run["target"]["context"].get("frozen_fingerprint")
            != fingerprint(frozen.model_dump(mode="json"))
            or fingerprint(run["target"].get("tools")) != frozen.tool_fingerprint
        ):
            raise ServiceError("facts_corrupt")
        await _verify_frozen_refs(conn, frozen)
        return canonical

    async def verify(self, conn: AsyncConnection, run_id: str) -> None:
        canonical = await self._verify_frozen(conn, run_id)
        frozen = self.frozen
        from krim_agent.storage.project_access import SQLiteAccessRepository

        # Reuse its canonical identity reader with this same connection. It does
        # not use store state or open a separate transaction.
        actor = await SQLiteAccessRepository._actor(conn, frozen.owner_id)
        if actor is None or actor.generation != frozen.authorization_generation:
            raise ServiceError("permission_denied", 403)
        actor.owns(frozen.workspace_id, frozen.owner_id, write=True)
        scope = (
            (
                await conn.execute(
                    select(
                        sandbox_session_bindings.c.project_id,
                        sandbox_session_bindings.c.principal_id,
                        sandbox_projects.c.workspace_id,
                        sandbox_projects.c.status.label("project_status"),
                        sessions.c.status.label("session_status"),
                    )
                    .join(
                        sandbox_projects,
                        sandbox_projects.c.project_id == sandbox_session_bindings.c.project_id,
                    )
                    .join(sessions, sessions.c.id == sandbox_session_bindings.c.session_id)
                    .where(
                        sandbox_session_bindings.c.session_id == frozen.session_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        run = canonical
        if (
            scope is None
            or scope["project_id"] != frozen.project_id
            or scope["principal_id"] != frozen.owner_id
            or scope["workspace_id"] != frozen.workspace_id
            or scope["project_status"] != "ready"
            or scope["session_status"] != "open"
            or run is None
            or run["run_status"] != "running"
            or run["stop_requested"]
        ):
            raise ServiceError("permission_denied", 403)
        schema_version = (await conn.exec_driver_sql("PRAGMA user_version")).scalar_one()
        if schema_version >= 19:
            from krim_agent.project_access.policy import resolve_project_access

            access = await resolve_project_access(conn, actor, frozen.project_id)
            # History needs project read access, not a separate shell license.
            # Each tool's guard still enforces the write ceiling and session mode.
            if access.access == "none":
                raise ServiceError("permission_denied", 403)
            return
        row = (
            (
                await conn.execute(
                    select(project_access_records).where(
                        project_access_records.c.project_id == frozen.project_id,
                        project_access_records.c.owner_id == frozen.owner_id,
                        project_access_records.c.kind == "grant",
                        project_access_records.c.resource_id == frozen.owner_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("permission_denied", 403)
        grant = ProjectGrant.model_validate_json(encoded(row["document"]))
        if (
            grant.revision != row["revision"]
            or grant.owner_id != frozen.owner_id
            or grant.workspace_id != frozen.workspace_id
            or grant.project_id != frozen.project_id
            or grant.actor_id != frozen.owner_id
            or grant.actor_generation != frozen.authorization_generation
            or grant.revoked
            or grant.expires_at is None
            or grant.expires_at <= time.time()
            or "shell.execute" not in grant.actions
        ):
            raise ServiceError("permission_denied", 403)

    async def page(
        self, conn: AsyncConnection, run_id: str, offset: int, limit: int
    ) -> list[dict[str, Any]]:
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 16:
            raise ServiceError("facts_corrupt")
        await self.verify(conn, run_id)
        result = []
        for ref in self.source_refs[offset : offset + limit]:
            row = (
                (
                    await conn.execute(
                        select(turns).where(
                            turns.c.id == ref["turn_id"],
                            turns.c.session_id == self.session_id,
                            turns.c.owner_id == self.frozen.owner_id,
                            turns.c.workspace_id == self.frozen.workspace_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("facts_corrupt")
            messages = _history_messages(row)
            if (
                fingerprint([message.model_dump(mode="json") for message in messages])
                != ref["transcript_fingerprint"]
            ):
                raise ServiceError("facts_corrupt")
            result.append({"ref": dict(ref), "messages": messages})
        await self.verify(conn, run_id)
        return result

    async def save_prepared(
        self, conn: AsyncConnection, run_id: str, prepared: PreparedContext
    ) -> None:
        await self.verify(conn, run_id)
        if (
            prepared.source_fingerprint != self.source_fingerprint
            or prepared.sources != self.frozen.sources
            or prepared.policy != self.compaction_policy
        ):
            raise ServiceError("facts_corrupt")
        state = DevelopmentContextState(
            frozen=self.frozen,
            prepared=prepared,
            prepared_hash=fingerprint(prepared.model_dump(mode="json")),
        )
        raw = state.model_dump(mode="json")
        state_from_raw(raw, fingerprint(self.frozen.model_dump(mode="json")))
        await conn.execute(
            update(turns)
            .where(
                turns.c.id == self.frozen.turn_id,
                turns.c.run_id == run_id,
            )
            .values(context_snapshot=raw)
        )
        await self.verify(conn, run_id)

    async def read_prepared(self, conn: AsyncConnection, run_id: str) -> PreparedContext | None:
        await self.verify(conn, run_id)
        raw = await conn.scalar(
            select(turns.c.context_snapshot).where(
                turns.c.id == self.frozen.turn_id,
            )
        )
        state = state_from_raw(raw, fingerprint(self.frozen.model_dump(mode="json")))
        await self.verify(conn, run_id)
        return state.prepared


def make_frozen(
    *,
    owner_id: str,
    workspace_id: str,
    project_id: str,
    session_id: str,
    turn_id: str,
    run_id: str,
    authorization_generation: int,
    sources: tuple[HistorySource, ...],
    current: Message,
    system_prompt: str,
    extensions: dict[str, JsonValue],
    policy: ContextPolicy,
    compaction: CompactionPolicy,
    tool_fingerprint: str,
) -> DevelopmentContextFrozen:
    scope: dict[str, JsonValue] = {
        "owner_id": owner_id,
        "workspace_id": workspace_id,
        "project_id": project_id,
        "session_id": session_id,
    }
    frozen = DevelopmentContextFrozen(
        owner_id=owner_id,
        workspace_id=workspace_id,
        project_id=project_id,
        session_id=session_id,
        turn_id=turn_id,
        run_id=run_id,
        authorization_generation=authorization_generation,
        sources=sources,
        current=current,
        system_prompt=system_prompt,
        extensions=extensions,
        policy=policy,
        compaction=compaction,
        tool_fingerprint=tool_fingerprint,
        projection_version=builder.VERSION,
        source_fingerprint=source_fingerprint(scope, sources, extensions),
    )
    return frozen
