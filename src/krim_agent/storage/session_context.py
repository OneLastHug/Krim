"""Ordinary-session frozen history, without a workspace or development grant.

Admission freezes these descriptors on the same transaction as the current run.
The host persists OrdinaryContextState in turns.context_snapshot and its frozen
hash in turns.context_fingerprint. No new history table or fabricated WorkFrame
is needed. The host supplies a static legacy actor when managed identity is off.
"""

from __future__ import annotations

from typing import Any, cast

from pydantic import JsonValue
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.context import builder
from krim_agent.context.serialization import encoded, fingerprint
from krim_agent.context.types import (
    CompactionPolicy,
    ContextPolicy,
    HistorySource,
    OrdinaryContextFrozen,
    OrdinaryContextState,
    PreparedContext,
)
from krim_agent.contracts import Message, ServiceError
from krim_agent.security import PrincipalContext
from krim_agent.storage.development_context import (
    _history_messages,
    _previous_rows,
    _source_rows,
    _terminal_sequences,
)
from krim_agent.storage.models import runs, sessions, turns

DOMAIN = "ordinary-session-history/4"


def scope_of(frozen: OrdinaryContextFrozen) -> dict[str, JsonValue]:
    return {
        key: getattr(frozen, key)
        for key in ("owner_id", "workspace_id", "session_id", "target_key")
    }


def source_fingerprint(
    scope: dict[str, JsonValue],
    sources: tuple[HistorySource, ...],
    extensions: dict[str, JsonValue],
) -> str:
    return fingerprint(
        {
            "version": DOMAIN,
            "scope": scope,
            "projection": builder.VERSION,
            "sources": [item.model_dump(mode="json") for item in sources],
            "extensions": extensions,
        }
    )


async def freeze_sources(
    conn: AsyncConnection, session_id: str, turn_id: str
) -> tuple[HistorySource, ...]:
    """Capture whole-turn references, scanning with bounded database pages."""
    return tuple(
        HistorySource(
            turn_id=str(ref["turn_id"]),
            run_id=cast(str | None, ref["run_id"]),
            last_seq=cast(int, ref["terminal_seq"]),
            fingerprint=cast(str, ref["transcript_fingerprint"]),
        )
        for ref in await _source_rows(conn, session_id, turn_id)
    )


def make_frozen(
    *,
    owner_id: str,
    workspace_id: str,
    session_id: str,
    turn_id: str,
    run_id: str,
    target_key: str,
    sources: tuple[HistorySource, ...],
    current: Message,
    system_prompt: str,
    extensions: dict[str, JsonValue],
    policy: ContextPolicy,
    compaction: CompactionPolicy,
    tool_fingerprint: str,
    authorization_generation: int | None = None,
) -> OrdinaryContextFrozen:
    scope: dict[str, JsonValue] = {
        "owner_id": owner_id,
        "workspace_id": workspace_id,
        "session_id": session_id,
        "target_key": target_key,
    }
    return OrdinaryContextFrozen(
        owner_id=owner_id,
        workspace_id=workspace_id,
        session_id=session_id,
        turn_id=turn_id,
        run_id=run_id,
        target_key=target_key,
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


def state_from_raw(raw: Any, context_hash: str | None) -> OrdinaryContextState:
    try:
        # Versioned persisted documents must declare their version; Pydantic's
        # constructor defaults are for new objects, not a migration mechanism.
        if (
            not isinstance(raw, dict)
            or raw.get("schema_version") != 4
            or not isinstance(raw.get("frozen"), dict)
            or raw["frozen"].get("schema_version") != 4
        ):
            raise ValueError("missing ordinary context version")
        state = OrdinaryContextState.model_validate_json(encoded(raw))
        frozen = state.frozen
        if (
            context_hash != fingerprint(frozen.model_dump(mode="json"))
            or frozen.projection_version != builder.VERSION
            or frozen.target_key in {"target-development", "target-workspace"}
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


class OrdinaryCompactionSource:
    domain = DOMAIN

    def __init__(
        self, frozen: OrdinaryContextFrozen, *, legacy_actor: PrincipalContext | None = None
    ):
        self.frozen = OrdinaryContextFrozen.model_validate_json(frozen.model_dump_json())
        self.legacy_actor = legacy_actor
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
        self.context_policy, self.compaction_policy = frozen.policy, frozen.compaction
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
                        sessions.c.status.label("session_status"),
                        runs.c.request,
                        runs.c.target,
                        runs.c.request_key.label("run_request_key"),
                        runs.c.status.label("run_status"),
                        runs.c.stop_requested,
                    )
                    .join(sessions, sessions.c.id == turns.c.session_id)
                    .join(runs, runs.c.id == turns.c.run_id)
                    .where(turns.c.id == frozen.turn_id)
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
            or canonical["target_key"] != frozen.target_key
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
            or not isinstance(run["request"], dict)
            or run["request"].get("text") != frozen.current.content
            or not isinstance(run["target"], dict)
            or not isinstance(run["target"].get("context"), dict)
            or run["target"]["context"].get("schema_version") != 4
            or run["target"]["context"].get("source_fingerprint") != frozen.source_fingerprint
            or run["target"]["context"].get("frozen_fingerprint")
            != fingerprint(frozen.model_dump(mode="json"))
            or fingerprint(run["target"].get("tools")) != frozen.tool_fingerprint
        ):
            raise ServiceError("facts_corrupt")
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
        return canonical

    async def verify(self, conn: AsyncConnection, run_id: str) -> None:
        canonical = await self._verify_frozen(conn, run_id)
        frozen = self.frozen
        if frozen.authorization_generation is not None:
            from krim_agent.storage.project_access import SQLiteAccessRepository

            actor = await SQLiteAccessRepository._actor(conn, frozen.owner_id)
            if actor is None or actor.generation != frozen.authorization_generation:
                raise ServiceError("permission_denied", 403)
        else:
            # A JSON null never grants access: this capability comes only from
            # the host's existing static authentication configuration.
            actor = self.legacy_actor
            if actor is None or actor.principal_id != frozen.owner_id:
                raise ServiceError("permission_denied", 403)
        actor.owns(frozen.workspace_id, frozen.owner_id, write=True)
        if (
            canonical["session_status"] != "open"
            or canonical["run_status"] != "running"
            or canonical["stop_requested"]
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
        state = OrdinaryContextState(
            frozen=self.frozen,
            prepared=prepared,
            prepared_hash=fingerprint(prepared.model_dump(mode="json")),
        )
        raw = state.model_dump(mode="json")
        state_from_raw(raw, fingerprint(self.frozen.model_dump(mode="json")))
        await conn.execute(
            update(turns)
            .where(turns.c.id == self.frozen.turn_id, turns.c.run_id == run_id)
            .values(context_snapshot=raw)
        )
        await self.verify(conn, run_id)

    async def read_prepared(self, conn: AsyncConnection, run_id: str) -> PreparedContext | None:
        await self.verify(conn, run_id)
        raw = await conn.scalar(
            select(turns.c.context_snapshot).where(turns.c.id == self.frozen.turn_id)
        )
        state = state_from_raw(raw, fingerprint(self.frozen.model_dump(mode="json")))
        await self.verify(conn, run_id)
        return state.prepared
