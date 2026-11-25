"""Frozen-history ports for Compactor; sources own authorization and provenance.

Methods use the caller's connection/transaction. No source opens a model request
or owns the run lifecycle. A development source is supplied by the host; it must
verify its versioned scope anchor rather than fabricate a workspace frame.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Protocol, cast

from pydantic import JsonValue
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.context import builder
from krim_agent.context.serialization import encoded, fingerprint
from krim_agent.context.types import CompactionPolicy, ContextPolicy, PreparedContext
from krim_agent.contracts import Message, RunEvent, ServiceError
from krim_agent.source_fingerprints import source_digest
from krim_agent.storage.models import run_events, work_turns
from krim_agent.storage.sessions import workspace_history
from krim_agent.workspace.types import ExecutionAccess, WorkFrame


class CompactionSource(Protocol):
    """Immutable source descriptors and same-connection storage operations.

    ``source_refs`` retain the ledger's turn_id/run_id/terminal_seq/
    transcript_fingerprint dictionary shape. ``policy_binding`` must include the
    source's authorization/scope and projection identity so summaries cannot be
    reused across owners/projects/source domains. ``verify`` checks the complete
    frozen anchor and current authorization even for an empty history.
    """

    @property
    def domain(self) -> str: ...
    @property
    def session_id(self) -> str: ...
    @property
    def source_refs(self) -> tuple[dict[str, JsonValue], ...]: ...
    @property
    def source_fingerprint(self) -> str: ...
    @property
    def policy_binding(self) -> dict[str, JsonValue]: ...
    @property
    def context_policy(self) -> ContextPolicy: ...
    @property
    def compaction_policy(self) -> CompactionPolicy: ...
    @property
    def extensions(self) -> dict[str, JsonValue]: ...

    async def verify(self, conn: AsyncConnection, run_id: str) -> None: ...
    async def page(
        self, conn: AsyncConnection, run_id: str, offset: int, limit: int
    ) -> list[dict[str, Any]]: ...
    async def save_prepared(
        self, conn: AsyncConnection, run_id: str, prepared: PreparedContext
    ) -> None: ...
    async def read_prepared(self, conn: AsyncConnection, run_id: str) -> PreparedContext | None: ...


class WorkspaceCompactionSource:
    """The original workspace source, preserving its reference/hash conventions."""

    domain = "workspace-history/2"

    def __init__(self, frame: WorkFrame, access: ExecutionAccess, *, source_anchor: str):
        self.frame = WorkFrame.model_validate_json(frame.model_dump_json())
        self.access = access
        self.session_id = self.frame.session_id
        self.source_refs = tuple(json.loads(encoded(list(self.frame.source_refs))))
        if (
            not isinstance(source_anchor, str)
            or fingerprint(list(self.source_refs)) != source_anchor
        ):
            raise ServiceError("facts_corrupt")
        self.source_fingerprint = source_anchor
        context = cast(dict[str, JsonValue], self.frame.conditions["context"])
        self.context_policy = ContextPolicy.model_validate_json(encoded(context["policy"]))
        self.compaction_policy = CompactionPolicy.model_validate_json(
            encoded(context["compaction"])
        )
        self.extensions = cast(
            dict[str, JsonValue],
            json.loads(
                encoded(
                    context.get(
                        "extensions",
                        {"version": "extensions/2", "revision": 0, "items": []},
                    )
                )
            ),
        )
        self.policy_binding: dict[str, JsonValue] = {
            "grant": self.frame.grant.model_dump(mode="json"),
        }

    async def verify(self, conn: AsyncConnection, run_id: str) -> None:
        await workspace_history(
            conn, run_id, 0, 1, self.access, source_anchor=self.source_fingerprint
        )

    async def page(
        self, conn: AsyncConnection, run_id: str, offset: int, limit: int
    ) -> list[dict[str, Any]]:
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 16:
            raise ServiceError("facts_corrupt")
        return await workspace_history(
            conn, run_id, offset, limit, self.access, source_anchor=self.source_fingerprint
        )

    async def save_prepared(
        self, conn: AsyncConnection, run_id: str, prepared: PreparedContext
    ) -> None:
        await conn.execute(
            update(work_turns)
            .where(work_turns.c.run_id == run_id)
            .values(
                prepared_context=prepared.model_dump(mode="json"),
            )
        )

    async def read_prepared(self, conn: AsyncConnection, run_id: str) -> PreparedContext | None:
        raw = await conn.scalar(
            select(work_turns.c.prepared_context).where(
                work_turns.c.run_id == run_id,
            )
        )
        return None if raw is None else PreparedContext.model_validate_json(encoded(raw))


_EVENT_KINDS = ("model_end", "text", "tool_start", "tool_end", "tool_intent")


async def _stored_events(
    conn: AsyncConnection, run_id: str, after_seq: int
) -> list[RunEvent]:
    rows = (
        (
            await conn.execute(
                select(
                    run_events.c.seq,
                    run_events.c.event_id,
                    run_events.c.kind,
                    run_events.c.data,
                )
                .where(
                    run_events.c.run_id == run_id,
                    run_events.c.seq > after_seq,
                    run_events.c.kind.in_(_EVENT_KINDS),
                )
                .order_by(run_events.c.seq)
            )
        )
        .all()
    )
    return [
        RunEvent(run_id=run_id, seq=seq, event_id=event_id, kind=kind, data=data)
        for seq, event_id, kind, data in rows
    ]


def _closed_groups(events: tuple[RunEvent, ...], run_id: str, turn_id: str) -> list[dict[str, Any]]:
    """The run's closed assistant->tool_end groups in canonical projection.

    Validation and projection reuse the frozen-history builder verbatim, so a
    compacted segment is byte-identical to what the persisted events say. The
    prefix stops before the first group with any missing result: unknown
    outcomes stay in the live conversation and are never summarized. Each ref
    carries the real owner turn, this run, the last event seq of its group and
    the canonical projection fingerprint; several segments of one turn are
    expected and never invent a turn id.
    """
    index = builder._event_index(events, run_id)
    steps = 0
    width = 0
    model_end_seq: dict[int, int] = {}
    for event in events:
        if event.kind == "model_end":
            step = event.data.get("step_index")
            if type(step) is int:
                model_end_seq[step] = event.seq
    for _step, _text, calls in index.steps:
        if any(index.ends.get(call["id"]) is None for call in calls):
            break
        steps += 1
        width += 1 + len(calls)
    if not steps:
        return []
    projection = builder.transcript(cast(Any, SimpleNamespace(
        request=SimpleNamespace(text=""),
        run=SimpleNamespace(id=run_id, status="running", code=None),
        events=events,
    )))[1 : 1 + width]
    groups: list[dict[str, Any]] = []
    cursor = 0
    for step, _text, calls in index.steps[:steps]:
        messages = projection[cursor : cursor + 1 + len(calls)]
        ends = [index.ends[call["id"]] for call in calls]
        for offset, end in enumerate(ends, start=1):
            if end.data.get("status") not in {"ok", "denied"}:
                # The SDK keeps failed observations structured, including input
                # diagnostics. Historical transcripts expose only the value;
                # current-run compaction must preserve the actual failure view.
                messages[offset] = Message(
                    role="tool",
                    call_id=messages[offset].call_id,
                    content=encoded({
                        key: value for key, value in end.data.items() if key != "arguments"
                    }),
                )
        terminal = max((end.seq for end in ends), default=model_end_seq.get(step, 0))
        cursor += len(messages)
        groups.append({
            "ref": {
                "turn_id": turn_id,
                "run_id": run_id,
                "terminal_seq": terminal,
                "transcript_fingerprint": fingerprint(
                    [message.model_dump(mode="json") for message in messages]
                ),
            },
            "messages": messages,
        })
    return groups


class RunGroupSource:
    """The live run's completed groups as additional compactable ledger rows.

    Rows are derived from the run's own persisted events, never from model
    memory. Liveness and authorization stay with the owner source's verify.
    """

    def __init__(
        self,
        turn_id: str,
        owner_verify: Any,
        *,
        events: Any = _stored_events,
    ) -> None:
        if not isinstance(turn_id, str) or not turn_id or not callable(owner_verify):
            raise ServiceError("facts_corrupt")
        self._turn_id = turn_id
        self._owner_verify = owner_verify
        self._events = events
        # One derivation per event-stream boundary: only events past the cached
        # seq are read again, so compaction never rescans a long run's events
        # for every model request. Row dicts are fresh copies per call because
        # the caller overlays its conversation view onto them.
        self._cache: dict[str, tuple[int, list[RunEvent], list[dict[str, Any]]]] = {}

    async def verify(self, conn: AsyncConnection, run_id: str) -> None:
        await self._owner_verify(conn, run_id)

    async def rows(self, conn: AsyncConnection, run_id: str) -> list[dict[str, Any]]:
        await self._owner_verify(conn, run_id)
        boundary, events, groups = self._cache.get(run_id, (-1, [], []))
        fresh = await self._events(conn, run_id, boundary)
        if fresh:
            events = [*events, *fresh]
            groups = _closed_groups(tuple(events), run_id, self._turn_id)
            boundary = events[-1].seq
            self._cache[run_id] = (boundary, events, groups)
        await self._owner_verify(conn, run_id)
        return [
            {"ref": dict(group["ref"]), "messages": list(group["messages"])}
            for group in groups
        ]


def binding_token() -> tuple[object, ...]:
    return (
        id(WorkspaceCompactionSource),
        id(WorkspaceCompactionSource.__init__),
        id(WorkspaceCompactionSource.verify),
        id(WorkspaceCompactionSource.page),
        id(WorkspaceCompactionSource.save_prepared),
        id(WorkspaceCompactionSource.read_prepared),
        id(RunGroupSource),
        id(RunGroupSource.__init__),
        id(RunGroupSource.verify),
        id(RunGroupSource.rows),
        id(_closed_groups),
        id(workspace_history),
        source_digest(__file__),
    )
