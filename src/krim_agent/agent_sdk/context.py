"""SDK request preparation using the host's existing frozen-history Compactor.

Only the current run conversation belongs in the SDK history when this adapter
is enabled. Compactor owns the system prompt, frozen prior turns, extensions,
summary calls and durable summary rows. Active tool exchanges are never silently
dropped or assigned invented source references.
"""

from __future__ import annotations

import hashlib
from typing import Any, Protocol

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.context import builder
from krim_agent.context.compaction import PROMPT, Compactor
from krim_agent.context.manifest import ItemMetadata
from krim_agent.context.serialization import fingerprint
from krim_agent.context.service import ContextSnapshotItem
from krim_agent.contracts import Message, ServiceError, parse_json
from krim_agent.storage.models import compactions, runs


class ContextBoundary(Protocol):
    async def pending(self) -> str | None: ...
    async def complete(self, token: str, receipt: dict[str, JsonValue] | None = None) -> None: ...
    async def prepare(self) -> list[ContextSnapshotItem]: ...


class RunGroupHistory(Protocol):
    """Port to the current run's persisted closed groups (see RunGroupSource)."""

    async def verify(self, conn: AsyncConnection, run_id: str) -> None: ...
    async def rows(self, conn: AsyncConnection, run_id: str) -> list[dict[str, Any]]: ...


def _run_ref_valid(ref: Any, run_id: str) -> bool:
    return (
        isinstance(ref, dict)
        and set(ref) == {"turn_id", "run_id", "terminal_seq", "transcript_fingerprint"}
        and isinstance(ref["turn_id"], str)
        and bool(ref["turn_id"])
        and ref["run_id"] == run_id
        and type(ref["terminal_seq"]) is int
        and ref["terminal_seq"] >= 0
        and isinstance(ref["transcript_fingerprint"], str)
        and len(ref["transcript_fingerprint"]) == 64
    )


_TOOL_BIND_FIELDS = (
    "status", "code", "call_id", "provider_call_id", "name",
    "executor_started", "result_available", "decision",
)


def _tool_value_consistent(actual: str, projected: str) -> bool:
    """The conversation's tool result must agree with the ledger projection.

    Equal encodings always pass. Unequal plain values and ok payloads can only
    pass by exact equality; unequal dicts are accepted solely as a structured
    non-ok observation whose SDK view is a legitimate reduction: both sides
    declare the same non-ok status and code, every field present on both sides
    agrees (a summary must never lose or rewrite a code, decision, executor
    fact or value diagnostic), and one-sided fields are ignored.
    """
    if actual == projected:
        return True
    try:
        left, right = parse_json(actual), parse_json(projected)
    except ValueError:
        return False
    if not isinstance(left, dict) or not isinstance(right, dict):
        return left == right
    status = left.get("status")
    if (
        status is None
        or status == "ok"
        or status != right.get("status")
        or "code" not in left
        or "code" not in right
        or left["code"] != right["code"]
    ):
        return False
    for key in _TOOL_BIND_FIELDS:
        if key in left and key in right and left[key] != right[key]:
            return False
    if "value" in left and "value" in right and left["value"] != right["value"]:
        return False
    return True


class CompactorHistory:
    """One run's compaction state; lifecycle and budgets stay with the host.

    The enclosing SDK checkpoint must be integrity-checked by RunService before
    restore. Restoring this object does not replenish execution time, model steps,
    tool counts or output bytes. Summary generation uses Compactor.step, which
    must be the very same ModelStep used by HostModelProvider.
    """

    def __init__(
        self,
        compactor: Compactor,
        run_id: str,
        *,
        boundary: ContextBoundary | None = None,
        live_memory: bool = False,
        run_source: RunGroupHistory | None = None,
    ) -> None:
        if not run_id:
            raise ValueError("missing compactor run id")
        self.compactor = compactor
        self.run_id = run_id
        # Current-run source verification precedes Compactor.prepare(), which
        # formerly supplied this identity. Bind it before that first read.
        self.compactor.run_id = run_id
        if type(live_memory) is not bool or live_memory and boundary is None:
            raise ValueError("live memory requires a current snapshot boundary")
        self.boundary = boundary
        self.live_memory = live_memory
        self.run_source = run_source
        self.snapshot_refs: list[dict[str, JsonValue]] = []
        self.current_refs: list[dict[str, JsonValue]] = []
        self.current_count = 0
        self._restored = False
        self._binding = fingerprint(
            {
                "run_id": run_id,
                "source_anchor": compactor.source_anchor,
                "policy": compactor.record_policy,
                "provider": compactor.provider_identity,
            }
        )

    def _current_refs(self) -> list[dict[str, JsonValue]]:
        c = self.compactor
        run = c.run_refs or ()
        extra = max(0, c.covered - len(c.anchor_refs))
        if extra > len(run):
            raise ServiceError("facts_corrupt")
        return [dict(ref) for ref in run[:extra]]

    def checkpoint(self) -> dict[str, Any]:
        c = self.compactor
        state: dict[str, Any] = {
            "binding": self._binding,
            "passes": c.passes,
            "covered": c.covered,
            "summary": c.summary,
        }
        if self.boundary is not None:
            state["snapshot_refs"] = list(self.snapshot_refs)
        if self.run_source is not None:
            # The covered current-run segments are durable facts; restore must
            # re-derive them from the run's events before trusting this state.
            # current_count is the observed total at checkpoint time and keeps
            # the combined keep bound verifiable before any derivation.
            state["current_refs"] = self._current_refs()
            state["current_count"] = len(c.run_rows or ())
        return state

    def restore(self, state: dict[str, Any]) -> None:
        c = self.compactor
        passes, covered, summary = (state.get(k) for k in ("passes", "covered", "summary"))
        refs = state.get("current_refs", [])
        count = state.get("current_count")
        if not isinstance(refs, list) or any(
            not _run_ref_valid(ref, self.run_id) for ref in refs
        ):
            raise ServiceError("facts_corrupt")
        if self.run_source is None:
            # Legacy checkpoints stay on the strict anchor-only bound.
            if refs or "current_count" in state:
                raise ServiceError("facts_corrupt")
            anchor_bound = 0 <= covered <= max(
                0, len(c.anchor_refs) - c.compaction.keep_turns
            ) if type(covered) is int else False
        else:
            if type(count) is not int or count < 0 or len(refs) > count:
                raise ServiceError("facts_corrupt")
            anchor_part = covered - len(refs) if type(covered) is int else -1
            anchor_bound = (
                type(covered) is int
                # Current-run segments extend a complete anchor prefix.
                and 0 <= anchor_part <= len(c.anchor_refs)
                and (not refs or anchor_part == len(c.anchor_refs))
                # Enough current rows existed at checkpoint time for this
                # coverage to satisfy the combined keep bound.
                and covered <= max(0, len(c.anchor_refs) + count - c.compaction.keep_turns)
            )
        if (
            state.get("binding") != self._binding
            or type(passes) is not int
            or passes < 0
            or (c.compaction.max_passes is not None and passes > c.compaction.max_passes)
            or not anchor_bound
            or not isinstance(summary, str)
            or (
                c.compaction.summary_bytes is not None
                and len(summary.encode()) > c.compaction.summary_bytes
            )
            or bool(covered) != bool(summary.strip())
            or passes > 0
            and not covered
        ):
            raise ServiceError("facts_corrupt")
        snapshot_refs = state.get("snapshot_refs", [])
        if not isinstance(snapshot_refs, list) or any(
            not isinstance(ref, dict)
            or set(ref) != {"name", "revision", "fingerprint"}
            or not isinstance(ref["name"], str)
            or type(ref["revision"]) not in (str, int)
            or not isinstance(ref["fingerprint"], str)
            or len(ref["fingerprint"]) != 64
            for ref in snapshot_refs
        ):
            raise ServiceError("facts_corrupt")
        self.snapshot_refs = list(snapshot_refs)
        self.current_refs = [dict(ref) for ref in refs]
        self.current_count = count if self.run_source is not None else 0
        c.passes, c.covered, c.summary = passes, covered, summary
        # A newly built Compactor would otherwise restart pass 0 after the model
        # counters have advanced, conflicting with (or recharging) its durable row.
        c.history = None
        self._restored = bool(passes or covered)

    async def _verify_reused_summary(self) -> None:
        c = self.compactor

        async def read(conn: AsyncConnection) -> bool:
            await c.verify(conn)
            # No new model request: match the exact previously selected summary
            # and its frozen source/policy provenance in the existing ledger.
            expected = c._covered_refs(c.covered)
            rows = (
                (
                    await conn.execute(
                        select(compactions)
                        .join(runs, compactions.c.run_id == runs.c.id)
                        .where(
                            compactions.c.session_id == c.session_id,
                            compactions.c.run_id != self.run_id,
                            compactions.c.status == "complete",
                            (runs.c.status == "completed")
                            | (
                                (runs.c.status == "failed")
                                & (runs.c.code == "completion_unverified")
                            ),
                            compactions.c.source_hash == fingerprint(expected),
                            compactions.c.prompt_hash == fingerprint(PROMPT),
                            compactions.c.output == c.summary,
                            compactions.c.output_hash
                            == hashlib.sha256(c.summary.encode()).hexdigest(),
                        )
                        .order_by(compactions.c.created_at.desc())
                    )
                )
                .mappings()
                .all()
            )
            await c.verify(conn)
            return any(
                row["source_refs"] == expected
                and row["policy"] == c.record_policy
                and row["provider_identity"] == c.provider_identity
                for row in rows
            )

        if not await c.store.work_read(read):
            raise ServiceError("facts_corrupt")
        c.guard()

    async def _derive_run_rows(self) -> list[dict[str, Any]] | None:
        """Current-run rows from the store-backed port, or None without a port."""
        if self.run_source is None:
            return None
        c = self.compactor

        async def read(conn: AsyncConnection) -> list[dict[str, Any]]:
            await c.verify(conn)
            return await self.run_source.rows(conn, self.run_id)

        rows = await c.store.work_read(read)
        c.guard()
        return rows

    def _overlay(self, rows: list[dict[str, Any]], conversation: list[Message]) -> None:
        """Bind each persisted group to its real conversation position.

        Injected user envelopes (subagent results/messages between groups) are
        preserved untouched: each row records the conversation span of its own
        closed assistant/tool messages, and the seek only steps over user
        messages, never reorders or swallows them. Tool values keep the real
        model-view encoding: ok payloads stay unwrapped, denials and structured
        failures keep their status. The leading user goal never belongs to a
        segment.
        """
        if conversation[0].role != "user" or conversation[0].tool_calls:
            raise ServiceError("facts_corrupt")
        index = 1
        for row in rows:
            projection = row["messages"]
            while (
                index < len(conversation)
                and conversation[index].role == "user"
                and not conversation[index].tool_calls
            ):
                index += 1
            if index >= len(conversation):
                raise ServiceError("facts_corrupt")
            assistant = conversation[index]
            expected = projection[0]
            if (
                assistant.role != "assistant"
                or expected.role != "assistant"
                or assistant.content != expected.content
                or assistant.tool_calls != expected.tool_calls
            ):
                raise ServiceError("facts_corrupt")
            start = index
            index += 1
            for message in projection[1:]:
                actual = conversation[index] if index < len(conversation) else None
                if (
                    actual is None
                    or actual.role != "tool"
                    or actual.call_id != message.call_id
                    or not _tool_value_consistent(actual.content, message.content)
                ):
                    raise ServiceError("facts_corrupt")
                index += 1
            row["start"], row["end"] = start, index
            row["messages"] = list(conversation[start:index])

    async def _prepare_run_rows(
        self, conversation: list[Message]
    ) -> list[dict[str, Any]] | None:
        rows = await self._derive_run_rows()
        if rows is None:
            if self.compactor.run_refs:
                raise ServiceError("facts_corrupt")
            return None
        if rows:
            self._overlay(rows, conversation)
        return rows

    async def _restore_history(self) -> None:
        c = self.compactor
        c.run_id = self.run_id
        c.guard()
        await c.store.work_read(c.verify)
        # Derive and bind the current-run rows first: pass rows recorded under
        # their refs cannot be validated against the anchor prefix alone.
        derived = await self._derive_run_rows()
        if derived is not None:
            if [dict(row["ref"]) for row in derived][: len(self.current_refs)] != list(
                self.current_refs
            ):
                raise ServiceError("facts_corrupt")
            if (
                len(derived) < self.current_count
                or c.covered > max(0, len(c.anchor_refs) + len(derived) - c.compaction.keep_turns)
            ):
                raise ServiceError("facts_corrupt")
            c._set_run_history(derived)
        if not c.passes and c.covered:
            await self._verify_reused_summary()
        previous_covered = 0
        previous_step = -1
        for index in range(c.passes):
            row = await c.row(index)
            if (
                row is None
                or row["status"] != "complete"
                or type(row["model_step"]) is not int
                or not previous_step < row["model_step"] < c.step.index
                or not isinstance(row["output"], str)
                or not row["output"].strip()
                or not c.valid_summary(row["output"])
                or hashlib.sha256(row["output"].encode()).hexdigest() != row["output_hash"]
                or len(row["source_refs"]) <= previous_covered
            ):
                raise ServiceError("facts_corrupt")
            previous_covered = len(row["source_refs"])
            previous_step = row["model_step"]
            if index == c.passes - 1 and (
                previous_covered != c.covered or row["output"] != c.summary
            ):
                raise ServiceError("facts_corrupt")
        history = []
        for offset in range(0, len(c.anchor_refs), 16):

            async def page(conn: AsyncConnection, offset: int = offset) -> list[dict[str, Any]]:
                return await c.page(conn, offset, 16)

            history.extend(await c.store.work_read(page))
            c.guard()
        if len(history) != len(c.anchor_refs):
            raise ServiceError("facts_corrupt")
        await c.store.work_read(c.verify)
        c.guard()
        c.history = history
        self._restored = False

    async def prepare(
        self,
        conversation: list[Message],
        tools: list[dict[str, JsonValue]],
        *,
        max_group: int | None,
        remaining_tools: int | None,
    ) -> list[Message]:
        # Validate before touching the queue/snapshot port. A caller with a
        # pending tool group must wait for the real result, never compact it.
        builder.validate_groups(conversation)
        if self._restored:
            await self._restore_history()
        run_rows = await self._prepare_run_rows(conversation)
        token = None
        projections: list[Message] = []
        metadata: list[ItemMetadata | None] = []
        self.snapshot_refs = []
        if self.boundary is not None:
            combined = getattr(self.boundary, "prepare_context", None)
            if callable(combined):
                token, snapshot_items = await combined()
            else:
                # Existing host/fake ports retain the original two-method contract.
                token = await self.boundary.pending()
                snapshot_items = await self.boundary.prepare()
            for item in snapshot_items:
                digest = fingerprint(item.model_dump(mode="json"))
                self.snapshot_refs.append(
                    {"name": item.name, "revision": item.revision, "fingerprint": digest}
                )
                projections.append(
                    Message(
                        role="user",
                        content=f"[不可信上下文快照 {item.name} / {item.revision}] {item.content}",
                    )
                )
                metadata.append(
                    ItemMetadata(
                        category="memory" if item.name == "memory" else "unknown",
                        label=f"{item.name} snapshot / {item.revision}",
                        source_refs=({"fingerprint": digest}, *item.source_refs),
                    )
                )
        result = await self.compactor.prepare(
            self.run_id,
            conversation,
            tools,
            max_group=max_group,
            remaining_tools=remaining_tools,
            force=token is not None,
            projections=projections,
            projection_metadata=metadata,
            live_memory=self.live_memory,
            run_history=run_rows,
        )
        if token is not None and self.boundary is not None:
            await self.boundary.complete(
                token,
                {
                    "source_fingerprint": self.compactor.source_anchor,
                    "covered": self.compactor.covered,
                    "passes": self.compactor.passes,
                    "summary_hash": hashlib.sha256(self.compactor.summary.encode()).hexdigest(),
                },
            )
        return result
