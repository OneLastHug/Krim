"""SDK request preparation using the host's existing frozen-history Compactor.

Only the current run conversation belongs in the SDK history when this adapter
is enabled. Compactor owns the system prompt, frozen prior turns, extensions,
summary calls and durable summary rows. Active tool exchanges are never silently
dropped or assigned invented source references.
"""

from __future__ import annotations

import hashlib
from typing import Any

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.context.compaction import PROMPT, Compactor
from krim_agent.contracts import Message, ServiceError, fingerprint
from krim_agent.storage.models import compactions, runs


class CompactorHistory:
    """One run's compaction state; lifecycle and budgets stay with the host.

    The enclosing SDK checkpoint must be integrity-checked by RunService before
    restore. Restoring this object does not replenish execution time, model steps,
    tool counts or output bytes. Summary generation uses Compactor.step, which
    must be the very same ModelStep used by HostModelProvider.
    """

    def __init__(self, compactor: Compactor, run_id: str) -> None:
        if not run_id:
            raise ValueError("missing compactor run id")
        self.compactor = compactor
        self.run_id = run_id
        self._restored = False
        self._binding = fingerprint({
            "run_id": run_id,
            "source_anchor": compactor.source_anchor,
            "policy": compactor.record_policy,
            "provider": compactor.provider_identity,
        })

    def checkpoint(self) -> dict[str, Any]:
        c = self.compactor
        return {
            "binding": self._binding, "passes": c.passes,
            "covered": c.covered, "summary": c.summary,
        }

    def restore(self, state: dict[str, Any]) -> None:
        c = self.compactor
        passes, covered, summary = (state.get(k) for k in ("passes", "covered", "summary"))
        if (state.get("binding") != self._binding
                or type(passes) is not int or not 0 <= passes <= c.compaction.max_passes
                or type(covered) is not int
                or not 0 <= covered <= max(0, len(c.anchor_refs) - c.compaction.keep_turns)
                or not isinstance(summary, str)
                or len(summary.encode()) > c.compaction.summary_bytes
                or bool(covered) != bool(summary.strip())
                or passes > 0 and not covered):
            raise ServiceError("facts_corrupt")
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
            rows = (await conn.execute(
                select(compactions).join(runs, compactions.c.run_id == runs.c.id)
                .where(
                    compactions.c.session_id == c.session_id,
                    compactions.c.run_id != self.run_id,
                    compactions.c.status == "complete",
                    (runs.c.status == "completed")
                    | ((runs.c.status == "failed") & (runs.c.code == "completion_unverified")),
                    compactions.c.source_hash == fingerprint(list(c.anchor_refs[:c.covered])),
                    compactions.c.prompt_hash == fingerprint(PROMPT),
                    compactions.c.output == c.summary,
                    compactions.c.output_hash == hashlib.sha256(c.summary.encode()).hexdigest(),
                ).order_by(compactions.c.created_at.desc()).limit(32)
            )).mappings().all()
            await c.verify(conn)
            return any(
                row["source_refs"] == list(c.anchor_refs[:c.covered])
                and row["policy"] == c.record_policy
                and row["provider_identity"] == c.provider_identity
                for row in rows
            )

        if not await c.store.work_read(read):
            raise ServiceError("facts_corrupt")
        c.guard()

    async def _restore_history(self) -> None:
        c = self.compactor
        c.run_id = self.run_id
        c.guard()
        await c.store.work_read(c.verify)
        if not c.passes and c.covered:
            await self._verify_reused_summary()
        previous_covered = 0
        previous_step = -1
        for index in range(c.passes):
            row = await c.row(index)
            if (row is None or row["status"] != "complete"
                    or type(row["model_step"]) is not int
                    or not previous_step < row["model_step"] < c.step.index
                    or not isinstance(row["output"], str)
                    or not row["output"].strip()
                    or len(row["output"].encode()) > c.compaction.summary_bytes
                    or hashlib.sha256(row["output"].encode()).hexdigest() != row["output_hash"]
                    or len(row["source_refs"]) <= previous_covered):
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

    async def prepare(self, conversation: list[Message], tools: list[dict[str, JsonValue]],
                      *, max_group: int, remaining_tools: int) -> list[Message]:
        if any(message.role == "system" for message in conversation):
            raise ServiceError("facts_corrupt")
        if self._restored:
            await self._restore_history()
        return await self.compactor.prepare(
            self.run_id, conversation, tools,
            max_group=max_group, remaining_tools=remaining_tools,
        )
