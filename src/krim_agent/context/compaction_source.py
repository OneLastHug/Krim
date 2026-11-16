"""Frozen-history ports for Compactor; sources own authorization and provenance.

Methods use the caller's connection/transaction. No source opens a model request
or owns the run lifecycle. A development source is supplied by the host; it must
verify its versioned scope anchor rather than fabricate a workspace frame.
"""

from __future__ import annotations

import json
from typing import Any, Protocol, cast

from pydantic import JsonValue
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.context.types import CompactionPolicy, ContextPolicy, PreparedContext
from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.source_fingerprints import source_digest
from krim_agent.storage.models import work_turns
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
    async def page(self, conn: AsyncConnection, run_id: str, offset: int,
                   limit: int) -> list[dict[str, Any]]: ...
    async def save_prepared(self, conn: AsyncConnection, run_id: str,
                            prepared: PreparedContext) -> None: ...
    async def read_prepared(self, conn: AsyncConnection,
                            run_id: str) -> PreparedContext | None: ...


class WorkspaceCompactionSource:
    """The original workspace source, preserving its reference/hash conventions."""

    domain = "workspace-history/2"

    def __init__(self, frame: WorkFrame, access: ExecutionAccess, *, source_anchor: str):
        self.frame = WorkFrame.model_validate_json(frame.model_dump_json())
        self.access = access
        self.session_id = self.frame.session_id
        self.source_refs = tuple(json.loads(encoded(list(self.frame.source_refs))))
        if (not isinstance(source_anchor, str)
                or fingerprint(list(self.source_refs)) != source_anchor):
            raise ServiceError("facts_corrupt")
        self.source_fingerprint = source_anchor
        context = cast(dict[str, JsonValue], self.frame.conditions["context"])
        self.context_policy = ContextPolicy.model_validate_json(encoded(context["policy"]))
        self.compaction_policy = CompactionPolicy.model_validate_json(
            encoded(context["compaction"]))
        self.extensions = cast(dict[str, JsonValue], json.loads(encoded(context.get(
            "extensions", {"version": "extensions/2", "revision": 0, "items": []},
        ))))
        self.policy_binding: dict[str, JsonValue] = {
            "grant": self.frame.grant.model_dump(mode="json"),
        }

    async def verify(self, conn: AsyncConnection, run_id: str) -> None:
        await workspace_history(conn, run_id, 0, 1, self.access,
                                source_anchor=self.source_fingerprint)

    async def page(self, conn: AsyncConnection, run_id: str, offset: int,
                   limit: int) -> list[dict[str, Any]]:
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 16:
            raise ServiceError("facts_corrupt")
        return await workspace_history(conn, run_id, offset, limit, self.access,
                                       source_anchor=self.source_fingerprint)

    async def save_prepared(self, conn: AsyncConnection, run_id: str,
                            prepared: PreparedContext) -> None:
        await conn.execute(update(work_turns).where(work_turns.c.run_id == run_id).values(
            prepared_context=prepared.model_dump(mode="json"),
        ))

    async def read_prepared(self, conn: AsyncConnection, run_id: str) -> PreparedContext | None:
        raw = await conn.scalar(select(work_turns.c.prepared_context).where(
            work_turns.c.run_id == run_id,
        ))
        return None if raw is None else PreparedContext.model_validate_json(encoded(raw))


def binding_token() -> tuple[object, ...]:
    return (
        id(WorkspaceCompactionSource), id(WorkspaceCompactionSource.__init__),
        id(WorkspaceCompactionSource.verify), id(WorkspaceCompactionSource.page),
        id(WorkspaceCompactionSource.save_prepared), id(WorkspaceCompactionSource.read_prepared),
        id(workspace_history), source_digest(__file__),
    )
