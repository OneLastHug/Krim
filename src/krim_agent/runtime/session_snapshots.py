"""Compose live plan, private-memory and optional goal context projections.

The provider is injected into ContextService. It reads the current persisted
user turn for memory relevance, pins a plan revision before walking its pages,
and leaves the original extension/checkpoint records unchanged.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel, JsonValue
from sqlalchemy import select

from krim_agent.context.serialization import encoded, fingerprint
from krim_agent.context.service import ContextSnapshotItem
from krim_agent.contracts import ServiceError
from krim_agent.planning.contracts import MAX_PAGE_SIZE, plan_digest
from krim_agent.security import PrincipalContext
from krim_agent.storage.models import runs, sessions, turns
from krim_agent.storage.sandbox_models import sandbox_session_bindings
from krim_agent.tools.session import SessionToolScope

if TYPE_CHECKING:
    from krim_agent.planning.service import PlanningService
    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.tools.memory import MemoryToolService


class GoalSnapshotPort(Protocol):
    async def snapshot(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        check: Callable[[], None],
    ) -> BaseModel | Mapping[str, Any] | None: ...


class SessionSnapshotProvider:
    """Async callable(scope) -> immutable context snapshot items.

    Missing optional services produce no item. A disabled memory service does
    produce its disabled snapshot, so callers never fall back to frozen memory.
    """

    def __init__(
        self,
        store: SQLiteStore,
        *,
        planning: PlanningService | None = None,
        memory_tools: MemoryToolService | None = None,
        goals: GoalSnapshotPort | None = None,
        plan_page_size: int = 50,
        memory_limit: int = 8,
        memory_max_chars: int = 8192,
    ) -> None:
        if not 1 <= plan_page_size <= MAX_PAGE_SIZE or memory_limit < 1 or memory_max_chars < 1:
            raise ValueError("Invalid context projection page size")
        self.store, self.planning, self.memory_tools, self.goals = (
            store,
            planning,
            memory_tools,
            goals,
        )
        self.plan_page_size = plan_page_size
        self.memory_limit, self.memory_max_chars = memory_limit, memory_max_chars

    async def _query(self, scope: SessionToolScope) -> str:
        scope.check()
        scope.actor.member(scope.workspace_id)

        async def read(conn: Any) -> str:
            row = (
                (
                    await conn.execute(
                        select(
                            turns.c.text,
                            runs.c.status.label("run_status"),
                            runs.c.stop_requested,
                            sessions.c.status.label("session_status"),
                        )
                        .select_from(
                            turns.join(sessions, turns.c.session_id == sessions.c.id).join(
                                runs, turns.c.run_id == runs.c.id
                            )
                        )
                        .where(
                            turns.c.id == scope.turn_id,
                            turns.c.run_id == scope.run_id,
                            turns.c.owner_id == scope.actor.principal_id,
                            turns.c.workspace_id == scope.workspace_id,
                            sessions.c.id == scope.session_id,
                            sessions.c.workspace_id == scope.workspace_id,
                            sessions.c.owner_id == scope.actor.principal_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            if row["stop_requested"] or row["run_status"] != "running":
                raise ServiceError("cancelled")
            if row["session_status"] != "open":
                raise ServiceError("session_closed", 409)
            binding = (
                (
                    await conn.execute(
                        select(sandbox_session_bindings).where(
                            sandbox_session_bindings.c.session_id == scope.session_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if binding is not None and binding["principal_id"] != scope.actor.principal_id:
                raise ServiceError("facts_corrupt")
            if (binding["project_id"] if binding else None) != scope.project_id:
                raise ServiceError("context_binding_changed", 409)
            scope.check()
            return str(row["text"])

        result = await self.store.work_read(read)
        scope.check()
        return result

    async def _plan(self, scope: SessionToolScope) -> ContextSnapshotItem:
        assert self.planning is not None
        first = await self.planning.snapshot(
            scope.actor,
            scope.workspace_id,
            scope.session_id,
            limit=self.plan_page_size,
            check=scope.check,
        )
        steps = list(first.steps)
        cursor = first.next_offset
        while cursor is not None:
            scope.check()
            page = await self.planning.snapshot(
                scope.actor,
                scope.workspace_id,
                scope.session_id,
                revision=first.revision,
                offset=cursor,
                limit=self.plan_page_size,
                check=scope.check,
            )
            if (
                page.revision != first.revision
                or page.content_hash != first.content_hash
                or page.total != first.total
                or page.matched_total != first.matched_total
                or page.counts != first.counts
                or page.offset != cursor
                or page.next_offset is not None
                and page.next_offset <= cursor
            ):
                raise ServiceError("facts_corrupt")
            steps.extend(page.steps)
            cursor = page.next_offset
        values = [step.model_dump(mode="json") for step in steps]
        if len(values) != first.total or plan_digest(values) != first.content_hash:
            raise ServiceError("facts_corrupt")
        document = dict(
            version="planning-context/1",
            plan_version=first.version,
            session_id=first.session_id,
            revision=first.revision,
            content_hash=first.content_hash,
            updated_at=first.updated_at,
            counts=first.counts.model_dump(mode="json"),
            total=first.total,
            steps=values,
        )
        reference: dict[str, JsonValue] = dict(
            kind="plan",
            version=first.version,
            session_id=first.session_id,
            revision=first.revision,
            content_hash=first.content_hash,
            total=first.total,
        )
        return ContextSnapshotItem(
            name="plan",
            revision=first.revision,
            content=encoded(document),
            source_refs=(reference,),
        )

    async def _memory(self, scope: SessionToolScope, query: str) -> ContextSnapshotItem:
        assert self.memory_tools is not None
        snapshot = await self.memory_tools.recall(
            scope,
            query,
            limit=self.memory_limit,
            max_chars=self.memory_max_chars,
            offset=0,
        )
        raw = {key: value for key, value in snapshot.items() if key != "fingerprint"}
        if (
            snapshot.get("version") != "memory-recall/1"
            or snapshot.get("status") not in {"ok", "disabled"}
            or snapshot.get("owner_id") != scope.actor.principal_id
            or snapshot.get("workspace_id") != scope.workspace_id
            or snapshot.get("session_id") != scope.session_id
            or snapshot.get("project_id") != scope.project_id
            or snapshot.get("fingerprint") != fingerprint(raw)
        ):
            raise ServiceError("facts_corrupt")
        reference: dict[str, JsonValue] = dict(
            kind="memory",
            version=snapshot["version"],
            fingerprint=snapshot["fingerprint"],
            owner_id=scope.actor.principal_id,
            workspace_id=scope.workspace_id,
            session_id=scope.session_id,
            project_id=scope.project_id,
            preference_revision=snapshot["preference_revision"],
            session_extension_revision=snapshot["session_extension_revision"],
            query_hash=snapshot["query_hash"],
        )
        return ContextSnapshotItem(
            name="memory",
            revision=snapshot["fingerprint"],
            content=encoded(snapshot),
            source_refs=(reference,),
        )

    async def _goal(self, scope: SessionToolScope) -> ContextSnapshotItem | None:
        assert self.goals is not None
        result = await self.goals.snapshot(
            scope.actor, scope.workspace_id, scope.session_id, check=scope.check
        )
        if result is None:
            return None
        document = result.model_dump(mode="json") if isinstance(result, BaseModel) else dict(result)
        if (
            document.get("session_id", scope.session_id) != scope.session_id
            or document.get("workspace_id", scope.workspace_id) != scope.workspace_id
            or document.get("owner_id") not in {None, scope.actor.principal_id}
        ):
            raise ServiceError("facts_corrupt")
        digest = fingerprint(document)
        revision = document.get("revision", digest)
        reference: dict[str, JsonValue] = dict(
            kind="goal",
            session_id=scope.session_id,
            workspace_id=scope.workspace_id,
            revision=revision,
            fingerprint=digest,
        )
        return ContextSnapshotItem(
            name="goal", revision=revision, content=encoded(document), source_refs=(reference,)
        )

    async def __call__(self, scope: SessionToolScope) -> tuple[ContextSnapshotItem, ...]:
        query = await self._query(scope)
        items: list[ContextSnapshotItem] = []
        if self.planning is not None:
            items.append(await self._plan(scope))
        if self.memory_tools is not None:
            items.append(await self._memory(scope, query))
        if self.goals is not None:
            goal = await self._goal(scope)
            if goal is not None:
                items.append(goal)
        await self._query(scope)  # Recheck cancellation/binding after paginated reads.
        scope.check()
        return tuple(items)
