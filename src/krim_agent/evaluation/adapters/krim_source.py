"""Krim冷读边界；只借用已授权Store/SessionService，不恢复或执行源。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, cast

from krim_agent.contracts import ServiceError
from krim_agent.observability import project_trace
from krim_agent.runtime.facts import project_facts
from krim_agent.security import PrincipalContext
from krim_agent.sessions.service import SessionService
from krim_agent.storage.sqlite import SQLiteStore

from ..domain import Observation, clone, neutral_fingerprint
from ..trace_cases import observe_snapshot


class KrimSourceReader:
    def __init__(
        self,
        store: SQLiteStore,
        sessions: SessionService,
        principal: PrincipalContext,
        *,
        workspace_id: str = "local",
        scope: str = "local",
        allow_legacy: bool = False,
    ) -> None:
        self.store, self.sessions, self.principal = store, sessions, principal
        self.workspace_id, self.scope, self.allow_legacy = workspace_id, scope, allow_legacy

    def binding(self) -> tuple[object, ...]:
        return (
            self.store,
            self.sessions,
            self.principal,
            self.workspace_id,
            self.scope,
            self.allow_legacy,
            getattr(self.store.snapshot, "__func__", self.store.snapshot),
            getattr(self.sessions.get, "__func__", self.sessions.get),
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        )

    async def read(self, scope: str, selection: dict[str, Any]) -> dict[str, Any]:
        from sqlalchemy import select

        from krim_agent.storage import sessions as session_sql
        from krim_agent.storage.models import runs, turns

        if scope != self.scope or selection.get("run_id") is None:
            raise ServiceError("not_found", 404)
        self.principal.member(self.workspace_id)
        frozen = self.binding()

        def guard() -> None:
            if self.binding() != frozen or self.store.closed:
                raise ServiceError("target_binding_changed")

        async def association(conn: Any) -> tuple[Any, ...] | None:
            exists = (
                await conn.execute(select(runs.c.id).where(runs.c.id == selection["run_id"]))
            ).scalar_one_or_none()
            guard()
            if exists is None:
                raise ServiceError("not_found", 404)
            if self.store.schema_version not in {3, 4, 5, 6, 7}:
                return None
            row = (
                await conn.execute(
                    select(
                        turns.c.id, turns.c.session_id, turns.c.workspace_id, turns.c.owner_id
                    ).where(turns.c.run_id == selection["run_id"])
                )
            ).first()
            guard()
            return None if row is None else tuple(row)

        linked = await session_sql.read(self.store, association)
        guard()
        if linked is None:
            if not self.allow_legacy or not self.principal.legacy:
                raise ServiceError("not_found", 404)
        else:
            turn_id, session_id, workspace_id, owner_id = linked
            if workspace_id != self.workspace_id:
                raise ServiceError("not_found", 404)
            session = await self.sessions.get(self.principal, workspace_id, session_id)
            guard()
            if (
                session.owner_id != owner_id
                or await session_sql.read(self.store, association) != linked
            ):
                raise ServiceError("not_found", 404)
            guard()
        snapshot = await self.store.snapshot(selection["run_id"])
        guard()
        context = snapshot.session_context
        current = await session_sql.read(self.store, association)
        guard()
        if current != linked or (context is None) != (linked is None):
            raise ServiceError("not_found", 404)
        if (
            context is not None
            and linked is not None
            and (context.turn_id, context.session_id, context.workspace_id) != linked[:3]
        ):
            raise ServiceError("not_found", 404)
        if context is None:
            if not self.allow_legacy or not self.principal.legacy:
                raise ServiceError("not_found", 404)
        else:
            if context.workspace_id != self.workspace_id:
                raise ServiceError("not_found", 404)
            await self.sessions.get(self.principal, context.workspace_id, context.session_id)
            guard()
        observed = observe_snapshot(snapshot)
        facts, trace = project_facts(snapshot), project_trace(snapshot)
        output = Observation(
            execution_ref=snapshot.run.id,
            exit_code=None,
            stdout=None,
            stderr=None,
            artifacts={},
            process=None,
            cleanup="unknown",
            token_usage=facts.token_usage,
            usage_complete=facts.usage_complete,
            target_result={
                "run_id": snapshot.run.id,
                "status": facts.run.status,
                "code": facts.run.code,
                "text": observed.text,
                "model_steps": observed.model_steps,
                "tools": None
                if observed.tools is None
                else [t.model_dump(mode="json") for t in observed.tools],
            },
        ).model_dump(mode="json")
        if selection.get("evidence_hash") not in {None, neutral_fingerprint(output)}:
            raise ServiceError("facts_corrupt")
        terminal = next((e for e in reversed(snapshot.events) if e.kind == "terminal"), None)
        limits = snapshot.target.get("limits")
        if not isinstance(limits, dict):
            raise ServiceError("facts_corrupt")
        descriptor = {
            "key": "krim-script-v2",
            "version": "neutral-krim/1",
            "kind": "krim-run",
            "execution_conditions": {
                "schema_version": "krim-run/1",
                "run_target_key": snapshot.request.target,
                "binding_descriptor": snapshot.target,
                "binding_fingerprint": neutral_fingerprint(snapshot.target),
                "public_runtime_conditions": {"run_timeout": limits["run_timeout"]},
                "budget": {"item": 20, "task": 120},
            },
        }
        guard()
        return cast(
            dict[str, Any],
            clone(
                {
                    "source": {
                        "kind": "run",
                        "workspace_id": self.workspace_id,
                        "session_id": None if context is None else context.session_id,
                        "run_id": snapshot.run.id,
                        "trace_id": snapshot.run.trace_id,
                        "last_seq": observed.last_seq,
                        "terminal_event_id": None if terminal is None else terminal.event_id,
                        "snapshot_hash": observed.snapshot_fingerprint,
                        "evidence_hash": neutral_fingerprint(output),
                        "facts_complete": observed.facts_complete,
                        "trace_complete": trace.complete,
                    },
                    "target": descriptor,
                    "environment": {
                        "initial_state": "unknown",
                        "evidence": "krim-run-facts/1",
                        "execution_conditions": descriptor["execution_conditions"],
                    },
                    "output": output,
                    "terminal": terminal is not None
                    and snapshot.run.status not in {"queued", "running"},
                    "case": None,
                }
            ),
        )
