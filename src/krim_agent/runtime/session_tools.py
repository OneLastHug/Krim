"""Resolve generic tool authority from the active persisted turn, never arguments."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL, AccessPolicy
from krim_agent.storage.models import runs, sessions, turns
from krim_agent.storage.sandbox_models import sandbox_session_bindings
from krim_agent.tools.core import Invocation
from krim_agent.tools.pipeline import _CALL, _PRODUCER, CURRENT, _owns
from krim_agent.tools.session import SessionToolScope

if TYPE_CHECKING:
    from krim_agent.accounts.service import IdentityService
    from krim_agent.runtime.service import RunService
    from krim_agent.storage.sqlite import SQLiteStore


class SessionToolHost:
    def __init__(
        self, store: SQLiteStore, identities: IdentityService | None = None,
        access_policy: AccessPolicy | None = None,
    ) -> None:
        self.store = store
        self.identities = identities
        self.access_policy = access_policy
        self.runtime: RunService | None = None

    def _producer(self, run_id: str) -> None:
        runtime, producer = self.runtime, _PRODUCER.get()
        if (runtime is None or runtime.closed or producer is None
                or not producer.active or producer.service is not runtime
                or producer.run_id != run_id or producer.task.done()
                or runtime.tasks.get(run_id) is not producer.task
                or run_id in runtime._cancelled):
            raise ServiceError("permission_denied")

    def _active(self, invocation: Invocation) -> None:
        self._producer(invocation.run_id)
        runtime, call = self.runtime, _CALL.get()
        if (runtime is None or not _owns(runtime, runtime.tasks) or call is None
                or call.invocation != invocation):
            raise ServiceError("permission_denied")
        current = CURRENT.get()
        if current is not None:
            current.check()
            if current.invocation != invocation:
                raise ServiceError("permission_denied")

    async def resolve(self, invocation: Invocation) -> SessionToolScope:
        return await self._scope(invocation.run_id, lambda: self._active(invocation))

    async def by_run(
        self, run_id: str, check: Callable[[], None] = lambda: None,
    ) -> SessionToolScope:
        """Host model preparation is producer-owned, without a fabricated tool call."""
        def fence() -> None:
            self._producer(run_id)
            check()

        return await self._scope(run_id, fence)

    async def policy_scope(self, run_id: str) -> SessionToolScope:
        """Host-only reevaluation of persisted parked requests, never execution.

        This deliberately grants no producer/call ownership. The executor still
        resolves its ordinary live scope when the parked run actually resumes.
        """
        return await self._scope(run_id, lambda: None, allow_awaiting=True)

    async def _scope(
        self, run_id: str, fence: Callable[[], None], *, allow_awaiting: bool = False,
    ) -> SessionToolScope:
        fence()

        async def read(conn: Any) -> dict[str, Any]:
            row = (
                await conn.execute(
                    select(
                        sessions.c.id.label("session_id"), sessions.c.workspace_id,
                        sessions.c.owner_id, sessions.c.status.label("session_status"),
                        turns.c.id.label("turn_id"), turns.c.status.label("turn_status"),
                        runs.c.stop_requested, runs.c.status.label("run_status"),
                    )
                    .select_from(turns.join(sessions, turns.c.session_id == sessions.c.id)
                                 .join(runs, turns.c.run_id == runs.c.id))
                    .where(runs.c.id == run_id)
                )
            ).mappings().first()
            if row is None:
                raise ServiceError("workspace_contract_required", 409)
            result = dict(row)
            binding = (
                await conn.execute(select(sandbox_session_bindings).where(
                    sandbox_session_bindings.c.session_id == row["session_id"],
                ))
            ).mappings().first()
            if binding is not None and binding["principal_id"] != row["owner_id"]:
                raise ServiceError("facts_corrupt")
            result["project_id"] = binding["project_id"] if binding is not None else None
            return result

        record = await self.store.work_read(read)
        fence()
        allowed_statuses = {"running", "awaiting"} if allow_awaiting else {"running"}
        if record["stop_requested"] or record["run_status"] not in allowed_statuses:
            raise ServiceError("cancelled")
        if record["session_status"] != "open":
            raise ServiceError("session_closed", 409)
        current = CURRENT.get()
        actor = getattr(current, "actor", None)
        if actor is None and current is not None:
            actor = getattr(getattr(current, "access", None), "principal", None)
        if actor is None:
            if self.identities is not None:
                actor = self.identities.resolve(record["owner_id"])
            elif self.access_policy is not None:
                from krim_agent.security import PrincipalContext

                credential = next((item for item in self.access_policy.credentials
                                   if item.principal_id == record["owner_id"]), None)
                if credential is None:
                    raise ServiceError("unauthorized", 401)
                actor = PrincipalContext(credential.principal_id, credential.generation,
                                         credential.memberships, credential.legacy)
            elif record["owner_id"] == LOCAL.principal_id:
                actor = LOCAL
            else:
                raise ServiceError("unauthorized", 401)
        if actor.principal_id != record["owner_id"]:
            raise ServiceError("permission_denied")
        actor.member(record["workspace_id"])
        if self.identities is not None:
            self.identities.validate(actor)
        if record["project_id"] is not None and self.store.schema_version >= 19:
            from krim_agent.project_access.policy import resolve_project_access

            async def project_access(conn: Any) -> None:
                access = await resolve_project_access(conn, actor, record["project_id"])
                access.require()

            await self.store.work_read(project_access)
            fence()
        environment = getattr(current, "environment", None)
        if environment is not None and environment.project_id != record["project_id"]:
            raise ServiceError("target_binding_changed")

        def check() -> None:
            fence()
            if self.identities is not None:
                self.identities.validate(actor)

        return SessionToolScope(
            actor=actor, workspace_id=record["workspace_id"],
            session_id=record["session_id"], turn_id=record["turn_id"],
            run_id=run_id, project_id=record["project_id"], check=check,
        )
