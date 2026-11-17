"""Authorized, persistent session plans shared by all session targets."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.security import PrincipalContext
from krim_agent.storage import planning as planning_sql
from krim_agent.storage.models import sessions
from krim_agent.tools.session import SessionScopeResolver, ToolBundle

from .contracts import PAGE_SIZE, PlanRead, PlanSnapshot, PlanStatus, PlanUpdate, plan_digest

if TYPE_CHECKING:
    from krim_agent.accounts.service import IdentityService
    from krim_agent.storage.sqlite import SQLiteStore


def _noop() -> None:
    pass


class PlanningService:
    def __init__(self, store: SQLiteStore, identities: IdentityService | None = None) -> None:
        self.store, self.identities = store, identities

    def actor(self, actor: PrincipalContext) -> PrincipalContext:
        return self.identities.validate(actor) if self.identities is not None else actor

    async def _session(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        write: bool = False,
    ) -> Any:
        current = self.actor(actor)
        membership = current.member(workspace_id, write=write)
        row = (
            (
                await conn.execute(
                    select(sessions).where(
                        sessions.c.id == session_id,
                        sessions.c.workspace_id == workspace_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None or (row["owner_id"] != current.principal_id and membership.role != "admin"):
            raise ServiceError("not_found", 404)
        if write and row["owner_id"] != current.principal_id:
            raise ServiceError("permission_denied", 403)
        return row

    async def authorize(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        write: bool = False,
        check: Callable[[], None] = _noop,
    ) -> None:
        """Policy authorization; mutation also rechecks inside its transaction."""
        check()

        async def read(conn: AsyncConnection) -> None:
            await self._session(conn, actor, workspace_id, session_id, write=write)
            check()

        await self.store.work_read(read)
        self.actor(actor)
        check()

    async def read(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        query: PlanRead | None = None,
        *,
        check: Callable[[], None] = _noop,
    ) -> PlanSnapshot:
        query = query or PlanRead()
        self.actor(actor)
        check()

        async def read(conn: AsyncConnection) -> PlanSnapshot:
            session = await self._session(conn, actor, workspace_id, session_id)
            latest = await planning_sql.current_revision(
                conn, workspace_id, session_id, session["owner_id"]
            )
            revision = latest if query.revision is None else query.revision
            if revision > latest:
                raise ServiceError("not_found", 404)
            result = await planning_sql.snapshot(conn, session_id, revision, query)
            self.actor(actor)
            check()
            return result

        result = await self.store.work_read(read)
        self.actor(actor)
        check()
        return result

    async def snapshot(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        revision: int | None = None,
        offset: int = 0,
        limit: int = PAGE_SIZE,
        statuses: tuple[PlanStatus, ...] = (),
        check: Callable[[], None] = _noop,
    ) -> PlanSnapshot:
        """Immutable context/goal projection; continue with revision and next_offset.

        Counts always describe the whole revision, even when steps are filtered.
        Completing every plan step is not evidence that a user goal is complete.
        """
        return await self.read(
            actor,
            workspace_id,
            session_id,
            PlanRead(
                revision=revision,
                offset=offset,
                limit=limit,
                statuses=statuses,
            ),
            check=check,
        )

    async def update(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        body: PlanUpdate,
        *,
        check: Callable[[], None] = _noop,
    ) -> PlanSnapshot:
        self.actor(actor)
        check()
        digest = plan_digest(
            {
                "action": "plan_update",
                "workspace_id": workspace_id,
                "session_id": session_id,
                "body": body.model_dump(mode="json"),
            }
        )

        async def known(conn: AsyncConnection) -> PlanSnapshot | None:
            # A matching request key never bypasses current session/identity authorization.
            await self._session(conn, actor, workspace_id, session_id, write=True)
            check()
            result = await planning_sql.known_request(
                conn, session_id, actor.principal_id, body.request_key, digest
            )
            self.actor(actor)
            check()
            return result

        async def write(conn: AsyncConnection) -> PlanSnapshot:
            previous = await known(conn)
            if previous is not None:
                return previous
            session = await self._session(conn, actor, workspace_id, session_id, write=True)
            if session["status"] != "open":
                raise ServiceError("session_closed", 409)
            result = await planning_sql.replace(
                conn,
                workspace_id,
                session_id,
                session["owner_id"],
                actor.principal_id,
                body,
                digest,
            )
            self.actor(actor)
            check()
            return result

        async def confirm() -> PlanSnapshot | None:
            return await self.store.work_read(known)

        result = await self.store.work_write(write, confirm)
        self.actor(actor)
        check()
        return result

    def bundle(self, resolve: SessionScopeResolver) -> ToolBundle:
        from .tools import bundle

        return bundle(self, resolve)


# Explicit short form used by feature loading code.
Service = PlanningService
