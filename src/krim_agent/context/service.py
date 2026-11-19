"""Durable manual compaction scheduling and the injected snapshot port.

Manual ``compact_context`` requests are durable session state: a user or model
enqueues one at any time (even while no run is active), it survives restarts,
and a later run consumes it at the model boundary. The service never compacts
by itself and never touches a run: the host's ``CompactorHistory`` asks the
boundary returned by :meth:`ContextService.boundary` whether a request is
pending, runs the real ``Compactor`` with ``force=True`` on the next safe
prepare, and only then calls ``complete``. A failed or interrupted compaction
simply never completes, so the request stays pending for the next run.

Every boundary operation verifies that its run belongs to the session owner,
maps to the same session, and is running unstopped before anything else
happens; every operation also re-authorizes the caller against the session
owner, so a bare ``session_id`` never grants access and an idempotent
request-key replay must still pass the same checks. Fulfilment marks are
idempotent for the same run and receipt, and fail closed on any conflict.

The snapshot port is host-injected: a callable receiving the session
:class:`~krim_agent.tools.session.SessionToolScope` (with the run's real
``turn_id``) and returning immutable :class:`ContextSnapshotItem` values (or
mappings that validate against them). The service validates and freezes what
the provider returns but never reads other packages' tables and never
fabricates plan, goal or memory content. Snapshot identity uses the unlimited
canonical fingerprint from :mod:`krim_agent.context.serialization`; snapshots
carry no application storage-size ceiling — request budgets belong to the
model boundary.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import Field, JsonValue, model_validator
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.security import PrincipalContext
from krim_agent.storage.context_models import context_requests
from krim_agent.storage.models import runs, sessions, turns
from krim_agent.storage.sandbox_models import sandbox_projects, sandbox_session_bindings
from krim_agent.tools.session import SessionToolScope

from .serialization import fingerprint

if TYPE_CHECKING:
    from krim_agent.accounts.service import IdentityService
    from krim_agent.storage.sqlite import SQLiteStore

T = TypeVar("T")


class ContextSnapshotItem(StrictModel):
    """One immutable injected snapshot projected ahead of the conversation.

    ``content`` is the provider's serialized projection (plan text, goal state,
    or any other session-scope material the host chooses to inject); it is
    untrusted input at the model boundary, like every other history message.
    ``source_refs`` keep the provider's own provenance shape so the host can
    hash snapshot metadata/revision into prepared context and checkpoints.
    There is no application size ceiling on ``content``; request budgets are
    enforced at the model boundary.
    """

    name: str = Field(min_length=1, max_length=128)
    revision: str | int
    content: str = Field(min_length=1)
    source_refs: tuple[dict[str, JsonValue], ...] = ()

    @model_validator(mode="after")
    def bounded_revision(self) -> ContextSnapshotItem:
        if isinstance(self.revision, bool) or not isinstance(self.revision, (str, int)):
            raise ValueError("快照修订标识类型无效")
        if isinstance(self.revision, str):
            if not 1 <= len(self.revision) <= 128:
                raise ValueError("快照修订标识超限")
        elif self.revision < 0:
            raise ValueError("快照修订标识无效")
        return self


class CompactionReceipt(StrictModel):
    """Fixed-shape fulfilment receipt; structurally bounded, never free-form.

    ``source_fingerprint``/``summary_hash`` are the canonical hex digests of
    the completed pass; ``covered``/``passes`` mirror the Compactor state the
    host wants recorded with the fulfilled request.
    """

    source_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    covered: int = Field(default=0, ge=0)
    passes: int = Field(default=0, ge=0)
    summary_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


def snapshot_fingerprint(item: ContextSnapshotItem) -> str:
    """Stable identity of one snapshot item; the host records it with prepared context."""
    return fingerprint(item.model_dump(mode="json"))


def _noop() -> None:
    pass


def _validate_request_key(request_key: str) -> str:
    if (
        not isinstance(request_key, str)
        or not request_key
        or len(request_key) > 128
        or any(ord(char) < 33 or ord(char) > 126 for char in request_key)
    ):
        raise ServiceError("invalid_input", 422)
    return request_key


def _receipt(metadata: CompactionReceipt | dict[str, JsonValue] | None) -> CompactionReceipt:
    if metadata is None:
        return CompactionReceipt()
    if isinstance(metadata, CompactionReceipt):
        return metadata
    try:
        return CompactionReceipt.model_validate(metadata)
    except ValueError:
        raise ServiceError("invalid_input", 422) from None


class CompactionBoundary:
    """Run-bound view of one session's durable requests and snapshot port.

    The host creates one boundary per run from the run's resolved identity.
    Every operation first verifies the run: it must map to this session's
    owner and workspace through its turn, and be running without a stop
    request. ``pending`` returns the oldest pending request key (the "token")
    or None; ``complete`` marks that request fulfilled and must be called only
    after the model-boundary compaction actually succeeded. ``prepare``
    freezes the injected snapshot provider's current projection for injection
    ahead of the conversation, handing the provider the run's real scope.
    """

    def __init__(
        self,
        service: ContextService,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        run_id: str,
        *,
        scope: SessionToolScope | None = None,
    ) -> None:
        if not run_id:
            raise ServiceError("invalid_input", 422)
        if scope is not None and (
            scope.actor != actor
            or scope.workspace_id != workspace_id
            or scope.session_id != session_id
            or scope.run_id != run_id
        ):
            raise ServiceError("permission_denied", 403)
        self.scope = scope
        self._service = service
        self.actor = actor
        self.workspace_id = workspace_id
        self.session_id = session_id
        self.run_id = run_id

    async def _guard(self) -> tuple[str, str | None]:
        """Recheck the run and its current project before exposing any snapshot."""
        service = self._service
        if self.scope is not None:
            self.scope.check()
        service.actor(self.actor)

        async def read(conn: AsyncConnection) -> tuple[str, str | None]:
            return await service._boundary_scope(
                conn,
                self.actor,
                self.workspace_id,
                self.session_id,
                self.run_id,
            )

        turn_id, project_id = await service.store.work_read(read)
        service.actor(self.actor)
        if self.scope is not None:
            self.scope.check()
            if self.scope.turn_id != turn_id or self.scope.project_id != project_id:
                raise ServiceError("permission_denied", 403)
        return turn_id, project_id

    async def pending(self) -> str | None:
        """Oldest pending request key for this session, or None."""
        await self._guard()
        result = await self._service._oldest_pending(self.actor, self.workspace_id, self.session_id)
        if self.scope is not None:
            self.scope.check()
        return result

    async def complete(
        self,
        token: str,
        receipt: CompactionReceipt | dict[str, JsonValue] | None = None,
    ) -> None:
        """Mark the token fulfilled. Idempotent for the same run+receipt."""
        _turn_id, project_id = await self._guard()
        await self._service._fulfill(
            self.actor,
            self.workspace_id,
            self.session_id,
            token,
            self.run_id,
            receipt,
            project_id=project_id,
            check=self.scope.check if self.scope is not None else _noop,
        )

    async def prepare_context(self) -> tuple[str | None, list[ContextSnapshotItem]]:
        """Read queue+live scope once, then recheck after the injected provider."""
        service = self._service
        check = self.scope.check if self.scope is not None else _noop
        check()
        service.actor(self.actor)

        async def before(conn: AsyncConnection) -> tuple[str, str | None, str | None]:
            turn_id, project_id = await service._boundary_scope(
                conn,
                self.actor,
                self.workspace_id,
                self.session_id,
                self.run_id,
            )
            if self.scope is not None and (
                self.scope.turn_id != turn_id or self.scope.project_id != project_id
            ):
                raise ServiceError("permission_denied", 403)
            token = await conn.scalar(
                select(context_requests.c.request_key)
                .where(
                    context_requests.c.session_id == self.session_id,
                    context_requests.c.status == "pending",
                )
                .order_by(context_requests.c.created_at, context_requests.c.request_key)
                .limit(1)
            )
            return turn_id, project_id, token

        turn_id, project_id, token = await service.store.work_read(before)
        check()
        scope = SessionToolScope(
            actor=self.actor,
            workspace_id=self.workspace_id,
            session_id=self.session_id,
            turn_id=turn_id,
            run_id=self.run_id,
            project_id=project_id,
            check=check,
        )
        items = await service._snapshot_items(scope)

        async def after(conn: AsyncConnection) -> None:
            current = await service._boundary_scope(
                conn,
                self.actor,
                self.workspace_id,
                self.session_id,
                self.run_id,
            )
            if current != (turn_id, project_id):
                raise ServiceError("permission_denied", 403)

        await service.store.work_read(after)
        service.actor(self.actor)
        check()
        return token, items

    async def prepare(self) -> list[ContextSnapshotItem]:
        """Compatibility view of the same request-local snapshot preparation."""
        _token, items = await self.prepare_context()
        return items


class ContextService:
    def __init__(
        self,
        store: SQLiteStore,
        identities: IdentityService | None = None,
        snapshots: Callable[[SessionToolScope], Sequence[Any] | Awaitable[Sequence[Any]]]
        | None = None,
    ) -> None:
        if snapshots is not None and not callable(snapshots):
            raise ServiceError("invalid_input", 422)
        self.store, self.identities, self.snapshots = store, identities, snapshots

    def actor(self, actor: PrincipalContext) -> PrincipalContext:
        return self.identities.validate(actor) if self.identities is not None else actor

    async def _boundary_scope(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        run_id: str,
    ) -> tuple[str, str | None]:
        """One-query live boundary read; never retained beyond the caller's operation."""
        current = self.actor(actor)
        current.member(workspace_id)
        statement = (
            select(
                sessions.c.owner_id,
                sessions.c.status.label("session_status"),
                turns.c.id.label("turn_id"),
                turns.c.owner_id.label("turn_owner"),
                turns.c.workspace_id.label("turn_workspace"),
                runs.c.status.label("run_status"),
                runs.c.stop_requested,
            )
            .select_from(
                sessions.outerjoin(
                    turns,
                    (turns.c.session_id == sessions.c.id) & (turns.c.run_id == run_id),
                ).outerjoin(runs, runs.c.id == turns.c.run_id)
            )
            .where(
                sessions.c.id == session_id,
                sessions.c.workspace_id == workspace_id,
            )
        )
        if self.store.schema_version >= 9:
            statement = (
                statement.outerjoin(
                    sandbox_session_bindings,
                    sandbox_session_bindings.c.session_id == sessions.c.id,
                )
                .outerjoin(
                    sandbox_projects,
                    sandbox_projects.c.project_id == sandbox_session_bindings.c.project_id,
                )
                .add_columns(
                    sandbox_session_bindings.c.principal_id.label("binding_owner"),
                    sandbox_session_bindings.c.project_id,
                    sandbox_projects.c.principal_id.label("project_owner"),
                    sandbox_projects.c.workspace_id.label("project_workspace"),
                )
            )
        row = (await conn.execute(statement)).mappings().first()
        if (
            row is None
            or row["owner_id"] != current.principal_id
            or row["turn_id"] is None
            or row["turn_owner"] != current.principal_id
            or row["turn_workspace"] != workspace_id
        ):
            raise ServiceError("not_found", 404)
        if row["run_status"] != "running" or row["stop_requested"]:
            raise ServiceError("run_closed", 409)
        project_id = None
        if self.store.schema_version >= 9:
            if row["binding_owner"] is not None and row["binding_owner"] != current.principal_id:
                raise ServiceError("permission_denied", 403)
            project_id = row["project_id"]
            if project_id is not None and (
                row["project_owner"] != current.principal_id
                or row["project_workspace"] != workspace_id
            ):
                raise ServiceError("permission_denied", 403)
        return str(row["turn_id"]), project_id

    async def _project(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
    ) -> str | None:
        if self.store.schema_version < 9:
            return None
        binding = (
            (
                await conn.execute(
                    select(sandbox_session_bindings).where(
                        sandbox_session_bindings.c.session_id == session_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if binding is None:
            return None
        if binding["principal_id"] != actor.principal_id:
            raise ServiceError("permission_denied", 403)
        project_id = binding["project_id"]
        if project_id is not None:
            project = (
                (
                    await conn.execute(
                        select(
                            sandbox_projects.c.principal_id,
                            sandbox_projects.c.workspace_id,
                        ).where(sandbox_projects.c.project_id == project_id)
                    )
                )
                .mappings()
                .first()
            )
            if (
                project is None
                or project["principal_id"] != actor.principal_id
                or project["workspace_id"] != workspace_id
            ):
                raise ServiceError("permission_denied", 403)
        return project_id if isinstance(project_id, str) else None

    async def _session(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        require_open: bool = False,
    ) -> Any:
        current = self.actor(actor)
        current.member(workspace_id, write=require_open)
        row = (
            (
                await conn.execute(
                    select(sessions).where(
                        sessions.c.id == session_id, sessions.c.workspace_id == workspace_id
                    )
                )
            )
            .mappings()
            .first()
        )
        # Compaction requests belong to the session owner, including against
        # workspace admins: summaries bind to one owner's history.
        if row is None or row["owner_id"] != current.principal_id:
            raise ServiceError("not_found", 404)
        if require_open and row["status"] != "open":
            raise ServiceError("session_closed", 409)
        return row

    async def _verify_run(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        run_id: str,
    ) -> str:
        """The run must map to this session/owner and be running unstopped."""
        row = (
            (
                await conn.execute(
                    select(turns).where(
                        turns.c.run_id == run_id,
                        turns.c.session_id == session_id,
                        turns.c.workspace_id == workspace_id,
                        turns.c.owner_id == actor.principal_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        run = (
            (
                await conn.execute(
                    select(runs.c.status, runs.c.stop_requested).where(runs.c.id == run_id)
                )
            )
            .mappings()
            .first()
        )
        if run is None or run["status"] != "running" or run["stop_requested"]:
            raise ServiceError("run_closed", 409)
        return str(row["id"])

    async def _read(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        operation: Callable[[AsyncConnection], Awaitable[T]],
        *,
        require_open: bool = False,
    ) -> T:
        self.actor(actor)

        async def read(conn: AsyncConnection) -> T:
            await self._session(conn, actor, workspace_id, session_id, require_open=require_open)
            result = await operation(conn)
            self.actor(actor)
            return result

        result = await self.store.work_read(read)
        self.actor(actor)
        return result

    async def schedule_compaction(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        request_key: str,
        expected: int | None = None,
        *,
        reason: str | None = None,
        check: Callable[[], None] = _noop,
    ) -> dict[str, Any]:
        """Durably enqueue a manual compaction request for one session.

        Idempotent per ``(session_id, request_key)``: the same key and body
        return the original receipt without creating a second row; the same
        key with a different body (including a different ``expected``) fails
        with ``key_conflict`` so stale expectations surface instead of
        silently no-opping a pending request. ``expected``, when given, is the
        receipt revision the caller believes is current; it participates in
        the request hash. Requests preserve on failure by construction: the
        row only leaves ``pending`` through :meth:`CompactionBoundary.complete`
        after a successful model-boundary pass.
        """
        request_key = _validate_request_key(request_key)
        if expected is not None and (type(expected) is not int or expected < 0):
            raise ServiceError("invalid_input", 422)
        if reason is not None and (not isinstance(reason, str) or len(reason) > 2048):
            raise ServiceError("invalid_input", 422)
        self.actor(actor)
        check()
        body = {"reason": reason, "expected": expected}
        digest = fingerprint(
            {
                "action": "compact_context",
                "workspace_id": workspace_id,
                "session_id": session_id,
                "body": body,
            }
        )

        async def known(conn: AsyncConnection) -> dict[str, Any] | None:
            # Authorization is re-checked even for an idempotent replay.
            await self._session(conn, actor, workspace_id, session_id, require_open=True)
            check()
            row = (
                (
                    await conn.execute(
                        select(context_requests).where(
                            context_requests.c.session_id == session_id,
                            context_requests.c.request_key == request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            if row["request_hash"] != digest:
                raise ServiceError("key_conflict", 409)
            return _request_receipt(row)

        async def write(conn: AsyncConnection) -> dict[str, Any]:
            previous = await known(conn)
            if previous is not None:
                return previous
            revision = 1 + (
                await conn.scalar(
                    select(func.max(context_requests.c.revision)).where(
                        context_requests.c.session_id == session_id
                    )
                )
                or 0
            )
            now = time.time()
            await conn.execute(
                insert(context_requests).values(
                    session_id=session_id,
                    request_key=request_key,
                    actor_id=actor.principal_id,
                    request_hash=digest,
                    revision=revision,
                    reason=reason,
                    status="pending",
                    created_at=now,
                )
            )
            self.actor(actor)
            check()
            return {
                "request_key": request_key,
                "session_id": session_id,
                "revision": revision,
                "reason": reason,
                "status": "pending",
                "created_at": now,
                "created": True,
            }

        async def confirm() -> dict[str, Any] | None:
            return await self._read(actor, workspace_id, session_id, known, require_open=True)

        result = await self.store.work_write(write, confirm)
        self.actor(actor)
        check()
        return result

    async def schedule_auth(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        check: Callable[[], None] = _noop,
    ) -> None:
        """Policy-time authorization for scheduling, without enqueueing.

        Applies the same owner/open checks a real schedule would; the actual
        mutation re-checks inside its transaction regardless.
        """

        async def touch(conn: AsyncConnection) -> None:
            return None

        await self._read(actor, workspace_id, session_id, touch, require_open=True)
        check()

    async def pending(
        self, actor: PrincipalContext, workspace_id: str, session_id: str
    ) -> list[dict[str, Any]]:
        """All pending requests, oldest first. Fulfilled rows are never returned."""

        async def read(conn: AsyncConnection) -> list[dict[str, Any]]:
            rows = (
                (
                    await conn.execute(
                        select(context_requests)
                        .where(
                            context_requests.c.session_id == session_id,
                            context_requests.c.status == "pending",
                        )
                        .order_by(context_requests.c.created_at, context_requests.c.request_key)
                    )
                )
                .mappings()
                .all()
            )
            return [_request_receipt(row) for row in rows]

        return await self._read(actor, workspace_id, session_id, read)

    async def _oldest_pending(
        self, actor: PrincipalContext, workspace_id: str, session_id: str
    ) -> str | None:
        requests = await self.pending(actor, workspace_id, session_id)
        return requests[0]["request_key"] if requests else None

    async def _fulfill(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        token: str,
        run_id: str,
        receipt: CompactionReceipt | dict[str, JsonValue] | None,
        *,
        project_id: str | None = None,
        check: Callable[[], None] = _noop,
    ) -> None:
        check()
        token = _validate_request_key(token)
        receipt = _receipt(receipt)
        document = receipt.model_dump(mode="json")
        digest = fingerprint({"run_id": run_id, "receipt": document})
        self.actor(actor)

        async def write(conn: AsyncConnection) -> bool:
            check()
            await self._session(conn, actor, workspace_id, session_id)
            await self._verify_run(conn, actor, workspace_id, session_id, run_id)
            if await self._project(conn, actor, workspace_id, session_id) != project_id:
                raise ServiceError("permission_denied", 403)
            check()
            row = (
                (
                    await conn.execute(
                        select(context_requests).where(
                            context_requests.c.session_id == session_id,
                            context_requests.c.request_key == token,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            if row["status"] == "fulfilled":
                if row["result_hash"] == digest:
                    return False
                raise ServiceError("key_conflict", 409)
            await conn.execute(
                update(context_requests)
                .where(
                    context_requests.c.session_id == session_id,
                    context_requests.c.request_key == token,
                    context_requests.c.status == "pending",
                )
                .values(
                    status="fulfilled",
                    run_id=run_id,
                    result_hash=digest,
                    metadata=document,
                    fulfilled_at=time.time(),
                )
            )
            self.actor(actor)
            check()
            return True

        async def confirm() -> bool | None:
            async def read(conn: AsyncConnection) -> bool | None:
                await self._session(conn, actor, workspace_id, session_id)
                row = (
                    (
                        await conn.execute(
                            select(context_requests).where(
                                context_requests.c.session_id == session_id,
                                context_requests.c.request_key == token,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if row is None or row["status"] != "fulfilled":
                    return None
                return bool(row["result_hash"] == digest)

            return await self._read(actor, workspace_id, session_id, read)

        await self.store.work_write(write, confirm)
        self.actor(actor)
        check()

    async def _snapshot_items(self, scope: SessionToolScope) -> list[ContextSnapshotItem]:
        """Invoke only between the boundary's fresh before/after authorization reads."""
        scope.check()
        if self.snapshots is None:
            return []
        supplied = self.snapshots(scope)
        if isinstance(supplied, Awaitable):
            supplied = await supplied
        if not isinstance(supplied, (list, tuple)):
            raise ServiceError("invalid_output")
        items: list[ContextSnapshotItem] = []
        for item in supplied:
            try:
                items.append(
                    item
                    if isinstance(item, ContextSnapshotItem)
                    else ContextSnapshotItem.model_validate(item)
                )
            except ValueError:
                raise ServiceError("invalid_output") from None
        scope.check()
        return items

    def boundary(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        run_id: str,
        *,
        scope: SessionToolScope | None = None,
    ) -> CompactionBoundary:
        """Return the run-bound adapter for CompactorHistory.

        Authorization is not skipped by construction: every adapter operation
        verifies the session owner and the run binding before acting.
        """
        return CompactionBoundary(self, actor, workspace_id, session_id, run_id, scope=scope)

    def new_request_key(self) -> str:
        """Host-side helper for API callers without a natural request key."""
        return "compact-context." + str(uuid.uuid4())


def _request_receipt(row: Any) -> dict[str, Any]:
    """Replayed receipt for an existing request; replay is never "created"."""
    return {
        "request_key": row["request_key"],
        "session_id": row["session_id"],
        "revision": row["revision"],
        "reason": row["reason"],
        "status": row["status"],
        "created_at": row["created_at"],
        "created": False,
    }


__all__ = [
    "CompactionBoundary",
    "CompactionReceipt",
    "ContextService",
    "ContextSnapshotItem",
    "snapshot_fingerprint",
]
