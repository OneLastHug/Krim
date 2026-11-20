"""Authorized, persistent goal state with a single continuation decision point.

The service owns goal facts only. It never runs a model loop and never waits:
the host calls :meth:`GoalService.on_turn_finished` after a turn reaches a
terminal or parked state and submits the returned internal continuation turn
through its own idempotent queue. A bare ``session_id`` never grants access;
every read and mutation checks the live actor, workspace membership, session
ownership and open/archive state, and an idempotent request-key replay still
passes the same checks.

Goals are bound to the user turn that requested them:

* A goal may be created only from a real, non-internal user turn. The service
  resolves that turn from storage itself (tool: the scope's current turn;
  owner API: the latest user turn or an explicitly supplied one) and records
  its ``turn_id``/``run_id`` as the immutable goal origin.
* Host-created internal continuation turns (``goal-continue:`` /
  ``goal-resume:`` request keys) can never create, replace, pause, resume or
  cancel a goal; their text is internal continuation material, not a new user
  authorization.
* Replacing, pausing, resuming and cancelling are owner API operations with
  CAS and the same idempotent request-key protocol. An active goal cannot be
  silently redirected; replacement requires a paused or blocked goal.

Completion evidence is explicit: the service records criteria and evidence
references and never treats a completed plan (or "all todos ticked") as proof
that the user's goal is achieved.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, cast

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.planning.contracts import PlanSnapshot
from krim_agent.security import PrincipalContext
from krim_agent.storage import goals as sql
from krim_agent.storage.goal_models import goal_continuations
from krim_agent.storage.models import run_events, sessions
from krim_agent.tools.session import SessionScopeResolver, SessionToolScope, ToolBundle

from .continuation import (
    CONTINUE_TURN_PREFIX,
    INTERNAL_TURN_PREFIXES,
    PERMANENT_FAILURE_CODES,
    RESUME_TURN_PREFIX,
    TERMINAL_TURN_STATUSES,
    ContinuationDecision,
    ContinuationPolicy,
    ContinuationQueue,
    ContinuationRequest,
    StopReason,
    TurnFinishedContext,
    continuation_text,
    recovery_text,
)
from .contracts import (
    EvidenceRef,
    GoalContinuationView,
    GoalControlRequest,
    GoalCreateRequest,
    GoalPlanBridge,
    GoalRead,
    GoalSnapshot,
    GoalStatus,
    GoalUpdate,
    goal_digest,
)

if TYPE_CHECKING:
    from krim_agent.accounts.service import IdentityService
    from krim_agent.storage.sqlite import SQLiteStore

TOOL_ACTIONS = frozenset({"set", "progress", "block", "complete"})
REPLACEABLE_STATUSES = frozenset({"paused", "blocked"})
_SNAPSHOT_RAISE = object()


def _noop() -> None:
    pass


class GoalService:
    def __init__(
        self,
        store: SQLiteStore,
        identities: IdentityService | None = None,
        *,
        snapshots: Callable[[PrincipalContext, str, str], Awaitable[PlanSnapshot]] | None = None,
        queue: ContinuationQueue | None = None,
        continuation_policy: ContinuationPolicy | None = None,
    ) -> None:
        self.store, self.identities = store, identities
        # PlanningService.snapshot(actor, workspace_id, session_id) compatible port.
        self.snapshots, self.queue = snapshots, queue
        self.continuation_policy = continuation_policy or ContinuationPolicy()

    # ---- authorization -------------------------------------------------

    def actor(self, actor: PrincipalContext) -> PrincipalContext:
        return self.identities.validate(actor) if self.identities is not None else actor

    async def _session(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
    ) -> dict[str, Any]:
        current = self.actor(actor)
        current.member(workspace_id, write=True)
        row = (
            (
                await conn.execute(
                    sessions.select().where(
                        sessions.c.id == session_id,
                        sessions.c.workspace_id == workspace_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        # Goals belong to the session owner; workspace admins cannot write them.
        if row is None or row["owner_id"] != current.principal_id:
            raise ServiceError("not_found", 404)
        return cast(dict[str, Any], dict(row))

    async def authorize(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        check: Callable[[], None] = _noop,
    ) -> None:
        check()

        async def read(conn: AsyncConnection) -> None:
            await self._session(conn, actor, workspace_id, session_id)
            check()

        await self.store.work_read(read)
        self.actor(actor)
        check()

    async def _read(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        operation: Callable[[AsyncConnection], Awaitable[Any]],
        *,
        check: Callable[[], None] = _noop,
    ) -> Any:
        self.actor(actor)
        check()

        async def read(conn: AsyncConnection) -> Any:
            await self._session(conn, actor, workspace_id, session_id)
            result = await operation(conn)
            self.actor(actor)
            check()
            return result

        result = await self.store.work_read(read)
        self.actor(actor)
        check()
        return result

    # ---- projection ----------------------------------------------------

    async def snapshot(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        check: Callable[[], None] = _noop,
    ) -> GoalSnapshot:
        """Authorized latest snapshot used by tools, HTTP and context projection."""
        return cast(
            GoalSnapshot,
            await self._read(
                actor,
                workspace_id,
                session_id,
                lambda conn: self._project(conn, workspace_id, session_id),
                check=check,
            ),
        )

    async def read(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        query: GoalRead | None = None,
        *,
        check: Callable[[], None] = _noop,
    ) -> GoalSnapshot:
        """Read the latest goal or an immutable historical revision of it."""
        revision = (query or GoalRead()).revision
        if revision is None:
            return await self.snapshot(actor, workspace_id, session_id, check=check)

        async def read(conn: AsyncConnection) -> GoalSnapshot:
            head = await sql.current(conn, session_id)
            if head is None or revision < 1 or revision > head["revision"]:
                raise ServiceError("not_found", 404)
            row = await sql.version(conn, head["goal_id"], revision)
            if row is None:
                raise ServiceError("not_found", 404)
            return await self._project_version(conn, row, plan=False)

        return cast(
            GoalSnapshot,
            await self._read(actor, workspace_id, session_id, read, check=check),
        )

    async def project(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        check: Callable[[], None] = _noop,
    ) -> GoalSnapshot:
        """Snapshot plus read-only plan counts for context projection."""
        return cast(
            GoalSnapshot,
            await self._read(
                actor,
                workspace_id,
                session_id,
                lambda conn: self._project(conn, workspace_id, session_id, plan=True, actor=actor),
                check=check,
            ),
        )

    async def _project(
        self,
        conn: AsyncConnection,
        workspace_id: str,
        session_id: str,
        *,
        plan: bool = False,
        actor: PrincipalContext | None = None,
    ) -> GoalSnapshot:
        head = await sql.current(conn, session_id)
        if head is None:
            return GoalSnapshot(
                goal_id=None,
                session_id=session_id,
                workspace_id=workspace_id,
                owner_id=None,
                revision=0,
                status=None,
                objective=None,
                content_hash=goal_digest({"version": "goals/1", "goal": None}),
            )
        if head["workspace_id"] != workspace_id or head["revision"] < 1:
            raise ServiceError("facts_corrupt")
        row = await sql.version(conn, head["goal_id"], head["revision"])
        if row is None:
            raise ServiceError("facts_corrupt")
        return await self._project_version(conn, row, plan=plan, actor=actor)

    async def _project_version(
        self,
        conn: AsyncConnection,
        row: dict[str, Any],
        *,
        plan: bool,
        actor: PrincipalContext | None = None,
    ) -> GoalSnapshot:
        snapshot = _snapshot_from(row)
        latest = await sql.latest_continuation(conn, cast(str, row["session_id"]))
        continuation = (
            None
            if latest is None
            else GoalContinuationView.model_validate(_continuation_view(latest))
        )
        bridge = await self._plan_bridge(row, actor) if plan and actor is not None else None
        return snapshot.model_copy(update={"continuation": continuation, "plan": bridge})

    async def _plan_bridge(
        self, row: dict[str, Any], actor: PrincipalContext
    ) -> GoalPlanBridge | None:
        """Plan counts are context only; completed steps never prove goal completion."""
        if self.snapshots is None:
            return None
        try:
            plan = await self.snapshots(
                actor, cast(str, row["workspace_id"]), cast(str, row["session_id"])
            )
        except (ServiceError, TypeError):
            return None
        if not isinstance(plan, PlanSnapshot):
            return None
        return GoalPlanBridge(
            revision=plan.revision,
            content_hash=plan.content_hash,
            counts=plan.counts,
            total=plan.total,
        )

    async def projection(self, scope: SessionToolScope) -> Sequence[dict[str, JsonValue]]:
        """Context snapshot provider (G port): one item, or none without a goal."""
        scope.check()
        snapshot = await self.project(
            scope.actor, scope.workspace_id, scope.session_id, check=scope.check
        )
        if snapshot.goal_id is None:
            return ()
        return (
            {
                "name": "session_goal",
                "revision": snapshot.revision,
                "content": json.dumps(
                    snapshot.model_dump(mode="json"),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "source_refs": [
                    {
                        "kind": "goal",
                        "goal_id": snapshot.goal_id,
                        "revision": snapshot.revision,
                        "content_hash": snapshot.content_hash,
                    }
                ],
            },
        )

    # ---- mutations -----------------------------------------------------

    async def update_from_tool(self, scope: SessionToolScope, body: GoalUpdate) -> GoalSnapshot:
        """Host-bound model update; creation binds the scope's current user turn."""
        if body.action not in TOOL_ACTIONS:
            raise ServiceError("invalid_input", 422)
        return await self._mutate(
            scope.actor,
            scope.workspace_id,
            scope.session_id,
            body,
            turn_id=scope.turn_id,
            run_id=scope.run_id,
            creation="tool",
            check=scope.check,
        )

    async def create(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        body: GoalCreateRequest,
        *,
        check: Callable[[], None] = _noop,
    ) -> GoalSnapshot:
        """Owner API creation/replacement bound to a real, non-internal user turn."""
        update = GoalUpdate(
            request_key=body.request_key,
            expected_revision=body.expected_revision,
            action="set",
            objective=body.objective,
            completion_criteria=body.completion_criteria,
            evidence_refs=body.evidence_refs,
            note=body.note,
        )
        return await self._mutate(
            actor,
            workspace_id,
            session_id,
            update,
            turn_id=None,
            run_id=None,
            creation=(body.origin_turn_id, body.origin_run_id),
            check=check,
        )

    async def control(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        body: GoalControlRequest,
        *,
        action: str,
        check: Callable[[], None] = _noop,
    ) -> GoalSnapshot:
        """Owner API pause/resume/cancel: direct user authority, CAS + request key."""
        if action not in {"pause", "resume", "cancel"}:
            raise ServiceError("invalid_input", 422)
        update = GoalUpdate(
            request_key=body.request_key,
            expected_revision=body.expected_revision,
            action=cast(Any, action),
            reason=body.reason,
        )
        result = await self._mutate(
            actor,
            workspace_id,
            session_id,
            update,
            turn_id=None,
            run_id=None,
            creation=None,
            check=check,
        )
        if action == "resume" and result.status == "active":
            await self._queue_resume(actor, workspace_id, session_id, body.request_key, check)
            # Re-read so the response reflects the durable continuation row (and a replay
            # after a crash returns the same state, not a pre-queue snapshot).
            result = await self.snapshot(actor, workspace_id, session_id, check=check)
        return result

    async def _queue_resume(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        request_key: str,
        check: Callable[[], None],
    ) -> None:
        """Resume uses the same durable single-slot queue protocol as automatic continuation."""
        if self.queue is None or self.store.readonly or self.store.closed:
            return

        async def write(conn: AsyncConnection) -> ContinuationRequest | None:
            session = await self._session(conn, actor, workspace_id, session_id)
            if session["status"] != "open":
                return None
            goal = await self._project(conn, workspace_id, session_id)
            if goal.status != "active" or goal.goal_id is None:
                return None
            continue_key = goal_digest(
                {
                    "version": "goals/1",
                    "resume_request": request_key,
                    "goal_id": goal.goal_id,
                    "goal_revision": goal.revision,
                }
            )
            existing = await sql.continuation(conn, session_id, continue_key)
            if existing is not None:
                return _request_for(existing) if existing["status"] == "queued" else None
            request = ContinuationRequest(
                workspace_id=workspace_id,
                session_id=session_id,
                owner_id=session["owner_id"],
                goal_id=goal.goal_id,
                goal_revision=goal.revision,
                continue_key=continue_key,
                turn_request_key=RESUME_TURN_PREFIX + continue_key,
                origin_turn_id=cast(str, goal.origin_turn_id),
                origin_run_id=cast(str, goal.origin_run_id),
                source_turn_id=None,
                source_run_id=None,
                reason="goal_resumed",
                text=continuation_text(goal),
            )
            now = time.time()
            inserted = await sql.insert_continuation(
                conn,
                {
                    "session_id": session_id,
                    "request_key": continue_key,
                    "workspace_id": workspace_id,
                    "owner_id": session["owner_id"],
                    "goal_id": goal.goal_id,
                    "goal_revision_base": goal.revision,
                    "source_turn_id": None,
                    "source_run_id": None,
                    "origin_turn_id": goal.origin_turn_id,
                    "origin_run_id": goal.origin_run_id,
                    "status": "queued",
                    "internal": True,
                    "payload": request.model_dump(mode="json"),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            if not inserted:
                return None
            check()
            return request

        async def confirm() -> ContinuationRequest | None:
            return None

        request = cast(ContinuationRequest | None, await self.store.work_write(write, confirm))
        if request is None:
            return
        try:
            await self.queue.enqueue(request)
        except asyncio.CancelledError:
            raise
        except Exception:

            async def mark(conn: AsyncConnection) -> None:
                await sql.update_continuation(
                    conn,
                    session_id,
                    request.continue_key,
                    {"status": "failed", "code": "enqueue_failed", "updated_at": time.time()},
                )

            async def no_result() -> None:
                return None

            try:
                await self.store.work_write(mark, no_result)
            except ServiceError:
                pass

    async def _mutate(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        body: GoalUpdate,
        *,
        turn_id: str | None,
        run_id: str | None,
        creation: str | tuple[str | None, str | None] | None,
        check: Callable[[], None],
    ) -> GoalSnapshot:
        self.actor(actor)
        check()
        digest = goal_digest(
            {
                "action": "goal_update",
                "workspace_id": workspace_id,
                "session_id": session_id,
                "body": body.model_dump(mode="json"),
                "creation": list(creation) if isinstance(creation, tuple) else creation,
            }
        )

        async def known(conn: AsyncConnection) -> GoalSnapshot | None:
            # A matching request key never bypasses current session/identity authorization.
            await self._session(conn, actor, workspace_id, session_id)
            check()
            row = await sql.known_request(conn, session_id, body.request_key)
            if row is None:
                return None
            if row["request_hash"] != digest:
                raise ServiceError("key_conflict", 409)
            version = await sql.version(conn, row["goal_id"], row["revision"])
            if version is None:
                raise ServiceError("facts_corrupt")
            return await self._project_version(conn, version, plan=False)

        async def write(conn: AsyncConnection) -> GoalSnapshot:
            previous = await known(conn)
            if previous is not None:
                return previous
            session = await self._session(conn, actor, workspace_id, session_id)
            if session["status"] != "open":
                raise ServiceError("session_closed", 409)
            # target-workspace has no ordinary TurnRequest contract: a goal that
            # activates there could never be continued (SessionService.enqueue
            # rejects it), so creating/replacing/resuming one is refused up front.
            # Historical reads and pause/cancel on such a session stay available.
            if body.action in {"set", "resume"} and session["target_key"] == "target-workspace":
                raise ServiceError("workspace_contract_required", 409)
            lifecycle = await sql.lifecycle_row(conn, session_id)
            if lifecycle is not None and (
                lifecycle["deleted_at"] is not None or lifecycle["archived_at"] is not None
            ):
                raise ServiceError("session_closed", 409)
            if body.action == "set":
                origin = await self._creation_binding(
                    conn, actor, workspace_id, session_id, creation, turn_id, run_id
                )
                return await self._set(
                    conn,
                    workspace_id,
                    session_id,
                    actor.principal_id,
                    body,
                    digest,
                    origin,
                    check,
                )
            return await self._transition(
                conn,
                workspace_id,
                session_id,
                actor.principal_id,
                body,
                digest,
                turn_id=turn_id,
                run_id=run_id,
                check=check,
            )

        async def confirm() -> GoalSnapshot | None:
            return cast(GoalSnapshot | None, await self.store.work_read(known))

        result = await self.store.work_write(write, confirm)
        self.actor(actor)
        check()
        return result

    async def _creation_binding(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        creation: str | tuple[str | None, str | None] | None,
        turn_id: str | None,
        run_id: str | None,
    ) -> tuple[str, str]:
        if creation == "tool":
            if turn_id is None or run_id is None:
                raise ServiceError("invalid_input", 422)
            turn = await sql.turn_row(conn, turn_id)
            if turn is None or turn["run_id"] != run_id:
                raise ServiceError("invalid_input", 422)
        elif isinstance(creation, tuple):
            requested_turn, requested_run = creation
            if requested_turn is None:
                # Owner API without an explicit turn: bind the latest real user turn.
                turn = await sql.latest_user_turn(conn, session_id)
            else:
                turn = await sql.turn_row(conn, requested_turn)
                if turn is not None and turn["session_id"] != session_id:
                    raise ServiceError("not_found", 404)
                if (
                    turn is not None
                    and requested_run is not None
                    and turn["run_id"] != requested_run
                ):
                    raise ServiceError("invalid_input", 422)
        else:
            raise ServiceError("invalid_input", 422)
        if turn is None or turn["session_id"] != session_id:
            raise ServiceError("invalid_input", 422)
        if turn["owner_id"] != actor.principal_id or turn["workspace_id"] != workspace_id:
            raise ServiceError("not_found", 404)
        # Internal continuation turns are never a new user authorization.
        if str(turn["request_key"]).startswith(INTERNAL_TURN_PREFIXES):
            raise ServiceError("invalid_input", 422)
        if turn["run_id"] is None:
            raise ServiceError("invalid_input", 422)
        return (cast(str, turn["id"]), cast(str, turn["run_id"]))

    async def _set(
        self,
        conn: AsyncConnection,
        workspace_id: str,
        session_id: str,
        actor_id: str,
        body: GoalUpdate,
        digest: str,
        origin: tuple[str, str],
        check: Callable[[], None],
    ) -> GoalSnapshot:
        head = await sql.current(conn, session_id)
        if head is not None and head["revision"] != body.expected_revision:
            raise ServiceError("revision_conflict", 409)
        if head is None and body.expected_revision != 0:
            raise ServiceError("revision_conflict", 409)
        if head is not None:
            current = await self._project(conn, workspace_id, session_id)
            if current.status not in REPLACEABLE_STATUSES:
                # An active goal cannot be silently replaced or redirected.
                raise ServiceError("revision_conflict", 409)
        if not body.objective:
            raise ServiceError("invalid_input", 422)
        now = time.time()
        origin_turn_id, origin_run_id = origin
        goal_id = str(uuid.uuid4())
        revision = (head["revision"] if head is not None else 0) + 1
        criteria = body.completion_criteria
        evidence = body.evidence_refs
        note = body.note.strip() or None
        values = {
            "workspace_id": workspace_id,
            "owner_id": actor_id,
            "goal_id": goal_id,
            "revision": revision,
            "status": "active",
            "objective": body.objective,
            "completion_criteria": list(criteria),
            "evidence_refs": [item.model_dump(mode="json") for item in evidence],
            "status_note": note,
            "blocked_reason": None,
            "origin_turn_id": origin_turn_id,
            "origin_run_id": origin_run_id,
            "updated_turn_id": None,
            "updated_run_id": None,
            "created_at": now,
            "updated_at": now,
        }
        if head is None:
            if not await sql.insert_head(conn, {"session_id": session_id, **values}):
                raise ServiceError("revision_conflict", 409)
        elif not await sql.update_head(conn, session_id, body.expected_revision, values):
            raise ServiceError("revision_conflict", 409)
        await sql.insert_version(
            conn,
            {
                **_version_values(values, session_id),
                "content_hash": _content_hash(
                    goal_id, revision, "active", body.objective, criteria, evidence, note, None
                ),
            },
        )
        await sql.record_request(
            conn, session_id, body.request_key, actor_id, digest, goal_id, revision, now
        )
        await self._supersede_stale(conn, session_id, actor_id, turn_id=None, now=now)
        check()
        return await self._project(conn, workspace_id, session_id)

    async def _supersede_stale(
        self,
        conn: AsyncConnection,
        session_id: str,
        actor_id: str,
        *,
        turn_id: str | None,
        now: float,
    ) -> None:
        """Retire queued continuations made stale by a goal change.

        The internal continuation that is currently executing keeps its own row so
        its finish can settle progress; every other queued decision is superseded.
        """
        keep = (
            await _continue_key_for_turn(conn, session_id, turn_id) if turn_id is not None else None
        )
        await sql.set_continuations_status(
            conn, session_id, ("queued",), "superseded", now, keep_request_key=keep
        )

    async def _transition(
        self,
        conn: AsyncConnection,
        workspace_id: str,
        session_id: str,
        actor_id: str,
        body: GoalUpdate,
        digest: str,
        *,
        turn_id: str | None,
        run_id: str | None,
        check: Callable[[], None],
    ) -> GoalSnapshot:
        head = await sql.current(conn, session_id)
        if head is None:
            raise ServiceError("not_found", 404)
        if head["revision"] != body.expected_revision:
            raise ServiceError("revision_conflict", 409)
        current = await self._project(conn, workspace_id, session_id)
        target = _target_status(body.action, current.status)
        criteria = body.completion_criteria or current.completion_criteria
        if body.action == "complete" and not criteria:
            raise ServiceError("invalid_input", 422)
        evidence = _merged_evidence(current, body)
        note = body.note.strip() or current.status_note
        blocked_reason = body.reason.strip() or None if body.action == "block" else None
        now = time.time()
        revision = body.expected_revision + 1
        values = {
            "workspace_id": workspace_id,
            "owner_id": head["owner_id"],
            "goal_id": head["goal_id"],
            "revision": revision,
            "status": target,
            "objective": current.objective,
            "completion_criteria": list(criteria),
            "evidence_refs": [item.model_dump(mode="json") for item in evidence],
            "status_note": note,
            "blocked_reason": blocked_reason,
            "origin_turn_id": head["origin_turn_id"],
            "origin_run_id": head["origin_run_id"],
            "updated_turn_id": turn_id,
            "updated_run_id": run_id,
            "created_at": head["created_at"],
            "updated_at": now,
        }
        if not await sql.update_head(conn, session_id, body.expected_revision, values):
            raise ServiceError("revision_conflict", 409)
        await sql.insert_version(
            conn,
            {
                **_version_values(values, session_id),
                "content_hash": _content_hash(
                    cast(str, head["goal_id"]),
                    revision,
                    target,
                    cast(str, current.objective),
                    criteria,
                    evidence,
                    note,
                    blocked_reason,
                ),
            },
        )
        await sql.record_request(
            conn, session_id, body.request_key, actor_id, digest, head["goal_id"], revision, now
        )
        await self._supersede_stale(conn, session_id, actor_id, turn_id=turn_id, now=now)
        check()
        return await self._project(conn, workspace_id, session_id)

    # ---- continuation --------------------------------------------------

    async def on_turn_finished(self, context: TurnFinishedContext) -> ContinuationDecision:
        """Decide and at most enqueue one internal follow-up turn; never runs a loop."""
        context.check()
        if self.store.readonly or self.store.closed:
            return ContinuationDecision(action="stop", reason="queue_unavailable")

        async def write(conn: AsyncConnection) -> ContinuationDecision:
            return await self._decide(conn, context)

        async def confirm() -> ContinuationDecision | None:
            return None

        try:
            decision = await self.store.work_write(write, confirm)
        except ServiceError as error:
            return ContinuationDecision(action="stop", reason=_stop_for(error.code))
        if decision.action != "continue" or decision.request is None:
            return decision
        if self.queue is None:
            return decision.model_copy(update={"reason": "queue_unavailable"})
        try:
            await self.queue.enqueue(decision.request)
        except asyncio.CancelledError:
            raise
        except Exception:
            await self._mark_queue_failure(context, decision)
            return decision.model_copy(update={"reason": "enqueue_failed"})
        return decision.model_copy(update={"queued": True})

    async def _decide(
        self, conn: AsyncConnection, context: TurnFinishedContext
    ) -> ContinuationDecision:
        session = await self._session(conn, context.actor, context.workspace_id, context.session_id)
        if session["status"] != "open":
            return _stop("session_closed")
        lifecycle = await sql.lifecycle_row(conn, context.session_id)
        if lifecycle is not None and (
            lifecycle["deleted_at"] is not None or lifecycle["archived_at"] is not None
        ):
            return _stop("session_archived")
        if context.status == "awaiting" or context.parked or context.approval_pending:
            return _stop("approval_pending")
        if context.pending_interaction:
            # A durable question/plan confirmation owns the wake-up path.
            return _stop("waiting_input")
        turn = await sql.turn_row(conn, context.turn_id)
        if turn is None:
            raise ServiceError("not_found", 404)
        if turn["session_id"] != context.session_id or turn["run_id"] != context.run_id:
            raise ServiceError("facts_corrupt")
        if context.stop_requested or context.code == "cancelled":
            return _stop("user_stopped")
        if context.status == "interrupted" or context.code == "interrupted":
            return _stop("run_interrupted")
        if context.status not in TERMINAL_TURN_STATUSES:
            return _stop("already_finished")
        newer = await sql.newer_user_turns(conn, context.session_id, turn["ordinal"])
        if newer:
            return _stop("user_message_pending")
        head = await sql.current(conn, context.session_id)
        if head is None:
            return _stop("no_goal")
        goal = await self._project(conn, context.workspace_id, context.session_id)
        internal = str(turn["request_key"]).startswith(INTERNAL_TURN_PREFIXES)
        settled = await self._settle_internal(conn, context, turn, goal) if internal else None
        if settled is not None and settled["status"] == "superseded":
            return _stop("superseded", goal)
        if goal.status == "paused":
            return _stop("goal_paused", goal)
        if goal.status == "blocked":
            return _stop("goal_blocked", goal)
        if goal.status == "completed":
            return _stop("goal_completed", goal)
        if goal.status == "cancelled":
            return _stop("goal_cancelled", goal)
        if goal.status != "active" or goal.goal_id is None:
            return _stop("no_goal", goal)
        if context.code in PERMANENT_FAILURE_CODES:
            # Never re-ask for the same parked decision. A denied approval is the
            # user's answer (goal stays available for a different approach); an
            # expired approval window blocks the goal so the wait cannot loop.
            if context.code == "approval_expired":
                await self._auto_block(
                    conn,
                    context,
                    goal,
                    context.code,
                    reason=f"审批窗口已过期（{context.code}）；批准后由用户恢复继续。",
                )
                return _stop("approval_expired", goal)
            return _stop("user_stopped", goal)
        if await self._has_unknown_execution(conn, context.run_id):
            # An intent without a trustworthy terminal observation may represent
            # a committed side effect. Do not enqueue a turn that could repeat it.
            return _stop("operation_unknown", goal)

        recovery_signature: str | None = None
        recovery_attempt = 0
        targeted_recovery = False
        if settled is not None and settled["progressed"] is False:
            if context.status == "completed" and not await self._has_waiting_task(
                conn, context.run_id
            ):
                recovery_signature = await self._run_signature(conn, context.run_id)
                if recovery_signature is not None:
                    repeats = await self._repeated_signature_runs(
                        conn, goal.goal_id, recovery_signature
                    )
                    if repeats >= self.continuation_policy.identical_no_progress_limit:
                        used = await self._targeted_recovery_attempts(conn, goal.goal_id)
                        if used >= self.continuation_policy.max_targeted_recovery_attempts:
                            await self._auto_block(
                                conn,
                                context,
                                goal,
                                "repeated_no_progress",
                                reason=(
                                    "完成轮次连续重复相同的已观测执行轨迹且没有新增目标证据；"
                                    "定向纠偏次数已用尽，等待用户调整目标。"
                                ),
                            )
                            return _stop("repeated_no_progress", goal)
                        targeted_recovery = True
                        recovery_attempt = used + 1
            elif context.status != "completed":
                repeats = await self._repeated_failures(conn, goal.goal_id, context.code)
                if repeats >= self.continuation_policy.repeated_failure_limit:
                    await self._auto_block(conn, context, goal, context.code or "failed")
                    return _stop("repeated_failure", goal)
        continue_key = _continue_key(goal, context)
        queued = await sql.continuations_with_status(conn, context.session_id, ("queued",))
        for row in queued:
            if row["request_key"] == continue_key:
                # Same host decision replayed after a restart: re-emit the identical
                # idempotent request so the host's queue deduplicates it.
                return _continue_decision(goal, continue_key, _request_for(row))
        if queued:
            return _stop("continuation_pending", goal)
        request = ContinuationRequest(
            workspace_id=context.workspace_id,
            session_id=context.session_id,
            owner_id=session["owner_id"],
            goal_id=goal.goal_id,
            goal_revision=goal.revision,
            continue_key=continue_key,
            turn_request_key=CONTINUE_TURN_PREFIX + continue_key,
            origin_turn_id=cast(str, goal.origin_turn_id),
            origin_run_id=cast(str, goal.origin_run_id),
            source_turn_id=context.turn_id,
            source_run_id=context.run_id,
            reason=("repeated_no_progress_recovery" if targeted_recovery else "goal_active"),
            recovery_attempt=recovery_attempt,
            progress_signature=recovery_signature if targeted_recovery else None,
            text=recovery_text(goal) if targeted_recovery else continuation_text(goal),
        )
        now = time.time()
        inserted = await sql.insert_continuation(
            conn,
            {
                "session_id": context.session_id,
                "request_key": continue_key,
                "workspace_id": context.workspace_id,
                "owner_id": session["owner_id"],
                "goal_id": goal.goal_id,
                "goal_revision_base": goal.revision,
                "source_turn_id": context.turn_id,
                "source_run_id": context.run_id,
                "origin_turn_id": goal.origin_turn_id,
                "origin_run_id": goal.origin_run_id,
                "status": "queued",
                "internal": True,
                "payload": request.model_dump(mode="json"),
                "created_at": now,
                "updated_at": now,
            },
        )
        if not inserted:
            existing = await sql.continuation(conn, context.session_id, continue_key)
            if existing is not None and existing["status"] == "queued":
                return _continue_decision(goal, continue_key, _request_for(existing))
            return _stop("continuation_pending", goal)
        context.check()
        return _continue_decision(goal, continue_key, request)

    async def _settle_internal(
        self,
        conn: AsyncConnection,
        context: TurnFinishedContext,
        turn: dict[str, Any],
        goal: GoalSnapshot,
    ) -> dict[str, Any] | None:
        request_key = str(turn["request_key"])
        prefix = next((p for p in INTERNAL_TURN_PREFIXES if request_key.startswith(p)), None)
        if prefix is None:
            return None
        continue_key = request_key[len(prefix) :]
        row = await sql.continuation(conn, context.session_id, continue_key)
        if row is None or row["goal_id"] != goal.goal_id:
            raise ServiceError("facts_corrupt")
        if row["status"] == "superseded":
            # The host or the owner retired this decision while the turn was queued;
            # running it must not resurrect the continuation.
            return row
        if row["status"] != "queued":
            # Replayed finish: the settled row already records progress.
            return row
        baseline = await sql.version(conn, goal.goal_id, row["goal_revision_base"])
        if baseline is None:
            raise ServiceError("facts_corrupt")
        try:
            prior_evidence = [
                EvidenceRef.model_validate(item).model_dump(mode="json")
                for item in baseline["evidence_refs"]
            ]
        except (TypeError, ValueError):
            raise ServiceError("facts_corrupt") from None
        current_evidence = [item.model_dump(mode="json") for item in goal.evidence_refs]
        progressed = goal.status != "active" or any(
            item not in prior_evidence for item in current_evidence
        )
        values = {
            "status": "consumed",
            "turn_id": context.turn_id,
            "outcome": "completed" if context.status == "completed" else "failed",
            "code": context.code,
            "goal_revision": goal.revision,
            "progressed": progressed,
            "updated_at": time.time(),
        }
        await sql.update_continuation(conn, context.session_id, continue_key, values)
        return {**row, **values}

    async def _run_events(self, conn: AsyncConnection, run_id: str) -> tuple[Any, ...]:
        from krim_agent.contracts import RunEvent

        rows = (
            await conn.execute(
                select(run_events).where(run_events.c.run_id == run_id).order_by(run_events.c.seq)
            )
        ).mappings().all()
        try:
            return tuple(RunEvent.model_validate(dict(row)) for row in rows)
        except (TypeError, ValueError):
            raise ServiceError("facts_corrupt") from None

    async def _run_signature(self, conn: AsyncConnection, run_id: str) -> str | None:
        from krim_agent.context.builder import deterministic_signature

        return deterministic_signature(await self._run_events(conn, run_id))

    async def _has_unknown_execution(self, conn: AsyncConnection, run_id: str) -> bool:
        from krim_agent.context.builder import has_unknown_execution

        return has_unknown_execution(await self._run_events(conn, run_id))

    async def _has_waiting_task(self, conn: AsyncConnection, run_id: str) -> bool:
        from krim_agent.context.builder import has_waiting_task

        return has_waiting_task(await self._run_events(conn, run_id))

    async def _repeated_signature_runs(
        self, conn: AsyncConnection, goal_id: str, signature: str
    ) -> int:
        rows = (
            await conn.execute(
                select(goal_continuations)
                .where(
                    goal_continuations.c.goal_id == goal_id,
                    goal_continuations.c.status == "consumed",
                )
                .order_by(
                    goal_continuations.c.updated_at.desc(),
                    goal_continuations.c.request_key.desc(),
                )
            )
        ).mappings().all()
        count = 0
        for item in rows:
            if item["outcome"] != "completed" or item["progressed"] is not False:
                break
            turn = await sql.turn_row(conn, item["turn_id"]) if item["turn_id"] else None
            if turn is None or not isinstance(turn.get("run_id"), str):
                break
            previous = await self._run_signature(conn, turn["run_id"])
            if previous is None or previous != signature:
                break
            count += 1
        return count

    async def _targeted_recovery_attempts(
        self, conn: AsyncConnection, goal_id: str
    ) -> int:
        rows = (
            await conn.execute(
                select(
                    goal_continuations.c.payload,
                    goal_continuations.c.outcome,
                    goal_continuations.c.progressed,
                )
                .where(
                    goal_continuations.c.goal_id == goal_id,
                    goal_continuations.c.status == "consumed",
                )
                .order_by(
                    goal_continuations.c.updated_at.desc(),
                    goal_continuations.c.request_key.desc(),
                )
            )
        ).mappings().all()
        count = 0
        for item in rows:
            if item["outcome"] != "completed" or item["progressed"] is not False:
                break
            request = _request_for({"payload": item["payload"]})
            if request.reason == "repeated_no_progress_recovery":
                count += 1
        return count

    async def _repeated_failures(
        self, conn: AsyncConnection, goal_id: str, code: str | None
    ) -> int:
        rows = (
            await conn.execute(
                select(goal_continuations.c.code, goal_continuations.c.progressed)
                .where(
                    goal_continuations.c.goal_id == goal_id,
                    goal_continuations.c.status == "consumed",
                    goal_continuations.c.outcome == "failed",
                )
                .order_by(
                    goal_continuations.c.updated_at.desc(),
                    goal_continuations.c.request_key.desc(),
                )
            )
        ).mappings().all()
        count = 0
        for item in rows:
            if item["code"] != code or item["progressed"] is not False:
                break
            count += 1
        return count

    async def _auto_block(
        self,
        conn: AsyncConnection,
        context: TurnFinishedContext,
        goal: GoalSnapshot,
        code: str,
        *,
        reason: str | None = None,
    ) -> None:
        body = GoalUpdate(
            request_key="auto-blocked:" + _continue_key(goal, context),
            expected_revision=goal.revision,
            action="block",
            reason=reason
            or (
                f"连续{self.continuation_policy.repeated_failure_limit}次相同失败且没有新证据（{code}）；"
                "等待用户调整目标后继续。"
            ),
        )
        digest = goal_digest(
            {
                "action": "goal_update",
                "workspace_id": context.workspace_id,
                "session_id": context.session_id,
                "body": body.model_dump(mode="json"),
                "creation": None,
            }
        )
        await self._transition(
            conn,
            context.workspace_id,
            context.session_id,
            context.actor.principal_id,
            body,
            digest,
            turn_id=context.turn_id,
            run_id=context.run_id,
            check=context.check,
        )

    async def _mark_queue_failure(
        self, context: TurnFinishedContext, decision: ContinuationDecision
    ) -> None:
        if decision.continue_key is None:
            return
        continue_key = decision.continue_key

        async def write(conn: AsyncConnection) -> None:
            row = await sql.continuation(conn, context.session_id, continue_key)
            if row is None or row["status"] != "queued":
                return
            await sql.update_continuation(
                conn,
                context.session_id,
                continue_key,
                {"status": "failed", "code": "enqueue_failed", "updated_at": time.time()},
            )

        async def confirm() -> None:
            return None

        try:
            await self.store.work_write(write, confirm)
        except ServiceError:
            return

    async def supersede_continuations(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        reason: str = "user_message",
        *,
        check: Callable[[], None] = _noop,
    ) -> int:
        """Call before dispatching a new user message; queued internal turns lose priority."""
        now = time.time()

        async def write(conn: AsyncConnection) -> int:
            await self._session(conn, actor, workspace_id, session_id)
            rows = await sql.set_continuations_status(
                conn, session_id, ("queued",), "superseded", now
            )
            check()
            return len(rows)

        async def confirm() -> int | None:
            return None

        result = await self.store.work_write(write, confirm)
        return result

    async def latest_continuation(
        self, actor: PrincipalContext, workspace_id: str, session_id: str
    ) -> GoalContinuationView | None:
        row = await self._read(
            actor,
            workspace_id,
            session_id,
            lambda conn: sql.latest_continuation(conn, session_id),
        )
        return None if row is None else GoalContinuationView.model_validate(_continuation_view(row))

    async def continuation_by_key(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        continue_key: str,
    ) -> GoalContinuationView | None:
        """Host recovery lookup: the durable row behind one continue key, if any."""
        row = await self._read(
            actor,
            workspace_id,
            session_id,
            lambda conn: sql.continuation(conn, session_id, continue_key),
        )
        return None if row is None else GoalContinuationView.model_validate(_continuation_view(row))

    def bundle(self, resolve: SessionScopeResolver) -> ToolBundle:
        from .tools import bundle

        return bundle(self, resolve)


async def _continue_key_for_turn(
    conn: AsyncConnection, session_id: str, turn_id: str
) -> str | None:
    turn = await sql.turn_row(conn, turn_id)
    if turn is None or turn["session_id"] != session_id:
        return None
    request_key = str(turn["request_key"])
    prefix = next((p for p in INTERNAL_TURN_PREFIXES if request_key.startswith(p)), None)
    return None if prefix is None else request_key[len(prefix) :]


def _continuation_view(row: dict[str, Any]) -> dict[str, Any]:
    # The stored request key IS the continue key; the column keeps sessions scoped.
    return {
        "continue_key": row["request_key"],
        **{
            key: row[key]
            for key in GoalContinuationView.model_fields
            if key in row and key != "continue_key"
        },
    }


def _version_values(values: dict[str, Any], session_id: str) -> dict[str, Any]:
    return {
        "goal_id": values["goal_id"],
        "revision": values["revision"],
        "session_id": session_id,
        "workspace_id": values["workspace_id"],
        "owner_id": values["owner_id"],
        "status": values["status"],
        "objective": values["objective"],
        "completion_criteria": values["completion_criteria"],
        "evidence_refs": values["evidence_refs"],
        "status_note": values["status_note"],
        "blocked_reason": values["blocked_reason"],
        "origin_turn_id": values["origin_turn_id"],
        "origin_run_id": values["origin_run_id"],
        "updated_turn_id": values["updated_turn_id"],
        "updated_run_id": values["updated_run_id"],
        "created_at": values["created_at"],
        "updated_at": values["updated_at"],
    }


def _snapshot_from(row: dict[str, Any]) -> GoalSnapshot:
    try:
        evidence = tuple(EvidenceRef.model_validate(item) for item in row["evidence_refs"])
        criteria = tuple(str(item) for item in row["completion_criteria"])
    except (TypeError, ValueError):
        raise ServiceError("facts_corrupt") from None
    if row["content_hash"] != _content_hash(
        row["goal_id"],
        row["revision"],
        row["status"],
        row["objective"],
        criteria,
        evidence,
        row["status_note"],
        row["blocked_reason"],
    ):
        raise ServiceError("facts_corrupt")
    return GoalSnapshot(
        goal_id=row["goal_id"],
        session_id=row["session_id"],
        workspace_id=row["workspace_id"],
        owner_id=row["owner_id"],
        revision=row["revision"],
        status=row["status"],
        objective=row["objective"],
        completion_criteria=criteria,
        evidence_refs=evidence,
        status_note=row["status_note"],
        blocked_reason=row["blocked_reason"],
        origin_turn_id=row["origin_turn_id"],
        origin_run_id=row["origin_run_id"],
        updated_turn_id=row["updated_turn_id"],
        updated_run_id=row["updated_run_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        content_hash=row["content_hash"],
    )


def _request_for(row: dict[str, Any]) -> ContinuationRequest:
    try:
        return ContinuationRequest.model_validate(row["payload"])
    except (TypeError, ValueError):
        raise ServiceError("facts_corrupt") from None


def _continue_decision(
    goal: GoalSnapshot, continue_key: str, request: ContinuationRequest
) -> ContinuationDecision:
    return ContinuationDecision(
        action="continue",
        reason="goal_active",
        goal_id=goal.goal_id,
        goal_revision=goal.revision,
        continue_key=continue_key,
        queued=False,
        request=request,
    )


def _content_hash(
    goal_id: str,
    revision: int,
    status: str | None,
    objective: str | None,
    criteria: Sequence[str],
    evidence: Sequence[EvidenceRef],
    note: str | None,
    blocked_reason: str | None,
) -> str:
    return goal_digest(
        {
            "goal_id": goal_id,
            "revision": revision,
            "status": status,
            "objective": objective,
            "completion_criteria": list(criteria),
            "evidence_refs": [item.model_dump(mode="json") for item in evidence],
            "status_note": note,
            "blocked_reason": blocked_reason,
        }
    )


def _target_status(action: str, current: GoalStatus | None) -> GoalStatus:
    if current in {"completed", "cancelled"}:
        raise ServiceError("revision_conflict", 409)
    if action == "progress":
        if current not in {"active", "blocked"}:
            raise ServiceError("revision_conflict", 409)
        return "active"
    if action == "pause":
        if current not in {"active", "blocked"}:
            raise ServiceError("revision_conflict", 409)
        return "paused"
    if action == "resume":
        if current not in {"paused", "blocked"}:
            raise ServiceError("revision_conflict", 409)
        return "active"
    if action == "cancel":
        return "cancelled"
    if action == "block":
        return "blocked"
    if action == "complete":
        return "completed"
    raise ServiceError("invalid_input", 422)


def _merged_evidence(goal: GoalSnapshot, body: GoalUpdate) -> tuple[EvidenceRef, ...]:
    merged: dict[tuple[str, str], EvidenceRef] = {}
    for item in (*goal.evidence_refs, *body.evidence_refs):
        merged[item.identity] = item
    return tuple(merged.values())


def _continue_key(goal: GoalSnapshot, context: TurnFinishedContext) -> str:
    return goal_digest(
        {
            "version": "goals/1",
            "goal_id": goal.goal_id,
            "goal_revision": goal.revision,
            "turn_id": context.turn_id,
            "run_id": context.run_id,
        }
    )


def _stop(reason: StopReason, goal: GoalSnapshot | None = None) -> ContinuationDecision:
    return ContinuationDecision(
        action="stop",
        reason=reason,
        goal_id=None if goal is None else goal.goal_id,
        goal_revision=None if goal is None else goal.revision,
    )


def _stop_for(code: str) -> StopReason:
    return {
        "not_found": "permission_denied",
        "permission_denied": "permission_denied",
        "session_closed": "session_closed",
    }.get(code, "queue_unavailable")  # type: ignore[return-value]


# Explicit short form used by feature loading code.
Service = GoalService
