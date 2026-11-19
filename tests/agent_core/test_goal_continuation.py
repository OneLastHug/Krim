"""on_turn_finished decision matrix: this is the host's only continuation entry point.

Every case uses a real SQLite goal store plus a fake queue port. The fake queue
records the exact idempotent request the host would submit; there is no second
agent loop, no waiting and no model call in the service.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.goals.continuation import (
    CONTINUE_TURN_PREFIX,
    ContinuationRequest,
    TurnFinishedContext,
    continuation_text,
)
from krim_agent.goals.contracts import GoalControlRequest, GoalCreateRequest, GoalUpdate
from krim_agent.goals.service import GoalService
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.goal_models import GOAL_TABLES
from krim_agent.storage.models import metadata, sessions
from krim_agent.storage.session_lifecycle_models import session_lifecycle
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.session import SessionToolScope

WORKSPACE = "goals-continuation"


def member(principal_id: str, workspace_id: str = WORKSPACE, role: str = "member"):
    return PrincipalContext(principal_id, 1, (Membership(workspace_id=workspace_id, role=role),))


class FakeQueue:
    def __init__(self, *, fail: bool = False) -> None:
        self.requests: list[ContinuationRequest] = []
        self.fail = fail

    async def enqueue(self, request: ContinuationRequest) -> None:
        if self.fail:
            raise RuntimeError("queue down")
        self.requests.append(request)


@asynccontextmanager
async def goal_world(path: Path) -> AsyncIterator[tuple[SQLiteStore, dict]]:
    store = await SQLiteStore.open(path)
    async with store.lock, store.engine.begin() as conn:
        await conn.run_sync(lambda sync: metadata.create_all(sync, tables=list(GOAL_TABLES)))
    owner = member("owner")
    session = await session_sql.create_session(
        store,
        WORKSPACE,
        owner.principal_id,
        SessionCreate(request_key="session"),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )
    turn = await new_turn(store, session.id, "user-1", "请完成任务", status="completed")
    try:
        yield store, {"owner": owner, "session": session, "turn": turn}
    finally:
        await store.close()


async def new_turn(store: SQLiteStore, session_id: str, key: str, text: str, *, status="running"):
    async with store.lock, store.engine.begin() as conn:
        revision = (await session_sql.session_row(conn, session_id))["revision"]
    turn = await session_sql.submit(
        store,
        session_id,
        TurnRequest(request_key=key, text=text, expected_revision=revision),
        Settings(),
    )
    run, _ = await store.begin(
        RunRequest(request_key=f"run-{key}", text=f"run {key}", target="demo"),
        {"key": "demo"},
    )
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(
            update(session_sql.turns)
            .where(session_sql.turns.c.id == turn.id)
            .values(run_id=run.id, status=status)
        )
    return turn, run.id


async def create_goal(store, owner, session, turn, run_id, *, criteria=("全部测试通过",)):
    return await GoalService(store).create(
        owner,
        WORKSPACE,
        session.id,
        GoalCreateRequest(
            request_key="create",
            expected_revision=0,
            objective="完成用户任务",
            completion_criteria=criteria,
            origin_turn_id=turn.id,
            origin_run_id=run_id,
        ),
    )


def finished(turn, run_id, owner=None, **values) -> TurnFinishedContext:
    return TurnFinishedContext(
        actor=owner or member("owner"),
        workspace_id=WORKSPACE,
        session_id=values.pop("session_id", None) or turn.session_id,
        turn_id=turn.id,
        run_id=run_id,
        status=values.pop("status", "completed"),
        **values,
    )


@pytest.mark.asyncio
async def test_active_goal_enqueues_exactly_one_internal_turn(tmp_path):
    async with goal_world(tmp_path / "continue.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        queue = FakeQueue()
        service = GoalService(store, queue=queue)
        decision = await service.on_turn_finished(finished(turn, run_id))
        assert decision.action == "continue" and decision.queued
        assert len(queue.requests) == 1
        request = queue.requests[0]
        assert request.internal is True
        assert request.turn_request_key.startswith(CONTINUE_TURN_PREFIX)
        assert request.origin_turn_id == turn.id and request.origin_run_id == run_id
        assert request.goal_revision == 1
        assert "不是新的用户消息" in request.text
        assert f"goal_id={request.goal_id}" in request.text
        assert f"revision={request.goal_revision}" in request.text

        # No durability yet that a second, later turn is consumed: a new finish for the
        # same turn/source is the same idempotent key, not a second queued turn.
        again = await service.on_turn_finished(
            finished(turn, run_id),
        )
        assert again.action == "continue"
        assert again.continue_key == decision.continue_key
        assert queue.requests[-1].turn_request_key == request.turn_request_key


@pytest.mark.asyncio
async def test_restart_replays_the_same_idempotent_continue_request(tmp_path):
    path = tmp_path / "restart.sqlite"
    async with goal_world(path) as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        queue = FakeQueue()
        first = await GoalService(store, queue=queue).on_turn_finished(finished(turn, run_id))
    # Reopen the same database: no duplicate queue row, identical continue key.
    reopened = await SQLiteStore.open(path)
    async with reopened.lock, reopened.engine.begin() as conn:
        await conn.run_sync(lambda sync: metadata.create_all(sync, tables=list(GOAL_TABLES)))
    try:
        queue = FakeQueue()
        second = await GoalService(reopened, queue=queue).on_turn_finished(finished(turn, run_id))
        assert second.continue_key == first.continue_key
        assert queue.requests[0].turn_request_key == first.request.turn_request_key
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_completed_cancelled_and_paused_goals_do_not_continue(tmp_path):
    for action, expected in (("cancel", "goal_cancelled"), ("pause", "goal_paused")):
        async with goal_world(tmp_path / f"{action}.sqlite") as (store, world):
            owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
            await create_goal(store, owner, session, turn, run_id)
            await GoalService(store).control(
                owner,
                WORKSPACE,
                session.id,
                GoalControlRequest(request_key=f"{action}-1", expected_revision=1),
                action=action,
            )
            queue = FakeQueue()
            decision = await GoalService(store, queue=queue).on_turn_finished(
                finished(turn, run_id)
            )
            assert decision.action == "stop" and decision.reason == expected
            assert queue.requests == []

    async with goal_world(tmp_path / "complete.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        await GoalService(store).update_from_tool(
            SessionToolScope(owner, WORKSPACE, session.id, turn.id, run_id),
            GoalUpdate(
                request_key="done",
                expected_revision=1,
                action="complete",
                evidence_refs=({"kind": "test", "ref": "pytest:all"},),
            ),
        )
        queue = FakeQueue()
        decision = await GoalService(store, queue=queue).on_turn_finished(finished(turn, run_id))
        assert decision.reason == "goal_completed" and queue.requests == []


@pytest.mark.asyncio
async def test_waiting_approval_closed_and_archived_sessions_do_not_continue(tmp_path):
    async with goal_world(tmp_path / "park.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        service = GoalService(store, queue=FakeQueue())
        for flags, reason in (
            ({"parked": True}, "approval_pending"),
            ({"approval_pending": True}, "approval_pending"),
            ({"status": "awaiting"}, "approval_pending"),
            ({"pending_interaction": True}, "waiting_input"),
            ({"stop_requested": True}, "user_stopped"),
            ({"code": "cancelled"}, "user_stopped"),
            ({"status": "interrupted"}, "run_interrupted"),
            ({"code": "interrupted"}, "run_interrupted"),
            ({"status": "running"}, "already_finished"),
        ):
            decision = await service.on_turn_finished(finished(turn, run_id, **flags))
            assert decision.action == "stop" and decision.reason == reason, flags

        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(sessions).where(sessions.c.id == session.id).values(status="closed")
            )
        assert (await service.on_turn_finished(finished(turn, run_id))).reason == "session_closed"

        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(sessions).where(sessions.c.id == session.id).values(status="open")
            )
            await conn.execute(
                session_lifecycle.insert().values(
                    session_id=session.id,
                    archived_at=1.0,
                    deleted_at=None,
                    updated_at=1.0,
                    revision=1,
                )
            )
        assert (await service.on_turn_finished(finished(turn, run_id))).reason == "session_archived"


@pytest.mark.asyncio
async def test_new_user_message_wins_over_continuation(tmp_path):
    async with goal_world(tmp_path / "newer.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        await new_turn(store, session.id, "user-2", "新问题", status="queued")
        queue = FakeQueue()
        service = GoalService(store, queue=queue)
        decision = await service.on_turn_finished(finished(turn, run_id))
        assert decision.reason == "user_message_pending" and queue.requests == []

        # Host also supersedes any already queued internal continuation.
        assert await service.supersede_continuations(owner, WORKSPACE, session.id) == 0


@pytest.mark.asyncio
async def test_approval_expired_blocks_instead_of_re_asking(tmp_path):
    async with goal_world(tmp_path / "approval-expired.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        queue = FakeQueue()
        service = GoalService(store, queue=queue)
        decision = await service.on_turn_finished(
            finished(turn, run_id, status="failed", code="approval_expired")
        )
        assert decision.action == "stop" and decision.reason == "approval_expired"
        assert queue.requests == []
        snapshot = await service.snapshot(owner, WORKSPACE, session.id)
        assert snapshot.status == "blocked"
        assert "审批窗口已过期" in (snapshot.blocked_reason or "")

        # Replaying the same terminal scan is stable: still blocked, nothing queued.
        again = await service.on_turn_finished(
            finished(turn, run_id, status="failed", code="approval_expired")
        )
        assert again.reason == "goal_blocked" and queue.requests == []

        # The owner resume path is the only way forward, and it requeues through
        # the same idempotent protocol instead of re-asking inside a run.
        resumed = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="resume", expected_revision=snapshot.revision),
            action="resume",
        )
        assert resumed.status == "active"
        assert queue.requests[0].turn_request_key.startswith("goal-resume:")


@pytest.mark.asyncio
async def test_approval_denied_stops_without_retry_or_blocking(tmp_path):
    async with goal_world(tmp_path / "approval-denied.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        queue = FakeQueue()
        service = GoalService(store, queue=queue)
        decision = await service.on_turn_finished(
            finished(turn, run_id, status="failed", code="approval_denied")
        )
        assert decision.action == "stop" and decision.reason == "user_stopped"
        assert queue.requests == []
        snapshot = await service.snapshot(owner, WORKSPACE, session.id)
        # The user's refusal is not a stall: the goal stays active for a different
        # approach, and no continuation is enqueued behind their back.
        assert snapshot.status == "active"


@pytest.mark.asyncio
async def test_permission_loss_and_missing_goal_stop_without_queueing(tmp_path):
    async with goal_world(tmp_path / "permission.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        queue = FakeQueue()
        service = GoalService(store, queue=queue)
        stranger = member("stranger", workspace_id="elsewhere")
        decision = await service.on_turn_finished(finished(turn, run_id, owner=stranger))
        assert decision.reason == "permission_denied" and queue.requests == []

        # No goal at all: stop, never fabricate one.
        other = await session_sql.create_session(
            store,
            WORKSPACE,
            owner.principal_id,
            SessionCreate(request_key="other"),
            {"key": "demo"},
            ContextPolicy(),
            Settings(),
        )
        no_goal_turn, no_goal_run = await new_turn(store, other.id, "user-x", "无目标")
        decision = await service.on_turn_finished(
            finished(no_goal_turn, no_goal_run, status="completed")
        )
        assert decision.reason == "no_goal" and queue.requests == []


@pytest.mark.asyncio
async def test_internal_turn_is_settled_and_keeps_user_binding(tmp_path):
    async with goal_world(tmp_path / "internal.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        queue = FakeQueue()
        service = GoalService(store, queue=queue)
        decision = await service.on_turn_finished(finished(turn, run_id))
        assert decision.action == "continue"
        request = queue.requests[0]

        internal_turn, internal_run = await new_turn(
            store, session.id, request.turn_request_key, request.text, status="completed"
        )
        # The internal turn does not create or replace any goal through its own text.
        progressed = await service.update_from_tool(
            SessionToolScope(owner, WORKSPACE, session.id, internal_turn.id, internal_run),
            GoalUpdate(request_key="progress-internal", expected_revision=1, action="progress"),
        )
        assert progressed.origin_turn_id == turn.id
        with pytest.raises(ServiceError, match="invalid_input"):
            await service.update_from_tool(
                SessionToolScope(owner, WORKSPACE, session.id, internal_turn.id, internal_run),
                GoalUpdate(
                    request_key="set-internal",
                    expected_revision=2,
                    action="set",
                    objective="内部自建",
                ),
            )
        second = await service.on_turn_finished(finished(internal_turn, internal_run))
        assert second.action == "continue"
        assert second.request.origin_turn_id == turn.id
        assert second.goal_revision == 2
        latest = await service.continuation_by_key(
            owner, WORKSPACE, session.id, request.continue_key
        )
        assert latest.status == "consumed" and latest.progressed is True
        assert latest.outcome == "completed" and latest.turn_id == internal_turn.id


@pytest.mark.asyncio
async def test_repeated_failure_without_progress_becomes_blocked(tmp_path):
    async with goal_world(tmp_path / "failure.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        queue = FakeQueue()
        service = GoalService(store, queue=queue)

        first = await service.on_turn_finished(finished(turn, run_id))
        assert first.action == "continue"
        internal_one, run_one = await new_turn(
            store, session.id, first.request.turn_request_key, "续跑一", status="failed"
        )
        second = await service.on_turn_finished(
            finished(internal_one, run_one, status="failed", code="tool_error")
        )
        # First failure without progress still retries once.
        assert second.action == "continue"
        internal_two, run_two = await new_turn(
            store, session.id, second.request.turn_request_key, "续跑二", status="failed"
        )
        third = await service.on_turn_finished(
            finished(internal_two, run_two, status="failed", code="tool_error")
        )
        assert third.action == "stop" and third.reason == "repeated_failure"
        snapshot = await service.snapshot(owner, WORKSPACE, session.id)
        assert snapshot.status == "blocked"
        assert "连续2次相同失败" in (snapshot.blocked_reason or "")

        # The owner can adjust the goal: block -> paused -> replaced.
        paused = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="pause", expected_revision=snapshot.revision),
            action="pause",
        )
        assert paused.status == "paused"
        adjusted = await service.create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="adjusted",
                expected_revision=paused.revision,
                objective="调整后的目标",
                completion_criteria=("新条件",),
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        assert adjusted.status == "active" and adjusted.objective == "调整后的目标"


@pytest.mark.asyncio
async def test_resume_queues_through_the_same_idempotent_protocol(tmp_path):
    async with goal_world(tmp_path / "resume.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        queue = FakeQueue()
        service = GoalService(store, queue=queue)
        paused = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="pause", expected_revision=1),
            action="pause",
        )
        assert paused.status == "paused" and queue.requests == []
        resumed = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="resume", expected_revision=2),
            action="resume",
        )
        assert resumed.status == "active"
        assert len(queue.requests) == 1
        request = queue.requests[0]
        assert request.turn_request_key.startswith("goal-resume:")
        assert request.origin_turn_id == turn.id and request.internal is True
        # Same-request replay (retry after a crash) reuses the same durable row.
        replay = await service.control(
            owner,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="resume", expected_revision=2),
            action="resume",
        )
        assert replay == resumed
        # A manifest replay may re-emit the same idempotent request; the host queue
        # deduplicates by turn request key, so no second internal turn exists.
        assert {item.turn_request_key for item in queue.requests} == {request.turn_request_key}

        internal, internal_run = await new_turn(
            store, session.id, request.turn_request_key, request.text, status="completed"
        )
        decision = await service.on_turn_finished(finished(internal, internal_run))
        assert decision.action == "continue"
        settled = await service.continuation_by_key(
            owner, WORKSPACE, session.id, request.continue_key
        )
        assert settled.status == "consumed"


@pytest.mark.asyncio
async def test_queue_failure_is_recorded_and_not_retried_silently(tmp_path):
    async with goal_world(tmp_path / "queue-fail.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        service = GoalService(store, queue=FakeQueue(fail=True))
        decision = await service.on_turn_finished(finished(turn, run_id))
        assert decision.action == "continue"
        assert decision.queued is False and decision.reason == "enqueue_failed"
        latest = await service.latest_continuation(owner, WORKSPACE, session.id)
        assert latest.status == "failed" and latest.code == "enqueue_failed"


@pytest.mark.asyncio
async def test_no_queue_port_stops_with_explicit_reason(tmp_path):
    async with goal_world(tmp_path / "no-queue.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        decision = await GoalService(store).on_turn_finished(finished(turn, run_id))
        assert decision.action == "continue" and decision.queued is False
        assert decision.reason == "queue_unavailable"


@pytest.mark.asyncio
async def test_pending_interaction_pending_continuation_and_check_cancel(tmp_path):
    async with goal_world(tmp_path / "pending.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)

        def cancelled() -> None:
            raise ServiceError("cancelled")

        context = finished(turn, run_id, check=cancelled)
        with pytest.raises(ServiceError, match="cancelled"):
            await GoalService(store, queue=FakeQueue()).on_turn_finished(context)

        queue = FakeQueue()
        service = GoalService(store, queue=queue)
        first = await service.on_turn_finished(finished(turn, run_id))
        assert first.action == "continue"
        # A second source turn while the first internal turn is still queued does not
        # enqueue another: host keeps a single inert queue entry.
        other_turn, other_run = await new_turn(store, session.id, "user-2", "另一轮")
        decision = await service.on_turn_finished(finished(other_turn, other_run))
        assert decision.reason == "continuation_pending"
        assert len(queue.requests) == 1

        assert await service.supersede_continuations(owner, WORKSPACE, session.id) == 1


@pytest.mark.asyncio
async def test_long_goal_is_referenced_not_copied_or_truncated(tmp_path):
    """No app-side prompt clipping: the prompt references the live snapshot by id/revision."""
    async with goal_world(tmp_path / "bounded.sqlite") as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        objective = "目标" + "长" * 40_000
        criteria = ("条件" + "细" * 20_000,)
        created = await GoalService(store).create(
            owner,
            WORKSPACE,
            session.id,
            GoalCreateRequest(
                request_key="create",
                expected_revision=0,
                objective=objective,
                completion_criteria=criteria,
                origin_turn_id=turn.id,
                origin_run_id=run_id,
            ),
        )
        # The prompt stays small, identifies the goal, and copies neither field.
        text = continuation_text(created)
        assert len(text.encode()) < 2048
        assert f"goal_id={created.goal_id}" in text
        assert f"revision={created.revision}" in text
        assert "长" * 100 not in text and "细" * 100 not in text

        queue = FakeQueue()
        await GoalService(store, queue=queue).on_turn_finished(finished(turn, run_id))
        assert queue.requests[0].text == text

        # Full goal state remains intact in the authoritative snapshot / context port.
        snapshot = await GoalService(store).snapshot(owner, WORKSPACE, session.id)
        assert snapshot.objective == objective
        assert snapshot.completion_criteria == criteria
        items = await GoalService(store).projection(
            SessionToolScope(owner, WORKSPACE, session.id, turn.id, run_id)
        )
        assert len(items) == 1
        assert items[0]["source_refs"][0]["content_hash"] == snapshot.content_hash
        assert len(items[0]["content"]) > 60_000


@pytest.mark.asyncio
async def test_created_goal_survives_service_restart_with_full_text(tmp_path):
    path = tmp_path / "restart-long.sqlite"
    objective = "完整目标" * 10_000
    async with goal_world(path) as (store, world):
        owner, session, (turn, run_id) = world["owner"], world["session"], world["turn"]
        await create_goal(store, owner, session, turn, run_id)
        await GoalService(store).update_from_tool(
            SessionToolScope(owner, WORKSPACE, session.id, turn.id, run_id),
            GoalUpdate(
                request_key="rename-via-create-blocked",
                expected_revision=1,
                action="progress",
                note=objective,
            ),
        )
    reopened = await SQLiteStore.open(path)
    try:
        snapshot = await GoalService(reopened).snapshot(owner, WORKSPACE, session.id)
        assert snapshot.status_note == objective
        assert snapshot.revision == 2
    finally:
        await reopened.close()
