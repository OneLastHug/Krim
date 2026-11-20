"""Session dispatcher integration for durable goal continuation intents."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.goals.continuation import (
    CONTINUE_TURN_PREFIX,
    ContinuationRequest,
    TurnFinishedContext,
)
from krim_agent.goals.contracts import GoalControlRequest, GoalCreateRequest, GoalUpdate
from tests.support.fake_provider import FakeProvider
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage.goal_models import goal_continuations
from krim_agent.tools.session import SessionToolScope

WORKSPACE = "local"
WAIT_SECONDS = 30


class GatedProvider(FakeProvider):
    """Offline script provider with controllable internal and capacity-blocked turns."""

    def __init__(self) -> None:
        super().__init__(delay=0)
        self.internal_started = asyncio.Event()
        self.release_internal = asyncio.Event()
        self.hold_started = asyncio.Event()
        self.release_hold = asyncio.Event()

    async def stream(self, messages, tools) -> Any:
        prompt = messages[-1].content
        if prompt.startswith("内部续跑（宿主自动触发）"):
            self.internal_started.set()
            await self.release_internal.wait()
        elif prompt == "/hold-slot":
            self.hold_started.set()
            await self.release_hold.wait()
            from krim_agent.providers.types import ModelText

            yield ModelText(text="slot released")
            return
        async for event in super().stream(messages, tools):
            yield event


@asynccontextmanager
async def scheduler_application(
    path: Path,
    *,
    provider: GatedProvider | None = None,
    max_active_runs: int | None = None,
    bind_goals: bool = True,
):
    selected = provider or GatedProvider()
    settings = Settings(
        database=path,
        max_active_runs=max_active_runs,
        max_request_bytes=262144,
        run_timeout=30,
    )
    app = await open_application(path, settings=settings, provider=selected)
    try:
        assert app.sessions is not None and app.goals is not None
        if bind_goals:
            app.sessions.goals = app.goals
            app.goals.queue = app.sessions
            app.service.slot_changed.set()
        else:
            app.sessions.goals = None
            app.goals.queue = None
        yield app, selected
    finally:
        # Unblock providers before shutdown; active runs are otherwise cancelled by close.
        selected.release_internal.set()
        selected.release_hold.set()
        await app.close()
        await selected.close()


async def create_session(app, key: str):
    return await app.sessions.create(LOCAL, WORKSPACE, SessionCreate(request_key=key))


async def submit(app, session, key: str, text: str):
    current = await app.sessions.get(LOCAL, WORKSPACE, session.id, write=True)
    return await app.sessions.submit(
        LOCAL,
        WORKSPACE,
        session.id,
        TurnRequest(request_key=key, text=text, expected_revision=current.revision),
    )


async def complete_turn(app, session, key: str, text: str):
    turn = await submit(app, session, key, text)
    async with asyncio.timeout(WAIT_SECONDS):
        turn = await app.sessions.wait(LOCAL, WORKSPACE, session.id, turn.id)
    assert turn.status == "completed"
    assert turn.run_id is not None
    return turn


async def create_goal(app, session, origin, *, key: str = "goal-create"):
    return await app.goals.create(
        LOCAL,
        WORKSPACE,
        session.id,
        GoalCreateRequest(
            request_key=key,
            expected_revision=0,
            objective="完成会话目标",
            completion_criteria=("实际完成并验证",),
            origin_turn_id=origin.id,
            origin_run_id=origin.run_id,
        ),
    )


async def wait_until(awaitable_predicate):
    async with asyncio.timeout(WAIT_SECONDS):
        while True:
            value = await awaitable_predicate()
            if value:
                return value
            await asyncio.sleep(0.01)


async def session_turns(app, session_id: str):
    return await app.sessions.turns(LOCAL, WORKSPACE, session_id)


async def wait_for_internal(app, session_id: str, *, status: str | None = None):
    async def ready():
        matches = [
            turn
            for turn in await session_turns(app, session_id)
            if turn.request_key.startswith(CONTINUE_TURN_PREFIX)
            and (status is None or turn.status == status)
        ]
        return matches

    return await wait_until(ready)


def finished_context(session, turn) -> TurnFinishedContext:
    assert turn.run_id is not None
    return TurnFinishedContext(
        actor=LOCAL,
        workspace_id=WORKSPACE,
        session_id=session.id,
        turn_id=turn.id,
        run_id=turn.run_id,
        status="completed",
    )


async def stored_payload(app, session_id: str, continue_key: str) -> dict[str, Any]:
    async def read(conn):
        return await conn.scalar(
            select(goal_continuations.c.payload).where(
                goal_continuations.c.session_id == session_id,
                goal_continuations.c.request_key == continue_key,
            )
        )

    return await app.store.work_read(read)


@pytest.mark.asyncio
async def test_dispatcher_enqueues_exact_intent_once_and_runs_it_normally(tmp_path, monkeypatch):
    provider = GatedProvider()
    async with scheduler_application(tmp_path / "dispatch.sqlite", provider=provider) as (app, _):
        session = await create_session(app, "dispatch-session")
        origin = await complete_turn(app, session, "dispatch-origin", "完成原始任务")

        # The public input surface cannot mint a host-internal request key.
        with pytest.raises(ServiceError, match="invalid_input"):
            await app.sessions.submit(
                LOCAL,
                WORKSPACE,
                session.id,
                TurnRequest(
                    request_key=CONTINUE_TURN_PREFIX + "forged",
                    text="伪造的内部续跑",
                    expected_revision=1,
                ),
            )

        callbacks = []
        original = app.goals.on_turn_finished

        async def observe(context):
            callbacks.append(context)
            return await original(context)

        monkeypatch.setattr(app.goals, "on_turn_finished", observe)
        await create_goal(app, session, origin)
        await asyncio.wait_for(provider.internal_started.wait(), WAIT_SECONDS)

        # The dispatcher called the terminal-turn hook and produced a regular queued turn.
        assert [item.turn_id for item in callbacks].count(origin.id) == 1
        internal = await wait_for_internal(app, session.id)
        assert len(internal) == 1
        payload = await stored_payload(app, session.id, internal[0].request_key.removeprefix(
            CONTINUE_TURN_PREFIX
        ))
        request = ContinuationRequest.model_validate(payload)
        assert payload == request.model_dump(mode="json")
        assert request.internal is True
        assert request.turn_request_key == internal[0].request_key
        assert request.origin_turn_id == origin.id
        assert request.origin_run_id == origin.run_id
        assert request.source_turn_id == origin.id

        # Replaying both host entry points must resolve to the same durable turn.
        replay = await app.goals.on_turn_finished(finished_context(session, origin))
        assert replay.request == request
        assert replay.queued is True
        await app.sessions.enqueue(request)
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await app.sessions.enqueue(
                request.model_copy(update={"text": request.text + " tampered"})
            )
        assert len(await wait_for_internal(app, session.id)) == 1

        paused = await app.goals.control(
            LOCAL,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="pause-after-dispatch", expected_revision=1),
            action="pause",
        )
        assert paused.status == "paused"
        await app.sessions.enqueue(request)  # Known intent/turn replay doesn't enqueue after pause.
        assert len(await wait_for_internal(app, session.id)) == 1
        stopped = await asyncio.wait_for(
            app.sessions.wait(LOCAL, WORKSPACE, session.id, internal[0].id),
            WAIT_SECONDS,
        )
        assert stopped.status == "cancelled" and stopped.code == "cancelled"
        provider.release_internal.set()
        assert len(await session_turns(app, session.id)) == 2


@pytest.mark.parametrize("change", ["pause", "cancel", "revision"])
@pytest.mark.asyncio
async def test_stale_goal_intent_is_blocked_before_dispatch(tmp_path, change):
    provider = GatedProvider()
    async with scheduler_application(
        tmp_path / f"stale-{change}.sqlite", provider=provider, max_active_runs=1
    ) as (app, _):
        session = await create_session(app, "stale-session")
        origin = await complete_turn(app, session, "stale-origin", "建立目标来源")

        blocker = await create_session(app, "capacity-blocker")
        await submit(app, blocker, "capacity-hold", "/hold-slot")
        await asyncio.wait_for(provider.hold_started.wait(), WAIT_SECONDS)

        await create_goal(app, session, origin)
        queued = await wait_for_internal(app, session.id, status="queued")
        assert len(queued) == 1
        assert not provider.internal_started.is_set()

        if change in {"pause", "cancel"}:
            changed = await app.goals.control(
                LOCAL,
                WORKSPACE,
                session.id,
                GoalControlRequest(request_key=f"goal-{change}", expected_revision=1),
                action=change,
            )
        else:
            changed = await app.goals.update_from_tool(
                SessionToolScope(LOCAL, WORKSPACE, session.id, origin.id, origin.run_id),
                GoalUpdate(
                    request_key="goal-new-revision",
                    expected_revision=1,
                    action="progress",
                    note="修订目标进度",
                ),
            )
        assert changed.revision == 2
        latest = await app.goals.latest_continuation(LOCAL, WORKSPACE, session.id)
        assert latest is not None and latest.status == "superseded"

        async def stale_cancelled():
            turns = await session_turns(app, session.id)
            return next((turn for turn in turns if turn.id == queued[0].id
                         and turn.status == "cancelled"), None)

        assert await wait_until(stale_cancelled)
        assert not provider.internal_started.is_set()
        provider.release_hold.set()
        blocker_turn = (await session_turns(app, blocker.id))[0]
        assert (await app.sessions.wait(LOCAL, WORKSPACE, blocker.id, blocker_turn.id)).status == (
            "completed"
        )
        assert not provider.internal_started.is_set()


@pytest.mark.asyncio
async def test_new_user_turn_supersedes_queued_goal_turn_and_keeps_priority(tmp_path):
    provider = GatedProvider()
    async with scheduler_application(
        tmp_path / "user-priority.sqlite", provider=provider, max_active_runs=1
    ) as (app, _):
        session = await create_session(app, "priority-session")
        origin = await complete_turn(app, session, "priority-origin", "已有目标来源")

        blocker = await create_session(app, "priority-blocker")
        await submit(app, blocker, "priority-hold", "/hold-slot")
        await asyncio.wait_for(provider.hold_started.wait(), WAIT_SECONDS)

        await create_goal(app, session, origin)
        internal = await wait_for_internal(app, session.id, status="queued")
        assert len(internal) == 1

        user_turn = await submit(app, session, "priority-user", "用户的新指令")
        latest = await app.goals.latest_continuation(LOCAL, WORKSPACE, session.id)
        assert latest is not None and latest.status == "superseded"
        internal_after = next(
            turn for turn in await session_turns(app, session.id) if turn.id == internal[0].id
        )
        assert internal_after.status == "cancelled"
        assert user_turn.status == "queued"

        # Pausing prevents a later automatic continuation from obscuring user priority.
        await app.goals.control(
            LOCAL,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="pause-after-user", expected_revision=1),
            action="pause",
        )
        provider.release_hold.set()
        blocker_turn = (await session_turns(app, blocker.id))[0]
        await app.sessions.wait(LOCAL, WORKSPACE, blocker.id, blocker_turn.id)
        accepted = await app.sessions.wait(LOCAL, WORKSPACE, session.id, user_turn.id)
        assert accepted.status == "completed"
        assert not provider.internal_started.is_set()
        assert len([turn for turn in await session_turns(app, session.id)
                    if turn.request_key.startswith(CONTINUE_TURN_PREFIX)]) == 1


@pytest.mark.asyncio
async def test_restart_recovers_durable_goal_intent_once_without_lock_reentry(tmp_path):
    path = tmp_path / "restart-intent.sqlite"
    first_provider = GatedProvider()
    async with scheduler_application(path, provider=first_provider, bind_goals=False) as (first, _):
        session = await create_session(first, "restart-session")
        origin = await complete_turn(first, session, "restart-origin", "恢复前来源")
        await create_goal(first, session, origin)
        intent = await first.goals.on_turn_finished(finished_context(session, origin))
        assert intent.request is not None
        assert intent.queued is False
        assert intent.reason == "queue_unavailable"
        request = intent.request

    provider = GatedProvider()
    async with scheduler_application(path, provider=provider) as (reopened, _):
        session = await reopened.sessions.get(LOCAL, WORKSPACE, session.id)
        # Startup recovery calls the goal callback outside _input_lock; enqueue reacquires it.
        await asyncio.wait_for(provider.internal_started.wait(), WAIT_SECONDS)
        internal = await wait_for_internal(reopened, session.id)
        assert len(internal) == 1
        assert internal[0].request_key == request.turn_request_key
        assert await stored_payload(reopened, session.id, request.continue_key) == (
            request.model_dump(mode="json")
        )

        await reopened.sessions.enqueue(request)
        assert len(await wait_for_internal(reopened, session.id)) == 1
        await reopened.goals.control(
            LOCAL,
            WORKSPACE,
            session.id,
            GoalControlRequest(request_key="pause-after-restart", expected_revision=1),
            action="pause",
        )
        # Pausing stops an already running continuation as well as future ones.
        stopped = await asyncio.wait_for(
            reopened.sessions.wait(LOCAL, WORKSPACE, session.id, internal[0].id),
            WAIT_SECONDS,
        )
        assert stopped.status == "cancelled" and stopped.code == "cancelled"
        provider.release_internal.set()
        assert len(await session_turns(reopened, session.id)) == 2
