"""会话输入停靠与运行时收敛集成：真实SQLite、RunService、SDK循环与调度器。

验证D范围行为：答复/取消提交后的停靠唤醒（含通知丢失）、重启后已答复
问题的调度器收敛、输入与审批混合批次全部等待、重复答复不重复执行、
新消息接纳后取代旧问题并取消旧等待运行、被拒提交或旧提交重放不动
当前问题、被取代运行的reconcile收敛不续跑。
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from krim_agent.api.interaction import mount_interaction
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, JsonValue, Message, ServiceError
from krim_agent.interaction.contracts import Answer, AnswerRequest, CancelRequest
from krim_agent.providers.types import ModelEvent, ModelText, ModelToolCall
from krim_agent.security import LOCAL
from krim_agent.sessions.types import ControlRequest, SessionCreate, TurnRequest
from tests.support.builtins import builtin_registry, demo_policy
from krim_agent.tools.core import Invocation


class ScriptedInputProvider:
    """Deterministic SDK host provider: ask once for `/ask ...`, then finish."""

    mode = "scripted-input"

    def __init__(self):
        self.requests: list[tuple[Message, ...]] = []
        self.closed = False

    async def stream(
        self, messages: Sequence[Message], _tools: list[dict[str, JsonValue]]
    ) -> AsyncIterator[ModelEvent]:
        self.requests.append(tuple(messages))
        if messages[-1].role == "user" and messages[-1].content.startswith("/ask"):
            question_id = messages[-1].content[4:].strip() or "q1"
            yield ModelToolCall(
                id=f"ask-{len(self.requests)}",
                name="ask_user",
                arguments={"questions": [{
                    "id": question_id,
                    "prompt": "Question " + question_id,
                    "mode": "text",
                    "options": [],
                    "allow_text": True,
                }]},
            )
        else:
            yield ModelText(text="done")

    async def close(self):
        self.closed = True


@pytest_asyncio.fixture
async def wire():
    value = ScriptedInputProvider()
    yield value
    await value.close()


def app_settings():
    return Settings(
        run_timeout=None,
        tool_timeout=30,
        approval_timeout=60,
        max_input_bytes=256,
        max_request_bytes=262144,
    )


@asynccontextmanager
async def interaction_client(app):
    api = FastAPI()
    api.state.application = app

    @api.middleware("http")
    async def local_identity(request, call_next):
        request.state.principal = LOCAL
        return await call_next(request)

    mount_interaction(api)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://test"
    ) as client:
        yield client


async def session_input_policy(invocation):
    if invocation.name == "ask_user":
        return "allow"
    return await demo_policy(invocation)


@asynccontextmanager
async def input_application(provider, tmp_path, name, *, policy=session_input_policy):
    app = None
    try:
        app = await open_application(
            Path(tmp_path) / name,
            settings=app_settings(),
            provider=provider,
            registry=builtin_registry(),
            policy=policy,
        )
        yield app
    finally:
        if app is not None:
            await app.close()


async def submit_turn(app, session, text, key):
    current = await app.sessions.get(LOCAL, "local", session.id)
    return await app.sessions.submit(
        LOCAL, "local", session.id,
        TurnRequest(text=text, request_key=key, expected_revision=current.revision),
    )


async def wait_input_park(app, session, turn):
    """等待turn的运行停靠并返回(run_id, 待答问题行)。"""
    async with asyncio.timeout(60):
        while True:
            current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
            if current.run_id:
                view = await app.service.get(current.run_id)
                if view.status == "awaiting":
                    state = await app.interactions.state(LOCAL, "local", session.id)
                    if state["pending"]:
                        return current.run_id, state["pending"][0]
                task = app.service.tasks.get(current.run_id)
                if task is not None and task.done() and view.status != "awaiting":
                    events = await app.service.events(current.run_id)
                    detail = [(event.kind, event.data.get("code")) for event in events]
                    raise AssertionError(
                        f"run ended before park: {view.status}/{view.code}; events={detail}"
                    )
            await asyncio.sleep(0.01)


async def wait_run(app, run_id, status):
    async with asyncio.timeout(60):
        while True:
            view = await app.service.get(run_id)
            if view.status == status:
                return view
            task = app.service.tasks.get(run_id)
            if (task is not None and task.done()
                    and view.status not in {"running", "awaiting", status}):
                raise AssertionError(f"run ended as {view.status}/{view.code}, wanted {status}")
            await asyncio.sleep(0.01)


def answer_body(row, key, text="integration answer"):
    return AnswerRequest(
        request_key=key,
        expected_revision=row["revision"],
        answers=[Answer(question_id=question["id"], text=text)
                 for question in row["payload"]["questions"]],
    )


async def events_of(app, run_id):
    return list(await app.service.events(run_id))


async def test_answer_wakes_parked_run_and_completes_once(tmp_path, wire):
    async with input_application(wire, tmp_path, "answer-wake.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, turn)
        assert len(wire.requests) == 1
        assert not [event for event in await events_of(app, run_id) if event.kind == "tool_end"]

        async with interaction_client(app) as client:
            answered_response = await client.post(
                f"/v2/workspaces/local/sessions/{session.id}/interactions/{row['id']}/answer",
                json=answer_body(row, "answer-1").model_dump(mode="json"),
            )
        assert answered_response.status_code == 200
        assert answered_response.json()["status"] == "answered"
        finished = await wait_run(app, run_id, "completed")
        assert finished.code is None
        assert len(wire.requests) == 2  # 停靠步 + 恢复收尾步
        end_events = [event for event in await events_of(app, run_id) if event.kind == "tool_end"]
        assert len(end_events) == 1 and end_events[0].data["name"] == "ask_user"
        replies = [
            event for event in await events_of(app, run_id)
            if event.kind == "user_input_reply"
        ]
        assert len(replies) == 1 and replies[0].data["status"] == "answered"
        assert (await app.interactions.state(LOCAL, "local", session.id))["pending"] == []
        saved = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert saved.status == "completed"


async def test_answer_survives_lost_wake_up_notification(tmp_path, wire):
    async def broken_notify(_result):
        raise RuntimeError("wake-up unavailable")

    async with input_application(wire, tmp_path, "notify-race.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, turn)
        app.interactions.on_answer = broken_notify
        result = await app.interactions.answer(
            LOCAL, "local", session.id, row["id"], answer_body(row, "answer-1")
        )
        assert result["status"] == "answered"
        await wait_run(app, run_id, "completed")
        assert (await app.interactions.state(LOCAL, "local", session.id))["pending"] == []


async def test_reopen_recovers_committed_answer_without_new_user_action(tmp_path, wire):
    async with input_application(wire, tmp_path, "restart-recovery.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, turn)
        # 模拟答复提交后、唤醒前宿主崩溃：关闭调度收敛，只留持久答复。
        app.service.interactions = None
        answered = await app.interactions.answer(
            LOCAL, "local", session.id, row["id"], answer_body(row, "answer-1")
        )
        assert answered["status"] == "answered"
        await asyncio.sleep(1.5)  # 超过一个调度周期：本实例不得自行唤醒
        assert (await app.service.get(run_id)).status == "awaiting"
        requests_before = len(wire.requests)
    async with input_application(wire, tmp_path, "restart-recovery.sqlite") as reopened:
        view = await wait_run(reopened, run_id, "completed")
        assert view.code is None
        assert len(wire.requests) == requests_before + 1  # 只补收尾步，不重放停靠步
        assert (await reopened.interactions.state(LOCAL, "local", session.id))["pending"] == []


async def test_input_waits_for_all_requests_in_mixed_parked_batch(tmp_path, wire):
    async with input_application(wire, tmp_path, "mixed-batch.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, turn)

        # Add the sibling durable approval from the same parked batch. The SDK
        # checkpoint remains real; the synthetic sibling isolates the invariant
        # that neither answer path may resume while either persisted wait is open.
        echo = app.service.bindings["demo"].loop.executor.registry.get("echo")
        invocation = Invocation(
            run_id=run_id,
            call_id="sibling-approval-call",
            span_id="sibling-approval-span",
            step_index=2,
            name="echo",
            arguments_json='{"text":"approval sibling"}',
            tool_hash=echo.fingerprint,
        )
        approval = await app.service._approval_request(invocation, None)
        await app.store.append(run_id, "approval_request", {**approval, "hook_id": "sibling"})
        approved = await app.service.reply_approval(
            run_id,
            approval["request_id"],
            ApprovalReply(
                request_key="approval-1",
                call_id=approval["call_id"],
                arguments_hash=approval["arguments_hash"],
                tool_hash=approval["tool_hash"],
                allow=True,
            ),
        )
        assert approved["replayed"] is False
        assert (await app.service.get(run_id)).status == "awaiting"
        assert len(wire.requests) == 1  # 批次中的输入尚未答复，不能先恢复模型
        assert not [
            event for event in await events_of(app, run_id)
            if event.kind == "tool_end" and event.data.get("name") == "echo"
        ]

        answered = await app.interactions.answer(
            LOCAL, "local", session.id, row["id"], answer_body(row, "answer-1")
        )
        assert answered["status"] == "answered"
        finished = await wait_run(app, run_id, "completed")
        assert finished.code is None
        assert len(wire.requests) == 2
        replies = [event for event in await events_of(app, run_id)
                   if event.kind == "user_input_reply"]
        assert len(replies) == 1 and replies[0].data["status"] == "answered"
        approval_replies = [event for event in await events_of(app, run_id)
                            if event.kind == "approval_reply"]
        assert len(approval_replies) == 1



async def test_duplicate_answer_is_idempotent_and_executes_once(tmp_path, wire):
    async with input_application(wire, tmp_path, "duplicate-answer.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, turn)
        body = answer_body(row, "answer-1")
        first = await app.interactions.answer(LOCAL, "local", session.id, row["id"], body)
        replay = await app.interactions.answer(LOCAL, "local", session.id, row["id"], body)
        assert replay == first
        await wait_run(app, run_id, "completed")
        assert len(wire.requests) == 2
        ends = [event for event in await events_of(app, run_id) if event.kind == "tool_end"]
        assert len(ends) == 1 and ends[0].data["name"] == "ask_user"
        replies = [event for event in await events_of(app, run_id)
                   if event.kind == "user_input_reply"]
        assert len(replies) == 1
        assert (await app.service.get(run_id)).status == "completed"


async def test_new_message_supersedes_old_question_and_cancels_old_run(tmp_path, wire):
    async with input_application(wire, tmp_path, "supersede-message.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        first = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, first)

        current = await app.sessions.get(LOCAL, "local", session.id)
        second = await app.sessions.submit(
            LOCAL, "local", session.id,
            TurnRequest(text="/ask q2", request_key="turn-2",
                        expected_revision=current.revision),
        )
        await wait_run(app, run_id, "aborted")
        cancelled = await app.service.get(run_id)
        assert cancelled.code == "cancelled"
        old = await app.interactions.read_request(LOCAL, "local", session.id, row["id"])
        assert old["status"] == "superseded" and old["response"] == {"reason": "user_message"}
        saved_first = await app.sessions.turn(LOCAL, "local", session.id, first.id)
        assert saved_first.status == "cancelled"
        pending = (await app.interactions.state(LOCAL, "local", session.id))["pending"]
        assert all(item["id"] != row["id"] for item in pending)  # 旧问题不得仍待答

        turn2, row2 = await wait_input_park(app, session, second)
        replay = await app.sessions.submit(
            LOCAL, "local", session.id,
            TurnRequest(text="/ask q2", request_key="turn-2", expected_revision=current.revision),
        )
        assert replay.id == second.id
        pending = await app.interactions.state(LOCAL, "local", session.id)
        assert [item["id"] for item in pending["pending"]] == [row2["id"]]
        await app.interactions.answer(
            LOCAL, "local", session.id, row2["id"], answer_body(row2, "answer-2")
        )
        await wait_run(app, turn2, "completed")


async def test_rejected_or_replayed_submit_preserves_pending_question(tmp_path, wire):
    async with input_application(wire, tmp_path, "rejected-submit.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, turn)

        current = await app.sessions.get(LOCAL, "local", session.id)
        with pytest.raises(ServiceError, match="input_limit"):
            await app.sessions.submit(
                LOCAL, "local", session.id,
                TurnRequest(text="x" * 300, request_key="turn-2",
                            expected_revision=current.revision),
            )
        view = await app.service.get(run_id)
        assert view.status == "awaiting"
        pending = (await app.interactions.state(LOCAL, "local", session.id))["pending"]
        assert [item["id"] for item in pending] == [row["id"]]
        assert len(wire.requests) == 1

        replay = await app.sessions.submit(
            LOCAL, "local", session.id,
            TurnRequest(text="/ask q1", request_key="turn-1",
                        expected_revision=turn.accepted_revision),
        )
        assert replay.id == turn.id
        assert (await app.service.get(run_id)).status == "awaiting"
        pending = (await app.interactions.state(LOCAL, "local", session.id))["pending"]
        assert [item["id"] for item in pending] == [row["id"]]

        await app.interactions.answer(
            LOCAL, "local", session.id, row["id"], answer_body(row, "answer-1")
        )
        await wait_run(app, run_id, "completed")


async def test_reconcile_cancels_run_with_superseded_question(tmp_path, wire):
    async with input_application(wire, tmp_path, "superseded-reconcile.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, turn)

        state = await app.interactions.supersede(LOCAL, "local", session.id, "manual-supersede")
        assert state["pending"] == []
        await app.service.reconcile_inputs()
        view = await wait_run(app, run_id, "aborted")
        assert view.code == "cancelled"
        assert len(wire.requests) == 1  # 被取代的运行不得带旧问题答复续跑
        assert not [event for event in await events_of(app, run_id)
                    if event.kind == "user_input_reply"]
        saved = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
        assert saved.status == "cancelled"


async def test_cancel_wakes_parked_run_durably(tmp_path, wire):
    async with input_application(wire, tmp_path, "cancel-wake.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, turn)

        async with interaction_client(app) as client:
            cancel_response = await client.post(
                f"/v2/workspaces/local/sessions/{session.id}/interactions/{row['id']}/cancel",
                json=CancelRequest(
                    request_key="cancel-1",
                    expected_revision=row["revision"],
                    reason="not needed",
                ).model_dump(mode="json"),
            )
        assert cancel_response.status_code == 200
        assert cancel_response.json()["status"] == "cancelled"
        finished = await wait_run(app, run_id, "completed")
        assert finished.code is None
        replies = [event for event in await events_of(app, run_id)
                   if event.kind == "user_input_reply"]
        assert len(replies) == 1 and replies[0].data["status"] == "cancelled"
        assert (await app.interactions.state(LOCAL, "local", session.id))["pending"] == []
        saved = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert saved.status == "completed"


async def test_crash_between_turn_admission_and_supersede_recovers_on_restart(tmp_path, wire):
    """接纳已提交但取代清理未落盘的崩溃窗口：重启后调度先收敛再派发。"""
    from krim_agent.storage import sessions as session_sql

    async with input_application(wire, tmp_path, "crash-admission.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        first = await submit_turn(app, session, "/ask q1", "turn-1")
        run_id, row = await wait_input_park(app, session, first)
        # 直接落库新turn，绕过SessionService提交围栏：模拟接纳后进程崩溃。
        current = await app.sessions.get(LOCAL, "local", session.id)
        admitted = await session_sql.submit(
            app.store, session.id,
            TurnRequest(text="/final", request_key="turn-2",
                        expected_revision=current.revision),
            app.settings,
        )
        assert admitted.status == "queued"
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            await app.interactions.answer(
                LOCAL, "local", session.id, row["id"], answer_body(row, "late-answer")
            )
        still_pending = await app.interactions.read_request(LOCAL, "local", session.id, row["id"])
        assert still_pending["status"] == "pending"
        requests_before = len(wire.requests)
    async with input_application(wire, tmp_path, "crash-admission.sqlite") as reopened:
        await wait_run(reopened, run_id, "aborted")
        cancelled = await reopened.service.get(run_id)
        assert cancelled.code == "cancelled"
        old = await reopened.interactions.read_request(LOCAL, "local", session.id, row["id"])
        assert old["status"] == "superseded" and old["response"] == {"reason": "user_message"}
        current = await reopened.sessions.get(LOCAL, "local", session.id)
        if current.dispatch_paused:
            # Existing crash recovery deliberately pauses queued user turns;
            # settling the old question must not silently override that control.
            await reopened.sessions.control(
                LOCAL, "local", session.id, "resume",
                ControlRequest(
                    request_key="resume-after-crash", expected_revision=current.revision
                ),
            )
        saved = await asyncio.wait_for(
            reopened.sessions.wait(LOCAL, "local", session.id, admitted.id), 60
        )
        assert saved.status == "completed"
        assert (await reopened.interactions.state(LOCAL, "local", session.id))["pending"] == []
        assert len(wire.requests) == requests_before + 1  # 只跑新turn，不续跑旧问题
