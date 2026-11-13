"""真实producer、队列、控制和资源owner行为。"""

import asyncio
import hashlib

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, RunRequest, ServiceError
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.security import LOCAL
from krim_agent.sessions.types import ControlRequest, SessionCreate, TurnRequest
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.builtins import EchoInput
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.registry import ToolRegistry


async def submit(app, session, text, key):
    current = await app.sessions.get(LOCAL, "local", session.id)
    return await app.sessions.submit(
        LOCAL,
        "local",
        session.id,
        TurnRequest(text=text, request_key=key, expected_revision=current.revision),
    )


async def pending(app, turn):
    async with asyncio.timeout(5):
        while True:
            current = await app.sessions.turn(LOCAL, "local", turn.session_id, turn.id)
            if current.run_id:
                approvals = await app.service.approvals(current.run_id)
                if approvals:
                    return current, approvals[0]
            await asyncio.sleep(0.005)


async def test_fifo_context_and_shared_capacity_with_eval(tmp_path, wire):
    wire.hold_at.add(1)
    async with wire.application(tmp_path / "fifo.sqlite", Settings(max_active_runs=1)) as app:
        first = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="A"))
        other = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="B"))
        a = await submit(app, first, "请记住基数17", "one")
        async with asyncio.timeout(15), wire.changed:
            await wire.changed.wait_for(lambda: bool(wire.requests))
        b = await submit(app, first, "与3相加", "two")
        c = await submit(app, other, "/add 9 1", "one")
        again = await app.sessions.submit(
            LOCAL,
            "local",
            first.id,
            TurnRequest(text="请记住基数17", request_key="one", expected_revision=0),
        )
        assert again.id == a.id
        with pytest.raises(ServiceError, match="key_conflict"):
            await app.sessions.submit(
                LOCAL,
                "local",
                first.id,
                TurnRequest(text="不同", request_key="one", expected_revision=0),
            )
        assert (await app.sessions.turn(LOCAL, "local", first.id, b.id)).run_id is None
        with pytest.raises(ServiceError, match="capacity"):
            await app.service.start(RunRequest(text="/add 1 2", request_key="no-second-pool"))
        revision = await app.evaluations.freeze(
            FreezeRequest.model_validate(
                dict(
                    name="queue-eval",
                    version="1",
                    cases=[
                        dict(
                            case_id="one",
                            source_kind="manual",
                            input="/add 2 3",
                            reason="合成参考",
                            reference=dict(
                                expected="5",
                                expected_status="completed",
                                expected_code=None,
                                expected_tools=None,
                                trajectory=None,
                                expected_schema=None,
                            ),
                        )
                    ],
                )
            )
        )
        request = EvalRequest.model_validate(
            dict(
                request_key="eval-during-session",
                dataset={k: getattr(revision, k) for k in ("name", "version", "fingerprint")},
                target_key="target-v2",
                rules=[rule("status_code").model_dump(mode="json")],
                execution=dict(
                    repetitions=1,
                    concurrency=1,
                    item_timeout_seconds=12,
                    scorer_timeout_seconds=1,
                    batch_timeout_seconds=600,
                ),
            )
        )
        evaluation = await app.evaluations.start(request)
        await app.evaluations.wait(evaluation.id)
        assert app.service.bindings["target-v2"].loop.provider.calls == 0
        assert len(wire.requests) == 1
        assert sum(not t.done() for t in app.service.tasks.values()) == 1
        wire.release.set()
        for value in [a, b, c]:
            finished = await app.sessions.wait(LOCAL, "local", value.session_id, value.id)
            assert finished.status == "completed"
        b = await app.sessions.turn(LOCAL, "local", first.id, b.id)
        assert (await app.service.result(b.run_id)).text == "20"
        snapshot = await app.store.snapshot(b.run_id)
        assert snapshot.session_context.context.sources[0].turn_id == a.id
        assert len(wire.requests) == 6 and len(wire.executions) == 3


async def test_cancel_queue_active_approval_and_repeated_close(tmp_path, wire):
    wire.hold_at.add(1)
    database = tmp_path / "cancel.sqlite"
    async with wire.application(database, Settings(max_active_runs=1)) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        a = await submit(app, session, "请记住基数17", "active")
        await wire.wait_requests(1)
        b = await submit(app, session, "与3相加", "queued")
        current = await app.sessions.get(LOCAL, "local", session.id)
        body = ControlRequest(request_key="cancel-queued", expected_revision=current.revision)
        left, right = await asyncio.gather(
            *(
                app.sessions.control(LOCAL, "local", session.id, "cancel_turn", body, turn_id=b.id)
                for _ in range(2)
            )
        )
        assert left.id == right.id
        assert left.result["status"] == "cancelled" and left.result["run_id"] is None
        current = await app.sessions.get(LOCAL, "local", session.id)
        await app.sessions.control(
            LOCAL,
            "local",
            session.id,
            "cancel_turn",
            ControlRequest(request_key="cancel-active", expected_revision=current.revision),
            turn_id=a.id,
        )
        assert (await app.sessions.turn(LOCAL, "local", session.id, a.id)).status == "cancelled"
        assert app.provider.active == 0 and app.provider.streams_closed == 1
        assert not wire.executions and len(wire.requests) == 1
    probe = await SQLiteStore.open(database)
    await probe.close()


async def test_control_caller_repeated_cancel_waits_real_tool_cleanup(tmp_path):
    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    executions = []

    async def execute(args):
        executions.append(args.text)
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()

    registry = ToolRegistry(
        (
            ToolSpec(
                "echo",
                EchoInput,
                execute,
                {"type": "string"},
                "fixture/1",
                hashlib.sha256(b"cleanup-gate").hexdigest(),
            ),
        )
    )
    app = await open_application(tmp_path / "cleanup.sqlite", registry=registry)
    try:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit(app, session, "/echo wait", "turn")
        await asyncio.wait_for(entered.wait(), 5)
        current = await app.sessions.get(LOCAL, "local", session.id)
        body = ControlRequest(request_key="stop", expected_revision=current.revision)
        caller = asyncio.create_task(
            app.sessions.control(LOCAL, "local", session.id, "cancel_turn", body, turn_id=turn.id)
        )
        await asyncio.wait_for(cleaning.wait(), 5)
        caller.cancel()
        await asyncio.sleep(0)
        caller.cancel()
        await asyncio.sleep(0)
        assert not caller.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await caller
        result = await app.sessions.control(
            LOCAL, "local", session.id, "cancel_turn", body, turn_id=turn.id
        )
        assert result.status == "completed" and result.result["status"] == "cancelled"
        assert executions == ["wait"] and all(t.done() for t in app.sessions.controls.values())
    finally:
        release.set()
        await asyncio.gather(app.close(), app.close())
    assert app.provider.active == 0 and app.store.closed


@pytest.mark.parametrize("allow", [True, False])
async def test_approval_uses_original_decision_and_scope(tmp_path, allow):
    async with await open_application(tmp_path / "approval.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        turn = await submit(app, session, "/note welcome", "turn")
        turn, item = await pending(app, turn)
        current = await app.sessions.get(LOCAL, "local", session.id)
        body = ApprovalReply(
            request_key="original-decision",
            call_id=item["call_id"],
            arguments_hash=item["arguments_hash"],
            tool_hash=item["tool_hash"],
            allow=allow,
        )
        control = ControlRequest(request_key=body.request_key, expected_revision=current.revision)
        result = await app.sessions.control(
            LOCAL,
            "local",
            session.id,
            "approval",
            control,
            turn_id=turn.id,
            approval=body,
            request_id=item["request_id"],
        )
        assert result.status == "completed"
        duplicate = await app.sessions.control(
            LOCAL,
            "local",
            session.id,
            "approval",
            control,
            turn_id=turn.id,
            approval=body,
            request_id=item["request_id"],
        )
        assert duplicate.id == result.id
        finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        # ADR-0015：拒绝回传模型收尾——两种批复下轮次均完成，工具事实区分
        assert finished.status == "completed"
        facts = await app.service.result(turn.run_id)
        assert facts.tools[0].executor_started is allow
        if not allow:
            assert facts.tools[0].status == "denied"
            assert facts.tools[0].code == "approval_denied"
        with pytest.raises(ServiceError, match="key_conflict"):
            await app.sessions.control(
                LOCAL,
                "local",
                session.id,
                "approval",
                control,
                turn_id=turn.id,
                approval=body.model_copy(update={"allow": not allow}),
                request_id=item["request_id"],
            )


async def test_post_disconnect_does_not_own_session_producer(tmp_path, wire, monkeypatch):
    async with wire.application(tmp_path / "unknown.sqlite") as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        original = app.store._write
        committed = asyncio.Event()
        release = asyncio.Event()
        once = True

        async def lost(operation):
            nonlocal once
            result = await original(operation)
            if once:
                once = False
                committed.set()
                await release.wait()
            return result

        monkeypatch.setattr(app.store, "_write", lost)
        caller = asyncio.create_task(
            app.sessions.submit(
                LOCAL,
                "local",
                session.id,
                TurnRequest(request_key="original-key", text="/add 2 3", expected_revision=0),
            )
        )
        await asyncio.wait_for(committed.wait(), 5)
        caller.cancel()
        await asyncio.sleep(0)
        assert not caller.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await caller
        found = await app.sessions.turn_by_key(LOCAL, "local", session.id, "original-key")
        result = await app.sessions.wait(LOCAL, "local", session.id, found.id)
        assert (
            result.status == "completed" and len(wire.requests) == 2 and len(wire.executions) == 1
        )
        for _ in range(3):
            assert (
                await app.sessions.turn_by_key(LOCAL, "local", session.id, "original-key")
            ).id == found.id
        assert len(wire.requests) == 2


async def test_session_close_returns_committed_state(tmp_path, wire):
    wire.hold_at.add(1)
    async with wire.application(
        tmp_path / "session-close.sqlite", Settings(max_active_runs=1)
    ) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="close"))
        active = await submit(app, session, "/add 2 3", "active")
        await wire.wait_requests(1)
        queued = await submit(app, session, "不得执行", "queued")
        current = await app.sessions.get(LOCAL, "local", session.id)
        body = ControlRequest(request_key="close-original", expected_revision=current.revision)
        closed = await app.sessions.control(LOCAL, "local", session.id, "close", body)
        actual = await app.sessions.get(LOCAL, "local", session.id)
        assert actual.status == "closed" and actual.closed_at is not None
        assert closed.result == actual.model_dump(mode="json")
        assert (
            await app.sessions.control(LOCAL, "local", session.id, "close", body)
        ).id == closed.id
        assert (
            await app.sessions.turn(LOCAL, "local", session.id, active.id)
        ).status == "cancelled"
        assert (await app.sessions.turn(LOCAL, "local", session.id, queued.id)).run_id is None
        assert app.provider.active == 0 and len(wire.requests) == 1 and wire.executions == []


@pytest.mark.parametrize("operation", ["submit", "resume"])
async def test_pending_close_fences_new_revision_and_joins_cleanup(tmp_path, operation):
    import json

    from test_session_read_sides import Connections

    from krim_agent.storage import sessions as sql

    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    executions, ordering = [], []

    async def execute(args):
        executions.append(args.text)
        if args.text != "wait":
            return args.text
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            ordering.append("tool_cleanup_enter")
            cleaning.set()
            await release.wait()
            ordering.append("tool_cleanup_exit")

    registry = ToolRegistry(
        (
            ToolSpec(
                "echo",
                EchoInput,
                execute,
                {"type": "string"},
                "repair/1",
                hashlib.sha256(b"close-transaction-fence").hexdigest(),
            ),
        )
    )
    database = tmp_path / "close-fence.sqlite"
    app = await open_application(database, registry=registry, settings=Settings(max_active_runs=1))
    probe = Connections(app.store)
    callers = []
    record = {"operation": operation, "ordering": ordering}
    try:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="closing"))
        other = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="other"))
        original = TurnRequest(text="/echo wait", request_key="accepted", expected_revision=0)
        active = await app.sessions.submit(LOCAL, "local", session.id, original)
        await asyncio.wait_for(entered.wait(), 5)
        queued = await submit(app, session, "/echo never", "accepted-queued")
        current = await app.sessions.get(LOCAL, "local", session.id)
        body = ControlRequest(request_key="close-original", expected_revision=current.revision)
        caller = asyncio.create_task(
            app.sessions.control(LOCAL, "local", session.id, "close", body)
        )
        callers.append(caller)
        await asyncio.wait_for(cleaning.wait(), 5)
        durable = await sql.control_by_key(app.store, session.id, body.request_key)
        assert durable.status == "pending"
        current = await app.sessions.get(LOCAL, "local", session.id)
        record.update(close_id=durable.id, observed_revision=current.revision)
        ordering.append("pending_close_committed")
        duplicate = asyncio.create_task(
            app.sessions.control(LOCAL, "local", session.id, "close", body)
        )
        callers.append(duplicate)
        caller.cancel()
        await asyncio.sleep(0)
        caller.cancel()
        await asyncio.sleep(0)
        assert not caller.done() and not duplicate.done()
        # 原已接受请求先于closing/revision判断，仍返回原身份。
        accepted_duplicate = await app.sessions.submit(LOCAL, "local", session.id, original)
        assert accepted_duplicate.id == active.id
        assert accepted_duplicate.request_key == original.request_key
        with pytest.raises(ServiceError, match="key_conflict"):
            await app.sessions.submit(
                LOCAL,
                "local",
                session.id,
                TurnRequest(text="不同请求", request_key="accepted", expected_revision=0),
            )
        request, _ = await app.store.turn_request(queued.id)
        with pytest.raises(ServiceError, match="session_closed"):
            await sql.begin_run(
                app.store,
                queued.id,
                request,
                json.loads(app.service.bindings["demo"].descriptor_json),
                app.settings,
            )
        received = None
        try:
            if operation == "submit":
                accepted = await app.sessions.submit(
                    LOCAL,
                    "local",
                    session.id,
                    TurnRequest(
                        text="/echo forbidden",
                        request_key="new-key",
                        expected_revision=current.revision,
                    ),
                )
            else:
                accepted = await app.sessions.control(
                    LOCAL,
                    "local",
                    session.id,
                    "resume",
                    ControlRequest(request_key="new-resume", expected_revision=current.revision),
                )
            record["unexpected_acceptance"] = accepted.model_dump(mode="json")
        except ServiceError as error:
            received = error.code
        record["rejection"] = received
        record["counts_before_release"] = {"model": app.provider.calls, "tools": list(executions)}
        assert received == "session_closed", record
        assert app.provider.calls == 1 and executions == ["wait"]
        assert not caller.done() and not duplicate.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await caller
        result = await duplicate
        ordering.append("close_returned")
        assert result.status == "completed" and result.result["status"] == "closed"
        assert (await app.sessions.control(LOCAL, "local", session.id, "close", body)) == result
        history = await app.sessions.turns(LOCAL, "local", session.id)
        assert len(history) == 2 and all(v.status not in {"queued", "running"} for v in history)
        assert all(t.done() for t in app.service.tasks.values())
        assert app.provider.active == 0 and app.provider.calls == 1 and executions == ["wait"]
        fresh = await submit(app, other, "/echo other", "other-turn")
        assert (await app.sessions.wait(LOCAL, "local", other.id, fresh.id)).status == "completed"
        assert app.provider.calls == 3 and executions == ["wait", "other"]
        assert ordering.index("tool_cleanup_exit") < ordering.index("close_returned")
    finally:
        release.set()
        await asyncio.gather(*callers, return_exceptions=True)
        await asyncio.gather(app.close(), app.close())
        second = await SQLiteStore.open(database)
        await second.close()
        record.update(
            connection_events=probe.events,
            pending=list(probe.pending),
            unsafe_gc=probe.unsafe,
            producer_done=all(t.done() for t in app.service.tasks.values()),
            controls_done=all(t.done() for t in app.sessions.controls.values()),
            dispatcher_done=app.sessions._dispatcher.done(),
            provider_active=app.provider.active,
            second_writer=True,
        )
        (tmp_path / "close-fence.json").write_text(json.dumps(record, ensure_ascii=False, indent=2))
        assert not probe.pending and probe.unsafe == 0 and record["producer_done"]
        assert record["controls_done"] and record["dispatcher_done"] and app.provider.active == 0
