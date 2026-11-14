"""审批停靠与无状态恢复专项（ADR-0015）：跨进程存活、指纹硬门、恢复点防篡改。"""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, RunRequest, ServiceError
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.tools.registry import ToolRegistry


@asynccontextmanager
async def park_application(wire, tmp_path, name, *, settings=None):
    async def ask(_invocation):
        return "ask"

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=f"http://127.0.0.1:{wire.port}/v1", model="fixture")
    )
    app = None
    try:
        app = await open_application(
            Path(tmp_path) / name,
            settings=settings or Settings(run_timeout=5, tool_timeout=2),
            provider=provider,
            registry=ToolRegistry(tuple(spec for spec, _ in wire.registry()._frozen)),
            policy=ask,
        )
        yield app
    finally:
        if app is not None:
            await app.close()
        await provider.close()


async def park_echo_turn(app):
    session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="park-session"))
    turn = await app.sessions.submit(
        LOCAL,
        "local",
        session.id,
        TurnRequest(text="/echo park me", request_key="park-turn", expected_revision=0),
    )
    async with asyncio.timeout(5):
        while True:
            current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
            if current.run_id:
                view = await app.service.get(current.run_id)
                if view.status == "awaiting":
                    request = next(
                        (
                            dict(event.data)
                            for event in await app.service.events(current.run_id)
                            if event.kind == "approval_request"
                        ),
                        None,
                    )
                    if request is not None:
                        return current.run_id, request
                task = app.service.tasks.get(current.run_id)
                if task is not None and task.done() and view.status != "awaiting":
                    raise AssertionError(f"run ended before park: {view.status}/{view.code}")
            await asyncio.sleep(0.005)


def allow_reply(request):
    return ApprovalReply(
        request_key="park-turn",
        call_id=request["call_id"],
        tool_hash=request["tool_hash"],
        arguments_hash=request["arguments_hash"],
        allow=True,
    )


@pytest.mark.parametrize("phase", ["before-awaiting", "after-awaiting"])
@pytest.mark.parametrize("session_run", [False, True], ids=["direct", "session"])
async def test_cancel_during_parking_is_terminal_and_cannot_resume(
    wire, tmp_path, monkeypatch, phase, session_run
):
    database = "cancel-transition.sqlite"
    async with park_application(wire, tmp_path, database) as app:
        entered, release, stopped = (asyncio.Event() for _ in range(3))
        captured = {}
        original_awaiting = app.store.set_awaiting
        original_stop = app.store.request_stop

        async def held_awaiting(run_id):
            captured["run_id"] = run_id
            parked = None
            if phase == "after-awaiting":
                parked = await original_awaiting(run_id)
            entered.set()
            await release.wait()
            if phase == "before-awaiting":
                parked = await original_awaiting(run_id)
            return parked

        async def observed_stop(run_id):
            await original_stop(run_id)
            stopped.set()

        monkeypatch.setattr(app.store, "set_awaiting", held_awaiting)
        monkeypatch.setattr(app.store, "request_stop", observed_stop)
        if session_run:
            session = await app.sessions.create(
                LOCAL, "local", SessionCreate(request_key="cancel-session")
            )
            turn = await app.sessions.submit(
                LOCAL,
                "local",
                session.id,
                TurnRequest(text="/echo never run", request_key="cancel-turn", expected_revision=0),
            )
        else:
            await app.service.start(RunRequest(text="/echo never run", request_key="cancel-direct"))
        caller = None
        try:
            await asyncio.wait_for(entered.wait(), 5)
            run_id = captured["run_id"]
            request = next(
                dict(event.data)
                for event in await app.service.events(run_id)
                if event.kind == "approval_request"
            )
            assert await app.store.read_checkpoint(run_id) is not None
            if phase == "before-awaiting":
                assert await app.service.approvals(run_id) == []
            caller = asyncio.create_task(app.service.cancel(run_id))
            await asyncio.wait_for(stopped.wait(), 5)
            release.set()
            result = await asyncio.wait_for(caller, 5)
            assert result.status == "aborted"
            assert result.code == "cancelled"
            assert await app.service.approvals(run_id) == []
            for _ in range(2):
                with pytest.raises(ServiceError, match="approval_unavailable"):
                    await app.service.reply_approval(
                        run_id, request["request_id"], allow_reply(request)
                    )
            assert (await app.service.cancel(run_id)).status == "aborted"
            assert not wire.executions
            assert len(wire.requests) == 1
            facts = await app.service.result(run_id)
            assert sum(event.kind == "terminal" for event in facts.events) == 1
            assert not any(event.kind == "approval_reply" for event in facts.events)
            if session_run:
                saved_turn = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
                assert saved_turn.status == "cancelled"
        finally:
            release.set()
            if caller is not None:
                await asyncio.gather(caller, return_exceptions=True)

    async with park_application(wire, tmp_path, database) as reopened:
        assert (await reopened.service.get(run_id)).status == "aborted"
        assert await reopened.service.approvals(run_id) == []
        with pytest.raises(ServiceError, match="approval_unavailable"):
            await reopened.service.reply_approval(
                run_id, request["request_id"], allow_reply(request)
            )
        assert len(wire.requests) == 1
        assert not wire.executions


@pytest.mark.parametrize("state", ["queued", "awaiting"])
async def test_stopped_run_cannot_enter_running(state, tmp_path):
    async with await open_application(tmp_path / "stopped.sqlite") as app:
        target = json.loads(app.service.bindings["demo"].descriptor_json)
        run, created = await app.store.begin(
            RunRequest(text="/echo stopped", request_key="stopped"), target
        )
        assert created
        if state == "awaiting":
            await app.store.set_running(run.id)
            assert await app.store.set_awaiting(run.id) is True
        await app.store.request_stop(run.id)
        with pytest.raises(ServiceError, match="cancelled"):
            await app.store.set_running(run.id)
        saved = await app.service.get(run.id)
        assert saved.status == state and saved.stop_requested
        await app.store.finish(run.id, "aborted", "cancelled")


@pytest.mark.parametrize("cancel_first", [False, True], ids=["resume-first", "cancel-first"])
@pytest.mark.asyncio
async def test_cancel_blocks_replay_of_a_committed_approval_before_resume(
    wire, tmp_path, monkeypatch, cancel_first
):
    async with park_application(wire, tmp_path, "cancel-resume.sqlite") as app:
        run_id, request = await park_echo_turn(app)
        await app.service.wait(run_id)
        entered, release, stopping, stop_release = (asyncio.Event() for _ in range(4))
        original = app.store.set_running
        original_stop = app.store.request_stop

        async def hold_resume(selected_run):
            entered.set()
            await release.wait()
            await original(selected_run)

        async def hold_stop(selected_run):
            stopping.set()
            await stop_release.wait()
            await original_stop(selected_run)

        monkeypatch.setattr(app.store, "set_running", hold_resume)
        caller = None
        try:
            if cancel_first:
                monkeypatch.setattr(app.store, "request_stop", hold_stop)
                caller = asyncio.create_task(app.service.cancel(run_id))
                await asyncio.wait_for(stopping.wait(), 5)
            await app.service.reply_approval(run_id, request["request_id"], allow_reply(request))
            await asyncio.wait_for(entered.wait(), 5)
            if caller is None:
                caller = asyncio.create_task(app.service.cancel(run_id))
            stop_release.set()
            result = await asyncio.wait_for(caller, 5)
            assert result.status == "aborted" and result.code == "cancelled"
            assert app.service.tasks[run_id].done()
            release.set()
            assert (
                await app.service.reply_approval(
                    run_id, request["request_id"], allow_reply(request)
                )
            )["replayed"] is True
            assert (await app.service.wait(run_id)).status == "aborted"
            assert not wire.executions
            assert len(wire.requests) == 1
            facts = await app.service.result(run_id)
            assert sum(event.kind == "approval_reply" for event in facts.events) == 1
            assert sum(event.kind == "terminal" for event in facts.events) == 1
        finally:
            release.set()
            stop_release.set()
            if caller is not None:
                await asyncio.gather(caller, return_exceptions=True)


@pytest.mark.asyncio
async def test_park_survives_reopen_and_resumes_without_extra_model_call(wire, tmp_path):
    """停靠跨应用重开存活：答复后从恢复点续跑，不多付一次模型调用。"""
    async with park_application(wire, tmp_path, "park-reopen.sqlite") as app:
        run_id, request = await park_echo_turn(app)
        assert (await app.service.get(run_id)).status == "awaiting"
        await app.close()  # 关闭不误伤停靠（无可取消协程）
    calls_before = len(wire.requests)
    assert calls_before == 1  # 只有停靠步的一次模型请求
    async with park_application(wire, tmp_path, "park-reopen.sqlite") as app2:
        assert (await app2.service.get(run_id)).status == "awaiting"  # 重开保留停靠
        request2 = (
            await app2.service.approvals(run_id)
            or [
                dict(e.data)
                for e in await app2.service.events(run_id)
                if e.kind == "approval_request"
            ][0:1]
        )[0]
        await app2.service.reply_approval(run_id, request2["request_id"], allow_reply(request2))
        result = await app2.service.wait(run_id)
        assert result.status == "completed", result.code
        assert len(wire.requests) == calls_before + 1  # 恢复只跑收尾步，不重放停靠步


@pytest.mark.asyncio
async def test_park_tool_fingerprint_drift_refuses_resume(wire, tmp_path):
    """停靠期间工具被换：答复后拒绝续跑，终态 tool_binding_changed。"""
    async with park_application(wire, tmp_path, "park-drift.sqlite") as app:
        run_id, request = await park_echo_turn(app)
        app.service.bindings["demo"].loop.executor.registry._items = {}  # 篡改目录
        await app.service.reply_approval(run_id, request["request_id"], allow_reply(request))
        result = await app.service.wait(run_id)
        assert result.status == "failed" and result.code == "tool_binding_changed"


@pytest.mark.asyncio
async def test_park_checkpoint_tamper_refuses_resume(wire, tmp_path):
    """恢复点被改动：指纹核对失败，facts_corrupt 终态，不猜着续跑。"""
    async with park_application(wire, tmp_path, "park-tamper.sqlite") as app:
        run_id, request = await park_echo_turn(app)
        data, stored = await app.store.read_checkpoint(run_id)
        data = dict(data)
        data["step"] = 99  # 篡改恢复点内容（指纹不再匹配）
        await app.store.write_checkpoint(run_id, data, stored)
        await app.service.reply_approval(run_id, request["request_id"], allow_reply(request))
        result = await app.service.wait(run_id)
        assert result.status == "failed" and result.code == "facts_corrupt"


@pytest.mark.asyncio
async def test_policy_deny_feeds_back_and_run_completes(wire, tmp_path):
    """策略拒绝回传继续（ADR-0015 §7）：运行完成，工具从未进入执行器。"""
    from krim_agent.config import Settings as AppSettings
    from krim_agent.tools.registry import ToolRegistry as Registry

    async def deny_policy(_invocation):
        return "deny"

    from pathlib import Path as P

    from krim_agent.composition import open_application as open_app
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleConfig as Cfg,
    )
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleProvider as Prv,
    )

    provider = Prv(Cfg(base_url=f"http://127.0.0.1:{wire.port}/v1", model="fixture"))
    async with await open_app(
        P(tmp_path) / "deny-soft.sqlite",
        settings=AppSettings(run_timeout=5, tool_timeout=2),
        provider=provider,
        registry=Registry(tuple(spec for spec, _ in wire.registry()._frozen)),
        policy=deny_policy,
    ) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="deny-soft"))
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(text="/echo denied", request_key="deny-turn", expected_revision=0),
        )
        finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert finished.status == "completed"
        final = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
        assert final.run_id is not None
        facts = await app.service.result(final.run_id)
        assert facts.tools[0].status == "denied"
        assert facts.tools[0].code == "permission_denied"
        assert facts.tools[0].executor_started is False
        assert not wire.executions
    await provider.close()
