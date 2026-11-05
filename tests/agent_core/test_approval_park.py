"""审批停靠与无状态恢复专项（ADR-0015）：跨进程存活、指纹硬门、恢复点防篡改。"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply
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
    session = await app.sessions.create(
        LOCAL, "local", SessionCreate(request_key="park-session")
    )
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
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key="deny-soft")
        )
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
