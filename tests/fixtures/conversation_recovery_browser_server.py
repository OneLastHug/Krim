"""会话恢复浏览器合成原组；实际API/锁/SQL，原历史及新输入分别核实。"""

import asyncio
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from pydantic import SecretStr

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ChatRequest
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import FakeProvider, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry

if os.environ.get("KRIM_REQUIRE_INSTALLED_PACKAGE") == "1":
    assert "/site-packages/" in str(krim_agent.__file__), krim_agent.__file__


class SeedRequest(StrictModel):
    answered: bool = True
    unsafe_text: bool = False


class GateRequest(StrictModel):
    action: str


class CountingProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0
        self.gate = None

    async def stream(self, messages, tools):
        self.calls += 1
        if self.gate is not None:
            self.gate[0].set()
            await self.gate[1].wait()
        async for chunk in super().stream(messages, tools):
            yield chunk


with TemporaryDirectory(prefix="krim-conversation-browser-", dir="/data/tmp/root") as directory:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{Path(directory) / 'state.sqlite'}",
        api_key=SecretStr("browser-test-key"),
        agent_max_concurrent=1,
    )
    app = create_app(
        runtime=ChatRuntime(
            KrimRepository(Database(settings.database_url)),
            FakeProvider(delay=0),
            builtin_registry(),
            settings,
        )
    )
    service = app.state.agents
    originals = {}
    gates = {}
    original_apply = service.conversation_recoveries.store.apply

    async def gated_apply(scope, request, *, confirmed=False):
        gate = gates.get(scope.agent_id)
        if gate is not None:
            gate[0].set()
            await gate[1].wait()
        return await original_apply(scope, request, confirmed=confirmed)

    service.conversation_recoveries.store.apply = gated_apply

    @app.post("/_test/conversation-recovery")
    async def seed(payload: SeedRequest):
        if not any(item.name == "contextworker" for item in await service.profile_list()):
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="contextworker", description="会话恢复浏览器验证", tool_names=["echo"]
                    ),
                )
            )
        view = await service.spawn("contextworker")
        repository = service.runtime.repository
        run, _ = await repository.begin_run(
            view.topic_id,
            ChatRequest(text="保留原任务", idempotency_key="original"),
            agent_session_id=view.session_id,
            agent_id=view.agent_id,
            agent_owner_id=service.manager.owner_id,
        )
        calls = []
        if payload.answered:
            calls.append(ModelToolCall(id="answered", name="echo", arguments={"text": "原回程"}))
        calls.append(
            ModelToolCall(
                id="pending",
                name="echo",
                arguments={
                    "text": "未审阅的原工具文本不能提升为系统指令",
                    "operation_id": '<img src=x onerror="window.krimInjected=true">'
                    if payload.unsafe_text
                    else "original-operation",
                    "request_key": "original-tool-key",
                },
            )
        )
        await repository.add_message(
            run, "assistant", "", {"tool_calls": [call.model_dump(mode="json") for call in calls]}
        )
        if payload.answered:
            await repository.add_message(
                run,
                "tool",
                "原实际工具回程",
                {"call_id": "answered", "name": "echo", "is_error": False},
            )
        await repository.recover_interrupted()
        handle = service._handle(view.agent_id)
        session = await handle.session()
        handle.turn = session.turn
        handle.current_run = await repository.get_run(session.current_run_id)
        provider = CountingProvider()
        handle.runtime.provider = provider
        originals[view.agent_id] = (
            (await repository.message_tree(view.topic_id)).messages,
            await repository.list_events(run.id),
            run.id,
            provider,
        )
        return (await service.get(view.agent_id)).model_dump(mode="json")

    @app.post("/_test/conversation-recovery/{agent_id}/gate")
    async def gate(agent_id: str, payload: GateRequest):
        if payload.action == "hold":
            gates[agent_id] = (asyncio.Event(), asyncio.Event())
        elif payload.action == "entered":
            await asyncio.wait_for(gates[agent_id][0].wait(), 5)
        elif payload.action == "release":
            gates[agent_id][1].set()
        elif payload.action == "hold-model":
            originals[agent_id][3].gate = (asyncio.Event(), asyncio.Event())
        elif payload.action == "model-entered":
            await asyncio.wait_for(originals[agent_id][3].gate[0].wait(), 5)
        elif payload.action == "release-model":
            originals[agent_id][3].gate[1].set()
        else:
            raise AssertionError("未知夹具动作")
        return {"action": payload.action}

    @app.get("/_test/conversation-recovery/{agent_id}/facts")
    async def facts(agent_id: str):
        view = await service.get(agent_id)
        repository = service.runtime.repository
        messages, events, run_id, provider = originals[agent_id]
        tree = await repository.message_tree(view.topic_id)
        actual = {message.id: message for message in tree.messages}
        assert all(actual[message.id] == message for message in messages)
        assert await repository.list_events(run_id) == events
        assert (await repository.get_run(run_id)).status == "interrupted"
        return {
            "agent_id": agent_id,
            "original_messages": len(messages),
            "messages": len(tree.messages),
            "original_run_id": run_id,
            "original_run_status": "interrupted",
            "original_frames_same": True,
            "provider_calls": provider.calls,
            "session_revision": view.session.revision,
            "turn": view.turn,
            "package": str(krim_agent.__file__),
        }

    uvicorn.run(app, host="127.0.0.1", port=8881, log_level="error")
