"""后台 Agent 服务任务所有权、幂等、断开与真实停止。"""

import asyncio

import pytest

from krim_agent.application.agents import AgentPromptRequest, AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText
from krim_agent.sdk import PythonAI


class Waiting:
    name = "background-agent"

    def __init__(self):
        self.entered = asyncio.Event()
        self.closed = asyncio.Event()
        self.calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        self.entered.set()
        try:
            yield ModelText(text="partial")
            await asyncio.Event().wait()
        finally:
            self.closed.set()


async def test_background_start_deduplicates_concurrent_inputs_and_waits_for_real_stop(runtime):
    provider = Waiting()
    runtime.provider = provider
    service = AgentService(runtime, runtime.settings)
    try:
        agent = await service.spawn("default")
        request = AgentPromptRequest(prompt="wait", idempotency_key="same")
        accepted = await asyncio.gather(*(service.start(agent.agent_id, request) for _ in range(4)))
        assert all(value.status in {"queued", "running"} for value in accepted)
        await asyncio.wait_for(provider.entered.wait(), 5)
        assert provider.calls == 1
        with pytest.raises(ServiceError) as busy:
            await service.start(
                agent.agent_id, AgentPromptRequest(prompt="different", idempotency_key="same")
            )
        assert busy.value.code == "agent_busy"
        snapshot = await service.get(agent.agent_id)
        reader, state = await service.stream_state(agent.agent_id, 0, snapshot.current_run_id)
        subscription = reader.subscribe(state)
        await anext(subscription)
        await subscription.aclose()
        assert not provider.closed.is_set()
        await service.stop(agent.agent_id)
        finished = await service.wait(agent.agent_id)
        assert finished.status == "aborted" and provider.closed.is_set()
        assert len(await service.session_turns(agent.agent_id)) == 1
        assert service.manager._semaphore._value == runtime.settings.agent_max_concurrent
    finally:
        await service.close()


async def test_background_completed_key_checks_input_before_accepting_new_job(runtime):
    service = AgentService(runtime, runtime.settings)
    try:
        agent = await service.spawn("default")
        payload = AgentPromptRequest(prompt="/add 2 3", idempotency_key="first")
        await service.start(agent.agent_id, payload)
        first = await service.wait(agent.agent_id)
        with pytest.raises(ServiceError) as conflict:
            await service.start(agent.agent_id, payload.model_copy(update={"prompt": "different"}))
        assert conflict.value.code == "idempotency_conflict"
        await service.start(agent.agent_id, payload)
        replay = await service.wait(agent.agent_id)
        assert replay.current_run_id == first.current_run_id and replay.turn == 1
        assert len(await runtime.repository.audits(first.current_run_id)) == 1
    finally:
        await service.close()


async def test_sdk_background_start_events_wait_and_close_cancel_real_production():
    provider = Waiting()
    async with await PythonAI.create(provider=provider) as ai:
        session = await ai.agent()
        accepted = await session.start("wait", idempotency_key="one")
        assert accepted.status in {"queued", "running"}
        await asyncio.wait_for(provider.entered.wait(), 5)
        frames = session.events()
        assert (await anext(frames)).kind == "start"
        await frames.aclose()
        assert not provider.closed.is_set()
        assert (await session.close()).status == "closed"
        result = await session.wait()
        assert result.status == "aborted" and provider.closed.is_set()


async def test_service_close_drains_background_tasks_and_refuses_new_start(runtime):
    provider = Waiting()
    runtime.provider = provider
    service = AgentService(runtime, runtime.settings)
    agent = await service.spawn("default", persistent=True)
    await service.start(agent.agent_id, AgentPromptRequest(prompt="wait"))
    await asyncio.wait_for(provider.entered.wait(), 5)
    await service.close()
    assert provider.closed.is_set() and not service._tasks and not service.manager._handles
    assert (await service.session(agent.agent_id)).owner_id is None
    with pytest.raises(ServiceError) as closed:
        await service.start(agent.agent_id, AgentPromptRequest(prompt="closed"))
    assert closed.value.code == "agent_service_closed"


async def test_http_background_start_returns_202_and_topic_projection(api):
    agent = (await api.post("/v1/agents", json={"name": "default"})).json()
    response = await api.post(
        f"/v1/agents/{agent['agent_id']}/start",
        json={"prompt": "/add 2 3", "idempotency_key": "async"},
    )
    assert response.status_code == 202
    topic = (await api.get(f"/v1/topics/{agent['topic_id']}/tree")).json()["topic"]
    assert topic["agent_session_id"] == agent["session_id"]
    listing = (await api.get("/v1/topics")).json()["topics"]
    assert (
        next(t for t in listing if t["id"] == agent["topic_id"])["agent_session_id"]
        == agent["session_id"]
    )


async def test_background_turn_limit_returns_failure_without_an_extra_run(runtime):
    from krim_agent.config import Settings

    service = AgentService(runtime, Settings(agent_max_turns=1))
    try:
        agent = await service.spawn("default")
        await service.start(agent.agent_id, AgentPromptRequest(prompt="one", idempotency_key="one"))
        first = await service.wait(agent.agent_id)
        await service.start(agent.agent_id, AgentPromptRequest(prompt="two", idempotency_key="two"))
        failed = await service.wait(agent.agent_id)
        assert failed.status == "failed" and failed.result is not None
        assert failed.result.code == "max_turns" and failed.current_run_id == first.current_run_id
        assert len(await service.session_turns(agent.agent_id)) == 1
        await service.destroy(agent.agent_id)
        assert agent.agent_id not in service._tasks
    finally:
        await service.close()


async def test_sdk_session_does_not_reopen_resources_after_owner_close():
    async with await PythonAI.create() as ai:
        session = await ai.agent()
    with pytest.raises(ServiceError) as closed:
        await session.state()
    assert closed.value.code == "sdk_closed"


async def test_session_stream_text_uses_exact_new_run_and_reports_queue_rejection():
    from krim_agent.config import Settings

    async with await PythonAI.create(
        settings=Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            agent_max_turns=2,
        )
    ) as ai:
        session = await ai.agent()
        first = await session.generate_text("/add 2 3", idempotency_key="one")
        second = await session.generate_text("/echo second", idempotency_key="two")
        assert first.run.id != second.run.id and second.text.endswith("second")
        assert len(first.steps) == 2 and first.steps[0].tool_calls[0].name == "add"
        replay = await session.generate_text("/add 2 3", idempotency_key="one")
        assert replay.run.id == first.run.id and (await session.state()).turn == 2
        with pytest.raises(ServiceError) as denied:
            await asyncio.wait_for(session.stream_text("third", idempotency_key="third"), 3)
        assert denied.value.code == "max_turns"
        assert len(await session.turns()) == 2


async def test_tcp_client_cli_background_start_wait_and_fixed_events(server_url):
    import json
    import sys

    from krim_agent.client import KrimClient

    def client():
        with KrimClient(server_url) as client:
            agent = client.agent_spawn("default")
            client.agent_start(agent.agent_id, "/add 2 3", "client-first")
            result = client.agent_wait(agent.agent_id)
            assert result.status == "completed" and '"sum": 5.0' in result.result.text
            assert (
                list(client.agent_events(agent.agent_id, run_id=result.current_run_id))[-1].kind
                == "end"
            )
            return agent.agent_id

    identifier = await asyncio.to_thread(client)

    async def cli(*args):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "agent",
            *args,
            "--url",
            server_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)

    await cli("start", identifier, "/echo cli-next", "--idempotency-key", "cli-next")
    result = await cli("wait", identifier)
    assert result["status"] == "completed" and result["turn"] == 2
    assert "cli-next" in result["result"]["text"]
    assert (await cli("destroy", identifier))["status"] == "destroyed"
