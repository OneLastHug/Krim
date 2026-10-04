import asyncio
import json
import sys

import httpx
from pydantic import SecretStr

from krim_agent.agents import AgentDefinition
from krim_agent.api import create_app
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.providers import FakeProvider, ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolRegistry


async def test_agent_http_catalog_lifecycle_events_and_persistent_run(api, repository):
    catalog = await api.get("/v1/agents/catalog")
    assert catalog.status_code == 200
    assert catalog.json()[0]["name"] == "default"
    assert catalog.json()[0]["exposed_as_tool"] is False

    unknown = await api.post("/v1/agents", json={"name": "missing"})
    assert unknown.status_code == 404 and unknown.json()["error"]["code"] == "agent_not_found"
    invalid = await api.post("/v1/agents", json={"name": "default", "provider": "forged"})
    assert invalid.status_code == 422

    spawned = await api.post("/v1/agents", json={"name": "default"})
    assert spawned.status_code == 201
    agent = spawned.json()
    agent_id = agent["agent_id"]
    assert agent["status"] == "created" and agent["current_run_id"] is None
    listing = await api.get("/v1/agents")
    assert [item["agent_id"] for item in listing.json()] == [agent_id]

    executed = await api.post(
        f"/v1/agents/{agent_id}/execute",
        json={"prompt": "/add 2 3", "idempotency_key": "api-agent"},
    )
    assert executed.status_code == 200
    view = executed.json()
    assert view["status"] == "completed" and "5.0" in view["result"]["text"]
    run_id = view["current_run_id"]
    retry = await api.post(
        f"/v1/agents/{agent_id}/execute",
        json={"prompt": "/add 2 3", "idempotency_key": "api-agent"},
    )
    assert retry.status_code == 200 and retry.json()["current_run_id"] == run_id
    events = await api.get(f"/v1/agents/{agent_id}/events")
    assert events.status_code == 200
    assert "event: start" in events.text and "event: end" in events.text
    assert events.headers["x-krim-agent-run-id"] == run_id
    bad_cursor = await api.get(f"/v1/agents/{agent_id}/events", headers={"Last-Event-ID": "-1"})
    assert bad_cursor.status_code == 416
    destroyed = await api.delete(f"/v1/agents/{agent_id}")
    assert destroyed.status_code == 200 and destroyed.json()["status"] == "destroyed"
    assert (await api.get(f"/v1/agents/{agent_id}")).status_code == 404
    assert (await repository.get_run(run_id)).status == "completed"


async def test_agent_http_resume_retains_topic_and_finished_run(api, repository):
    spawned = await api.post("/v1/agents", json={"name": "default"})
    agent_id = spawned.json()["agent_id"]
    first = await api.post(
        f"/v1/agents/{agent_id}/execute", json={"prompt": "/echo first", "idempotency_key": "first"}
    )
    assert first.json()["status"] == "completed"
    resumed = await api.post(
        f"/v1/agents/{agent_id}/resume",
        json={"prompt": "/echo second", "idempotency_key": "second"},
    )
    assert resumed.json()["status"] == "completed" and "second" in resumed.json()["result"]["text"]
    # 当前测试运行时没有持久 handle 接管；底层消息/运行仍可读取。
    run_id = resumed.json()["current_run_id"]
    assert (await repository.get_run(run_id)).status == "completed"
    assert await repository.run_messages(run_id)


async def test_agent_client_and_cli_consume_real_tcp_api(server_url):
    def client_scenario():
        with KrimClient(server_url) as client:
            assert client.agents_catalog()[0].name == "default"
            spawned = client.agent_spawn("default")
            executed = client.agent_execute(spawned.agent_id, "/add 4 5", "tcp-agent")
            frames = list(client.agent_events(spawned.agent_id))
            return spawned, executed, frames

    spawned, executed, frames = await asyncio.to_thread(client_scenario)
    assert executed.status == "completed" and executed.result is not None
    assert "9.0" in executed.result.text
    assert frames[-1].kind == "end"

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
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, stderr.decode()
        return stdout.decode()

    catalog = json.loads(await cli("catalog"))
    assert catalog[0]["name"] == "default"
    view = json.loads(await cli("get", spawned.agent_id))
    assert view["status"] == "completed"
    listed = json.loads(await cli("list"))
    assert any(item["agent_id"] == spawned.agent_id for item in listed)
    cli_created = json.loads(await cli("spawn", "default"))
    cli_id = cli_created["agent_id"]
    first = json.loads(
        await cli("execute", cli_id, "/echo CLI-first", "--idempotency-key", "cli-first")
    )
    assert first["status"] == "completed" and "CLI-first" in first["result"]["text"]
    resumed = json.loads(
        await cli("resume", cli_id, "/echo CLI-second", "--idempotency-key", "cli-second")
    )
    assert resumed["turn"] == 2 and "CLI-second" in resumed["result"]["text"]
    frames = [
        json.loads(line) for line in (await cli("events", cli_id, "--from-seq", "1")).splitlines()
    ]
    assert frames[0]["seq"] == 1 and frames[-1]["kind"] == "end"
    assert {frame["stream_id"] for frame in frames} == {resumed["current_run_id"]}
    assert json.loads(await cli("stop", cli_id))["status"] == "completed"
    assert json.loads(await cli("destroy", cli_id))["status"] == "destroyed"

    def destroy_client():
        with KrimClient(server_url) as client:
            assert not list(client.agent_events(spawned.agent_id, frames[-1]["seq"] + 1))
            return client.agent_destroy(spawned.agent_id)

    assert (await asyncio.to_thread(destroy_client)).status == "destroyed"


class WaitingAgentProvider:
    name = "agent-api-wait"

    def __init__(self):
        self.entered = asyncio.Event()
        self.closed = asyncio.Event()

    async def stream(self, _messages, _tools):
        self.entered.set()
        try:
            yield ModelText(text="partial")
            await asyncio.Event().wait()
        finally:
            self.closed.set()


async def test_live_http_status_busy_stop_and_persisted_terminal(repository, monkeypatch):
    provider = WaitingAgentProvider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings(run_timeout_seconds=10))
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        spawned = (await api.post("/v1/agents", json={"name": "default"})).json()
        agent_id = spawned["agent_id"]
        handle = app.state.agents.manager._handles[agent_id]
        ready = asyncio.Event()
        original = handle.runtime.stream_state

        async def stream_ready(*args, **kwargs):
            state = await original(*args, **kwargs)
            ready.set()
            return state

        monkeypatch.setattr(handle.runtime, "stream_state", stream_ready)
        task = asyncio.create_task(
            api.post(f"/v1/agents/{agent_id}/execute", json={"prompt": "wait"})
        )
        await asyncio.wait_for(ready.wait(), 5)
        await asyncio.wait_for(provider.entered.wait(), 5)
        try:
            live = await api.get(f"/v1/agents/{agent_id}")
            assert live.json()["status"] == "running" and live.json()["result"] is None
            busy = await api.post(f"/v1/agents/{agent_id}/execute", json={"prompt": "again"})
            assert busy.status_code == 409 and busy.json()["error"]["code"] == "agent_busy"
            stopped = await api.post(f"/v1/agents/{agent_id}/stop")
            result = await asyncio.wait_for(task, 5)
            assert stopped.json()["status"] == result.json()["status"] == "aborted"
            assert provider.closed.is_set()
            run_id = stopped.json()["current_run_id"]
            assert (await repository.get_run(run_id)).status == "aborted"
            frames = await api.get(f"/v1/agents/{agent_id}/events")
            assert '"status":"aborted"' in frames.text
        finally:
            if not task.done():
                await api.post(f"/v1/agents/{agent_id}/stop")
                await task


async def test_http_queue_stop_and_budget_failure_have_real_agent_results(repository, monkeypatch):
    provider = WaitingAgentProvider()
    runtime = ChatRuntime(
        repository,
        provider,
        ToolRegistry(),
        Settings(run_timeout_seconds=10, agent_max_concurrent=1),
    )
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        service = app.state.agents
        service.catalog.register(
            "short",
            AgentDefinition(
                name="short",
                provider=FakeProvider(delay=0),
                tools=ToolRegistry(),
                timeout_seconds=0.1,
            ),
            description="短排队预算",
            expose_as_tool=False,
        )
        service.catalog.register(
            "one_turn",
            AgentDefinition(
                name="one_turn", provider=FakeProvider(delay=0), tools=ToolRegistry(), max_turns=1
            ),
            description="一轮预算",
            expose_as_tool=False,
        )
        first = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
        second = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
        third = (await api.post("/v1/agents", json={"name": "short"})).json()["agent_id"]
        first_task = asyncio.create_task(
            api.post(f"/v1/agents/{first}/execute", json={"prompt": "hold"})
        )
        await asyncio.wait_for(provider.entered.wait(), 5)
        queued = asyncio.Event()
        acquire = service.manager._semaphore.acquire

        async def acquire_with_event():
            queued.set()
            return await acquire()

        monkeypatch.setattr(service.manager._semaphore, "acquire", acquire_with_event)
        second_task = asyncio.create_task(
            api.post(f"/v1/agents/{second}/execute", json={"prompt": "queued"})
        )
        await asyncio.wait_for(queued.wait(), 5)
        try:
            assert (await api.get(f"/v1/agents/{second}")).json()["status"] == "queued"
            stopped = await api.post(f"/v1/agents/{second}/stop")
            queued_reply = await asyncio.wait_for(second_task, 5)
            assert stopped.json()["status"] == queued_reply.json()["status"] == "aborted"
            assert queued_reply.json()["current_run_id"] is None
            expired = await api.post(f"/v1/agents/{third}/execute", json={"prompt": "expire"})
            assert (
                expired.json()["status"] == "aborted"
                and expired.json()["result"]["code"] == "timeout"
            )
            assert (await api.get(f"/v1/agents/{third}")).json()["result"]["code"] == "timeout"
        finally:
            await api.post(f"/v1/agents/{first}/stop")
            await first_task
            if not second_task.done():
                await api.post(f"/v1/agents/{second}/stop")
                await second_task
        limited = (await api.post("/v1/agents", json={"name": "one_turn"})).json()["agent_id"]
        completed = await api.post(
            f"/v1/agents/{limited}/execute", json={"prompt": "first", "idempotency_key": "first"}
        )
        refused = await api.post(
            f"/v1/agents/{limited}/resume", json={"prompt": "second", "idempotency_key": "second"}
        )
        assert completed.json()["status"] == "completed" and refused.json()["status"] == "failed"
        assert refused.json()["result"]["code"] == "max_turns"
        assert (await api.get(f"/v1/agents/{limited}")).json()["result"]["code"] == "max_turns"
        assert (await repository.get_run(completed.json()["current_run_id"])).status == "completed"


async def test_http_close_and_reopen_drop_handles_but_preserve_runs(tmp_path):
    config = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'agents-api.sqlite'}")
    app = create_app(config)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        agent = (await api.post("/v1/agents", json={"name": "default"})).json()
        finished = (
            await api.post(
                f"/v1/agents/{agent['agent_id']}/execute", json={"prompt": "/echo persisted"}
            )
        ).json()
    assert not app.state.agents.manager._handles
    reopened = create_app(config)
    async with (
        reopened.router.lifespan_context(reopened),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=reopened), base_url="http://127.0.0.1"
        ) as api,
    ):
        assert (await api.get(f"/v1/agents/{agent['agent_id']}")).status_code == 404
        assert (await api.get("/v1/agents")).json() == []
        run_id = finished["current_run_id"]
        assert (await api.get(f"/v1/runs/{run_id}")).json()["status"] == "completed"
        assert "persisted" in (await api.get(f"/v1/runs/{run_id}/messages")).text
        assert '"status":"completed"' in (await api.get(f"/v1/runs/{run_id}/events")).text


async def test_http_agent_errors_limits_and_authentication(repository):
    runtime = ChatRuntime(
        repository,
        FakeProvider(delay=0),
        ToolRegistry(),
        Settings(api_key=SecretStr("local-test"), agent_max_agents=1),
    )
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        assert (await api.get("/v1/agents/catalog")).status_code == 401
        api.headers["Authorization"] = "Bearer local-test"
        agent_id = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
        assert (await api.post("/v1/agents", json={"name": "default"})).status_code == 429
        assert (await api.get(f"/v1/agents/{agent_id}/events")).status_code == 409
        assert (await api.get("/v1/agents/missing/events")).status_code == 404
        assert (
            await api.post(f"/v1/agents/{agent_id}/resume", json={"prompt": "no run"})
        ).status_code == 409
        assert (
            await api.post(f"/v1/agents/{agent_id}/execute", json={"prompt": " "})
        ).status_code == 422
        first = await api.post(
            f"/v1/agents/{agent_id}/execute", json={"prompt": "one", "idempotency_key": "same"}
        )
        conflict = await api.post(
            f"/v1/agents/{agent_id}/execute",
            json={"prompt": "different", "idempotency_key": "same"},
        )
        assert conflict.status_code == 409
        assert (await api.get(f"/v1/agents/{agent_id}/events?from_seq=999999")).status_code == 416
        assert first.json()["status"] == "completed"
