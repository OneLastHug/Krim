import asyncio
import json
import socket

import httpx
import uvicorn
from pydantic import SecretStr
from sqlalchemy import inspect

from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.protocol.models import StreamFrame
from krim_agent.providers import ModelText
from krim_agent.runtime import ChatRuntime
from krim_agent.tools import ToolRegistry


def parse_sse(text):
    return [
        StreamFrame.model_validate_json(line[6:])
        for line in text.splitlines()
        if line.startswith("data: ")
    ]


async def test_http_roundtrip_openapi_idempotency_and_refresh(api):
    health = await api.get("/healthz")
    assert health.json()["provider"] == "fake-offline"
    schema = (await api.get("/openapi.json")).json()
    assert schema["info"]["title"] == "Krim Agent"
    assert "ChatRequest" in schema["components"]["schemas"]
    assert "ErrorResponse" in schema["components"]["schemas"]
    topic = (await api.post("/v1/topics", json={"title": "API"})).json()
    payload = {"text": "/add 2 3", "idempotency_key": "once"}
    response = await api.post(f"/v1/topics/{topic['id']}/messages", json=payload)
    assert response.status_code == 202
    run = response.json()
    events = await api.get(f"/v1/runs/{run['id']}/events")
    frames = parse_sse(events.text)
    assert events.headers["content-type"].startswith("text/event-stream")
    assert frames[-1].data["status"] == "completed"
    history = (await api.get(f"/v1/topics/{topic['id']}/messages")).json()["messages"]
    assert history[-1]["content"] == '[fake] 工具结果：{"sum": 5.0}'
    repeat = await api.post(f"/v1/topics/{topic['id']}/messages", json=payload)
    assert repeat.json()["id"] == run["id"]
    assert len((await api.get(f"/v1/runs/{run['id']}/audits")).json()) == 1
    replay = await api.get(f"/v1/runs/{run['id']}/events", headers={"Last-Event-ID": "1"})
    assert parse_sse(replay.text)[0].seq == 2
    assert (await api.post(f"/v1/runs/{run['id']}/stop")).json()["status"] == "completed"


async def test_request_security_and_error_envelopes(api):
    assert (await api.get("/v1/topics", headers={"Host": "evil.test"})).status_code == 403
    assert (
        await api.post("/v1/topics", json={"title": "x"}, headers={"Origin": "http://evil.test"})
    ).status_code == 403
    big = await api.post("/v1/topics", content=b"x" * 65537)
    assert big.status_code == 413 and big.json()["error"]["code"] == "body_too_large"
    invalid = await api.post("/v1/topics", json={"title": "", "unknown": True})
    assert invalid.status_code == 422 and invalid.json()["error"]["code"] == "invalid_request"
    missing = await api.get("/v1/topics/missing/messages")
    assert missing.status_code == 404 and missing.json()["error"]["code"] == "topic_not_found"
    assert (await api.get("/v1/runs/missing/events?from_seq=-1")).status_code == 422


async def test_nonlocal_client_cannot_spoof_local_host(runtime):
    app = create_app(runtime=runtime)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=("203.0.113.1", 12345)),
        base_url="http://127.0.0.1",
    ) as client:
        assert (await client.get("/v1/topics")).status_code == 403


async def test_audit_primary_key_is_scoped_to_run(repository):
    async with repository.database.engine.connect() as connection:
        fields = await connection.run_sync(
            lambda sync: inspect(sync).get_pk_constraint("tool_audits")["constrained_columns"]
        )
    assert fields == ["call_id", "run_id"]


async def test_unicode_line_separators_do_not_break_sse(api):
    topic = (await api.post("/v1/topics", json={"title": "Unicode"})).json()
    run = (
        await api.post(
            f"/v1/topics/{topic['id']}/messages",
            json={
                "text": "hello\u0085world\u2028paragraph\u2029end",
                "idempotency_key": "unicode",
            },
        )
    ).json()
    frames = parse_sse((await api.get(f"/v1/runs/{run['id']}/events")).text)
    text = "".join(
        frame.data["text"]
        for frame in frames
        if isinstance(frame.data, dict) and frame.data.get("type") == "text_delta"
    )
    assert "\u0085" in text and "\u2028" in text and "\u2029" in text


async def test_authentication_is_required_when_configured():
    settings = Settings(database_url="sqlite+aiosqlite:///:memory:", api_key=SecretStr("test-key"))
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client,
    ):
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.get("/v1/tools")).status_code == 401
        assert (await client.get("/openapi.json")).status_code == 401
        authorized = await client.get("/v1/tools", headers={"Authorization": "Bearer test-key"})
        assert authorized.status_code == 200
        assert {item["name"] for item in authorized.json()["tools"]} == {
            "add",
            "echo",
            "plan",
            "todo",
            "question",
            "skills",
            "read_skill",
        }


async def test_restart_restores_messages_without_replaying_tools(runtime, repository):
    from krim_agent.protocol.domain import ChatRequest

    topic = await repository.create_topic("重启")
    run = await runtime.start(topic.id, ChatRequest(text="/echo retained", idempotency_key="first"))
    state = await runtime.stream_state(run.id, 0)
    async for _frame in runtime.subscribe(state):
        pass
    abandoned, _created = await repository.begin_run(
        topic.id, ChatRequest(text="abandoned", idempotency_key="pending")
    )
    database_url = str(repository.database.engine.url)
    await runtime.close()
    await repository.database.close()
    app = create_app(Settings(database_url=database_url))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client,
    ):
        history = (await client.get(f"/v1/topics/{topic.id}/messages")).json()["messages"]
        assert "retained" in history[-2]["content"]
        assert (await client.get(f"/v1/runs/{abandoned.id}")).json()["status"] == "interrupted"
        repeat = await client.post(
            f"/v1/topics/{topic.id}/messages",
            json={
                "text": "abandoned",
                "idempotency_key": "pending",
            },
        )
        assert repeat.json()["id"] == abandoned.id and repeat.json()["status"] == "interrupted"
        assert len((await client.get(f"/v1/runs/{run.id}/audits")).json()) == 1
        replay = await client.get(f"/v1/runs/{run.id}/events")
        assert replay.status_code == 200
        assert parse_sse(replay.text)[-1].data["status"] == "completed"


class GatedProvider:
    name = "test-live-sse"

    def __init__(self):
        self.release = asyncio.Event()

    async def stream(self, _messages, _tools):
        yield ModelText(text="FIRST")
        await self.release.wait()
        yield ModelText(text="SECOND")


async def test_real_tcp_sse_is_live_and_disconnect_is_safe(repository):
    provider = GatedProvider()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    app = create_app(runtime=runtime)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="on"))
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=5) as client:
            topic = (await client.post("/v1/topics", json={"title": "real TCP"})).json()
            run = (
                await client.post(
                    f"/v1/topics/{topic['id']}/messages",
                    json={
                        "text": "gated",
                        "idempotency_key": "live",
                    },
                )
            ).json()
            async with client.stream("GET", f"/v1/runs/{run['id']}/events") as response:
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        frame = json.loads(line[5:])
                        if frame.get("data", {}).get("type") == "text_delta":
                            assert frame["data"]["text"] == "FIRST"
                            assert (await repository.get_run(run["id"])).status == "running"
                            break
            provider.release.set()
            state = await runtime.stream_state(run["id"], 0)
            await asyncio.wait_for(state.task, 5)
            history = (await client.get(f"/v1/topics/{topic['id']}/messages")).json()["messages"]
            assert history[-1]["content"] == "FIRSTSECOND"
    finally:
        provider.release.set()
        server.should_exit = True
        await asyncio.wait_for(server_task, 5)
        listener.close()
