import asyncio
import json
import socket
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pydantic import SecretStr

from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.persistence import Database
from krim_agent.providers import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.sdk import PythonAI


def event(delta, reason=None):
    value = {"choices": [{"index": 0, "delta": delta, "finish_reason": reason}]}
    return f"data: {json.dumps(value, ensure_ascii=False)}\n\n"


def end(reason):
    return event({}, reason) + "data: [DONE]\n\n"


@asynccontextmanager
async def tcp_app(app):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", timeout_graceful_shutdown=2))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 5)
        finally:
            listener.close()


@pytest.fixture
async def provider_server():
    records = []
    release = asyncio.Event()
    closed = asyncio.Event()
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        payload = await request.json()
        records.append(payload)
        assert request.headers["Authorization"] == "Bearer test-key"
        assert payload["model"] == "local-model" and payload["stream"] is True
        messages = payload["messages"]
        prompt = next(item["content"] for item in reversed(messages) if item["role"] == "user")

        async def stream():
            if prompt == "gate":
                try:
                    yield event({"content": "FIRST"})
                    await release.wait()
                    yield event({"content": "SECOND"})
                    yield end("stop")
                finally:
                    closed.set()
                return
            if messages[-1]["role"] == "tool":
                assert messages[-1]["tool_call_id"] == messages[-2]["tool_calls"][0]["id"]
                assert json.loads(messages[-1]["content"])["sum"] == 5
                yield event({"content": "5"})
                yield end("stop")
                return
            assert payload["tools"][0]["function"]["name"] == "add"
            yield event({"content": "计算中"})
            await asyncio.sleep(0.005)
            yield event(
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": f"local-{len(records)}",
                            "type": "function",
                            "function": {"name": "add", "arguments": '{"left":2'},
                        },
                    ]
                }
            )
            await asyncio.sleep(0.005)
            yield event(
                {
                    "tool_calls": [
                        {"index": 0, "function": {"arguments": ',"right":3}'}},
                    ]
                }
            )
            yield end("tool_calls")

        return StreamingResponse(stream(), media_type="text/event-stream")

    async with tcp_app(app) as url:
        try:
            yield SimpleNamespace(url=url + "/v1", records=records, release=release, closed=closed)
        finally:
            release.set()


def settings(server, **options):
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        provider="openai-compatible",
        provider_base_url=server.url,
        provider_model="local-model",
        provider_api_key=SecretStr("test-key"),
        **options,
    )


async def test_real_tcp_sdk_factory_and_two_step_tool_loop(provider_server):
    ai = await PythonAI.create(settings=settings(provider_server))
    provider = ai.runtime.provider
    try:
        result = await ai.generate_text("计算 2 + 3", idempotency_key="tcp-sdk")
        assert result.text == "5" and result.run.status == "completed"
        assert result.all_text == "计算中5" and len(result.steps) == 2
        assert result.steps[0].tool_results[0].data == {"sum": 5.0}
        assert len(result.audits) == 1 and result.audits[0].ok
        assert len(provider_server.records) == 2
        again = await ai.generate_text(
            "计算 2 + 3",
            topic_id=result.run.topic_id,
            idempotency_key="tcp-sdk",
        )
        assert again.run.id == result.run.id and len(provider_server.records) == 2
    finally:
        await ai.close()
    assert provider._http.is_closed


async def test_real_tcp_first_text_arrives_before_provider_finishes(provider_server):
    async with await PythonAI.create(settings=settings(provider_server)) as ai:
        stream = await ai.stream_text("gate")
        text_stream = stream.text_stream()
        try:
            assert await asyncio.wait_for(anext(text_stream), 3) == "FIRST"
            assert not provider_server.release.is_set()
            provider_server.release.set()
            assert "".join([text async for text in text_stream]) == "SECOND"
            assert (await stream.collect()).run.status == "completed"
        finally:
            provider_server.release.set()
            await text_stream.aclose()


async def test_real_tcp_sdk_abort_closes_remote_stream_and_keeps_partial_text(provider_server):
    async with await PythonAI.create(settings=settings(provider_server)) as ai:
        stream = await ai.stream_text("gate")
        text_stream = stream.text_stream()
        try:
            assert await asyncio.wait_for(anext(text_stream), 3) == "FIRST"
            stopped = await asyncio.wait_for(stream.abort(), 3)
            assert stopped.status == "aborted"
            await asyncio.wait_for(provider_server.closed.wait(), 3)
            result = await stream.collect()
            assert result.finish_reason == "abort" and result.text == "FIRST"
            assert result.steps[-1].partial
        finally:
            provider_server.release.set()
            await text_stream.aclose()


async def test_real_tcp_http_and_cli_consume_configured_provider(provider_server, monkeypatch):
    monkeypatch.setenv("KRIM_AGENT_PROVIDER", "openai-compatible")
    monkeypatch.setenv("KRIM_AGENT_PROVIDER_BASE_URL", provider_server.url)
    monkeypatch.setenv("KRIM_AGENT_PROVIDER_MODEL", "local-model")
    monkeypatch.setenv("KRIM_AGENT_PROVIDER_API_KEY", "test-key")
    monkeypatch.setenv("KRIM_AGENT_DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    app = create_app()
    provider = app.state.runtime.provider
    async with tcp_app(app) as url:
        async with httpx.AsyncClient(base_url=url, trust_env=False, timeout=5) as client:
            assert (await client.get("/healthz")).json()["provider"] == "openai-compatible"
            topic = (await client.post("/v1/topics", json={"title": "provider"})).json()
            run = (
                await client.post(
                    f"/v1/topics/{topic['id']}/messages",
                    json={"text": "计算", "idempotency_key": "tcp-http"},
                )
            ).json()
            events = await client.get(f"/v1/runs/{run['id']}/events")
            assert '"status":"completed"' in events.text
            messages = (await client.get(f"/v1/runs/{run['id']}/messages")).json()["messages"]
            assert messages[-1]["content"] == "5"
            audits = (await client.get(f"/v1/runs/{run['id']}/audits")).json()
            assert len(audits) == 1
            count = len(provider_server.records)
            await client.get(f"/v1/runs/{run['id']}/resume")
            assert len(provider_server.records) == count
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "chat",
            "计算",
            "--url",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, stderr.decode()
        assert "计算中5" in stdout.decode()
        assert len(provider_server.records) == count + 2
    assert provider._http.is_closed


async def test_owned_provider_is_closed_on_sdk_and_api_initialization_failure(monkeypatch):
    providers = []

    def factory(_settings):
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url="http://127.0.0.1:9000/v1", model="test"),
            transport=httpx.MockTransport(lambda _request: httpx.Response(500)),
        )
        providers.append(provider)
        return provider

    async def fail(_database):
        raise RuntimeError("controlled database initialization failure")

    monkeypatch.setattr(Database, "initialize", fail)
    monkeypatch.setattr("krim_agent.sdk.text.create_provider", factory)
    with pytest.raises(RuntimeError, match="initialization"):
        await PythonAI.create()
    assert providers[-1]._http.is_closed
    monkeypatch.setattr("krim_agent.api.server.create_provider", factory)
    app = create_app(Settings(database_url="sqlite+aiosqlite:///:memory:"))
    with pytest.raises(RuntimeError, match="initialization"):
        async with app.router.lifespan_context(app):
            pass
    assert providers[-1]._http.is_closed
