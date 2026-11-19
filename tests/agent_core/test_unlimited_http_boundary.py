"""Complete HTTP bodies retain authorization and finite JSON validation."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from test_extensions_api import bootstrap, client_for, running_api

from krim_agent.api.boundary import LocalBoundary
from krim_agent.config import Settings


@pytest.mark.asyncio
async def test_memory_body_above_old_http_cap_is_saved_read_and_replayed(tmp_path):
    path = tmp_path / "large-http-memory.sqlite"
    token = await bootstrap(path)
    body = "用户提供的长期资料 中🙂\n" * 6000
    payload = {"request_key": "large-memory", "body": body}
    assert len(json.dumps(payload, ensure_ascii=False).encode()) > 65536
    async with running_api(path) as api:
        async with client_for(api, token) as client:
            created = await client.post("/v2/me/memories", json=payload)
            assert created.status_code == 201, created.text
            memory_id = created.json()["id"]
            read = await client.get("/v2/me/memories/" + memory_id)
            assert read.status_code == 200 and read.json()["body"] == body
            repeated = await client.post("/v2/me/memories", json=payload)
            assert repeated.status_code == 201 and repeated.json()["id"] == memory_id
            denied = await client.post(
                "/v2/me/memories", json={**payload, "request_key": "cross-origin"},
                headers={"Origin": "https://elsewhere.invalid"},
            )
            assert denied.status_code == 403
            invalid = await client.post(
                "/v2/me/memories", content=b'{"body":"a","body":"b"}',
                headers={"Content-Type": "application/json"},
            )
            assert invalid.status_code == 422


async def run_boundary(parts, *, headers=None, remote=False, settings=None):
    messages = list(parts)
    sent = []
    consumed = []
    delivered = []

    async def receive():
        if messages:
            value = messages.pop(0)
            consumed.append(value)
            return value
        return {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    async def inner(scope, receive, send):
        delivered.append(await receive())
        # Body must not replay a second time.
        delivered.append(await receive())
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    scope = {
        "type": "http", "method": "POST", "path": "/save", "scheme": "http",
        "headers": [(b"host", b"127.0.0.1"), *(headers or [])],
        "client": ("198.51.100.1" if remote else "127.0.0.1", 1),
        "app": SimpleNamespace(state=SimpleNamespace()),
    }
    await LocalBoundary(inner, settings or Settings())(scope, receive, send)
    return sent, consumed, delivered


@pytest.mark.asyncio
async def test_large_chunked_body_is_replayed_once_and_disconnect_aborts():
    body = json.dumps({"body": "中🙂" * 50000}, ensure_ascii=False).encode()
    parts = [{"type": "http.request", "body": body[i:i + 8192],
              "more_body": i + 8192 < len(body)} for i in range(0, len(body), 8192)]
    sent, consumed, delivered = await run_boundary(parts)
    assert len(consumed) == len(parts)
    assert delivered == [{"type": "http.request", "body": body, "more_body": False},
                         {"type": "http.disconnect"}]
    assert sent[0]["status"] == 204
    sent, _, delivered = await run_boundary([
        {"type": "http.request", "body": body[:100000], "more_body": True},
        {"type": "http.disconnect"},
    ])
    assert not sent and not delivered


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [
    b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'"\xff"',
    ("[" * 18 + "0" + "]" * 18).encode(),
])
async def test_structure_validation_remains_before_app_dispatch(raw):
    sent, _, delivered = await run_boundary([
        {"type": "http.request", "body": raw, "more_body": False},
    ])
    assert sent[0]["status"] == 422 and not delivered


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["key", "origin", "nonlocal"])
async def test_auth_origin_and_local_rejections_do_not_consume_body(mode):
    settings = Settings(api_key="SYNTHETIC_BOUNDARY_TEST_KEY") if mode == "key" else Settings()
    headers = [(b"origin", b"https://elsewhere.invalid")] if mode == "origin" else []
    sent, consumed, delivered = await run_boundary([
        {"type": "http.request", "body": b"x" * 100000, "more_body": False},
    ], settings=settings, headers=headers, remote=mode == "nonlocal")
    assert sent[0]["status"] == (401 if mode == "key" else 403)
    assert not consumed and not delivered
