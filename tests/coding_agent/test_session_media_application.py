"""Formal composition/producer: view_image reaches real SDK wire and records acceptance."""

from __future__ import annotations

import base64
import json

import httpx
import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.media.artifacts import SQLiteMediaArtifacts
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.tools.session import SessionToolScope
from tests.coding_agent.test_session_media import GIF


class SSE(httpx.AsyncByteStream):
    def __init__(self, delta, finish):
        self.delta, self.finish = delta, finish

    async def __aiter__(self):
        for delta, finish in [(self.delta, None), ({}, self.finish)]:
            body = {"id": "media-app", "object": "chat.completion.chunk", "created": 0,
                    "model": "fixture", "choices": [
                        {"index": 0, "delta": delta, "finish_reason": finish},
                    ]}
            yield b"data: " + json.dumps(body).encode() + b"\n\n"
        yield b"data: [DONE]\n\n"

    async def aclose(self):
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize("vision", [True, False])
async def test_formal_session_view_image_uses_authorized_sdk_wire(tmp_path, vision):
    requests, ref = [], {}

    async def receive(request):
        value = json.loads(request.content)
        requests.append(value)
        if len(requests) == 1:
            stream = SSE({"tool_calls": [{
                "index": 0, "id": "view-call", "type": "function", "function": {
                    "name": "view_image",
                    "arguments": json.dumps({"reference": ref["id"]}),
                },
            }]}, "tool_calls")
        else:
            stream = SSE({"content": "image request handled"}, "stop")
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

    provider = OpenAICompatibleProvider(OpenAICompatibleConfig(
        base_url="http://127.0.0.1/v1", model="fixture", allow_images=vision,
        max_request_bytes=2_000_000, max_output_tokens=8192,
    ))
    provider._client._transport = httpx.MockTransport(receive)
    provider._sealed = provider.binding_token()
    app = await open_application(
        tmp_path / "media-app.sqlite", provider=provider,
        settings=Settings(provider="http", http_mode="agent", max_steps=4,
                          max_request_bytes=2_000_000),
    )
    try:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="session"))
        # Simulate an authorized pre-existing uploaded/MCP artifact. The real tool
        # later resolves the live run scope itself; model args carry only the ref.
        artifact_scope = SessionToolScope(LOCAL, "local", session.id, "ingest-turn", "ingest-run")
        saved = await SQLiteMediaArtifacts(app.store).save(artifact_scope, GIF, "image/gif")
        ref["id"] = saved.reference.reference
        turn = await app.sessions.submit(
            LOCAL, "local", session.id,
            TurnRequest(request_key="inspect", text="Inspect my authorized image",
                        expected_revision=0),
        )
        result = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert result.status == "completed", result.code
        assert len(requests) == 2
        snapshot = await app.store.snapshot(result.run_id)
        end = next(event for event in snapshot.events
                   if event.kind == "tool_end" and event.data.get("name") == "view_image")
        assert end.data["status"] == "ok"
        value = end.data["value"]
        selections = [event.data for event in snapshot.events if event.kind == "media_selection"]
        if vision:
            assert value["status"] == "ready"
            last = requests[-1]["messages"][-1]["content"]
            image = next(part for part in last if part["type"] == "image_url")
            url = image["image_url"]["url"]
            assert url.startswith("data:image/gif;base64,")
            assert base64.b64decode(url.partition(",")[2]) == GIF
            assert [event["status"] for event in selections] == ["prepared", "accepted"]
            assert selections[0]["selection"]["parts"][0]["reference"]["reference"] == ref["id"]
        else:
            assert value["status"] == "unavailable"
            assert value["reason"] == "model_does_not_support_images"
            assert not selections
            assert not any(
                isinstance(message.get("content"), list) and any(
                    part.get("type") == "image_url" for part in message["content"]
                ) for message in requests[-1]["messages"]
            )
    finally:
        await app.close()
