"""Host-authorized model images through the real ai/openai SDK wire path."""

from __future__ import annotations

import base64
import json
import struct
import zlib

import ai
import httpx
import pytest
from ai import messages as sdk

from krim_agent.agent_sdk.host_media import HostImageMessage
from krim_agent.agent_sdk.model import HostModelProvider
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.providers.types import ModelText
from krim_agent.runtime.model_step import ModelStep
from tests.support.builtins import builtin_registry


def _chunk(kind: bytes, payload: bytes) -> bytes:
    return (
        struct.pack(">I", len(payload)) + kind + payload
        + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)
    )


PNG = (
    b"\x89PNG\r\n\x1a\n"
    + _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    + _chunk(b"IDAT", zlib.compress(b"\x00\xff\x00\x00"))
    + _chunk(b"IEND", b"")
)


def _entropy_png(width: int, height: int) -> bytes:
    """Build a valid RGBA PNG with deterministic high-entropy pixels."""
    state = 0x12345678
    rows = bytearray()
    for _ in range(height):
        rows.append(0)
        for _ in range(width * 4):
            state ^= (state << 13) & 0xFFFFFFFF
            state ^= state >> 17
            state ^= (state << 5) & 0xFFFFFFFF
            rows.append(state & 0xFF)
    header = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + _chunk(b"IHDR", header)
        + _chunk(b"IDAT", zlib.compress(bytes(rows), level=1))
        + _chunk(b"IEND", b"")
    )


PNG_20K = _entropy_png(80, 80)


def _image(payload: bytes | str = PNG, media_type: str = "image/png") -> HostImageMessage:
    return HostImageMessage(
        role="user",
        content="Inspect the latest browser screenshot.",
        images=[sdk.FilePart(data=payload, media_type=media_type, filename="screen.png")],
    )


class _SSE(httpx.AsyncByteStream):
    async def __aiter__(self):
        for choice in (
            {"delta": {"content": "seen"}, "finish_reason": None},
            {"delta": {}, "finish_reason": "stop"},
        ):
            event = {
                "id": "chatcmpl-local",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": "fixture",
                "choices": [{"index": 0, **choice}],
            }
            yield b"data: " + json.dumps(event).encode() + b"\n\n"
        yield b"data: [DONE]\n\n"

    async def aclose(self) -> None:
        return None


async def _provider(*, allow_images: bool = False, max_request_bytes: int = 32768):
    captured: list[dict] = []

    async def receive(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=_SSE(),
        )

    config = OpenAICompatibleConfig(
        base_url="http://127.0.0.1/v1",
        model="fixture",
        allow_images=allow_images,
        max_request_bytes=max_request_bytes,
    )
    provider = OpenAICompatibleProvider(config)
    # Keep the production OpenAI SDK, request hooks, and serializer; replace only
    # the network transport with an in-memory capture.
    provider._client._transport = httpx.MockTransport(receive)
    provider._sealed = provider.binding_token()
    return provider, captured


@pytest.mark.asyncio
async def test_real_sdk_serializes_authorized_png_as_data_url_after_tool_result():
    provider, captured = await _provider(allow_images=True, max_request_bytes=1_048_576)
    history = [
        Message(role="user", content="Read the page."),
        Message(
            role="assistant",
            content="",
            tool_calls=[{"id": "wire-1", "name": "inspect", "arguments": {"url": "x"}}],
        ),
        Message(role="tool", call_id="wire-1", content='{"status":"ok"}'),
        _image(),
    ]
    try:
        result = [event async for event in provider.stream(history, [])]
        assert any(isinstance(event, ModelText) and event.text == "seen" for event in result)
        messages = captured[0]["messages"]
        assert messages[-2] == {
            "role": "tool", "tool_call_id": "wire-1", "content": '{"status":"ok"}'
        }
        content = messages[-1]["content"]
        assert content[0] == {"type": "text", "text": history[-1].content}
        image = content[1]
        assert image["type"] == "image_url"
        data_url = image["image_url"]["url"]
        assert data_url.startswith("data:image/png;base64,")
        assert base64.b64decode(data_url.partition(",")[2], validate=True) == PNG
        assert provider._client.is_closed is False
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_images_are_disabled_by_default_and_urls_are_never_fetched():
    disabled, disabled_requests = await _provider()
    try:
        with pytest.raises(ServiceError, match="invalid_input"):
            _ = [event async for event in disabled.stream([_image()], [])]
        assert disabled_requests == []
    finally:
        await disabled.close()

    enabled, enabled_requests = await _provider(allow_images=True)
    try:
        with pytest.raises(ServiceError, match="invalid_input"):
            _ = [
                event async for event in enabled.stream(
                    [_image("https://example.invalid/screen.png")], []
                )
            ]
        assert enabled_requests == []
    finally:
        await enabled.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "image",
    [
        pytest.param(_image(b"not a PNG"), id="invalid-png"),
        pytest.param(_image(PNG[:-1] + bytes([PNG[-1] ^ 1])), id="bad-crc"),
        pytest.param(_image(PNG + b"trailing"), id="trailing-bytes"),
        pytest.param(_image(PNG, "image/jpeg"), id="mime-mismatch"),
    ],
)
async def test_invalid_images_are_rejected(image: HostImageMessage):
    provider, captured = await _provider(allow_images=True, max_request_bytes=1_048_576)
    try:
        with pytest.raises(ServiceError, match="invalid_input"):
            _ = [event async for event in provider.stream([image], [])]
        assert captured == []
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_images_above_old_product_quota_fit_actual_provider_request():
    payload = _entropy_png(400, 400)
    assert len(payload) > 512 * 1024
    provider, captured = await _provider(allow_images=True, max_request_bytes=1_048_576)
    try:
        _ = [event async for event in provider.stream([_image(payload)], [])]
        image = captured[0]["messages"][-1]["content"][1]
        assert base64.b64decode(image["image_url"]["url"].partition(",")[2]) == payload
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_wire_base64_counts_toward_exact_request_limit():
    payload = _entropy_png(40, 40)
    provider, captured = await _provider(allow_images=True, max_request_bytes=2048)
    try:
        with pytest.raises(ProviderError, match="provider_request_limit"):
            _ = [event async for event in provider.stream([_image(payload)], [])]
        assert captured == []
    finally:
        await provider.close()


def test_request_limit_can_be_explicitly_raised_but_default_stays_small():
    assert OpenAICompatibleConfig(
        base_url="https://example.invalid/v1", model="fixture"
    ).max_request_bytes == 32768
    assert OpenAICompatibleConfig(
        base_url="https://example.invalid/v1",
        model="fixture",
        allow_images=True,
        max_request_bytes=1_048_576,
    ).max_request_bytes == 1_048_576
    assert OpenAICompatibleConfig(
        base_url="https://example.invalid/v1", model="fixture",
        max_request_bytes=8_388_608,
    ).max_request_bytes == 8_388_608
    with pytest.raises(ValueError):
        OpenAICompatibleConfig(
            base_url="https://example.invalid/v1",
            model="fixture",
            allow_images=1,
        )
@pytest.mark.asyncio
async def test_text_only_execution_fingerprint_omits_new_default_flag():
    legacy, _ = await _provider()
    enabled, _ = await _provider(allow_images=True)
    try:
        assert "allow_images" not in legacy.execution_limits()
        assert "allow_images" not in legacy.binding_token()[3]
        assert enabled.execution_limits()["allow_images"] is True
        assert '"allow_images":true' in enabled.binding_token()[3]
    finally:
        await legacy.close()
        await enabled.close()


class _Source:
    def __init__(self) -> None:
        self.histories: list[list[Message]] = []

    async def stream(self, messages, tools):
        self.histories.append(messages)
        yield ModelText(text="ok")


@pytest.mark.asyncio
async def test_host_media_callback_runs_per_request_without_checkpointing_images():
    source = _Source()
    emitted: list[tuple[str, dict]] = []

    async def emit(kind, data):
        emitted.append((kind, data))
        return None

    stepper = ModelStep(
        source,
        emit,
        lambda: None,
        max_steps=3,
        max_output=4096,
        deadline=None,
        workspace=False,
    )
    attachments = 0

    async def attach(history: list[Message]) -> list[Message]:
        nonlocal attachments
        attachments += 1
        return [*history, _image(PNG_20K)]

    provider = HostModelProvider(
        stepper,
        builtin_registry(),
        max_group=1,
        max_calls=1,
        request_bytes=65536,
        context_policy=ContextPolicy(max_bytes=131072, reserved_output_tokens=1024),
        host_media=attach,
    )
    sdk_history = [sdk.Message(role="user", parts=[sdk.TextPart(text="Inspect screen")])]
    model = ai.Model(id="host-media-test", provider=provider)
    for _ in range(2):
        _ = [event async for event in provider.stream(model, sdk_history)]
    assert attachments == 2
    assert all(isinstance(history[-1], HostImageMessage) for history in source.histories)
    assert all(history[-1].images[0].data == PNG_20K for history in source.histories)
    assert "image" not in json.dumps(provider.checkpoint())
    assert len(sdk_history) == 1 and isinstance(sdk_history[0].parts[0], sdk.TextPart)
    manifests = [data["context_manifest"] for kind, data in emitted if kind == "model_start"]
    image_b64 = base64.b64encode(PNG_20K).decode("ascii")
    serialized_manifest = json.dumps(manifests)
    assert image_b64 not in serialized_manifest
    request_image = next(
        item for item in manifests[-1]["items"] if item["key"] == "message:2:user"
    )
    assert request_image["input_bytes"] >= len(image_b64)
    assert len(PNG_20K) > 20 * 1024
