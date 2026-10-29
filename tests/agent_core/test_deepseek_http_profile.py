import asyncio
import json

import httpx
import pytest

from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.providers.types import ModelText, ModelToolCall


def event(delta=None, finish=None):
    return {
        "choices": [
            {"index": 0, "delta": delta or {}, "finish_reason": finish},
        ]
    }


def frame(value):
    data = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    return f"data: {data}\n\n".encode()


_ASYNC_CLIENT = httpx.AsyncClient


def mock_provider(monkeypatch, handler, *, thinking=None):
    def make_client(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _ASYNC_CLIENT(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", make_client)
    return OpenAICompatibleProvider(
        OpenAICompatibleConfig(
            base_url="https://api.deepseek.com",
            model="deepseek-flash",
            thinking=thinking,
        )
    )


class StreamBody(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        self.closed = True


class WaitingBody(httpx.AsyncByteStream):
    def __init__(self):
        self.started = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        self.started.set()
        await asyncio.Event().wait()
        yield b"unreachable"

    async def aclose(self):
        self.closed = True


async def test_disabled_thinking_projects_tools_and_followup_history(monkeypatch):
    records = []
    bodies = [
        StreamBody(
            [
                frame(event({"reasoning_content": None})),
                frame(
                    event(
                        {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call-",
                                    "type": "function",
                                    "function": {"name": "ad", "arguments": '{"left":2'},
                                }
                            ]
                        }
                    )
                ),
                frame(
                    event(
                        {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "1",
                                    "function": {
                                        "name": "d",
                                        "arguments": ',"right":3}',
                                    },
                                }
                            ]
                        }
                    )
                ),
                frame(event(finish="tool_calls")),
                frame("[DONE]"),
            ]
        ),
        StreamBody([frame(event({"content": "5"})), frame(event(finish="stop")), frame("[DONE]")]),
    ]

    async def handler(request):
        records.append(json.loads(request.content))
        body = bodies[len(records) - 1]
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=body,
        )

    provider = mock_provider(monkeypatch, handler, thinking="disabled")
    tool = {
        "name": "add",
        "input_schema": {
            "type": "object",
            "properties": {"left": {"type": "number"}, "right": {"type": "number"}},
            "required": ["left", "right"],
            "additionalProperties": False,
        },
    }
    user = Message(role="user", content="add 2 and 3")
    try:
        first = [item async for item in provider.stream([user], [tool])]
        call = next(item for item in first if isinstance(item, ModelToolCall))
        assert call.id == "call-1" and call.arguments == {"left": 2, "right": 3}

        history = [
            user,
            Message(
                role="assistant",
                content="",
                tool_calls=[call.model_dump(mode="json")],
            ),
            Message(role="tool", content='{"sum":5}', call_id=call.id),
        ]
        second = [item async for item in provider.stream(history, [tool])]
        assert "".join(item.text for item in second if isinstance(item, ModelText)) == "5"

        assert [body["thinking"] for body in records] == [
            {"type": "disabled"},
            {"type": "disabled"},
        ]
        assert records[0]["tools"] == [
            {"type": "function", "function": {"name": "add", "parameters": tool["input_schema"]}}
        ]
        assert records[1]["messages"][-2]["tool_calls"][0]["id"] == call.id
        assert records[1]["messages"][-1] == {
            "role": "tool",
            "content": '{"sum":5}',
            "tool_call_id": call.id,
        }
        assert provider.identity()["thinking"] == "disabled"
        assert all(body.closed for body in bodies)
    finally:
        await provider.close()


@pytest.mark.parametrize(
    "thinking,delta,accepted",
    [
        ("disabled", {"reasoning_content": None}, True),
        ("disabled", {"reasoning_content": ""}, True),
        (None, {"reasoning_content": None}, False),
        (None, {"reasoning_content": ""}, False),
        ("disabled", {"reasoning_content": "private reasoning"}, False),
        ("disabled", {"reasoning_content": []}, False),
        ("disabled", {"audio": {"data": "unknown"}}, False),
    ],
)
async def test_reasoning_content_is_only_accepted_empty_when_disabled(
    monkeypatch, thinking, delta, accepted
):
    body = StreamBody([frame(event(delta)), frame(event(finish="stop")), frame("[DONE]")])

    async def handler(_request):
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)

    provider = mock_provider(monkeypatch, handler, thinking=thinking)
    try:
        if accepted:
            [item async for item in provider.stream([Message(role="user", content="hi")], [])]
        else:
            with pytest.raises(ProviderError) as error:
                [item async for item in provider.stream([Message(role="user", content="hi")], [])]
            assert error.value.code == "provider_protocol"
        assert body.closed
    finally:
        await provider.close()


async def test_provider_error_and_cancellation_close_mock_http_stream(monkeypatch):
    error_body = StreamBody([b"data: {bad}\n\n"])

    async def error_handler(_request):
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=error_body,
        )

    provider = mock_provider(monkeypatch, error_handler, thinking="disabled")
    try:
        with pytest.raises(ProviderError):
            [
                item
                async for item in provider.stream([Message(role="user", content="bad")], [])
            ]
        assert error_body.closed
    finally:
        await provider.close()

    cancel_body = WaitingBody()

    async def cancel_handler(_request):
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=cancel_body,
        )

    provider = mock_provider(monkeypatch, cancel_handler, thinking="disabled")

    async def consume():
        return [item async for item in provider.stream([Message(role="user", content="wait")], [])]

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(cancel_body.started.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancel_body.closed
    finally:
        await provider.close()


def test_thinking_configuration_is_literal_and_from_env_is_explicit(monkeypatch):
    for name in (
        "BASE_URL",
        "MODEL",
        "API_KEY",
        "MAX_OUTPUT_TOKENS",
        "REASONING_EFFORT",
        "THINKING",
        "READ_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(f"KRIM_AGENT_HTTP_{name}", raising=False)
    with pytest.raises(ValueError):
        OpenAICompatibleConfig(
            base_url="https://api.deepseek.com",
            model="deepseek-flash",
            thinking="enabled",
        )

    monkeypatch.setenv("KRIM_AGENT_HTTP_BASE_URL", "https://api.deepseek.com")
    monkeypatch.setenv("KRIM_AGENT_HTTP_MODEL", "deepseek-flash")
    monkeypatch.setenv("KRIM_AGENT_HTTP_THINKING", "disabled")
    config = OpenAICompatibleConfig.from_env()
    assert config is not None and config.thinking == "disabled"

    monkeypatch.setenv("KRIM_AGENT_HTTP_THINKING", "enabled")
    with pytest.raises(ProviderError) as error:
        OpenAICompatibleConfig.from_env()
    assert error.value.code == "provider_config"


async def test_thinking_option_is_part_of_frozen_provider_binding(monkeypatch):
    requests = []

    async def handler(request):
        requests.append(request)
        return httpx.Response(500)

    provider = mock_provider(monkeypatch, handler)
    sealed = provider.binding_token()
    try:
        object.__setattr__(provider.config, "thinking", "disabled")
        assert provider.binding_token() != sealed
        with pytest.raises(ServiceError) as error:
            [item async for item in provider.stream([Message(role="user", content="hi")], [])]
        assert error.value.code == "target_binding_changed"
        assert requests == []
    finally:
        await provider.close()


def test_invalid_bypassed_thinking_config_is_rejected_before_client_creation(monkeypatch):
    created = False

    def unexpected_client(*_args, **_kwargs):
        nonlocal created
        created = True
        raise AssertionError("invalid config must not construct an HTTP client")

    monkeypatch.setattr(httpx, "AsyncClient", unexpected_client)
    invalid = OpenAICompatibleConfig.model_construct(
        base_url="https://api.deepseek.com",
        model="deepseek-flash",
        thinking="enabled",
    )
    with pytest.raises(ProviderError) as error:
        OpenAICompatibleProvider(invalid)
    assert error.value.code == "provider_config" and not created


async def test_default_profile_omits_thinking_from_identity_and_request(monkeypatch):
    records = []

    async def handler(request):
        records.append(json.loads(request.content))
        body = StreamBody(
            [
                frame(event({"content": "ok"})),
                frame(event(finish="stop")),
                frame("[DONE]"),
            ]
        )
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)

    provider = mock_provider(monkeypatch, handler)
    try:
        assert "thinking" not in provider.identity()
        [item async for item in provider.stream([Message(role="user", content="hi")], [])]
        assert "thinking" not in records[0]
    finally:
        # 默认配置保持原有请求字段不变。
        await provider.close()
