"""Provider aggregate limits are opt-in; protocol checks still gate tool release."""

from __future__ import annotations

import json

import httpx
import pytest
from pydantic import ValidationError

from krim_agent.contracts import Message
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.providers.types import ModelRequestBudget, ModelText, ModelToolCall


class SSE(httpx.AsyncByteStream):
    def __init__(self, events):
        self.events = events

    async def __aiter__(self):
        for event in self.events:
            raw = event if isinstance(event, str) else json.dumps(event, ensure_ascii=False)
            yield ("data: " + raw + "\n\n").encode()

    async def aclose(self):
        return None


def choice(delta=None, finish=None):
    return {"choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish}]}


def tool(index, arguments, identifier=None):
    return {"index": index, "id": identifier or f"call-{index}", "type": "function",
            "function": {"name": "write", "arguments": arguments}}


def responses(arguments):
    item = {"type": "function_call", "id": "fc_1", "call_id": "call_1",
            "name": "write", "arguments": "", "status": "in_progress"}
    final = dict(item, arguments=arguments, status="completed")
    return [
        {"type": "response.created",
         "response": {"id": "response", "status": "in_progress", "output": []}},
        {"type": "response.output_item.added", "output_index": 0, "item": item},
        {"type": "response.function_call_arguments.delta", "item_id": "fc_1",
         "output_index": 0, "delta": arguments},
        {"type": "response.function_call_arguments.done", "item_id": "fc_1",
         "output_index": 0, "arguments": arguments},
        {"type": "response.output_item.done", "output_index": 0, "item": final},
        {"type": "response.completed", "response": {"id": "response",
         "status": "completed", "output": [final]}},
    ]


async def provider_for(events, **config):
    requests = []

    async def receive(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=SSE(events),
        )

    provider = OpenAICompatibleProvider(OpenAICompatibleConfig(
        base_url="http://127.0.0.1/v1", model="fixture", **config,
    ))
    provider._client._transport = httpx.MockTransport(receive)
    provider._sealed = provider.binding_token()
    return provider, requests


def test_configuration_has_no_product_ceilings_and_none_aggregate_defaults():
    config = OpenAICompatibleConfig(base_url="http://127.0.0.1/v1", model="fixture")
    for name in ("max_response_bytes", "max_event_bytes", "max_events",
                 "max_arguments_bytes", "max_tool_calls"):
        assert getattr(config, name) is None
    explicit = config.model_copy(update={
        "max_output_tokens": 32768, "max_request_bytes": 16 * 1024 * 1024,
        "max_response_bytes": 32 * 1024 * 1024, "max_event_bytes": 2 * 1024 * 1024,
        "max_events": 100_000, "max_arguments_bytes": 8 * 1024 * 1024, "max_tool_calls": 64,
    })
    assert OpenAICompatibleConfig.model_validate(explicit.model_dump()) == explicit
    # Aggregate arguments can span events, so need not fit a single SSE frame.
    config.model_validate(config.model_dump() | {"max_event_bytes": 256,
                                               "max_arguments_bytes": 8192})
    for name in ("max_response_bytes", "max_event_bytes", "max_events",
                 "max_arguments_bytes", "max_tool_calls"):
        for invalid in (0, True, -1):
            with pytest.raises(ValidationError):
                config.model_validate(config.model_dump() | {name: invalid})


@pytest.mark.asyncio
@pytest.mark.parametrize("wire_api", ["chat_completions", "responses"])
async def test_large_file_arguments_over_one_megabyte_survive_sdk_and_strict_json(wire_api):
    text = "a" * (1024 * 1024 + 100)
    arguments = json.dumps({"content": text})
    events = (responses(arguments) if wire_api == "responses" else [
        choice({"tool_calls": [tool(0, arguments)]}), choice(finish="tool_calls"), "[DONE]",
    ])
    provider, requests = await provider_for(events, wire_api=wire_api, max_output_tokens=32768)
    try:
        history = [Message(role="user", content="write")]
        result = [event async for event in provider.stream(history, [])]
        call = next(event for event in result if isinstance(event, ModelToolCall))
        assert call.arguments == {"content": text}
        field = "max_output_tokens" if wire_api == "responses" else "max_tokens"
        assert requests[0][field] == 32768
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_large_arguments_span_explicitly_small_events():
    arguments = json.dumps({"content": "q" * 100_000})
    events = [choice({"tool_calls": [tool(0, arguments[:1000])]})]
    for offset in range(1000, len(arguments), 1000):
        events.append(choice({"tool_calls": [
            {"index": 0, "function": {"arguments": arguments[offset:offset + 1000]}},
        ]}))
    events += [choice(finish="tool_calls"), "[DONE]"]
    provider, _ = await provider_for(
        events, max_event_bytes=2048, max_arguments_bytes=200_000,
    )
    try:
        history = [Message(role="user", content="write")]
        result = [event async for event in provider.stream(history, [])]
        call = next(event for event in result if isinstance(event, ModelToolCall))
        assert call.arguments == {"content": "q" * 100_000}
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_request_larger_than_one_megabyte_uses_declared_provider_capacity():
    provider, requests = await provider_for(
        [choice({"content": "ok"}), choice(finish="stop"), "[DONE]"],
        max_request_bytes=4 * 1024 * 1024,
    )
    try:
        content = "z" * (1024 * 1024 + 100)
        history = [Message(role="user", content=content)]
        _ = [event async for event in provider.stream(history, [])]
        assert requests[0]["messages"][0]["content"] == content
    finally:
        await provider.close()


def test_optional_environment_limits_accept_none_and_unlimited(monkeypatch):
    for key in tuple(__import__("os").environ):
        if key.startswith("KRIM_AGENT_HTTP_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("KRIM_AGENT_HTTP_BASE_URL", "http://127.0.0.1/v1")
    monkeypatch.setenv("KRIM_AGENT_HTTP_MODEL", "fixture")
    for name in ("MAX_RESPONSE_BYTES", "MAX_EVENT_BYTES", "MAX_EVENTS",
                 "MAX_ARGUMENTS_BYTES", "MAX_TOOL_CALLS"):
        monkeypatch.setenv("KRIM_AGENT_HTTP_" + name, "unlimited")
    config = OpenAICompatibleConfig.from_env()
    assert config.max_response_bytes is None and config.max_tool_calls is None
    monkeypatch.setenv("KRIM_AGENT_HTTP_MAX_EVENTS", "None")
    assert OpenAICompatibleConfig.from_env().max_events is None


@pytest.mark.asyncio
async def test_text_over_old_response_limit_and_more_than_1024_events_are_preserved():
    part = "line" * 80
    events = [choice({"content": part}) for _ in range(1100)]
    events += [choice(finish="stop"), "[DONE]"]
    provider, _ = await provider_for(events)
    try:
        history = [Message(role="user", content="read")]
        result = [event async for event in provider.stream(history, [])]
        text = "".join(event.text for event in result if isinstance(event, ModelText))
        assert text == part * 1100
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_more_than_four_calls_and_explicit_host_budget_remain_separate():
    events = [choice({"tool_calls": [tool(i, "{}") for i in range(7)]}),
              choice(finish="tool_calls"), "[DONE]"]
    provider, _ = await provider_for(events)
    try:
        history = [Message(role="user", content="work")]
        result = [event async for event in provider.stream(history, [])]
        assert len([event for event in result if isinstance(event, ModelToolCall)]) == 7
        budget = ModelRequestBudget(None, None, 2)
        emitted = []
        with pytest.raises(ProviderError, match="tool_limit"):
            async for event in provider.stream_with_budget(
                [Message(role="user", content="work")], [], budget,
            ):
                emitted.append(event)
        assert not any(isinstance(event, ModelToolCall) for event in emitted)
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("limits,events,code", [
    ({"max_arguments_bytes": 20},
     [choice({"tool_calls": [tool(0, json.dumps({"text": "x" * 30}))]}),
      choice(finish="tool_calls"), "[DONE]"], "provider_tool_arguments"),
    ({"max_events": 2}, [choice({"content": "x"})] * 3, "provider_event_limit"),
    ({"max_event_bytes": 30}, [choice({"content": "x"})], "provider_event_limit"),
    ({"max_response_bytes": 100}, [choice({"content": "x" * 100})], "provider_response_limit"),
    ({"max_tool_calls": 1}, [choice({"tool_calls": [tool(0, "{}"), tool(1, "{}")]}),
     choice(finish="tool_calls"), "[DONE]"], "tool_limit"),
])
async def test_explicit_provider_limits_are_still_enforced(limits, events, code):
    provider, _ = await provider_for(events, **limits)
    try:
        emitted = []
        with pytest.raises(ProviderError, match=code):
            async for event in provider.stream([Message(role="user", content="test")], []):
                emitted.append(event)
        assert not any(isinstance(event, ModelToolCall) for event in emitted)
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("arguments", ['{"x":1,"x":2}', '{"x":NaN}', '{"x":1e999}',
                                      '{"x":9007199254740992}', '{broken'])
async def test_unlimited_arguments_keep_strict_json_validation(arguments):
    events = [choice({"tool_calls": [tool(0, arguments)]}),
              choice(finish="tool_calls"), "[DONE]"]
    provider, _ = await provider_for(events)
    try:
        with pytest.raises(ProviderError, match="provider_tool_arguments"):
            history = [Message(role="user", content="test")]
            _ = [event async for event in provider.stream(history, [])]
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_unlimited_call_count_still_rejects_duplicate_call_identity():
    events = [choice({"tool_calls": [tool(0, "{}", "same"), tool(1, "{}", "same")]}),
              choice(finish="tool_calls"), "[DONE]"]
    provider, _ = await provider_for(events)
    try:
        with pytest.raises(ProviderError, match="provider_protocol"):
            history = [Message(role="user", content="test")]
            _ = [event async for event in provider.stream(history, [])]
    finally:
        await provider.close()
