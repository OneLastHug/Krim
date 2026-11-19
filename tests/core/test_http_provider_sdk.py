"""Python AI SDK 接入：真实 TCP 流、重复元数据和工具结果往返。"""

import asyncio

import ai
import pytest
from fixtures.http_provider_server import HTTPFixture, choice, frame

from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.tools.builtins import builtin_registry


class RepeatedMetadataWire(HTTPFixture):
    def __init__(self, *, repeat_name):
        super().__init__()
        self.repeat_name = repeat_name

    def script(self, body):
        if body["messages"][-1]["role"] == "tool":
            return super().script(body)
        identifier = "call_moerouter_0123456789abcdef"
        arguments = '{"left":2.0,"right":3.0}'
        chunks = []
        for index, part in enumerate(arguments):
            function = {"arguments": part}
            if not index or self.repeat_name:
                function["name"] = "add"
            chunks.append(
                frame(
                    choice(
                        {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": identifier,
                                    "type": "function",
                                    "function": function,
                                }
                            ]
                        }
                    )
                )
            )
        return chunks + [frame(choice(finish="tool_calls")), frame("[DONE]")]


@pytest.mark.parametrize("repeat_name", [False, True])
async def test_repeated_complete_id_round_trip_uses_sdk(monkeypatch, repeat_name):
    real_stream = ai.stream
    calls = []

    def traced_stream(*args, **kwargs):
        calls.append((args, kwargs))
        return real_stream(*args, **kwargs)

    monkeypatch.setattr(ai, "stream", traced_stream)
    async with RepeatedMetadataWire(repeat_name=repeat_name) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        history = [Message(role="user", content="/add 2 3")]
        try:
            events = [
                event async for event in provider.stream(history, builtin_registry().schemas())
            ]
            tool_calls = [event for event in events if isinstance(event, ModelToolCall)]
            assert len(tool_calls) == 1
            call = tool_calls[0]
            assert call.id == "call_moerouter_0123456789abcdef"
            assert call.name == "add" and call.arguments == {"left": 2.0, "right": 3.0}
            history.extend(
                [
                    Message(
                        role="assistant", content="", tool_calls=[call.model_dump(mode="json")]
                    ),
                    Message(role="tool", content='{"sum":5}', call_id=call.id),
                ]
            )
            events = [
                event async for event in provider.stream(history, builtin_registry().schemas())
            ]
            assert "".join(event.text for event in events if isinstance(event, ModelText)) == "5"
            assert len(calls) == 2
            assert all(request["path"] == "/v1/chat/completions" for request in fixture.requests)
            assert fixture.requests[1]["body"]["messages"][-1]["tool_call_id"] == call.id
            assert provider._sdk_client.max_retries == 0
        finally:
            await provider.close()
        assert provider._owned_client.is_closed
    assert fixture.active == 0


async def test_identical_argument_fragments_are_not_deduplicated():
    fragments = ['{"text":"', "哈", "哈", "哈", '"}']
    chunks = [
        frame(
            choice(
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "same-id",
                            "function": {"name": "echo", "arguments": part},
                        }
                    ]
                }
            )
        )
        for part in fragments
    ]
    chunks += [frame(choice(finish="tool_calls")), frame("[DONE]")]
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        try:
            events = [
                event async for event in provider.stream([Message(role="user", content="x")], [])
            ]
            call = next(event for event in events if isinstance(event, ModelToolCall))
            assert call.arguments == {"text": "哈哈哈"}
        finally:
            await provider.close()


@pytest.mark.parametrize("mode", ["id_fragments", "arguments_before_id"])
async def test_fragmented_or_delayed_metadata_preserves_arguments(mode):
    if mode == "id_fragments":
        deltas = [
            {"index": 0, "id": "wire", "function": {"name": "ec", "arguments": '{"text":"'}},
            {"index": 0, "id": "-1", "function": {"name": "ho", "arguments": "hello"}},
        ]
    else:
        deltas = [
            {"index": 0, "function": {"arguments": '{"text":"'}},
            {"index": 0, "id": "wire-1", "function": {"name": "echo", "arguments": "hello"}},
        ]
    deltas.append({"index": 0, "function": {"arguments": '"}'}})
    chunks = [frame(choice({"tool_calls": [delta]})) for delta in deltas]
    chunks += [frame(choice(finish="tool_calls")), frame("[DONE]")]
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        try:
            events = [
                event async for event in provider.stream([Message(role="user", content="x")], [])
            ]
            calls = [event for event in events if isinstance(event, ModelToolCall)]
            assert len(calls) == 1
            assert calls[0].id == "wire-1" and calls[0].name == "echo"
            assert calls[0].arguments == {"text": "hello"}
        finally:
            await provider.close()


async def test_concurrent_streams_keep_separate_boundaries():
    async with HTTPFixture() as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )

        async def consume(text):
            events = [
                event
                async for event in provider.stream(
                    [Message(role="user", content=f"/echo {text}")], []
                )
            ]
            return next(event for event in events if isinstance(event, ModelToolCall)).arguments

        try:
            results = await asyncio.gather(consume("first"), consume("second"))
            assert results == [{"text": "first"}, {"text": "second"}]
            assert provider.active == 0 and provider.streams_closed == 2
        finally:
            await provider.close()


@pytest.mark.parametrize("mutation", ["sdk_url", "sdk_retries", "hooks", "sdk_model"])
async def test_sdk_binding_cannot_change_after_sealing(mutation):
    async with HTTPFixture() as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        try:
            if mutation == "sdk_url":
                provider._sdk_client.base_url = "https://different.invalid/v1"
            elif mutation == "sdk_retries":
                provider._sdk_client.max_retries = 2
            elif mutation == "hooks":
                provider._client.event_hooks["response"].clear()
            else:
                provider._sdk_model.id = "different"
            with pytest.raises(ServiceError, match="target_binding_changed"):
                _ = [
                    event
                    async for event in provider.stream([Message(role="user", content="x")], [])
                ]
            assert fixture.requests == []
        finally:
            await provider.close()


async def test_sdk_http_error_does_not_retry_or_leak_body():
    async with HTTPFixture(status=429) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        try:
            with pytest.raises(ProviderError, match="provider_http_error"):
                _ = [
                    event
                    async for event in provider.stream([Message(role="user", content="x")], [])
                ]
            assert len(fixture.requests) == 1
        finally:
            await provider.close()
