"""A gateway may batch complete data lines; never salvage damaged messages."""

import json

import pytest
from fixtures.http_provider_server import HTTPFixture, choice, frame

from krim_agent.contracts import Message
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.providers.types import ModelText, ModelToolCall


def batch(*messages):
    lines = [m if isinstance(m, str) else json.dumps(m, ensure_ascii=False) for m in messages]
    return ("\n".join(
        "data: " + line for message in lines for line in message.split("\n")
    ) + "\n\n").encode()


async def consume(chunks):
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        try:
            return [event async for event in provider.stream(
                [Message(role="user", content="Read only")], [],
            )]
        finally:
            await provider.close()


async def test_batched_finish_and_usage_are_separate_messages():
    events = await consume([
        frame(choice({"content": "完整中文🙂"})),
        batch(choice(finish="stop"), {
            "choices": [],
            "usage": {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3},
        }, "[DONE]"),
    ])
    assert "".join(e.text for e in events if isinstance(e, ModelText)) == "完整中文🙂"


async def test_batched_tool_fragments_preserve_utf8_and_order():
    deltas = [choice({"tool_calls": [{
        "index": 0, **({"id": "call-1"} if index == 0 else {}),
        "function": {**({"name": "read"} if index == 0 else {}), "arguments": part},
    }]}) for index, part in enumerate(['{"path":"', "完整🙂.txt", '"}'])]
    events = await consume([batch(*deltas, choice(finish="tool_calls"), "[DONE]")])
    call = next(e for e in events if isinstance(e, ModelToolCall))
    assert call.arguments == {"path": "完整🙂.txt"}
    assert call.id == "call-1" and call.name == "read"


async def test_standard_multiline_json_is_not_split():
    pretty = json.dumps(choice({"content": "保留\n换行"}), indent=2, ensure_ascii=False)
    data = ("\n".join("data: " + line for line in pretty.splitlines()) + "\n\n").encode()
    events = await consume([data, frame(choice(finish="stop")), frame("[DONE]")])
    assert "".join(e.text for e in events if isinstance(e, ModelText)) == "保留\n换行"


@pytest.mark.parametrize("tail", [
    "{broken}",
    '{"choices":[],"choices":[]}',
    '{"choices":[],"usage":{"prompt_tokens":NaN}}',
    '[]',
    '[DONE]\n' + json.dumps(choice(finish="stop")),
])
async def test_malformed_batched_messages_never_release_tool_call(tail):
    tool = choice({"tool_calls": [{
        "index": 0, "id": "call-1", "function": {"name": "write", "arguments": "{}"},
    }]})
    chunks = [batch(tool, choice(finish="tool_calls"), tail)]
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        seen = []
        try:
            with pytest.raises(ProviderError):
                async for event in provider.stream([Message(role="user", content="x")], []):
                    seen.append(event)
            assert not any(isinstance(e, ModelToolCall) for e in seen)
        finally:
            await provider.close()


async def test_batched_stream_still_requires_done_marker():
    with pytest.raises(ProviderError) as error:
        await consume([batch(choice({"content": "partial"}), choice(finish="stop"))])
    assert error.value.code == "provider_incomplete_stream"
