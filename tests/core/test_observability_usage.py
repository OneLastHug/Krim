"""供应商子计数保持未知语义，与累计快照共用单调验证。"""
import pytest
from pydantic import ValidationError

from krim_agent.providers.openai_compatible import ProviderError, _usage
from krim_agent.usage import TokenUsage


def test_detailed_provider_usage_is_a_subset_not_additional_input():
    usage = _usage({
        "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
        "prompt_tokens_details": {"cached_tokens": 4, "raw": "private"},
        "completion_tokens_details": {"reasoning_tokens": 3},
        "cost_usd": 900,
    })
    assert usage is not None
    assert usage.cached_input_tokens == 4
    assert usage.reasoning_output_tokens == 3
    assert usage.cache_write_input_tokens is None
    assert usage.total_tokens == 15
    assert "private" not in usage.model_dump_json()
    assert "cost" not in usage.model_dump_json()


async def test_stream_reports_preserve_detailed_final_usage():
    from krim_agent.contracts import Message
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleConfig,
        OpenAICompatibleProvider,
    )
    from krim_agent.providers.types import ModelUsage
    from tests.core.fixtures.http_provider_server import HTTPFixture, choice, frame

    chunks = [
        frame(choice({"content": "x"}, usage={"prompt_tokens": 10})),
        frame(choice(finish="stop")),
        frame({"choices": [], "usage": {
            "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
            "prompt_tokens_details": {"cached_tokens": 4},
            "completion_tokens_details": {"reasoning_tokens": 3},
        }}),
        frame("[DONE]"),
    ]
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture"))
        try:
            events = [item async for item in provider.stream(
                [Message(role="user", content="usage")], [])]
        finally:
            await provider.close()
    reports = [item for item in events if isinstance(item, ModelUsage)]
    assert len(reports) == 3
    assert reports[0].usage.cached_input_tokens is None
    assert reports[-1].final
    assert reports[-1].usage.cached_input_tokens == 4
    assert reports[-1].usage.reasoning_output_tokens == 3
    assert reports[-1].usage.total_tokens == 15


def test_legacy_usage_does_not_invent_zero_subcounts():
    usage = _usage({"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15})
    assert usage is not None
    assert usage.cached_input_tokens is None
    assert usage.reasoning_output_tokens is None
    assert usage.cache_write_input_tokens is None


@pytest.mark.parametrize("value", [
    {"prompt_tokens": 2, "prompt_tokens_details": {"cached_tokens": 3}},
    {"completion_tokens": 2, "completion_tokens_details": {"reasoning_tokens": 3}},
    {"prompt_tokens": 2, "prompt_tokens_details": []},
    {"completion_tokens": 2, "completion_tokens_details": []},
    {"prompt_tokens": 2, "prompt_tokens_details": {"cached_tokens": True}},
    {"prompt_tokens": 2, "prompt_tokens_details": {"cached_tokens": -1}},
    {"completion_tokens": 2, "completion_tokens_details": {"reasoning_tokens": "1"}},
])
def test_invalid_subcounts_rejected_before_persistence(value):
    with pytest.raises(ProviderError, match="provider_usage_conflict"):
        _usage(value)


def test_subcounts_do_not_establish_reported_total_or_complete_usage():
    with pytest.raises(ValidationError):
        TokenUsage(cached_input_tokens=1)
    with pytest.raises(ValidationError):
        TokenUsage(total_tokens=2, cached_input_tokens=3)
    # Without a reported parent count, this is partial information, not an invented zero.
    usage = TokenUsage(total_tokens=20, cached_input_tokens=3)
    assert usage.input_tokens is None
    assert not TokenUsage(input_tokens=10).follows(
        TokenUsage(input_tokens=5, cached_input_tokens=3)
    )
    assert not TokenUsage(input_tokens=10, cached_input_tokens=2).follows(
        TokenUsage(input_tokens=5, cached_input_tokens=3)
    )
    assert TokenUsage(input_tokens=10, cached_input_tokens=3).follows(
        TokenUsage(input_tokens=5, cached_input_tokens=3)
    )
