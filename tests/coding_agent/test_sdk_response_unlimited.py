"""The real SDK completion gate keeps complete large responses without a default quota."""

import ai
import pytest
from ai import events

from krim_agent.agent_sdk.response import CompleteResponseProvider, IncompleteTurn
from tests.coding_agent.sdk_fixtures import Scripted, model, turn


async def consume(source, **limits):
    calls = []

    async def prepare(batch):
        calls.extend(batch)

    provider = CompleteResponseProvider(model(source), prepare, **limits)
    wrapped = ai.Model(id="complete", provider=provider)
    async with ai.stream(wrapped, [ai.user_message("task")]) as stream:
        async for _ in stream:
            pass
        message = stream.message
    return message, calls


async def test_large_tool_arguments_pass_once_only_after_response_is_complete():
    body = "正文" * 300_000
    source = Scripted([turn(("write", "large-call", body))])
    message, admitted = await consume(source)
    assert len(message.tool_calls) == len(admitted) == 1
    assert body in admitted[0].tool_args or "\\u6b63" in admitted[0].tool_args
    assert len(admitted[0].tool_args.encode()) > 1_048_576


async def test_more_than_old_event_count_is_not_silently_dropped():
    response = [events.StreamStart(), events.TextStart(block_id="text")]
    response.extend(events.TextDelta(block_id="text", chunk="x") for _ in range(4200))
    response.extend([events.TextEnd(block_id="text"), events.StreamEnd(finish_reason="stop")])
    message, admitted = await consume(Scripted([response]))
    assert message.text == "x" * 4200
    assert admitted == []


@pytest.mark.parametrize("limits", [{"max_bytes": 100}, {"max_events": 2}])
async def test_explicit_response_capacity_still_rejects_before_admission(limits):
    with pytest.raises(IncompleteTurn, match="configured capacity"):
        await consume(Scripted([turn(("write", "call", "x" * 1000))]), **limits)


async def test_failed_finish_remains_non_dispatchable_without_a_size_quota():
    with pytest.raises(IncompleteTurn, match="finish reason"):
        await consume(Scripted([turn(("write", "call", "value"), finish="length")]))
