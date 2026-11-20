"""Real local TCP/SSE coverage for the SDK's native Responses protocol."""

import copy

import pytest
from fixtures.http_provider_server import HTTPFixture, frame

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import Message, RunRequest
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.providers.types import ModelToolCall
from tests.support.builtins import builtin_registry


def tool_events(arguments='{"left":2,"right":3}'):
    item = {
        "type": "function_call",
        "id": "fc_1",
        "call_id": "call_1",
        "name": "add",
        "arguments": "",
        "status": "in_progress",
    }
    final = dict(item, arguments=arguments, status="completed")
    return [
        {
            "type": "response.created",
            "response": {"id": "resp_1", "status": "in_progress", "output": []},
        },
        {"type": "response.output_item.added", "output_index": 0, "item": item},
        {
            "type": "response.function_call_arguments.delta",
            "item_id": "fc_1",
            "output_index": 0,
            "delta": arguments,
        },
        {
            "type": "response.function_call_arguments.done",
            "item_id": "fc_1",
            "output_index": 0,
            "arguments": arguments,
        },
        {"type": "response.output_item.done", "output_index": 0, "item": final},
        {
            "type": "response.completed",
            "response": {
                "id": "resp_1",
                "status": "completed",
                "output": [final],
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "total_tokens": 15,
                    "input_tokens_details": {"cached_tokens": 2},
                    "output_tokens_details": {"reasoning_tokens": 1},
                },
            },
        },
    ]


def text_events():
    part = {"type": "output_text", "text": "5", "annotations": []}
    item = {
        "type": "message",
        "id": "msg_1",
        "role": "assistant",
        "status": "in_progress",
        "content": [],
    }
    final = dict(item, status="completed", content=[part])
    return [
        {
            "type": "response.created",
            "response": {"id": "resp_2", "status": "in_progress", "output": []},
        },
        {"type": "response.output_item.added", "output_index": 0, "item": item},
        {
            "type": "response.content_part.added",
            "item_id": "msg_1",
            "output_index": 0,
            "content_index": 0,
            "part": dict(part, text=""),
        },
        {
            "type": "response.output_text.delta",
            "item_id": "msg_1",
            "output_index": 0,
            "content_index": 0,
            "delta": "5",
        },
        {
            "type": "response.output_text.done",
            "item_id": "msg_1",
            "output_index": 0,
            "content_index": 0,
            "text": "5",
        },
        {
            "type": "response.content_part.done",
            "item_id": "msg_1",
            "output_index": 0,
            "content_index": 0,
            "part": part,
        },
        {"type": "response.output_item.done", "output_index": 0, "item": final},
        {
            "type": "response.completed",
            "response": {
                "id": "resp_2",
                "status": "completed",
                "output": [final],
                "usage": {"input_tokens": 12, "output_tokens": 1, "total_tokens": 13},
            },
        },
    ]


class ResponsesFixture(HTTPFixture):
    def script(self, body):
        events = (
            text_events()
            if any(item.get("type") == "function_call_output" for item in body["input"])
            else tool_events()
        )
        return [frame(event) for event in events]


async def test_responses_sdk_tool_result_round_trip_is_stateless(tmp_path):
    async with ResponsesFixture() as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(
                base_url=fixture.url,
                model="responses-fixture",
                wire_api="responses",
                max_output_tokens=512,
            )
        )
        try:
            async with await open_application(
                tmp_path / "responses.sqlite",
                provider=provider,
                settings=Settings(
                    provider="http",
                    http_mode="agent",
                    max_steps=2,
                    max_tool_calls=1,
                    max_tools_per_step=1,
                ),
            ) as app:
                started = await app.service.start(
                    RunRequest(text="add 2 and 3", request_key="responses", target="target-http")
                )
                finished = await app.service.wait(started.id)
                assert finished.status == "completed", finished.code
                result = await app.service.result(started.id)
                assert result.text == "5" and result.model_steps == 2
                assert len(result.tools) == 1 and result.tools[0].executor_started
                assert result.tools[0].provider_call_id == "call_1"
                assert result.token_usage["total_tokens"] == 28
                usages = [event.data["usage"] for event in result.events if event.kind == "usage"]
                assert any(usage["cached_input_tokens"] == 2 for usage in usages)
                assert any(usage["reasoning_output_tokens"] == 1 for usage in usages)
                assert len(fixture.requests) == 2
                for request in fixture.requests:
                    assert request["path"] == "/v1/responses"
                    assert request["body"]["store"] is False
                    assert request["body"]["max_output_tokens"] == 512
                    assert "messages" not in request["body"] and "n" not in request["body"]
                assert fixture.requests[0]["body"]["tools"][0]["type"] == "function"
                assert any(
                    item.get("type") == "function_call_output" and item["call_id"] == "call_1"
                    for item in fixture.requests[1]["body"]["input"]
                )
                assert provider._sdk_client.max_retries == 0
        finally:
            await provider.close()


@pytest.mark.parametrize("shape", ["done_only", "index_only", "large_reasoning"])
async def test_responses_native_stream_shapes_and_large_bounded_event(shape):
    events = tool_events()
    if shape == "done_only":
        events.pop(2)
    elif shape == "index_only":
        for event in events:
            event.pop("item_id", None)
    else:
        events.insert(
            1,
            {
                "type": "response.output_item.added",
                "output_index": 2,
                "item": {
                    "type": "reasoning",
                    "id": "rs_1",
                    "encrypted_content": "x" * 70000,
                    "summary": [],
                },
            },
        )
    async with HTTPFixture(chunks=[frame(event) for event in events]) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(
                base_url=fixture.url,
                model="fixture",
                wire_api="responses",
                max_response_bytes=1048576,
                max_event_bytes=1048576,
            )
        )
        try:
            events_out = [
                event
                async for event in provider.stream(
                    [Message(role="user", content="test")],
                    builtin_registry().schemas(),
                )
            ]
            calls = [event for event in events_out if isinstance(event, ModelToolCall)]
            assert len(calls) == 1 and calls[0].id == "call_1"
            assert calls[0].arguments == {"left": 2, "right": 3}
        finally:
            await provider.close()


@pytest.mark.parametrize(
    "mode,code",
    [
        ("missing_terminal", "provider_incomplete_stream"),
        ("incomplete", "provider_output_limit"),
        ("arguments_conflict", "provider_protocol"),
        ("terminal_conflict", "provider_protocol"),
        ("duplicate_call_id", "provider_protocol"),
        ("oversize_arguments", "provider_tool_arguments"),
        ("failed", "provider_protocol"),
    ],
)
async def test_responses_never_releases_unverified_tools(mode, code):
    events = copy.deepcopy(tool_events())
    if mode == "missing_terminal":
        events.pop()
    elif mode == "incomplete":
        events[-1]["type"] = "response.incomplete"
    elif mode == "arguments_conflict":
        events[3]["arguments"] = '{"left":9,"right":3}'
    elif mode == "terminal_conflict":
        events[-1]["response"]["output"][0] = dict(
            events[-1]["response"]["output"][0], arguments='{"left":9,"right":3}'
        )
    elif mode == "duplicate_call_id":
        other = dict(events[1]["item"], id="fc_2")
        final = dict(events[4]["item"], id="fc_2")
        events[5:5] = [
            {"type": "response.output_item.added", "output_index": 1, "item": other},
            {
                "type": "response.function_call_arguments.delta",
                "item_id": "fc_2",
                "output_index": 1,
                "delta": final["arguments"],
            },
            {"type": "response.output_item.done", "output_index": 1, "item": final},
        ]
        events[-1]["response"]["output"].append(final)
    elif mode == "oversize_arguments":
        events = tool_events('{"text":"' + "x" * 4096 + '"}')
    elif mode == "failed":
        events[-1]["type"] = "response.failed"
    async with HTTPFixture(chunks=[frame(event) for event in events]) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(
                base_url=fixture.url,
                model="fixture",
                wire_api="responses",
                max_tool_calls=2,
                max_arguments_bytes=4096 if mode == "oversize_arguments" else None,
            )
        )
        emitted = []
        try:
            with pytest.raises(ProviderError, match=code):
                async for event in provider.stream(
                    [Message(role="user", content="test")], builtin_registry().schemas()
                ):
                    emitted.append(event)
            assert not any(isinstance(event, ModelToolCall) for event in emitted)
        finally:
            await provider.close()
