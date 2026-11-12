"""Moerouter 式工具流重复元数据兼容：上游每帧重发完整 id/name 时的网关行为。"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from test_budget_gateway import (
    _TOKEN_A,
    ChunkedBody,
    chat_body,
    config,
    frame,
    rows,
    running_gateway,
    usage,
)

from krim_agent.providers.budget_gateway import _GatewayFault, _validate_stream_event


def tool_schema(name: str) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": "Synthetic repeated-metadata tool",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    }


def call_part(index: int, call_id: str, name: str, arguments: str) -> dict[str, Any]:
    """Moerouter 风格分片：每帧都重发该 index 的完整 id 与 name。"""
    return {
        "index": index,
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


def arguments_part(index: int, arguments: str) -> dict[str, Any]:
    return {"index": index, "function": {"arguments": arguments}}


def delta_frame(*parts: dict[str, Any], finish_reason: str | None = None) -> dict[str, Any]:
    delta: dict[str, Any] = {"tool_calls": list(parts)} if parts else {}
    return {"choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]}


def parse_events(stream_text: str) -> list[dict[str, Any]]:
    return [
        json.loads(line[6:])
        for line in stream_text.strip().split("\n\n")
        if line.startswith("data: ") and line[6:] != "[DONE]"
    ]


def reassembled_calls(stream_text: str) -> dict[int, dict[str, str]]:
    """按下游逐帧累加语义重组转发流中的工具调用。"""
    calls: dict[int, dict[str, str]] = {}
    for event in parse_events(stream_text):
        for choice in event.get("choices", []):
            for part in choice.get("delta", {}).get("tool_calls", []) or []:
                entry = calls.setdefault(part["index"], {"id": "", "name": "", "arguments": ""})
                entry["id"] += part.get("id") or ""
                function = part.get("function") or {}
                entry["name"] += function.get("name") or ""
                entry["arguments"] += function.get("arguments") or ""
    return calls


def feed_stream(events: list[dict[str, Any]]) -> dict[int, dict[str, str]]:
    buffers: dict[int, dict[str, str]] = {}
    finish_seen = False
    for event in events:
        _, finish_seen = _validate_stream_event(event, finish_seen, buffers)
    return buffers


@pytest.mark.asyncio
async def test_repeated_complete_id_and_name_stream_reassembles_intact(tmp_path):
    # 每帧重发完整 id/name：朴素累加 id 为 45*5=225>128、name 为 15*5=75>64，旧实现必然超限。
    call_id = "call-" + "m" * 40
    name = "summarize_notes"
    fragments = ['{"topic":"', "notes", "notes", "notes", '"}']
    chunks = [frame(delta_frame(call_part(0, call_id, name, part))) for part in fragments]
    chunks += [
        frame(delta_frame(finish_reason="tool_calls")),
        frame({"choices": [], "usage": usage(23, 8)}),
        frame("[DONE]"),
    ]
    forwarded: list[httpx.Request] = []

    async def handler(_request: httpx.Request) -> httpx.Response:
        forwarded.append(_request)
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=ChunkedBody(chunks),
        )

    ledger = tmp_path / "repeat-meta.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        response = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(stream=True, tools=[tool_schema(name)]),
        )
        stream = response.text
        assert response.status_code == 200
        assert "GW_" not in stream and "data: [DONE]" in stream
        # 连续相同的 arguments 分片（notes 三次）必须逐片保留，不能去重。
        assert reassembled_calls(stream) == {
            0: {"id": call_id, "name": name, "arguments": '{"topic":"notesnotesnotes"}'}
        }
        assert any(event.get("usage") == usage(23, 8) for event in parse_events(stream))
        assert len(forwarded) == 1
    record = rows(ledger)[0]
    assert record[1] == "settled" and record[5:7] == (23, 8)


@pytest.mark.asyncio
async def test_interleaved_tool_indexes_with_repeated_metadata_reassemble_exactly(tmp_path):
    # 三个 index 交错；每个 index 的元数据重发与纯 arguments 帧混排，各分片必须按流序落位。
    specs = [
        (0, "call-alpha-01", "alpha_tool", '{"a":"x","k":1}', ['{"a":', '"x",', '"k":1}'], 1),
        (1, "call-beta-02", "beta_tool", '{"b":"yy","k":2}', ['{"b":', '"yy",', '"k":2}'], 2),
        (2, "call-gamma-3", "gamma_tool", '{"c":"zz","k":3}', ['{"c":', '"zz",', '"k":3}'], 0),
    ]
    frames = []
    for step in range(3):
        parts = []
        for index, call_id, name, _expected, fragments, args_only_step in specs:
            if step == args_only_step:
                parts.append(arguments_part(index, fragments[step]))
            else:
                parts.append(call_part(index, call_id, name, fragments[step]))
        frames.append(delta_frame(*parts))
    chunks = [frame(item) for item in frames]
    chunks += [
        frame(delta_frame(finish_reason="tool_calls")),
        frame({"choices": [], "usage": usage(31, 6)}),
        frame("[DONE]"),
    ]

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=ChunkedBody(chunks),
        )

    ledger = tmp_path / "repeat-interleaved.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        response = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(
                stream=True,
                tools=[
                    tool_schema("alpha_tool"),
                    tool_schema("beta_tool"),
                    tool_schema("gamma_tool"),
                ],
            ),
        )
        stream = response.text
        assert response.status_code == 200
        assert "GW_" not in stream and "data: [DONE]" in stream
        assert reassembled_calls(stream) == {
            0: {"id": "call-alpha-01", "name": "alpha_tool", "arguments": '{"a":"x","k":1}'},
            1: {"id": "call-beta-02", "name": "beta_tool", "arguments": '{"b":"yy","k":2}'},
            2: {"id": "call-gamma-3", "name": "gamma_tool", "arguments": '{"c":"zz","k":3}'},
        }
    record = rows(ledger)[0]
    assert record[1] == "settled" and record[5:7] == (31, 6)


@pytest.mark.asyncio
async def test_true_oversize_id_and_name_fragments_still_rejected(tmp_path):
    # 不相同分片仍逐片追加：真实超长的 id/name 在累加越限处被拒，而不是被幂等规则吞掉。
    id_overflow = [
        frame(delta_frame(call_part(0, "call-", "lookup", "{}"))),
        frame(delta_frame({"index": 0, "id": "z" * 130, "function": {"arguments": "{}"}})),
    ]
    name_overflow = [
        frame(delta_frame(call_part(0, "call-name-cap", "lookup", "{}"))),
        frame(delta_frame({"index": 0, "function": {"name": "q" * 60}})),
    ]
    streams = [id_overflow, name_overflow]
    forwarded: list[httpx.Request] = []

    async def handler(_request: httpx.Request) -> httpx.Response:
        forwarded.append(_request)
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=ChunkedBody(streams[len(forwarded) - 1]),
        )

    ledger = tmp_path / "repeat-oversize.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        headers = {"Authorization": f"Bearer {_TOKEN_A}"}
        body = chat_body(stream=True, tools=[tool_schema("lookup")])
        long_id = await client.post("/v1/chat/completions", headers=headers, json=body)
        assert long_id.status_code == 200
        assert "GW_UPSTREAM_SIZE" in long_id.text
        long_name = await client.post("/v1/chat/completions", headers=headers, json=body)
        assert long_name.status_code == 200
        assert "GW_UPSTREAM_SIZE" in long_name.text
    assert [record[1] for record in rows(ledger)] == ["unknown", "unknown"]


@pytest.mark.asyncio
async def test_same_id_across_indexes_still_rejected_after_repeat_idempotence(tmp_path):
    # 幂等只作用于同 index：两个 index 携带相同 id 在收尾校验处仍被拒绝。
    parts_first = [
        call_part(0, "call-duplicate-9", "left_tool", '{"a":'),
        call_part(1, "call-duplicate-9", "right_tool", '{"b":'),
    ]
    parts_repeat = [
        call_part(0, "call-duplicate-9", "left_tool", "1}"),
        call_part(1, "call-duplicate-9", "right_tool", "2}"),
    ]
    chunks = [frame(delta_frame(*parts_first)), frame(delta_frame(*parts_repeat))]
    chunks += [
        frame(delta_frame(finish_reason="tool_calls")),
        frame({"choices": [], "usage": usage(12, 5)}),
        frame("[DONE]"),
    ]

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=ChunkedBody(chunks),
        )

    ledger = tmp_path / "repeat-duplicate-id.sqlite"
    async with running_gateway(config(ledger), httpx.MockTransport(handler)) as (_app, client):
        response = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {_TOKEN_A}"},
            json=chat_body(
                stream=True, tools=[tool_schema("left_tool"), tool_schema("right_tool")]
            ),
        )
        assert response.status_code == 200
        assert "GW_UPSTREAM_PROTOCOL" in response.text
        assert "[DONE]" not in response.text
    assert rows(ledger)[0][1] == "unknown"


def test_exact_complete_repeats_are_idempotent_and_stripped_from_forwarded_delta():
    first = call_part(0, "call-x", "do_thing", '{"a":')
    second = call_part(0, "call-x", "do_thing", "1}")
    split_one = {"index": 0, "id": "call-"}
    split_two = {"index": 0, "id": "def"}
    split_repeat = {"index": 0, "id": "call-def"}
    repeat_buffers = feed_stream([delta_frame(first), delta_frame(second)])
    split_buffers = feed_stream(
        [delta_frame(split_one), delta_frame(split_two), delta_frame(split_repeat)]
    )
    assert repeat_buffers == {0: {"id": "call-x", "name": "do_thing", "arguments": '{"a":1}'}}
    assert split_buffers == {0: {"id": "call-def", "name": "", "arguments": ""}}
    # 首见分片保留在转发增量里；重复的完整 id/name 从转发增量中剔除。
    assert first["id"] == "call-x" and first["function"]["name"] == "do_thing"
    assert "id" not in second and "name" not in second["function"]
    assert second["function"]["arguments"] == "1}"
    assert "id" not in split_repeat


def test_dissimilar_fragments_append_and_arguments_stay_append_only():
    suffix_repeat = {"index": 0, "id": "abc"}
    args_first = {"index": 0, "function": {"arguments": "ab"}}
    args_second = {"index": 0, "function": {"arguments": "ab"}}
    buffers = feed_stream(
        [
            delta_frame({"index": 0, "id": "call-abc"}),
            delta_frame(suffix_repeat),
            delta_frame(args_first),
            delta_frame(args_second),
        ]
    )
    assert buffers[0]["id"] == "call-abcabc"
    assert suffix_repeat["id"] == "abc"
    assert buffers[0]["arguments"] == "abab"
    assert args_first["function"]["arguments"] == "ab"
    assert args_second["function"]["arguments"] == "ab"


def test_accumulation_length_caps_still_reject_and_boundary_values_pass():
    boundary = call_part(0, "call-" + "q" * 123, "t" * 64, '{"a":1}')
    assert feed_stream([delta_frame(boundary)]) == {
        0: {"id": "call-" + "q" * 123, "name": "t" * 64, "arguments": '{"a":1}'}
    }
    with pytest.raises(_GatewayFault) as id_fault:
        feed_stream([delta_frame(call_part(0, "call-" + "q" * 124, "ok_name", "{}"))])
    assert id_fault.value.code == "GW_UPSTREAM_SIZE"
    with pytest.raises(_GatewayFault) as name_fault:
        feed_stream([delta_frame(call_part(0, "call-ok", "t" * 65, "{}"))])
    assert name_fault.value.code == "GW_UPSTREAM_SIZE"
