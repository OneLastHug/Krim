"""工具输入实时流：provider层分片delta、最终ID/名称绑定与既有契约边界。"""

import asyncio
import json

from fixtures.http_provider_server import HTTPFixture, choice, frame

from krim_agent.contracts import Message
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.providers.types import (
    ModelProgress,
    ModelText,
    ModelToolCall,
    ModelToolInput,
    ModelUsage,
)
from krim_agent.runtime.model_step import ModelStep


def streamed_tool_chunks(steps):
    """每个步骤一帧；``name``/``id`` 为 None 表示该帧不带对应元数据。"""
    chunks = []
    for index, identifier, name, part in steps:
        function = {"arguments": part}
        if name is not None:
            function["name"] = name
        delta = {"index": index, "function": function}
        if identifier is not None:
            delta["id"] = identifier
        chunks.append(frame(choice({"tool_calls": [delta]})))
    return chunks + [frame(choice(finish="tool_calls")), frame("[DONE]")]


async def open_provider(fixture):
    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
    )
    return provider


async def test_tool_input_streams_multiple_deltas_before_completion():
    fragments = ['{"left":', "2", '.0,"right":', "3", ".0}"]
    chunks = streamed_tool_chunks(
        [(0, "call-delta-1", "add", part) for part in fragments]
    )
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = await open_provider(fixture)
        try:
            events = [
                event
                async for event in provider.stream_with_diagnostics(
                    [Message(role="user", content="x")], [], None
                )
            ]
        finally:
            await provider.close()
        assert fixture.active == 0
    inputs = [event for event in events if isinstance(event, ModelToolInput)]
    deltas = [event for event in inputs if event.delta]
    assert [event.delta for event in deltas] == fragments
    assert len(deltas) == 5
    started = [event for event in inputs if not event.delta and not event.complete]
    assert len(started) == 1
    assert started[0].name == "add" and started[0].delta == ""
    assert all(event.id == "call-delta-1" for event in inputs)
    assert all(event.stream_id == "call-delta-1" for event in inputs)
    complete = inputs[-1]
    assert complete.complete and complete.delta == ""
    assert complete.arguments == {"left": 2.0, "right": 3.0}
    call = next(event for event in events if isinstance(event, ModelToolCall))
    assert events.index(complete) < events.index(call)
    assert complete.id == call.id and complete.name == call.name
    assert call.arguments == {"left": 2.0, "right": 3.0}
    progress = [event for event in events if isinstance(event, ModelProgress)]
    argument_progress = [event for event in progress if event.phase == "tool_arguments"]
    sizes = [event.argument_bytes for event in argument_progress]
    assert sizes == sorted(sizes) and len(set(sizes)) == len(sizes)
    assert sizes[-1] == len(b'{"left":2.0,"right":3.0}')
    complete_progress = [event for event in progress if event.phase == "tool_complete"]
    assert len(complete_progress) == 1
    assert complete_progress[0].argument_bytes == sizes[-1]


async def test_tool_input_binds_fragmented_id_and_name_to_final_call():
    chunks = streamed_tool_chunks(
        [
            (0, "wire", "ec", '{"text":"'),
            (0, "-1", "ho", "hello"),
            (0, None, None, '"}'),
        ]
    )
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = await open_provider(fixture)
        try:
            events = [
                event
                async for event in provider.stream_with_diagnostics(
                    [Message(role="user", content="x")], [], None
                )
            ]
        finally:
            await provider.close()
    inputs = [event for event in events if isinstance(event, ModelToolInput)]
    assert [
        (event.id, event.stream_id, event.name, event.delta, event.complete)
        for event in inputs
    ] == [
        ("wire", "wire", "ec", "", False),
        ("wire", "wire", "ec", '{"text":"', False),
        ("wire-1", "wire", "echo", "hello", False),
        ("wire-1", "wire", "echo", '"}', False),
        ("wire-1", "wire", "echo", "", True),
    ]
    complete = inputs[-1]
    assert complete.arguments == {"text": "hello"}
    call = next(event for event in events if isinstance(event, ModelToolCall))
    assert call.id == "wire-1" and call.name == "echo"
    assert call.arguments == {"text": "hello"}
    assert complete.id == call.id and complete.name == call.name


async def test_plain_stream_keeps_model_event_contract():
    chunks = streamed_tool_chunks(
        [(0, "call-plain", "add", part) for part in ['{"left":', "1", '.0,"right":', "2", ".0}"]]
    )
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = await open_provider(fixture)
        try:
            events = [
                event
                async for event in provider.stream([Message(role="user", content="x")], [])
            ]
        finally:
            await provider.close()
    assert events
    assert all(
        isinstance(event, (ModelText, ModelToolCall, ModelUsage)) for event in events
    )
    assert not any(isinstance(event, (ModelToolInput, ModelProgress)) for event in events)
    assert any(isinstance(event, ModelToolCall) for event in events)


async def test_unicode_and_escapes_survive_fragmented_tool_input():
    fragments = ['{"text":"', "中文✅", '\\"q\\"', "\\\\", "\\n", '尾"}']
    wire = "".join(fragments)
    expected = {"text": '中文✅"q"\\\n尾'}
    assert json.loads(wire) == expected
    chunks = streamed_tool_chunks(
        [(0, "call-中文", "echo", part) for part in fragments]
    )
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = await open_provider(fixture)
        try:
            events = [
                event
                async for event in provider.stream_with_diagnostics(
                    [Message(role="user", content="x")], [], None
                )
            ]
        finally:
            await provider.close()
    inputs = [event for event in events if isinstance(event, ModelToolInput)]
    deltas = [event for event in inputs if event.delta]
    assert [event.delta for event in deltas] == fragments
    assert "".join(event.delta for event in deltas) == wire
    assert deltas[4].delta == "\\n"
    assert all(event.stream_id == "call-中文" for event in inputs)
    complete = inputs[-1]
    assert complete.complete and complete.arguments == expected
    assert complete.id == "call-中文"
    call = next(event for event in events if isinstance(event, ModelToolCall))
    assert call.id == "call-中文" and call.arguments == expected


async def test_parallel_tool_input_streams_stay_isolated():
    chunks = streamed_tool_chunks(
        [
            (0, "call-a", "add", '{"left":1.0,'),
            (1, "call-b", "echo", '{"text":"'),
            (0, None, None, '"right":2.0}'),
            (1, None, None, "世界\"}"),
        ]
    )
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = await open_provider(fixture)
        try:
            events = [
                event
                async for event in provider.stream_with_diagnostics(
                    [Message(role="user", content="x")], [], None
                )
            ]
        finally:
            await provider.close()
    inputs = [event for event in events if isinstance(event, ModelToolInput)]
    assert all(event.id in {"call-a", "call-b"} for event in inputs)
    started = [event for event in inputs if not event.delta and not event.complete]
    assert len(started) == 2
    assert {event.id: event.name for event in started} == {"call-a": "add", "call-b": "echo"}
    a_deltas = [event.delta for event in inputs if event.id == "call-a" and event.delta]
    b_deltas = [event.delta for event in inputs if event.id == "call-b" and event.delta]
    assert "".join(a_deltas) == '{"left":1.0,"right":2.0}'
    assert "".join(b_deltas) == '{"text":"世界"}'
    completes = [event for event in inputs if event.complete]
    assert {event.id: event.arguments for event in completes} == {
        "call-a": {"left": 1.0, "right": 2.0},
        "call-b": {"text": "世界"},
    }
    calls = [event for event in events if isinstance(event, ModelToolCall)]
    assert {call.id: call.arguments for call in calls} == {
        "call-a": {"left": 1.0, "right": 2.0},
        "call-b": {"text": "世界"},
    }
    for complete in completes:
        call = next(call for call in calls if call.id == complete.id)
        assert complete.name == call.name and complete.arguments == call.arguments


class SlowSSEWire:
    """最小本机SSE服务：帧间真实间隔>预览节流INTERVAL(0.2s)，驱动实时路径。"""

    def __init__(self, chunks, gap=0.25):
        self.chunks, self.gap = chunks, gap
        self.server = None
        self.url = ""

    async def __aenter__(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/v1"
        return self

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()

    async def _handle(self, reader, writer):
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            size = 0
            for line in header.decode("ascii").split("\r\n"):
                if line.lower().startswith("content-length:"):
                    size = int(line.split(":", 1)[1])
            await reader.readexactly(size)
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                b"Connection: close\r\n\r\n"
            )
            await writer.drain()
            for chunk in self.chunks:
                writer.write(chunk)
                await writer.drain()
                await asyncio.sleep(self.gap)
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass


async def test_model_step_emits_previews_live_before_model_end():
    fragments = ['{"path":"notes/a.md","content":"alpha\\n', "beta\\ngamma\\n", 'delta\\n"}']
    chunks = streamed_tool_chunks(
        [(0, "call-write-1", "write", part) for part in fragments]
    )
    tools = [{
        "name": "write",
        "description": "write a file",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
            "required": ["path", "content"],
        },
    }]
    events = []

    async def emit(kind, data):
        events.append((kind, data, asyncio.get_running_loop().time()))

    async with SlowSSEWire(chunks) as wire:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=wire.url, model="fixture")
        )
        step = ModelStep(
            provider,
            emit,
            lambda: None,
            max_steps=1,
            max_output=65536,
            deadline=asyncio.get_running_loop().time() + 30,
            workspace=False,
        )
        try:
            await step.call([Message(role="user", content="写文件")], tools)
        finally:
            await provider.close()
    kinds = [kind for kind, _, _ in events]
    assert "model_start" in kinds and "model_end" in kinds
    previews = [data for kind, data, _ in events if kind == "file_change_preview"]
    assert len(previews) >= 2
    counts = {(data["lines_added"], data["lines_removed"]) for data in previews}
    assert len(counts) >= 2
    assert all(
        data["name"] == "write"
        and data["path"] == "notes/a.md"
        and data["provider_call_id"] == "call-write-1"
        for data in previews
    )
    preview_positions = [
        index for index, kind in enumerate(kinds) if kind == "file_change_preview"
    ]
    assert max(preview_positions) < kinds.index("model_end")
    preview_times = [time for kind, _, time in events if kind == "file_change_preview"]
    assert preview_times[1] - preview_times[0] >= 0.2
    assert previews[-1]["complete"] is True and previews[-1]["lines_added"] == 4
