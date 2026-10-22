"""固定旧提取逻辑先RED；新适配器仅经真实本机TCP验证，不冒充商业模型。"""

import asyncio

import pytest
from fixtures.http_provider_server import HTTPFixture, choice, frame

from krim_agent.contracts import Message
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
)
from krim_agent.providers.types import ModelText, ModelToolCall, ModelUsage
from krim_agent.tools.builtins import builtin_registry


@pytest.mark.parametrize(
    "arguments", ['{"left":2,"left":9,"right":3}'], ids=["duplicate-argument-key"]
)
async def test_incomplete_or_invalid_stream_never_releases_tool_calls(arguments):
    chunks = [
        frame(
            choice(
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "wire-1",
                            "type": "function",
                            "function": {"name": "add", "arguments": arguments},
                        }
                    ]
                }
            )
        ),
        frame(choice(finish="tool_calls")),
        frame("[DONE]"),
    ]
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        events, rejected = [], False
        try:
            try:
                async for event in provider.stream([Message(role="user", content="/add 2 3")], []):
                    events.append(event)
            except Exception as error:
                assert str(getattr(error, "code", "")).startswith("provider_"), type(error)
                rejected = True
            assert rejected, "重复参数键必须拒绝；旧json.loads已接受并释放工具调用"
            assert not any(isinstance(e, ModelToolCall) for e in events)
            assert len(fixture.requests) == 1
        finally:
            await provider.close()
    assert fixture.active == 0


async def collect(fixture, *, config=None, messages=None, tools=None):
    provider = OpenAICompatibleProvider(
        config or OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
    )
    try:
        return [
            event
            async for event in provider.stream(
                messages or [Message(role="user", content="你好")], tools or []
            )
        ]
    finally:
        await provider.close()


async def test_messages_and_tools_are_projected_without_runtime_metadata():
    async with HTTPFixture() as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        messages = [
            Message(role="system", content="受控"),
            Message(role="user", content="/add 2 3"),
        ]
        try:
            first = [e async for e in provider.stream(messages, builtin_registry().schemas())]
            call = next(e for e in first if isinstance(e, ModelToolCall))
            messages += [
                Message(role="assistant", content="", tool_calls=[call.model_dump(mode="json")]),
                Message(role="tool", content='{"sum":5.0}', call_id=call.id),
            ]
            second = [e async for e in provider.stream(messages, builtin_registry().schemas())]
            assert "".join(e.text for e in second if isinstance(e, ModelText)) == "5"
            assert len(fixture.requests) == 2
            body = fixture.requests[1]["body"]
            assert set(body) == {
                "model",
                "messages",
                "stream",
                "n",
                "max_tokens",
                "stream_options",
                "tools",
            }
            function = body["tools"][0]["function"]
            assert set(function) == {"name", "parameters"}
            assert "implementation_digest" not in str(body["tools"])
            assert body["messages"][-1]["tool_call_id"] == call.id
            assert (
                body["messages"][-2]["tool_calls"][0]["function"]["arguments"]
                == '{"left":2.0,"right":3.0}'
            )
            assert all(r["accept_encoding"] == "identity" for r in fixture.requests)
        finally:
            await provider.close()


async def test_text_stream_arrives_before_done_and_closes_early():
    async with HTTPFixture(mode="hold") as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        stream = provider.stream([Message(role="user", content="hold")], [])
        try:
            first = await anext(stream)
            assert isinstance(first, ModelText) and first.text == "等待"
            assert not fixture.release.is_set()
            await stream.aclose()
            await asyncio.wait_for(fixture.idle.wait(), 2)
            assert (
                provider.active == 0
                and provider.streams_closed == 1
                and fixture.closed_connections == 1
            )
        finally:
            await stream.aclose()
            await provider.close()


async def test_fragmented_crlf_multiline_unicode_and_tool_json():
    payload = frame(choice({"content": "中文🙂" * 600})).replace(b"\n", b"\r\n")
    payload = payload.replace(b', "finish_reason"', b',\r\ndata: "finish_reason"')
    async with HTTPFixture(
        mode="bytewise", chunks=[payload, frame(choice(finish="stop")), frame("[DONE]")]
    ) as fixture:
        events = await collect(fixture)
        assert "".join(e.text for e in events if isinstance(e, ModelText)) == "中文🙂" * 600
        assert all(len(e.text.encode()) <= 4096 for e in events if isinstance(e, ModelText))
    calls = [
        frame(
            choice(
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "wire",
                            "function": {"name": "ad", "arguments": '{"left":2.0,'},
                        }
                    ]
                }
            )
        ),
        frame(
            choice(
                {
                    "tool_calls": [
                        {"index": 0, "function": {"name": "d", "arguments": '"right":3.0}'}}
                    ]
                }
            )
        ),
        frame(choice(finish="tool_calls")),
        frame("[DONE]"),
    ]
    async with HTTPFixture(mode="bytewise", chunks=calls) as fixture:
        events = await collect(fixture)
        assert [e.arguments for e in events if isinstance(e, ModelToolCall)] == [
            {"left": 2.0, "right": 3.0}
        ]


@pytest.mark.parametrize(
    "chunks,code",
    [
        ([frame(choice({"content": "partial"}))], "provider_incomplete_stream"),
        ([frame(choice(finish="stop")), b"data: [DONE]"], "provider_incomplete_stream"),
        ([frame(choice({"content": "partial"})), frame("[DONE]")], "provider_incomplete_stream"),
        ([b'data: {"choices":[],"choices":[]}\n\n'], "provider_protocol"),
        ([b'data: {"choices":NaN}\n\n'], "provider_protocol"),
        ([b"data: \xff\n\n"], "provider_protocol"),
        (
            [frame({"choices": [{"index": True, "delta": {}, "finish_reason": "stop"}]})],
            "provider_protocol",
        ),
        (
            [frame(choice(finish="stop")), frame(choice({"content": "late"})), frame("[DONE]")],
            "provider_protocol",
        ),
        ([frame(choice(finish="length"))], "provider_output_limit"),
        ([frame({"error": {"message": "SYNTHETIC_REMOTE_SECRET"}})], "provider_protocol"),
        (
            [
                frame(
                    choice(
                        {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "id",
                                    "function": {"name": "add", "arguments": "[]"},
                                }
                            ]
                        }
                    )
                ),
                frame(choice(finish="tool_calls")),
                frame("[DONE]"),
            ],
            "provider_tool_arguments",
        ),
        (
            [
                frame(
                    choice(
                        {
                            "tool_calls": [
                                {
                                    "index": 1,
                                    "id": "id",
                                    "function": {"name": "add", "arguments": "{}"},
                                }
                            ]
                        }
                    )
                )
            ],
            "provider_protocol",
        ),
    ],
)
async def test_bad_stream_vectors_zero_release(chunks, code):
    async with HTTPFixture(chunks=chunks) as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        events = []
        try:
            with pytest.raises(ProviderError) as error:
                async for event in provider.stream([Message(role="user", content="bad")], []):
                    events.append(event)
            assert error.value.code == code
            assert not any(isinstance(e, ModelToolCall) for e in events)
            assert "SECRET" not in str(error.value)
            assert len(fixture.requests) == 1
        finally:
            await provider.close()


async def test_usage_snapshots_preserve_unknowns_and_reject_conflicts():
    chunks = [
        frame(choice({"content": "x"}, usage={"prompt_tokens": 3})),
        frame(choice(finish="stop")),
        frame(
            {
                "choices": [],
                "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
            }
        ),
        frame("[DONE]"),
    ]
    async with HTTPFixture(chunks=chunks) as fixture:
        events = await collect(fixture)
        usage = [e for e in events if isinstance(e, ModelUsage)]
        assert [(e.final, e.usage.total_tokens) for e in usage] == [
            (False, None),
            (False, 5),
            (True, 5),
        ]
    async with HTTPFixture(mode="no-usage") as fixture:
        events = await collect(fixture)
        assert not any(isinstance(e, ModelUsage) for e in events)
    for bad in [
        {"prompt_tokens": True},
        {"prompt_tokens": 1.5},
        {"prompt_tokens": -1},
        {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 4},
        {"prompt_tokens": 2},
    ]:
        chunks = [
            frame(choice({"content": "x"}, usage={"prompt_tokens": 3})),
            frame(choice(finish="stop")),
            frame({"choices": [], "usage": bad}),
            frame("[DONE]"),
        ]
        async with HTTPFixture(chunks=chunks) as fixture:
            with pytest.raises(ProviderError, match="provider_usage_conflict"):
                await collect(fixture)


@pytest.mark.parametrize("kind", ["request", "response", "frame", "events", "arguments"])
async def test_request_response_frame_event_and_argument_limits(kind):
    limits = {}
    chunks = None
    code = {
        "request": "provider_request_limit",
        "response": "provider_response_limit",
        "frame": "provider_event_limit",
        "events": "provider_event_limit",
        "arguments": "provider_tool_arguments",
    }[kind]
    text = "hello"
    if kind == "request":
        limits = {"max_request_bytes": 256}
        text = "x" * 1000
    elif kind == "response":
        limits = {"max_response_bytes": 256, "max_event_bytes": 128, "max_arguments_bytes": 128}
        chunks = [b": " + b"x" * 80 + b"\n\n"] * 5
    elif kind == "frame":
        limits = {"max_event_bytes": 128, "max_arguments_bytes": 128}
        chunks = [b"data: " + b"x" * 129]
    elif kind == "events":
        limits = {"max_events": 1}
        chunks = [b": keepalive\n\n", b": keepalive\n\n"]
    else:
        limits = {"max_arguments_bytes": 2}
        chunks = [frame(choice({"tool_calls": [{"index": 0, "function": {"arguments": "xxx"}}]}))]
    async with HTTPFixture(chunks=chunks) as fixture:
        config = OpenAICompatibleConfig(base_url=fixture.url, model="fixture", **limits)
        with pytest.raises(ProviderError, match=code):
            await collect(fixture, config=config, messages=[Message(role="user", content=text)])
        assert len(fixture.requests) == (0 if kind == "request" else 1)


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:99/v1",
        "http://evil.invalid/v1",
        "https://u:p@host/v1",
        "https://host/v1?key=x",
        "https://host/v1#x",
        "https://host/%2e",
        "https://host/a/../b",
        "https://host/\\bad",
        "https://host:0/v1",
        "http://127.0.0.2/v1",
    ],
)
def test_unsafe_url_is_rejected_without_client(url):
    with pytest.raises(ValueError):
        OpenAICompatibleConfig(base_url=url, model="fixture")


async def test_url_credentials_redirect_http_and_transport_failures():
    secure = OpenAICompatibleConfig(base_url="https://example.invalid/v1", model="fixture")
    assert secure.base_url.startswith("https://")  # 纯配置，无网络。
    async with HTTPFixture() as destination:
        for status in [301, 302, 307, 308, 401, 429, 500]:
            async with HTTPFixture(
                status=status, headers={"Location": destination.url + "/SECRET"}
            ) as fixture:
                code = "provider_redirect" if status < 400 else "provider_http_error"
                with pytest.raises(ProviderError, match=code):
                    await collect(fixture)
                assert len(fixture.requests) == 1 and destination.requests == []
    for headers in [{"Content-Type": "application/json"}, {"Content-Encoding": "gzip"}]:
        async with HTTPFixture(headers=headers) as fixture:
            with pytest.raises(ProviderError, match="provider_protocol"):
                await collect(fixture)
    async with HTTPFixture(mode="hold") as fixture:
        with pytest.raises(ProviderError, match="provider_timeout"):
            await collect(
                fixture,
                config=OpenAICompatibleConfig(
                    base_url=fixture.url, model="fixture", read_timeout_seconds=0.05
                ),
            )
        assert len(fixture.requests) == 1
    async with HTTPFixture() as fixture:
        missing = fixture.url
    with pytest.raises(ProviderError, match="provider_network"):
        await collect(None, config=OpenAICompatibleConfig(base_url=missing, model="fixture"))


async def test_non_text_delta_is_not_silently_accepted():
    async with HTTPFixture(
        chunks=[frame(choice({"audio": {"data": "unhandled"}}, finish="stop")), frame("[DONE]")]
    ) as fixture:
        with pytest.raises(ProviderError, match="provider_protocol"):
            await collect(fixture)


async def test_event_limit_is_independent_of_crlf_packet_boundaries():
    """同一真实TCP内容被拆成单字节后，不能少算跨块CRLF中的LF。"""
    import httpx

    from krim_agent.providers.openai_compatible import _sse_data

    raw = b": x\r\n" * 27 + b"data: {}\r\n\r\n"
    assert len(raw) > 128
    async with HTTPFixture(chunks=[raw]) as fixture:
        config = OpenAICompatibleConfig(
            base_url=fixture.url, model="fixture", max_event_bytes=128, max_arguments_bytes=128
        )
        async with httpx.AsyncClient(trust_env=False) as client:
            async with client.stream(
                "POST", fixture.url + "/chat/completions", json={"messages": []}
            ) as response:
                original = response.aiter_raw

                async def bytes_from_real_socket():
                    async for chunk in original():
                        for byte in chunk:
                            yield bytes([byte])

                # 只拆分实际网络收到的字节；不伪造响应/协议数据。
                response.aiter_raw = bytes_from_real_socket
                with pytest.raises(ProviderError, match="provider_event_limit"):
                    _ = [value async for value in _sse_data(response, config)]


# D2：真实TCP分组屏障；夹具只发SSE字节，不复制Provider解析或工具执行。
def d2_call(index, *, arguments='{"text":"合成🙂"}', identity=None, name="echo"):
    return {"index": index, "id": identity or "d2-call-"+str(index), "type": "function",
            "function": {"name": name, "arguments": arguments}}


class D2Wire(HTTPFixture):
    def __init__(self, parts):
        super().__init__()
        self.parts = parts
        self.sent = [asyncio.Event() for _ in parts]
        self.gates = [asyncio.Event() for _ in parts]
        self.consumed = [asyncio.Event() for _ in parts]
        self.bodies = []

    async def handle(self, reader, writer):
        import json

        task = asyncio.current_task()
        self.tasks.add(task)
        self.writers.add(writer)
        self.active += 1
        self.idle.clear()
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            headers = dict(line.split(":",
                1) for line in header.decode().split("\r\n")[1:] if ":" in line)
            length = int(next(v for k, v in headers.items() if k.lower() == "content-length"))
            body = json.loads(await reader.readexactly(length))
            self.bodies.append(body)
            self.requests.append({"body": body})
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
            await writer.drain()
            chunks = self.parts if body["messages"][-1]["role"] != "tool" else [
                frame(choice({"content": "工具组已观察"}))
                + frame(choice(finish="stop")) + frame("[DONE]")]
            for index, chunk in enumerate(chunks):
                writer.write(chunk)
                await writer.drain()
                self.sent[index].set()
                release = asyncio.create_task(self.gates[index].wait())
                eof = asyncio.create_task(reader.read(1))
                try:
                    await asyncio.wait((release, eof), return_when=asyncio.FIRST_COMPLETED)
                    if eof.done():
                        return
                finally:
                    for child in (release, eof):
                        if not child.done():
                            child.cancel()
                    await asyncio.gather(release, eof, return_exceptions=True)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self.writers.discard(writer)
            self.tasks.discard(task)
            self.active -= 1
            self.closed_connections += 1
            if self.active == 0:
                self.idle.set()

    async def __aexit__(self, *args):
        for gate in self.gates:
            gate.set()
        await super().__aexit__(*args)


@pytest.fixture(autouse=True)
async def d2_http_resource_record(request, monkeypatch):
    if request.node.name.split("[")[0] not in {
        "test_workspace_complete_tool_groups_are_atomic",
        "test_workspace_bad_tool_groups_zero_release",
        "test_single_call_default_and_sealed_cap",
    }:
        yield
        return
    import hashlib
    import json
    import os
    from pathlib import Path

    from krim_agent.execution.linux import fds
    from krim_agent.providers import openai_compatible

    before = fds()
    existing = asyncio.all_tasks()
    loop = asyncio.get_running_loop()
    factory = loop.get_task_factory()
    owned_tasks = []

    def created(coro, **kwargs):
        task = factory(loop, coro, **kwargs) if factory is not None else asyncio.Task(coro,
            loop=loop, **kwargs)
        owned_tasks.append(task)
        return task

    loop.set_task_factory(lambda active_loop, coro, **kwargs: created(coro, **kwargs))
    notes = {"node": request.node.nodeid, "database": None, "providers": [], "wires": []}
    request.node.d2_notes = notes
    import httpx
    actual_send = httpx.AsyncClient.send
    async def observed_send(client, *args, **kwargs):
        response = await actual_send(client, *args, **kwargs)
        wire = next((wire for wire in notes["wires"] if isinstance(wire, D2Wire)), None)
        if wire is not None:
            original_raw = response.aiter_raw
            async def consumed_bytes():
                size = 0
                async for chunk in original_raw():
                    yield chunk
                    # yield返回说明原_sse_data已处理前一真实TCP块并请求下一块。
                    size += len(chunk)
                    total = 0
                    for i, part in enumerate(wire.parts):
                        total += len(part)
                        if size >= total:
                            wire.consumed[i].set()
            response.aiter_raw = consumed_bytes
        return response
    monkeypatch.setattr(httpx.AsyncClient, "send", observed_send)
    try:
        yield
    finally:
        loop.set_task_factory(factory)
        # 精确捕获本scope创建的句柄，包括协议流的async-generator finalizer；不强GC或过滤任务。
        joining = [task for task in owned_tasks if task is not asyncio.current_task()
            and not task.done()]
        notes["joined_task_coroutines"] = [str(task.get_coro()) for task in joining]
        if joining:
            async with asyncio.timeout(5):
                await asyncio.gather(*joining)
        notes["owned_tasks_created"] = len(owned_tasks)
        notes["owned_tasks_joined"] = all(task.done()
            or task is asyncio.current_task() for task in owned_tasks)
        path = Path(os.environ["KRIM_D2_EVIDENCE"]) / "cases"
        path.mkdir(exist_ok=True)
        consumer = Path(openai_compatible.__file__)
        assert consumer.is_relative_to(Path(os.environ["KRIM_D2_ROOT"]) / "source/src")
        unexpected = [t.get_name() for t in asyncio.all_tasks()
                      if t is not asyncio.current_task() and t not in existing and not t.done()]
        providers = [{"calls": p.calls, "active": p.active, "closed": p.closed,
                      "streams_closed": p.streams_closed} for p in notes.pop("providers")]
        wires = [{"requests": len(w.requests), "active": w.active,
                  "connections_closed": w.closed_connections, "tasks": len(w.tasks),
                  "server_serving": w.server.is_serving()} for w in notes.pop("wires")]
        result = notes | {"consumer": {"path": str(consumer),
            "sha256": hashlib.sha256(await asyncio.to_thread(consumer.read_bytes)).hexdigest()},
            "providers": providers, "wires": wires, "FD_before": before, "FD_after": fds(),
            "FD_equal": before == fds(), "unexpected_tasks": unexpected}
        (
            path / (
            hashlib.sha256(
            request.node.nodeid.encode()).hexdigest()+".json")).write_text(json.dumps(result,
            indent=2))
        assert all(p["closed"] and p["active"] == 0
            and p["calls"] == p["streams_closed"] for p in providers)
        assert all(w["active"] == 0 and w["tasks"] == 0 and not w["server_serving"] for w in wires)
        assert before == fds() and not unexpected


@pytest.mark.parametrize("cap", [2, 3, 4])
async def test_workspace_complete_tool_groups_are_atomic(cap, request):

    # 第一call完整/最后call未完/finish后DONE前，均不得释放部分工具。
    first = frame(choice({"tool_calls": [d2_call(0)]}))
    partial = [d2_call(i, arguments='{"text":"尾') for i in reversed(range(1, cap))]
    rest = [{"index": i, "function": {"arguments": '🙂"}'}} for i in range(1, cap)]
    parts = [first, frame(choice({"tool_calls": partial})),
             frame(choice({"tool_calls": rest}))+frame(choice(finish="tool_calls")),
        frame("[DONE]")]
    async with D2Wire(parts) as fixture:
        request.node.d2_notes["wires"].append(fixture)
        provider = OpenAICompatibleProvider(OpenAICompatibleConfig(
            base_url=fixture.url, model="fixture", max_tool_calls=cap))
        request.node.d2_notes["providers"].append(provider)
        events = []
        async def consume():
            async for event in provider.stream([Message(role="user", content="组")], []):
                events.append(event)
        task = asyncio.create_task(consume())
        try:
            for index in range(3):
                async with asyncio.timeout(5):
                    await fixture.sent[index].wait()
                    await fixture.consumed[index].wait()
                assert not task.done()
                assert not any(type(e) is ModelToolCall for e in events)
                fixture.gates[index].set()
            fixture.gates[3].set()
            await task
            calls = [e for e in events if type(e) is ModelToolCall]
            assert [e.id for e in calls] == ["d2-call-"+str(i) for i in range(cap)]
            assert all(e.arguments == {"text": "合成🙂" if i == 0 else "尾🙂"} for i,
                e in enumerate(calls))
            assert len(fixture.requests) == provider.calls == 1
            request.node.d2_notes["calls_released"] = [e.model_dump(mode="json") for e in calls]
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await provider.close()


@pytest.mark.parametrize("case", [
    "missing-index", "bool", "float", "string", "negative", "over-cap", "sparse",
    "duplicate-id", "last-json", "duplicate-json-key", "argument-limit", "no-finish",
    "no-done", "length", "cancel-first", "cancel-last", "cancel-finish",
])
async def test_workspace_bad_tool_groups_zero_release(case, request):
    raw = [d2_call(0), d2_call(1)]
    invalid = {"missing-index": None, "bool": True, "float": 1.0,
               "string": "1", "negative": -1, "over-cap": 4, "sparse": 2}
    if case in invalid:
        if case == "missing-index":
            raw[1].pop("index")
        else:
            raw[1]["index"] = invalid[case]
    if case == "duplicate-id":
        raw[1]["id"] = raw[0]["id"]
    if case in {"last-json", "duplicate-json-key", "argument-limit"}:
        raw[1]["function"]["arguments"] = {"last-json": "{bad",
            "duplicate-json-key": '{"x":1,"x":2}',
            "argument-limit": '{"text":"'+"x"*4096+'"}'}[case]
    parts = [frame(choice({"tool_calls": raw[:1]})), frame(choice({"tool_calls": raw[1:]})),
             b"" if case == "no-finish" else frame(
        choice(finish="length" if case == "length" else "tool_calls")),
             b"" if case == "no-done" else frame("[DONE]")]
    async with D2Wire(parts) as fixture:
        request.node.d2_notes["wires"].append(fixture)
        provider = OpenAICompatibleProvider(OpenAICompatibleConfig(
            base_url=fixture.url, model="fixture", max_tool_calls=4))
        request.node.d2_notes["providers"].append(provider)
        events, errors = [], []
        async def consume():
            try:
                async for event in provider.stream([Message(role="user", content="拒绝")], []):
                    events.append(event)
            except ProviderError as error:
                errors.append(error.code)
        task = asyncio.create_task(consume())
        try:
            if case.startswith("cancel-"):
                stop = {"cancel-first": 0, "cancel-last": 1, "cancel-finish": 2}[case]
                for index in range(stop+1):
                    async with asyncio.timeout(5):
                        await fixture.sent[index].wait()
                        await fixture.consumed[index].wait()
                    if index != stop:
                        fixture.gates[index].set()
                task.cancel()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                for gate in fixture.gates:
                    gate.set()
                await task
                assert errors, "完整组任一无效时必须实际拒绝，不能释放前面的合法call"
            assert not any(type(e) is ModelToolCall for e in events)
            assert len(fixture.requests) == provider.calls == 1
            request.node.d2_notes["protocol_errors"] = errors
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await provider.close()


@pytest.mark.parametrize("case", ["default-one", "default-two", "explicit-four", "invalid-cap",
     "constructed", "seal-config", "seal-client"])
async def test_single_call_default_and_sealed_cap(case, request):
    from pydantic import ValidationError

    from krim_agent.contracts import ServiceError

    chunks = [
     frame(
     choice(
     {
     "tool_calls": [
     d2_call(
     i) for i in range(4 if case == "explicit-four" else 2 if case == "default-two" else 1)]})),
              frame(choice(finish="tool_calls")), frame("[DONE]")]
    async with HTTPFixture(chunks=chunks) as fixture:
        request.node.d2_notes["wires"].append(fixture)
        if case == "invalid-cap":
            for value in [0, 5, True, False, 1.0, "2", None]:
                with pytest.raises(ValidationError):
                    OpenAICompatibleConfig(base_url=fixture.url, model="fixture",
                        max_tool_calls=value)
            assert not fixture.requests
            return
        config = OpenAICompatibleConfig(base_url=fixture.url, model="fixture",
                                       **({"max_tool_calls": 4} if case == "explicit-four" else {}))
        if case == "constructed":
            config = OpenAICompatibleConfig.model_construct(
                **(config.model_dump() | {"max_tool_calls": 5}))
            with pytest.raises((ValidationError, ServiceError)):
                OpenAICompatibleProvider(config)
            assert not fixture.requests
            return
        provider = OpenAICompatibleProvider(config)
        request.node.d2_notes["providers"].append(provider)
        replacement = None
        try:
            if case == "seal-config":
                object.__setattr__(provider.config, "max_tool_calls", 4)
            if case == "seal-client":
                import httpx

                replacement = httpx.AsyncClient(base_url=fixture.url+"/", trust_env=False)
                provider._client = replacement
            if case.startswith("seal-"):
                with pytest.raises(ServiceError, match="target_binding_changed"):
                    _ = [e async for e in provider.stream([Message(role="user", content="x")], [])]
                assert not fixture.requests
            elif case == "default-two":
                with pytest.raises(ProviderError):
                    _ = [e async for e in provider.stream([Message(role="user", content="x")], [])]
            else:
                events = [e async for e in provider.stream([Message(role="user", content="x")], [])]
                assert len(
                    [
                    e for e in events if type(
                    e) is ModelToolCall]) == (4 if case == "explicit-four" else 1)
            assert provider.execution_limits(
                )["max_tool_calls"] == (4 if case in {"explicit-four", "seal-config"} else 1)
        finally:
            await provider.close()
            if replacement is not None:
                await replacement.aclose()
                assert replacement.is_closed
