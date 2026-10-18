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
