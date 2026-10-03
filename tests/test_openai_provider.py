import asyncio
import json

import httpx
import pytest

from krim_agent.config import Settings
from krim_agent.protocol.domain import MessageView
from krim_agent.providers import (
    FakeProvider,
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
    ProviderError,
    create_provider,
)
from krim_agent.sdk import PythonAI, StreamTextResult
from krim_agent.tools.builtins import builtin_registry


def message(role="user", content="hello", details=None):
    return MessageView(
        id="m",
        topic_id="t",
        run_id="r",
        sequence=1,
        role=role,
        content=content,
        details=details or {},
        created_at="2026-10-03T00:00:00Z",
    )


def frame(delta, reason=None):
    return {"choices": [{"index": 0, "delta": delta, "finish_reason": reason}]}


def sse(*events, reason="stop", done=True):
    payload = "".join(f"data: {json.dumps(event, ensure_ascii=False)}\n\n" for event in events)
    if reason is not None:
        payload += f"data: {json.dumps(frame({}, reason))}\n\n"
    return payload + ("data: [DONE]\n\n" if done else "")


def provider_for(handler, **options):
    return OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url="http://127.0.0.1:9000/v1", model="local-model", **options),
        transport=httpx.MockTransport(handler),
    )


def response(payload):
    return httpx.Response(
        200, headers={"Content-Type": "text/event-stream"}, content=payload.encode()
    )


async def test_text_and_interleaved_tool_fragments_preserve_request_schema():
    async def handler(request):
        payload = json.loads(request.content)
        assert request.url.path == "/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer test-key"
        assert payload["stream"] is True and payload["n"] == 1
        assert payload["model"] == "local-model" and payload["max_tokens"] == 256
        function = payload["tools"][0]["function"]
        assert function["name"] == "add"
        assert function["parameters"]["additionalProperties"] is False
        assert set(function["parameters"]["required"]) == {"left", "right"}
        events = [
            frame({"content": "hello"}),
            frame(
                {
                    "tool_calls": [
                        {
                            "index": 1,
                            "id": "call-b",
                            "function": {"name": "echo", "arguments": '{"text":"好"}'},
                        },
                        {
                            "index": 0,
                            "id": "call-",
                            "function": {"name": "a", "arguments": '{"left":2'},
                        },
                    ]
                }
            ),
            frame(
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "a",
                            "function": {"name": "dd", "arguments": ',"right":3}'},
                        },
                    ]
                }
            ),
        ]
        return response(sse(*events, reason="tool_calls"))

    provider = provider_for(handler, api_key="test-key", max_output_tokens=256)
    try:
        outputs = [
            item async for item in provider.stream([message()], builtin_registry().definitions)
        ]
        assert outputs[0].text == "hello"
        assert [item.id for item in outputs[1:]] == ["call-a", "call-b"]
        assert outputs[1].name == "add" and outputs[1].arguments == {"left": 2, "right": 3}
        assert outputs[2].arguments == {"text": "好"}
    finally:
        await provider.close()


def test_messages_convert_tool_history_and_reject_missing_identity():
    from krim_agent.providers.openai_compatible import _messages

    converted = _messages(
        [
            message(
                "assistant",
                "",
                {"tool_calls": [{"id": "c", "name": "add", "arguments": {"left": 1}}]},
            ),
            message("tool", '{"sum": 1}', {"call_id": "c"}),
        ]
    )
    assert converted[0]["tool_calls"][0]["function"]["arguments"] == '{"left": 1}'
    assert converted[1]["tool_call_id"] == "c"
    for invalid in [
        message("tool", "result"),
        message("assistant", "", {"tool_calls": [None]}),
        message("assistant", "", {"tool_calls": [{"id": 1, "name": "add", "arguments": {}}]}),
    ]:
        with pytest.raises(ProviderError) as error:
            _messages([invalid])
        assert error.value.code == "provider_messages"


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/v1",
        "https://u:p@example.com/v1",
        "https://example.com/v1?q=1",
        "https://example.com/v1#",
        "https://example.com:bad/v1",
        "https://example.com:99999/v1",
        "https://example.com/\n",
        "file:///tmp/model",
        "https:///v1",
    ],
)
def test_url_rejects_unsafe_or_ambiguous_endpoints(url):
    with pytest.raises(ValueError):
        OpenAICompatibleConfig(base_url=url, model="test")


@pytest.mark.parametrize(
    "event,code",
    [
        ([], "provider_response"),
        ({"choices": None}, "provider_response"),
        ({"choices": [None]}, "provider_response"),
        (frame([]), "provider_response"),
        (frame({"content": ["image"]}), "provider_response"),
        (frame({}, []), "provider_response"),
        (frame({}, "unsupported"), "provider_response"),
        ({"error": {"message": "do not expose test-key"}}, "provider_error"),
        (frame({"tool_calls": False}), "provider_tool_call"),
        (frame({"tool_calls": [{"index": True}]}), "provider_tool_call"),
        (frame({"tool_calls": [{"index": -1}]}), "provider_tool_limit"),
        (frame({"tool_calls": [{"index": 8}]}), "provider_tool_limit"),
        (frame({"tool_calls": [{"index": 0, "function": []}]}), "provider_tool_call"),
    ],
)
async def test_protocol_errors_are_stable_and_do_not_expose_remote_messages(event, code):
    provider = provider_for(lambda _request: response(sse(event)))
    try:
        with pytest.raises(ProviderError) as error:
            [item async for item in provider.stream([message()], [])]
        assert error.value.code == code
        assert "test-key" not in str(error.value)
    finally:
        await provider.close()


@pytest.mark.parametrize(
    "arguments,code",
    [
        ("{", "provider_tool_arguments"),
        ("[]", "provider_tool_arguments"),
        ('{"value":NaN}', "provider_tool_arguments"),
        ('{"value":9007199254740992}', "provider_tool_arguments"),
        ('{"value":"' + "x" * 65536 + '"}', "provider_tool_arguments"),
    ],
)
async def test_tool_arguments_are_complete_finite_objects(arguments, code):
    event = frame(
        {
            "tool_calls": [
                {"index": 0, "id": "c", "function": {"name": "add", "arguments": arguments}},
            ]
        }
    )
    provider = provider_for(lambda _request: response(sse(event, reason="tool_calls")))
    try:
        with pytest.raises(ProviderError) as error:
            [item async for item in provider.stream([message()], [])]
        assert error.value.code == code
    finally:
        await provider.close()


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        self.closed = True


async def test_sse_multiline_unicode_and_crlf_split_across_network_chunks():
    payload = (
        ': heartbeat\r\nid: 1\r\ndata: {"choices":\r\n'
        'data: [{"index":0,"delta":{"content":"中文"},"finish_reason":null}]}\r\n\r\n'
        + sse(reason="stop").replace("\n", "\r")
    ).encode()
    body = Chunks([payload[index : index + 7] for index in range(0, len(payload), 7)])
    provider = provider_for(
        lambda _request: httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream; charset=utf-8"},
            stream=body,
        )
    )
    try:
        outputs = [item async for item in provider.stream([message()], [])]
        assert [item.text for item in outputs] == ["中文"]
        assert body.closed
    finally:
        await provider.close()


@pytest.mark.parametrize(
    "payload,options,code",
    [
        ("data: {bad}\n\n", {}, "provider_invalid_json"),
        ("data: " + "x" * 300, {"max_event_bytes": 256}, "provider_event_limit"),
        (": heartbeat\n\n" * 100, {"max_response_bytes": 1024}, "provider_response_limit"),
        (sse(frame({"content": "partial"}), done=False), {}, "provider_incomplete_stream"),
        (sse(frame({"content": "partial"}), reason=None), {}, "provider_incomplete_stream"),
    ],
)
async def test_broken_or_oversized_sse_closes_response(payload, options, code):
    body = Chunks([payload.encode()])
    provider = provider_for(
        lambda _request: httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=body,
        ),
        **options,
    )
    try:
        with pytest.raises(ProviderError) as error:
            [item async for item in provider.stream([message()], [])]
        assert error.value.code == code and body.closed
    finally:
        await provider.close()


@pytest.mark.parametrize(
    "status,headers,code",
    [
        (429, {}, "provider_http_error"),
        (302, {"Location": "https://untrusted.example/steal"}, "provider_redirect"),
        (200, {"Content-Type": "application/json"}, "provider_content_type"),
    ],
)
async def test_http_errors_do_not_follow_redirects_or_return_secrets(status, headers, code):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, headers=headers, content=b"test-key and user content")

    provider = provider_for(handler, api_key="test-key")
    try:
        with pytest.raises(ProviderError) as error:
            [item async for item in provider.stream([message()], [])]
        assert error.value.code == code and "test-key" not in str(error.value)
        assert len(requests) == 1
    finally:
        await provider.close()


@pytest.mark.parametrize(
    "failure,code",
    [
        (httpx.ReadTimeout, "provider_timeout"),
        (httpx.ConnectError, "provider_network"),
    ],
)
async def test_network_failure_has_stable_error(failure, code):
    async def handler(request):
        raise failure("do not expose credentials", request=request)

    provider = provider_for(handler)
    try:
        with pytest.raises(ProviderError) as error:
            [item async for item in provider.stream([message()], [])]
        assert error.value.code == code
    finally:
        await provider.close()


async def test_cancel_and_early_consumer_close_release_http_response():
    entered = asyncio.Event()

    class Waiting(Chunks):
        async def __aiter__(self):
            entered.set()
            await asyncio.Event().wait()
            yield b"never"

    body = Waiting([])
    provider = provider_for(
        lambda _request: httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=body,
        )
    )

    async def consume():
        return [item async for item in provider.stream([message()], [])]

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert body.closed
    finally:
        await provider.close()

    body = Chunks([sse(frame({"content": "first"})).encode()])
    provider = provider_for(
        lambda _request: httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=body,
        )
    )
    stream = provider.stream([message()], [])
    try:
        assert (await anext(stream)).text == "first"
        await stream.aclose()
        assert body.closed
    finally:
        await provider.close()


async def test_incomplete_tool_stream_never_executes_tool():
    event = frame(
        {
            "tool_calls": [
                {
                    "index": 0,
                    "id": "c",
                    "function": {"name": "add", "arguments": '{"left":2,"right":3}'},
                },
            ]
        }
    )
    provider = provider_for(lambda _request: response(sse(event, reason="tool_calls", done=False)))
    try:
        async with await PythonAI.create(provider=provider) as ai:
            result = await ai.generate_text("calculate")
            assert result.run.status == "failed" and result.code == "provider_incomplete_stream"
            assert result.audits == [] and all(not step.tool_calls for step in result.steps)
        assert not provider._http.is_closed
    finally:
        await provider.close()


@pytest.mark.parametrize(
    "reason,code,finish",
    [
        ("length", "provider_output_limit", "length"),
        ("content_filter", "provider_content_filter", "error"),
    ],
)
async def test_provider_failure_and_partial_text_survive_database_reopen(
    tmp_path, reason, code, finish
):
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'provider.sqlite'}")
    provider = provider_for(
        lambda _request: response(sse(frame({"content": "partial"}), reason=reason))
    )
    try:
        async with await PythonAI.create(settings=settings, provider=provider) as ai:
            result = await ai.generate_text("failure")
            assert result.code == code and result.finish_reason == finish
            assert result.text == "partial" and result.steps[-1].partial
            run_id = result.run.id
        async with await PythonAI.create(settings=settings) as reopened:
            restored = await StreamTextResult(
                reopened.runtime, await reopened.runtime.repository.get_run(run_id)
            ).collect()
            assert restored.code == code and restored.finish_reason == finish
            assert restored.text == "partial"
    finally:
        await provider.close()


async def test_runtime_output_limit_deterministically_closes_provider_generator():
    body = Chunks([sse(frame({"content": "x" * 9000})).encode()])
    provider = provider_for(
        lambda _request: httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=body,
        )
    )
    try:
        async with await PythonAI.create(provider=provider) as ai:
            result = await ai.generate_text("too large")
            assert result.code == "output_limit" and body.closed
    finally:
        await provider.close()


async def test_settings_require_explicit_provider_and_mask_credentials(monkeypatch):
    assert isinstance(create_provider(Settings()), FakeProvider)
    with pytest.raises(ValueError, match="provider_base_url"):
        Settings(provider="openai-compatible")
    monkeypatch.setenv("KRIM_AGENT_PROVIDER", "openai-compatible")
    monkeypatch.setenv("KRIM_AGENT_PROVIDER_BASE_URL", "http://127.0.0.1:9000/v1")
    monkeypatch.setenv("KRIM_AGENT_PROVIDER_MODEL", "local-model")
    monkeypatch.setenv("KRIM_AGENT_PROVIDER_API_KEY", "test-key")
    settings = Settings.from_env()
    assert settings.provider_api_key.get_secret_value() == "test-key"
    assert "test-key" not in repr(settings)
    provider = create_provider(settings)
    assert isinstance(provider, OpenAICompatibleProvider)
    await provider.close()
    with pytest.raises(ProviderError) as error:
        [item async for item in provider.stream([message()], [])]
    assert error.value.code == "provider_closed"
