import asyncio
import gzip
import json
import time

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.web_access import TavilyWebAccess, WebAccessConfig

_SECRET = "tvly-test-secret"


def make_access(handler, *, api_key=_SECRET):
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        trust_env=False,
        follow_redirects=False,
    )
    config = WebAccessConfig(api_key=SecretStr(api_key) if api_key is not None else None)
    return TavilyWebAccess(config, client=client), client


def response_body(*, content="hello", url="https://example.com/page"):
    return {
        "results": [{"title": "Example", "url": url, "content": content}],
        "usage": {"credits": 1},
        "request_id": "request-1",
    }


class ByteStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        self.closed = True


class WaitingStream(httpx.AsyncByteStream):
    def __init__(self):
        self.started = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        self.started.set()
        await asyncio.Event().wait()
        yield b"unreachable"

    async def aclose(self):
        self.closed = True


async def test_missing_key_fails_before_any_request():
    calls = 0

    async def handler(_request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=response_body())

    access, client = make_access(handler, api_key=None)
    try:
        with pytest.raises(ServiceError) as error:
            await access.search("hello")
        assert error.value.code == "web_config"
        assert error.value.status == 422
        assert calls == 0
    finally:
        await access.close()
        await client.aclose()


async def test_search_uses_bearer_header_and_only_default_payload_fields():
    requests = []

    async def handler(request):
        requests.append(request)
        return httpx.Response(200, json=response_body())

    access, client = make_access(handler)
    try:
        result = await access.search("hello")
        assert len(requests) == 1
        request = requests[0]
        assert request.headers["Authorization"] == f"Bearer {_SECRET}"
        assert request.url.path == "/search"
        assert request.url.query == b""
        payload = json.loads(request.content)
        assert payload == {
            "query": "hello",
            "search_depth": "basic",
            "max_results": 5,
            "include_answer": False,
            "include_raw_content": False,
            "include_usage": True,
        }
        assert _SECRET not in request.content.decode()
        assert _SECRET not in str(request.url)
        assert result == {
            "ok": True,
            "operation": "search",
            "results": [
                {"title": "Example", "url": "https://example.com/page", "content": "hello"}
            ],
            "usage": {"credits": 1},
            "request_id": "request-1",
            "truncated": False,
        }
    finally:
        await access.close()
        await client.aclose()


async def test_search_rejects_invalid_query_and_result_limits_without_request():
    calls = 0

    async def handler(_request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"results": []})

    access, client = make_access(handler)
    try:
        for query, limit in [
            (" ", 5),
            ("x" * 4097, 5),
            (_SECRET, 5),
            ("hello", 11),
            ("hello", True),
        ]:
            with pytest.raises(ServiceError) as error:
                await access.search(query, limit)
            assert error.value.code == "web_invalid_input"
        assert calls == 0
    finally:
        await access.close()
        await client.aclose()


async def test_search_clips_content_and_keeps_tool_json_under_limit():
    async def handler(_request):
        return httpx.Response(200, json=response_body(content="界" * 20_000))

    access, client = make_access(handler)
    try:
        result = await access.search("hello")
        encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()
        assert result["truncated"] is True
        assert result["results"][0]["content"].endswith("[truncated]")
        assert len(encoded) <= 60 * 1024
    finally:
        await access.close()
        await client.aclose()


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (302, "web_redirect"),
        (401, "web_unauthorized"),
        (402, "web_payment_required"),
        (429, "web_rate_limited"),
        (432, "web_usage_limit"),
        (433, "web_credits_exhausted"),
    ],
)
async def test_http_failures_keep_status_without_leaking_body_or_url(status, code):
    async def handler(_request):
        return httpx.Response(
            status,
            headers={"Location": "https://private.example/path?secret=body"},
            json={"error": f"{_SECRET} https://private.example/path?token=hidden"},
        )

    access, client = make_access(handler)
    try:
        with pytest.raises(ServiceError) as error:
            await access.search("hello")
        assert error.value.code == code
        assert error.value.status == status
        assert _SECRET not in str(error.value)
        assert "private.example" not in str(error.value)
        assert "token=hidden" not in repr(error.value)
    finally:
        await access.close()
        await client.aclose()


async def test_malformed_body_returns_generic_protocol_error():
    async def handler(_request):
        return httpx.Response(200, content=f"{_SECRET} not-json".encode())

    access, client = make_access(handler)
    try:
        with pytest.raises(ServiceError) as error:
            await access.search("hello")
        assert error.value.code == "web_protocol"
        assert _SECRET not in str(error.value)
    finally:
        await access.close()
        await client.aclose()


async def test_compressed_response_is_limited_after_decompression():
    inflated = b'{"results":[],"padding":"' + b"x" * (2 * 1024 * 1024) + b'"}'
    compressed = gzip.compress(inflated)
    stream = ByteStream([compressed[:100], compressed[100:]])

    async def handler(_request):
        return httpx.Response(
            200,
            headers={"Content-Encoding": "gzip", "Content-Type": "application/json"},
            stream=stream,
        )

    access, client = make_access(handler)
    try:
        with pytest.raises(ServiceError) as error:
            await access.search("hello")
        assert error.value.code == "web_response_too_large"
        assert stream.closed
    finally:
        await access.close()
        await client.aclose()


async def test_output_metadata_limit_is_enforced_even_without_content():
    large_title = "T" * (70 * 1024)

    async def handler(_request):
        return httpx.Response(
            200,
            json={"results": [{"title": large_title, "url": "https://example.com", "content": ""}]},
        )

    access, client = make_access(handler)
    try:
        with pytest.raises(ServiceError) as error:
            await access.search("hello")
        assert error.value.code == "web_output_too_large"
    finally:
        await access.close()
        await client.aclose()


async def test_fetch_sends_one_url_markdown_and_marks_content_truncation():
    requests = []
    input_url = "https://example.com/article?q=1#section"
    service_url = "https://example.com/article?q=1"

    async def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "results": [{"url": service_url, "raw_content": "body" * 6000}],
                "failed_results": [],
                "usage": {"credits": 1},
                "request_id": "extract-1",
            },
        )

    access, client = make_access(handler)
    try:
        result = await access.fetch(input_url)
        assert len(requests) == 1
        request = requests[0]
        assert request.headers["Authorization"] == f"Bearer {_SECRET}"
        assert json.loads(request.content) == {"urls": [service_url], "format": "markdown"}
        assert request.url.path == "/extract"
        assert result["operation"] == "fetch"
        assert result["results"][0]["url"] == service_url
        assert result["results"][0]["content"].endswith("[truncated]")
        assert len(result["results"][0]["content"]) == 15_000
        assert result["usage"] == {"credits": 1}
        assert result["request_id"] == "extract-1"
        assert result["truncated"] is True
    finally:
        await access.close()
        await client.aclose()


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/file",
        "https://user:pass@example.com/",
        "http://localhost/",
        "https://sub.localhost/",
        "http://127.0.0.1/",
        "https://10.0.0.1/",
        "https://[::1]/",
        "https://127.1/",
        "https://service.internal/",
        f"https://example.com/{_SECRET}",
        "not-a-url",
    ],
)
async def test_fetch_rejects_nonpublic_or_non_http_urls_without_request(url):
    calls = 0

    async def handler(_request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"results": [], "failed_results": []})

    access, client = make_access(handler)
    try:
        with pytest.raises(ServiceError) as error:
            await access.fetch(url)
        assert error.value.code == "web_invalid_input"
        assert calls == 0
    finally:
        await access.close()
        await client.aclose()


async def test_fetch_rejects_failed_or_wrong_url_results():
    requested = "https://example.com/article"

    async def failed_handler(_request):
        return httpx.Response(
            200,
            json={
                "results": [],
                "failed_results": [{"url": requested, "error": "secret failure"}],
            },
        )

    access, client = make_access(failed_handler)
    try:
        with pytest.raises(ServiceError) as error:
            await access.fetch(requested)
        assert error.value.code == "web_extract_failed"
        assert _SECRET not in str(error.value)
        assert "secret failure" not in str(error.value)
    finally:
        await access.close()
        await client.aclose()

    async def wrong_url_handler(_request):
        return httpx.Response(
            200,
            json={
                "results": [{"url": "https://elsewhere.example/article", "content": "wrong"}],
                "failed_results": [],
            },
        )

    access, client = make_access(wrong_url_handler)
    try:
        with pytest.raises(ServiceError) as error:
            await access.fetch(requested)
        assert error.value.code == "web_protocol"
    finally:
        await access.close()
        await client.aclose()


async def test_rate_limit_retries_once_only_after_real_retry_after_delay():
    calls = []
    delay = 0.05

    async def handler(_request):
        calls.append(time.monotonic())
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": str(delay)}, json={"error": "busy"})
        return httpx.Response(200, json=response_body())

    access, client = make_access(handler)
    try:
        result = await access.search("hello")
        assert result["ok"] is True
        assert len(calls) == 2
        assert calls[1] - calls[0] >= delay * 0.8
    finally:
        await access.close()
        await client.aclose()


async def test_rate_limit_with_delay_over_budget_is_not_retried():
    calls = 0

    async def handler(_request):
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "21"}, json={"error": "busy"})

    access, client = make_access(handler)
    try:
        with pytest.raises(ServiceError) as error:
            await access.search("hello")
        assert error.value.code == "web_rate_limited"
        assert error.value.status == 429
        assert calls == 1
    finally:
        await access.close()
        await client.aclose()


async def test_concurrency_is_limited_to_four_requests():
    active = 0
    peak = 0
    started = 0
    four_started = asyncio.Event()
    release = asyncio.Event()

    async def handler(_request):
        nonlocal active, peak, started
        active += 1
        started += 1
        peak = max(peak, active)
        if started == 4:
            four_started.set()
        try:
            await release.wait()
            return httpx.Response(200, json={"results": []})
        finally:
            active -= 1

    access, client = make_access(handler)
    tasks = [asyncio.create_task(access.search(f"query-{index}")) for index in range(8)]
    try:
        await asyncio.wait_for(four_started.wait(), timeout=2)
        assert started == 4
        assert peak == 4
        release.set()
        await asyncio.gather(*tasks)
        assert peak == 4
        assert started == 8
    finally:
        release.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await access.close()
        await client.aclose()


async def test_cancellation_closes_stream_and_borrowed_client_stays_open():
    stream = WaitingStream()

    async def handler(_request):
        return httpx.Response(200, stream=stream)

    access, client = make_access(handler)
    task = asyncio.create_task(access.search("hello"))
    try:
        await asyncio.wait_for(stream.started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed
        await access.close()
        assert not client.is_closed
        with pytest.raises(ServiceError) as error:
            await access.search("hello")
        assert error.value.code == "web_closed"
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await access.close()
        await client.aclose()


async def test_close_is_idempotent_and_closes_only_owned_client():
    owned = TavilyWebAccess(WebAccessConfig(api_key=SecretStr(_SECRET)))
    owned_client = owned._client
    assert not owned_client.is_closed
    await owned.close()
    await owned.close()
    assert owned_client.is_closed

    async def handler(_request):
        return httpx.Response(200, json={"results": []})

    borrowed = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
    access = TavilyWebAccess(WebAccessConfig(api_key=SecretStr(_SECRET)), client=borrowed)
    await access.close()
    await access.close()
    assert not borrowed.is_closed
    await borrowed.aclose()


async def test_config_is_frozen_local_http_is_allowed_and_digest_hides_key():
    first = WebAccessConfig(api_key=SecretStr(_SECRET))
    second = WebAccessConfig(api_key=SecretStr("another-secret"))
    different_endpoint = WebAccessConfig(endpoint="https://search.example")
    access1 = TavilyWebAccess(first)
    access2 = TavilyWebAccess(second)
    access3 = TavilyWebAccess(different_endpoint)
    try:
        assert first.endpoint == "https://api.tavily.com"
        assert "tvly-test-secret" not in repr(first)
        assert "tvly-test-secret" not in str(first.api_key)
        assert access1.configuration_digest == access2.configuration_digest
        assert access1.configuration_digest != access3.configuration_digest
        assert WebAccessConfig(endpoint="http://127.0.0.1:8765").endpoint == "http://127.0.0.1:8765"
        assert WebAccessConfig(endpoint="http://localhost:8765").endpoint == "http://localhost:8765"
        with pytest.raises((TypeError, ValidationError)):
            first.endpoint = "https://changed.example"
        with pytest.raises(ValidationError):
            WebAccessConfig(endpoint="http://api.example.com")
        with pytest.raises(ValidationError):
            WebAccessConfig(endpoint="https://user:pass@api.example.com")
        with pytest.raises(ValidationError):
            WebAccessConfig(endpoint="https://api.example.com/path?token=secret")
    finally:
        await access1.close()
        await access2.close()
        await access3.close()
