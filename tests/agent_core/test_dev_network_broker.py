from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from test_dev_environment_backend import make_record

from krim_agent.dev_environment.model import EnvironmentError, RuntimeSpec
from krim_agent.dev_network.broker import MAX_ACTIVE, DownloadBroker
from krim_agent.dev_network.policy import bind
from krim_agent.network.egress_policy import EgressViolation
from krim_agent.network.http_transport import PinnedAsyncTransport
from krim_agent.project_access.egress import ProjectEgressPolicy
from krim_agent.project_access.types import NetworkGrant

POLICY_ID = "policy-1"
OWNER_ID = "owner-1"
PROJECT_ID = "project-1"
REGISTRY = "registry.npmjs.org"
CDN = "cdn.example"


class PolicySource:
    """Authoritative project-policy resolver used by every broker test."""

    def __init__(self, policy: ProjectEgressPolicy) -> None:
        self.policy = policy
        self.specs: list[RuntimeSpec] = []

    async def __call__(self, spec: RuntimeSpec) -> ProjectEgressPolicy:
        self.specs.append(spec)
        return self.policy


def _policy(
    *,
    revision: int = 1,
    expires_at: float = 4_000_000_000.0,
    revoked: bool = False,
    owner_id: str = OWNER_ID,
    project_id: str = PROJECT_ID,
    policy_id: str = POLICY_ID,
) -> ProjectEgressPolicy:
    return ProjectEgressPolicy(
        NetworkGrant(
            owner_id=owner_id,
            project_id=project_id,
            policy_id=policy_id,
            revision=revision,
            hosts=(REGISTRY, CDN),
            ports=(443,),
            expires_at=expires_at,
            revoked=revoked,
        )
    )


def _runtime_spec(tmp_path: Path) -> RuntimeSpec:
    # The record is the shared I-owned value; only its network binding is
    # derived for this test.  It deliberately has no Docker network ID.
    record = make_record(tmp_path / "project")
    record = replace(record, network_id=None, network_policy_ref=POLICY_ID)
    return RuntimeSpec(record)


async def _binding(tmp_path: Path, source: PolicySource):
    spec = _runtime_spec(tmp_path)
    binding = await bind(spec, source)
    # Use a deterministic clock for expiry tests after the initial bind.
    return replace(binding, clock=lambda: 100.0)


async def _broker(
    tmp_path: Path,
    *,
    source: PolicySource | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    resolver: Any | None = None,
) -> tuple[DownloadBroker, PolicySource]:
    source = source or PolicySource(_policy())
    binding = await _binding(tmp_path, source)
    kwargs: dict[str, Any] = {}
    if transport is not None:
        kwargs["transport"] = transport
    if resolver is not None:
        kwargs["resolver"] = resolver
    return DownloadBroker(binding, **kwargs), source


def _message(
    request_id: int = 1,
    *,
    method: str = "GET",
    url: str = f"https://{REGISTRY}/package.tgz",
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "id": request_id,
        "method": method,
        "url": url,
        "headers": {} if headers is None else headers,
    }


def _frame(message: object) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode() + b"\n"


async def _download(broker: DownloadBroker, message: dict[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []

    async def send(value: dict[str, Any]) -> None:
        output.append(value)

    await broker.download(message, send)
    return output


class BytesStream(httpx.AsyncByteStream):
    """An unconsumed async body stream for httpx.MockTransport responses."""

    def __init__(self, body: bytes) -> None:
        self.body = body

    async def __aiter__(self):
        if self.body:
            yield self.body

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_policy_binding_allows_authoritative_scope_and_denies_offlist_target(
    tmp_path: Path,
) -> None:
    source = PolicySource(_policy())
    binding = await _binding(tmp_path, source)

    current = await binding.current()
    assert current is source.policy
    assert source.specs
    assert all(spec.network.network_id is None for spec in source.specs)
    assert all(spec.network.policy_ref == POLICY_ID for spec in source.specs)
    target = current.validate_target(f"https://{REGISTRY}/package.tgz?signature=mock")
    assert target.host == REGISTRY

    with pytest.raises(EgressViolation):
        current.validate_target("https://offlist.example/package.tgz")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        pytest.param({"expires_at": 99.0}, id="expired"),
        pytest.param({"revoked": True}, id="revoked"),
        pytest.param({"revision": 2}, id="revision-rotated"),
    ],
)
async def test_policy_binding_rejects_expiry_revocation_and_revision_drift(
    tmp_path: Path, change: dict[str, object]
) -> None:
    source = PolicySource(_policy())
    binding = await _binding(tmp_path, source)
    source.policy = ProjectEgressPolicy(source.policy.grant.model_copy(update=change))

    with pytest.raises(EnvironmentError) as raised:
        await binding.current()

    assert raised.value.code == "network_authorization_expired"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("owner_id", "other-owner"),
        ("project_id", "other-project"),
        ("policy_id", "other-policy"),
    ],
)
async def test_policy_binding_rejects_scope_drift(
    tmp_path: Path, field: str, value: str
) -> None:
    source = PolicySource(_policy())
    binding = await _binding(tmp_path, source)
    source.policy = ProjectEgressPolicy(
        source.policy.grant.model_copy(update={field: value})
    )

    with pytest.raises(EnvironmentError) as raised:
        await binding.current()

    assert raised.value.code == "network_authorization_expired"


@pytest.mark.asyncio
async def test_broker_allows_get_and_head_over_explicit_mock_transport(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = b"mock package bytes" if request.method == "GET" else b""
        return httpx.Response(
            200,
            headers={"Content-Type": "application/octet-stream"},
            stream=BytesStream(body),
        )

    # This is an in-memory httpx.MockTransport; it does not perform HTTP I/O.
    broker, _ = await _broker(tmp_path, transport=httpx.MockTransport(upstream))
    try:
        get_output = await _download(broker, _message(1, method="GET"))
        head_output = await _download(broker, _message(2, method="HEAD"))
    finally:
        await broker.close()

    assert [request.method for request in requests] == ["GET", "HEAD"]
    assert all(request.headers["accept-encoding"] == "identity" for request in requests)
    assert get_output[0]["status"] == 200
    assert base64.b64decode(get_output[1]["data"]) == b"mock package bytes"
    assert get_output[-1] == {"id": 1, "end": True}
    assert head_output[0]["status"] == 200
    assert not any("data" in message for message in head_output)
    assert head_output[-1] == {"id": 2, "end": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["POST", "CONNECT"])
async def test_broker_rejects_post_and_connect_before_mock_transport(
    tmp_path: Path, method: str
) -> None:
    calls: list[httpx.Request] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, stream=BytesStream(b""))

    # The mock is intentionally present so the assertion proves no request was sent.
    broker, _ = await _broker(tmp_path, transport=httpx.MockTransport(upstream))
    try:
        with pytest.raises(EgressViolation):
            await _download(broker, _message(method=method))
    finally:
        await broker.close()

    assert calls == []


@pytest.mark.asyncio
async def test_broker_filters_host_credentials_sni_and_proxy_headers(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, stream=BytesStream(b"ok"))

    # MockTransport records the final request only; no external network is used.
    broker, _ = await _broker(tmp_path, transport=httpx.MockTransport(upstream))
    try:
        await _download(
            broker,
            _message(
                headers={
                    "Host": "attacker.example",
                    "Authorization": "Bearer secret",
                    "SNI": "attacker.example",
                    "sni_hostname": "attacker.example",
                    "Proxy-Authorization": "Basic secret",
                    "Proxy-Connection": "keep-alive",
                    "Forwarded": "for=10.0.0.1",
                    "X-Forwarded-For": "10.0.0.1",
                    "User-Agent": "krim-broker-test",
                    "Accept": "application/octet-stream",
                },
            ),
        )
    finally:
        await broker.close()

    assert len(requests) == 1
    headers = requests[0].headers
    # httpx supplies the URL authority, not the untrusted Host value.
    assert headers["host"] == REGISTRY
    assert headers["user-agent"] == "krim-broker-test"
    assert headers["accept"] == "application/octet-stream"
    for forbidden in (
        "authorization",
        "sni",
        "sni_hostname",
        "proxy-authorization",
        "proxy-connection",
        "forwarded",
        "x-forwarded-for",
    ):
        assert forbidden not in headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_host", "location"),
    [
        (REGISTRY, f"https://{CDN}/package.tgz?signature=mock"),
        (CDN, f"https://{REGISTRY}/package.tgz?signature=mock"),
    ],
)
async def test_broker_allows_registry_cdn_redirects_with_mock_transport(
    tmp_path: Path, source_host: str, location: str
) -> None:
    seen: list[httpx.Request] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            302, headers={"Location": location}, stream=BytesStream(b"")
        )

    broker, _ = await _broker(tmp_path, transport=httpx.MockTransport(upstream))
    try:
        output = await _download(
            broker,
            _message(url=f"https://{source_host}/package.tgz"),
        )
    finally:
        await broker.close()

    assert len(seen) == 1
    assert output[0]["status"] == 302
    assert output[0]["headers"]["location"] == location
    assert output[-1] == {"id": 1, "end": True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "location",
    [
        "https://offlist.example/package.tgz",
        "https://127.0.0.1/private.tgz",
        f"http://{CDN}/package.tgz",
    ],
)
async def test_broker_rejects_offlist_private_and_http_redirects(
    tmp_path: Path, location: str
) -> None:
    calls: list[httpx.Request] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            302, headers={"Location": location}, stream=BytesStream(b"")
        )

    # This mock represents the first registry response; redirect validation is local.
    broker, _ = await _broker(tmp_path, transport=httpx.MockTransport(upstream))
    try:
        with pytest.raises(EgressViolation):
            await _download(broker, _message())
    finally:
        await broker.close()

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_broker_real_pinned_transport_rejects_mixed_public_private_dns(
    tmp_path: Path,
) -> None:
    class MixedResolver:
        calls: list[str] = []

        async def resolve(self, host: str) -> tuple[str, ...]:
            self.calls.append(host)
            return ("1.1.1.1", "10.0.0.1")

    resolver = MixedResolver()
    broker, _ = await _broker(tmp_path, resolver=resolver)
    try:
        assert isinstance(broker.transport, PinnedAsyncTransport)
        with pytest.raises(EgressViolation):
            await _download(broker, _message())
    finally:
        await broker.close()

    # Address approval fails before connect_tcp, so this test never contacts the network.
    assert resolver.calls == [REGISTRY]


class TwoChunkStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        yield b"first chunk"
        await self.release.wait()
        yield b"second chunk"

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_stream_revocation_stops_subsequent_chunks(tmp_path: Path) -> None:
    stream = TwoChunkStream()
    first_chunk_sent = asyncio.Event()
    output: list[dict[str, Any]] = []

    async def upstream(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    source = PolicySource(_policy())
    broker, source = await _broker(
        tmp_path, source=source, transport=httpx.MockTransport(upstream)
    )

    async def send(message: dict[str, Any]) -> None:
        output.append(message)
        if "data" in message:
            first_chunk_sent.set()

    task = asyncio.create_task(broker.download(_message(), send))
    try:
        await asyncio.wait_for(first_chunk_sent.wait(), 1)
        source.policy = _policy(revoked=True)
        stream.release.set()
        with pytest.raises(EnvironmentError) as raised:
            await task
        assert raised.value.code == "network_authorization_expired"
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await broker.close()

    chunks = [base64.b64decode(message["data"]) for message in output if "data" in message]
    assert chunks == [b"first chunk"]
    assert not any(message.get("end") is True for message in output)
    assert stream.closed


class BlockingStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.closed = False
        self._never = asyncio.Event()

    async def __aiter__(self):
        self.entered.set()
        try:
            await self._never.wait()
        finally:
            self.cancelled.set()
        if False:  # Make this method an async iterator without yielding a chunk.
            yield b""

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_close_cancels_active_download_stream(tmp_path: Path) -> None:
    stream = BlockingStream()

    async def upstream(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    broker, _ = await _broker(tmp_path, transport=httpx.MockTransport(upstream))
    task = asyncio.create_task(_download(broker, _message()))
    await asyncio.wait_for(stream.entered.wait(), 1)

    await broker.close()

    assert task.cancelled()
    assert stream.cancelled.is_set()
    assert stream.closed
    assert broker.active == set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (b"{not-json\n", json.JSONDecodeError),
        (b"[]\n", EgressViolation),
    ],
)
async def test_serve_rejects_bad_protocol_frames(
    tmp_path: Path, frame: bytes, expected: type[Exception]
) -> None:
    broker, _ = await _broker(tmp_path)
    reader = asyncio.StreamReader()
    output: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        output.append(message)

    reader.feed_data(frame)
    with pytest.raises(expected):
        await broker.serve(reader, send)

    assert output == []
    assert broker.closed


@pytest.mark.asyncio
async def test_serve_rejects_duplicate_request_ids(tmp_path: Path) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def upstream(_request: httpx.Request) -> httpx.Response:
        started.set()
        await release.wait()
        return httpx.Response(200, stream=BytesStream(b""))

    broker, _ = await _broker(tmp_path, transport=httpx.MockTransport(upstream))
    reader = asyncio.StreamReader()
    task = asyncio.create_task(broker.serve(reader, lambda _message: asyncio.sleep(0)))
    frame = _frame(_message())
    reader.feed_data(frame)
    await asyncio.wait_for(started.wait(), 1)
    reader.feed_data(frame)

    with pytest.raises(EgressViolation):
        await task

    assert broker.closed
    assert broker.active == set()


@pytest.mark.asyncio
async def test_serve_rejects_when_max_active_limit_is_reached(tmp_path: Path) -> None:
    started = [asyncio.Event() for _ in range(MAX_ACTIVE)]
    release = asyncio.Event()
    started_count = 0

    async def upstream(_request: httpx.Request) -> httpx.Response:
        nonlocal started_count
        started[started_count].set()
        started_count += 1
        await release.wait()
        return httpx.Response(200, stream=BytesStream(b""))

    broker, _ = await _broker(tmp_path, transport=httpx.MockTransport(upstream))
    reader = asyncio.StreamReader()
    task = asyncio.create_task(broker.serve(reader, lambda _message: asyncio.sleep(0)))

    for request_id in range(1, MAX_ACTIVE + 1):
        reader.feed_data(_frame(_message(request_id)))
        await asyncio.wait_for(started[request_id - 1].wait(), 1)

    reader.feed_data(_frame(_message(MAX_ACTIVE + 1)))
    with pytest.raises(EgressViolation):
        await task

    assert started_count == MAX_ACTIVE
    assert broker.closed
    assert broker.active == set()
