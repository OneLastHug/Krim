"""Real socketpair HTTP tests and explicit fake-client WS configuration tests."""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest

from krim_agent.dev_preview.http_transport import ConnectedHTTPTransport
from krim_agent.dev_preview.service import PreviewError
from krim_agent.dev_preview.websockets_client import WebsocketsConnector
from krim_agent.development.preview_ports import RouteBinding, RouteLease


@pytest.fixture
def lease():
    return RouteLease(
        RouteBinding(
            preview_id="preview",
            owner_id="o",
            project_id="p",
            environment_id="e",
            generation=1,
            service_id="s",
            process_id="s",
            operation_id="op",
            request_digest="0" * 64,
            execution_id="a" * 64,
            started_at="2026-11-15T00:00:00.000000001Z",
            port=3000,
            route_epoch=1,
        ),
        "opaque",
    )


class SocketRoutes:
    def __init__(self):
        self.entered = False
        self.exited = False
        self.peer = None
        self.sock = None

    @asynccontextmanager
    async def connect(self, lease):
        owner = asyncio.current_task()
        self.entered = True
        self.sock, self.peer = socket.socketpair()
        self.sock.setblocking(False)
        self.peer.setblocking(False)
        try:
            yield self.sock
        finally:
            assert asyncio.current_task() is owner
            assert self.sock.fileno() == -1
            self.exited = True
            self.peer.close()


async def test_websocket_adapter_opaque_socket_public_host_and_limits(monkeypatch, lease):
    import krim_agent.dev_preview.websockets_client as module

    calls = []
    routes = SocketRoutes()

    async def forbidden(*args, **kwargs):
        pytest.fail("E must not resolve or dial")

    monkeypatch.setattr(asyncio.get_running_loop(), "sock_connect", forbidden)
    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", forbidden)

    @asynccontextmanager
    async def connect(url, **kwargs):
        calls.append((url, kwargs))
        yield SimpleNamespace(subprotocol="vite-hmr")

    monkeypatch.setattr(
        module.importlib, "import_module", lambda name: SimpleNamespace(connect=connect)
    )
    async with WebsocketsConnector(routes).connect(
        lease,
        b"/hmr?x=1",
        origin="https://p-123.preview.example.net",
        headers=[],
        subprotocols=["vite-hmr"],
    ) as connection:
        assert connection.subprotocol == "vite-hmr"
    url, kwargs = calls[0]
    assert url == "ws://p-123.preview.example.net/hmr?x=1"
    assert kwargs["sock"] is routes.sock and kwargs["proxy"] is None
    assert kwargs["max_size"] == 1_048_576 and kwargs["max_queue"] == 16
    assert kwargs["logger"].disabled and not kwargs["logger"].propagate
    assert routes.entered and routes.exited


async def test_missing_websocket_dependency_fails_before_connection(monkeypatch, lease):
    import krim_agent.dev_preview.websockets_client as module

    def missing(name):
        raise ImportError(name)

    monkeypatch.setattr(module.importlib, "import_module", missing)
    routes = SocketRoutes()
    with pytest.raises(PreviewError, match="preview_websocket_unavailable"):
        async with WebsocketsConnector(routes).connect(
            lease,
            b"/",
            origin="https://p-123.preview.example.net",
            headers=[],
            subprotocols=[],
        ):
            pytest.fail("missing client must not connect")
    assert not routes.entered


async def test_http_socketpair_chunked_stream_no_dns_or_dial(monkeypatch):
    client, peer = socket.socketpair()
    client.setblocking(False)
    peer.setblocking(False)
    loop = asyncio.get_running_loop()

    async def forbidden(*args, **kwargs):
        pytest.fail("network backend must not resolve or dial")

    monkeypatch.setattr(loop, "getaddrinfo", forbidden)
    monkeypatch.setattr(loop, "sock_connect", forbidden)
    first = asyncio.Event()
    proceed = asyncio.Event()
    request = bytearray()

    async def server():
        while b"\r\n\r\n" not in request:
            request.extend(await loop.sock_recv(peer, 8192))
        await loop.sock_sendall(peer, b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
        await loop.sock_sendall(peer, b"5\r\nfirst\r\n")
        await proceed.wait()
        await loop.sock_sendall(peer, b"6\r\nsecond\r\n0\r\n\r\n")

    task = asyncio.create_task(server())
    received = []
    try:
        async with httpx.AsyncClient(
            transport=ConnectedHTTPTransport(client, "no-dns.invalid"), trust_env=False
        ) as http:
            async with http.stream("GET", "http://no-dns.invalid/path?q=1") as response:
                async for chunk in response.aiter_raw():
                    received.append(chunk)
                    if chunk == b"first":
                        first.set()
                        assert not task.done()
                        proceed.set()
        await task
        assert first.is_set() and b"".join(received) == b"firstsecond"
        assert b"GET /path?q=1 HTTP/1.1" in request and b"Host: no-dns.invalid" in request
        assert client.fileno() == -1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        client.close()
        peer.close()


async def test_http_socket_cannot_reconnect_or_switch_target():
    client, peer = socket.socketpair()
    client.setblocking(False)
    transport = ConnectedHTTPTransport(client, "public.invalid")
    try:
        with pytest.raises(PreviewError, match="preview_transport_target_denied"):
            await transport.backend.connect_tcp("other.invalid", 80)
        await transport.backend.connect_tcp("public.invalid", 80)
        with pytest.raises(PreviewError, match="preview_transport_target_denied"):
            await transport.backend.connect_tcp("public.invalid", 80)
        with pytest.raises(PreviewError, match="preview_transport_target_denied"):
            await transport.backend.connect_unix_socket("/any/path")
    finally:
        await transport.aclose()
        peer.close()


async def test_ws_cancel_closes_socket_before_route_context(monkeypatch, lease):
    import krim_agent.dev_preview.websockets_client as module

    routes = SocketRoutes()
    entered = asyncio.Event()

    @asynccontextmanager
    async def connect(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()
        yield

    monkeypatch.setattr(
        module.importlib, "import_module", lambda name: SimpleNamespace(connect=connect)
    )

    async def run():
        async with WebsocketsConnector(routes).connect(
            lease,
            b"/",
            origin="https://public.invalid",
            headers=[],
            subprotocols=[],
        ):
            pytest.fail("handshake is blocked")

    task = asyncio.create_task(run())
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert routes.exited
