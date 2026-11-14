"""Real websockets over AF_UNIX socketpair, skipped explicitly until I installs it.

No listening socket, dev server, DNS, Docker or host networking is used. The peer
uses the library's Sans-I/O server to exercise its actual client implementation.
"""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager

import pytest

from krim_agent.dev_preview.websockets_client import WebsocketsConnector
from krim_agent.development.preview_ports import RouteBinding, RouteLease


@pytest.fixture
def real_ws():
    module = pytest.importorskip(
        "websockets", minversion="15.0", reason="I must provide websockets 15"
    )
    if module.__version__.split(".")[0] != "15":
        pytest.skip("socketpair adapter targets websockets 15")
    return module


class LibraryPeer:
    def __init__(self, mode="echo"):
        self.mode = mode
        self.connected = 0
        self.closed = False
        self.headers = None
        self.path = None
        self.received = asyncio.Event()
        self.lease = RouteLease(
            RouteBinding(
                preview_id="preview",
                owner_id="owner",
                project_id="project",
                environment_id="env",
                generation=1,
                service_id="process",
                process_id="process",
                operation_id="op",
                request_digest="0" * 64,
                execution_id="a" * 64,
                started_at="2026-11-15T00:00:00.000000001Z",
                port=3000,
                route_epoch=1,
            ),
            "lease",
        )

    @asynccontextmanager
    async def connect(self, lease):
        owner = asyncio.current_task()
        assert lease == self.lease
        client, peer = socket.socketpair()
        client.setblocking(False)
        peer.setblocking(False)
        self.connected += 1
        task = asyncio.create_task(self.serve(peer))
        try:
            yield client
        finally:
            assert asyncio.current_task() is owner and client.fileno() == -1
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            peer.close()
            self.closed = True

    async def serve(self, peer):
        from websockets.frames import OP_BINARY, OP_TEXT, Frame
        from websockets.http11 import Request
        from websockets.server import ServerProtocol

        protocol = ServerProtocol(subprotocols=["vite-hmr"])
        loop = asyncio.get_running_loop()
        while True:
            data = await loop.sock_recv(peer, 65536)
            if not data:
                return
            protocol.receive_data(data)
            for event in protocol.events_received():
                if isinstance(event, Request):
                    self.headers = event.headers
                    self.path = event.path
                    self.received.set()
                    if self.mode == "hang":
                        await asyncio.Event().wait()
                    if self.mode == "redirect":
                        response = protocol.reject(302, "redirect")
                        response.headers["Location"] = "ws://must-not-dial.invalid/"
                        protocol.send_response(response)
                    else:
                        protocol.send_response(protocol.accept(event))
                elif isinstance(event, Frame):
                    if event.opcode == OP_TEXT:
                        protocol.send_text(event.data)
                    elif event.opcode == OP_BINARY:
                        protocol.send_binary(event.data)
            for outgoing in protocol.data_to_send():
                if outgoing:
                    await loop.sock_sendall(peer, outgoing)
                else:
                    peer.shutdown(socket.SHUT_WR)


def forbid_network(monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("socketpair WebSocket must never resolve or dial")

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", forbidden)
    monkeypatch.setattr(loop, "sock_connect", forbidden)


async def test_real_ws_socketpair_text_binary_and_close(real_ws, monkeypatch):
    forbid_network(monkeypatch)
    routes = LibraryPeer()
    async with asyncio.timeout(5):
        async with WebsocketsConnector(routes).connect(
            routes.lease,
            b"/hmr?v=1",
            origin="https://public.invalid",
            headers=[],
            subprotocols=["vite-hmr"],
        ) as channel:
            assert channel.subprotocol == "vite-hmr"
            messages = channel.messages()
            for value in ("reload", b"\x00\xff\x01"):
                await channel.send(value)
                assert await anext(messages) == value
    assert routes.headers["Host"] == "public.invalid"
    assert routes.headers["Origin"] == "https://public.invalid"
    assert routes.path == "/hmr?v=1" and routes.closed and routes.connected == 1


async def test_real_ws_redirect_refused_without_second_connection(real_ws, monkeypatch):
    forbid_network(monkeypatch)
    routes = LibraryPeer("redirect")
    async with asyncio.timeout(5):
        with pytest.raises(ValueError, match="redirect"):
            async with WebsocketsConnector(routes).connect(
                routes.lease,
                b"/",
                origin="https://public.invalid",
                headers=[],
                subprotocols=["vite-hmr"],
            ):
                pytest.fail("redirect was accepted")
    assert routes.connected == 1 and routes.closed


async def test_real_ws_cancel_during_handshake_closes_context(real_ws, monkeypatch):
    forbid_network(monkeypatch)
    routes = LibraryPeer("hang")

    async def run():
        async with WebsocketsConnector(routes).connect(
            routes.lease,
            b"/",
            origin="https://public.invalid",
            headers=[],
            subprotocols=["vite-hmr"],
        ):
            pytest.fail("handshake is blocked")

    task = asyncio.create_task(run())
    try:
        await asyncio.wait_for(routes.received.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert routes.closed
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
