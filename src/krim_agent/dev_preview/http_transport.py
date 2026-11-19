"""httpcore's public network port over one already-connected B-owned socket.

This adapter never resolves DNS or dials an address. The caller holds B's connect
context outside the transport, so context enter/exit stay in the same task even
when httpcore reads or closes a stream from a child task.
"""

# ruff: noqa: ASYNC109 -- exact public httpcore method signatures include timeout.
from __future__ import annotations

import asyncio
import socket
import ssl
from collections.abc import AsyncIterator, Iterable
from typing import Any

import httpcore
import httpx

from .service import PreviewError

SocketOption = tuple[int, int, int | bytes | bytearray] | tuple[int, int, None, int]


class ConnectedStream(httpcore.AsyncNetworkStream):
    def __init__(self, sock: socket.socket) -> None:
        if sock.getblocking() or (sock.type & socket.SOCK_STREAM) != socket.SOCK_STREAM:
            raise PreviewError("preview_socket_invalid", 503)
        self.sock = sock
        self.closed = False

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:  # noqa: ASYNC109
        try:
            async with asyncio.timeout(timeout):
                return await asyncio.get_running_loop().sock_recv(self.sock, max_bytes)
        except TimeoutError:
            raise httpcore.ReadTimeout("preview_read_timeout") from None
        except OSError:
            raise httpcore.ReadError("preview_read_failed") from None

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:  # noqa: ASYNC109
        try:
            async with asyncio.timeout(timeout):
                await asyncio.get_running_loop().sock_sendall(self.sock, buffer)
        except TimeoutError:
            raise httpcore.WriteTimeout("preview_write_timeout") from None
        except OSError:
            raise httpcore.WriteError("preview_write_failed") from None

    async def aclose(self) -> None:
        if not self.closed:
            self.closed = True
            self.sock.close()

    async def start_tls(  # noqa: ASYNC109
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise PreviewError("preview_tls_unavailable", 503)

    def get_extra_info(self, info: str) -> Any:
        # HTTP/1 keepalive is disabled; never expose the socket as an alternate
        # protocol/target to response consumers.
        return None


class ConnectedBackend(httpcore.AsyncNetworkBackend):
    """Only one consumption of one lease socket; reject HTTP retries as well."""

    def __init__(self, sock: socket.socket, host: str) -> None:
        self.stream = ConnectedStream(sock)
        self.host = host
        self.used = False

    async def connect_tcp(  # noqa: ASYNC109
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[SocketOption] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        if self.used or host != self.host or port != 80 or local_address or socket_options:
            raise PreviewError("preview_transport_target_denied", 503)
        self.used = True
        return self.stream

    async def connect_unix_socket(  # noqa: ASYNC109
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[SocketOption] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise PreviewError("preview_transport_target_denied", 503)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class _ResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream: Any) -> None:
        self.stream = stream

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for data in self.stream:
            yield data

    async def aclose(self) -> None:
        await self.stream.aclose()


class ConnectedHTTPTransport(httpx.AsyncBaseTransport):
    def __init__(self, sock: socket.socket, host: str) -> None:
        self.backend = ConnectedBackend(sock, host)
        self.pool = httpcore.AsyncConnectionPool(
            network_backend=self.backend,
            max_connections=1,
            max_keepalive_connections=0,
            retries=0,
            http1=True,
            http2=False,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.scheme != "http" or request.url.host != self.backend.host:
            raise PreviewError("preview_transport_target_denied", 503)
        if not isinstance(request.stream, httpx.AsyncByteStream):
            raise PreviewError("preview_stream_invalid", 503)
        response = await self.pool.handle_async_request(
            httpcore.Request(
                method=request.method,
                url=httpcore.URL(
                    scheme=request.url.raw_scheme,
                    host=request.url.raw_host,
                    port=request.url.port,
                    target=request.url.raw_path,
                ),
                headers=request.headers.raw,
                content=request.stream,
                # Never forward caller trace callbacks that could capture internals.
                extensions={"timeout": request.extensions.get("timeout", {})},
            )
        )
        return httpx.Response(
            response.status, headers=response.headers, stream=_ResponseStream(response.stream)
        )

    async def aclose(self) -> None:
        try:
            await self.pool.aclose()
        finally:
            await self.backend.stream.aclose()
