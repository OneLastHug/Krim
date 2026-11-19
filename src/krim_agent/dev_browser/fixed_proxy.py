"""Bounded container-local CONNECT proxy; only one host:443 maps to one UDS."""

from __future__ import annotations

import asyncio
import ipaddress
import re
from contextlib import suppress

HOST_RE = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+\Z"
)
MAX_HEADER = 16 * 1024
MAX_RELAY = 64 * 1024 * 1024


class ProxyError(Exception):
    pass


def fixed_host(value: str) -> str:
    if len(value) > 253 or not HOST_RE.fullmatch(value):
        raise ProxyError("proxy_config_invalid")
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return value
    raise ProxyError("proxy_config_invalid")


async def read_headers(reader: asyncio.StreamReader) -> tuple[str, dict[str, str]]:
    try:
        data = await reader.readuntil(b"\r\n\r\n")
        if len(data) > MAX_HEADER:
            raise ValueError
        lines = data[:-4].split(b"\r\n")
        method, target, version = lines[0].decode("ascii").split(" ")
        headers: dict[str, str] = {}
        for line in lines[1:]:
            name, value = line.decode("ascii").split(":", 1)
            if not re.fullmatch(r"[A-Za-z-]+", name):
                raise ValueError
            value = value.strip(" ")
            if name.lower() in headers or any(ord(c) < 32 or ord(c) == 127 for c in value):
                raise ValueError
            headers[name.lower()] = value
        return f"{method} {target} {version}", headers
    except (ValueError, UnicodeError, asyncio.LimitOverrunError):
        raise ProxyError("proxy_request_invalid") from None


async def relay(
    left: asyncio.StreamReader,
    left_writer: asyncio.StreamWriter,
    right: asyncio.StreamReader,
    right_writer: asyncio.StreamWriter,
) -> None:
    count = 0

    async def copy(source: asyncio.StreamReader, target: asyncio.StreamWriter) -> None:
        nonlocal count
        while chunk := await source.read(65536):
            count += len(chunk)
            if count > MAX_RELAY:
                raise ProxyError("proxy_bytes_exceeded")
            target.write(chunk)
            await target.drain()
        if target.can_write_eof():
            target.write_eof()
            await target.drain()

    tasks = [
        asyncio.create_task(copy(left, right_writer)),
        asyncio.create_task(copy(right, left_writer)),
    ]
    try:
        # Preserve half-close; the enclosing connection TTL bounds a peer that
        # never finishes its half. Any copy error stops both directions.
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with suppress(Exception):
        async with asyncio.timeout(1):
            await writer.wait_closed()


async def serve_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    socket_path: str,
    expected_host: str,
    expected_port: int = 443,
) -> None:
    peer_writer = None
    established = False
    try:
        async with asyncio.timeout(120):
            async with asyncio.timeout(3):
                request, headers = await read_headers(reader)
                if request != f"CONNECT {expected_host}:{expected_port} HTTP/1.1":
                    raise ProxyError("proxy_target_denied")
                if headers.get("host") != f"{expected_host}:{expected_port}":
                    raise ProxyError("proxy_target_denied")
                if set(headers) - {"host", "user-agent", "proxy-connection", "connection"}:
                    raise ProxyError("proxy_header_denied")
                peer_reader, peer_writer = await asyncio.open_unix_connection(socket_path)
                peer_writer.write(f"KRIM-PREVIEW/1 CONNECT {expected_host} 443\n".encode("ascii"))
                await peer_writer.drain()
                if await peer_reader.readexactly(3) != b"OK\n":
                    raise ProxyError("proxy_bridge_denied")
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
                established = True
            await relay(reader, writer, peer_reader, peer_writer)
    except asyncio.CancelledError:
        raise
    except Exception:
        if not established:
            with suppress(Exception):
                async with asyncio.timeout(1):
                    writer.write(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n")
                    await writer.drain()
    finally:
        try:
            await close_writer(writer)
        finally:
            if peer_writer is not None:
                await close_writer(peer_writer)


async def check_authority(socket_path: str, host: str) -> None:
    """No TCP side effect: ask the same host-bound socket to recheck its viewer."""
    writer = None
    try:
        async with asyncio.timeout(3):
            reader, writer = await asyncio.open_unix_connection(socket_path)
            writer.write(f"KRIM-PREVIEW/1 CHECK {fixed_host(host)} 443\n".encode("ascii"))
            await writer.drain()
            if await reader.readexactly(3) != b"OK\n":
                raise ProxyError("browser_authorization_lost")
    except (OSError, asyncio.IncompleteReadError, TimeoutError):
        raise ProxyError("browser_authorization_lost") from None
    finally:
        if writer is not None:
            await close_writer(writer)


class FixedProxy:
    def __init__(self, socket_path: str, host: str) -> None:
        self.socket_path = socket_path
        self.host = fixed_host(host)
        self.server: asyncio.AbstractServer | None = None
        self.tasks: set[asyncio.Task[None]] = set()
        self.closed = False

    async def __aenter__(self) -> FixedProxy:
        self.server = await asyncio.start_server(self.accept, "127.0.0.1", 18080, limit=MAX_HEADER)
        return self

    def accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.closed or len(self.tasks) >= 8:
            writer.close()
            return
        task = asyncio.create_task(serve_connection(reader, writer, self.socket_path, self.host))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def __aexit__(self, *args: object) -> None:
        self.closed = True
        if self.server is not None:
            self.server.close()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.server is not None:
            await self.server.wait_closed()
