"""One-preview host bridge: authorize every CHECK/CONNECT; never resolve DNS."""

from __future__ import annotations

import asyncio
import os
import socket
import stat
import struct
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path

from .fixed_proxy import ProxyError, close_writer, fixed_host, relay

Authorize = Callable[[], Awaitable[None]]
RUN_ROOT = Path("/run/krim-browser")
CHECK_INTERVAL = 0.5


class PreviewConnectBridge:
    """One host-owned socket binds the original viewer/preview/lease closure.

    CHECK has no network IO. CONNECT permits only host literal 127.0.0.1:443.
    No URL, headers, bearer or other target can be chosen by a worker command.
    TLS remains end-to-end; E authenticates exact Host + preview cookie.
    """

    def __init__(
        self,
        socket_path: str,
        expected_host: str,
        authorize: Authorize,
        *,
        expected_uid: int,
        expected_gid: int,
    ) -> None:
        self.socket_path = Path(socket_path)
        self.expected_host = fixed_host(expected_host)
        self.authorize = authorize
        self.expected_uid, self.expected_gid = expected_uid, expected_gid
        self.server: asyncio.AbstractServer | None = None
        self.tasks: set[asyncio.Task[None]] = set()
        self.tunnels = 0
        self.closed = False
        self.node: tuple[int, int] | None = None

    async def __aenter__(self) -> PreviewConnectBridge:
        path, parent = self.socket_path, self.socket_path.parent
        if not path.is_absolute() or not path.is_relative_to(RUN_ROOT):
            raise ProxyError("bridge_path_invalid")
        if (
            not parent.is_dir()
            or any(p.is_symlink() for p in (path, *path.parents))
            or path.exists()
            or parent.stat().st_uid != os.getuid()
            or stat.S_IMODE(parent.stat().st_mode) != 0o700
        ):
            raise ProxyError("bridge_directory_invalid")
        # The enclosing private 0700 directory blocks access while chmod runs;
        # avoid changing process-global umask in an async application.
        if (
            isinstance(self.expected_uid, bool)
            or isinstance(self.expected_gid, bool)
            or not isinstance(self.expected_uid, int)
            or not isinstance(self.expected_gid, int)
            or not 0 <= self.expected_uid <= 2**31 - 1
            or not 0 <= self.expected_gid <= 2**31 - 1
        ):
            raise ProxyError("bridge_peer_invalid")
        if os.getuid() != 0 and (self.expected_uid, self.expected_gid) != (
            os.getuid(),
            os.getgid(),
        ):
            raise ProxyError("bridge_owner_mismatch")
        self.server = await asyncio.start_unix_server(self.accept, path=str(path), limit=256)
        try:
            st = path.lstat()
            if not stat.S_ISSOCK(st.st_mode) or st.st_uid != os.getuid():
                raise ProxyError("bridge_socket_invalid")
            self.node = st.st_dev, st.st_ino
            # Only the newly created socket inode is shared. The root-owned
            # 0700 host directory stays private; Docker mounts this one node.
            if os.getuid() == 0:
                os.chown(path, 0, self.expected_gid, follow_symlinks=False)
            os.chmod(path, 0o660, follow_symlinks=False)
            verified = path.lstat()
            if (
                (verified.st_dev, verified.st_ino) != self.node
                or verified.st_uid != os.getuid()
                or verified.st_gid != self.expected_gid
                or stat.S_IMODE(verified.st_mode) != 0o660
            ):
                raise ProxyError("bridge_socket_invalid")
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

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
        with suppress(FileNotFoundError):
            st = self.socket_path.lstat()
            if self.node == (st.st_dev, st.st_ino) and stat.S_ISSOCK(st.st_mode):
                self.socket_path.unlink()

    def accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Four tunnels plus at most eight concurrent adapter authority checks.
        if self.closed or len(self.tasks) >= 12:
            writer.close()
            return
        task = asyncio.create_task(self.handle(reader, writer))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def check(self) -> None:
        async with asyncio.timeout(3):
            await self.authorize()

    async def watch(self) -> None:
        while True:
            await asyncio.sleep(CHECK_INTERVAL)
            await self.check()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream = None
        children: list[asyncio.Task[None]] = []
        tunnel = established = False
        try:
            async with asyncio.timeout(120):
                sock = writer.get_extra_info("socket")
                if sock is None or not hasattr(socket, "SO_PEERCRED"):
                    raise ProxyError("bridge_peer_unavailable")
                pid, uid, gid = struct.unpack(
                    "3i",
                    sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12),
                )
                if pid <= 0 or uid != self.expected_uid or gid != self.expected_gid:
                    raise ProxyError("bridge_peer_denied")
                async with asyncio.timeout(3):
                    line = await reader.readline()
                commands = {
                    f"KRIM-PREVIEW/1 CHECK {self.expected_host} 443\n".encode(): "check",
                    f"KRIM-PREVIEW/1 CONNECT {self.expected_host} 443\n".encode(): "connect",
                }
                operation = commands.get(line)
                if operation is None:
                    raise ProxyError("bridge_target_denied")
                await self.check()
                if operation == "check":
                    writer.write(b"OK\n")
                    async with asyncio.timeout(1):
                        await writer.drain()
                    return
                if self.tunnels >= 4:
                    raise ProxyError("bridge_capacity")
                self.tunnels += 1
                tunnel = True
                async with asyncio.timeout(3):
                    remote_reader, upstream = await asyncio.open_connection("127.0.0.1", 443)
                    await self.check()  # no business bytes before the second check
                    writer.write(b"OK\n")
                    await writer.drain()
                    established = True
                children = [
                    asyncio.create_task(relay(reader, writer, remote_reader, upstream)),
                    asyncio.create_task(self.watch()),
                ]
                done, _ = await asyncio.wait(children, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
        except asyncio.CancelledError:
            raise
        except Exception:
            if not established:
                with suppress(Exception):
                    async with asyncio.timeout(1):
                        writer.write(b"NO\n")
                        await writer.drain()
        finally:
            try:
                for task in children:
                    task.cancel()
                await asyncio.gather(*children, return_exceptions=True)
            finally:
                if tunnel:
                    self.tunnels -= 1
                try:
                    await close_writer(writer)
                finally:
                    if upstream is not None:
                        await close_writer(upstream)
