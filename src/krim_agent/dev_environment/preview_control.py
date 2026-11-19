"""Cancellable read-only Docker inspection for preview's short lock sections."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .docker import DockerCLI, Reply
from .model import EnvironmentError


@dataclass(frozen=True)
class PreviewLimits:
    control_seconds: float = 2.0
    connect_seconds: float = 2.0
    lease_seconds: float = 5.0
    heartbeat_seconds: float = 1.0
    max_age_seconds: float = 3600.0
    idle_seconds: float = 120.0
    service_connections: int = 8
    adapter_connections: int = 64

    def __post_init__(self) -> None:
        for name, ceiling in (("control_seconds", 2), ("connect_seconds", 2),
                              ("lease_seconds", 5), ("heartbeat_seconds", 1),
                              ("max_age_seconds", 3600), ("idle_seconds", 120)):
            value = getattr(self, name)
            if type(value) not in (int, float) or not 0.01 <= value <= ceiling:
                raise ValueError("invalid preview duration")
        if (type(self.service_connections) is not int or type(self.adapter_connections) is not int
                or not 1 <= self.service_connections <= 8
                or not self.service_connections <= self.adapter_connections <= 64):
            raise ValueError("invalid preview capacity")

    @property
    def relay_silence_seconds(self) -> float:
        return min(5.0, max(0.1, 3 * self.heartbeat_seconds))


class PreviewControl:
    """Only inspection verbs; never alter the lifecycle writer's cancellation fence."""

    prefix: tuple[str, ...] = DockerCLI.prefix

    def __init__(self, seconds: float = 2.0) -> None:
        if not 0.01 <= seconds <= 2:
            raise ValueError("invalid inspection budget")
        self.seconds = seconds

    async def run(self, args: tuple[str, ...]) -> Reply:
        if len(args) != 3 or args[:2] not in {
            ("container", "inspect"), ("network", "inspect"), ("image", "inspect"),
        }:
            raise ValueError("preview control is read-only")
        process = None
        readers: list[asyncio.Task[bytes]] = []
        spawn = asyncio.create_task(asyncio.create_subprocess_exec(
            *self.prefix, *args, stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
        ))
        try:
            async with asyncio.timeout(self.seconds):
                process = await asyncio.shield(spawn)
                total = 0

                async def read(stream: asyncio.StreamReader | None) -> bytes:
                    nonlocal total
                    assert stream is not None
                    data = bytearray()
                    while chunk := await stream.read(65536):
                        total += len(chunk)
                        if total > 1024 * 1024:
                            raise EnvironmentError("preview_inspect_too_large")
                        data.extend(chunk)
                    return bytes(data)

                readers = [asyncio.create_task(read(process.stdout)),
                           asyncio.create_task(read(process.stderr))]
                stdout, stderr = await asyncio.gather(*readers)
                await process.wait()
                return Reply(process.returncode or 0, stdout.decode("utf-8"),
                             stderr.decode("utf-8", "replace"))
        finally:
            # Spawn cancellation can happen after fork. Reap that CLI outside
            # the caller's lock without waiting for an unresponsive daemon.
            if process is None:
                if spawn.done() and not spawn.cancelled():
                    try:
                        process = spawn.result()
                    except Exception:
                        pass
                else:
                    spawn.add_done_callback(_kill_spawned)
            if process is not None and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                asyncio.create_task(process.wait())
            for reader in readers:
                if not reader.done():
                    reader.cancel()
            if readers:
                await asyncio.gather(*readers, return_exceptions=True)


def _kill_spawned(task: asyncio.Task[asyncio.subprocess.Process]) -> None:
    if task.cancelled():
        return
    try:
        process = task.result()
    except Exception:
        return
    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        asyncio.create_task(process.wait())
