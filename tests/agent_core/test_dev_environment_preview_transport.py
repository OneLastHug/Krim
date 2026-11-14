from __future__ import annotations

import asyncio

import pytest

from krim_agent.dev_environment.preview_control import PreviewLimits
from krim_agent.dev_environment.preview_transport import ExecRelay


class Sink:
    def write(self, data):
        pass

    async def drain(self):
        pass

    def close(self):
        pass


class Empty:
    async def read(self, count):
        return b""


class Process:
    def __init__(self):
        self.stdin = Sink()
        self.stdout = Empty()
        self.stderr = Empty()
        self.returncode = None
        self.reaped = asyncio.Event()

    async def wait(self):
        self.returncode = 0
        self.reaped.set()
        return 0


async def test_cancelled_pending_spawn_keeps_capacity_until_actual_reap(monkeypatch):
    entered = asyncio.Event()
    release_spawn = asyncio.Event()
    released_capacity = asyncio.Event()
    process = Process()

    async def spawn(*args, **kwargs):
        entered.set()
        await release_spawn.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    relay = ExecRelay("a" * 64, 8123, PreviewLimits(connect_seconds=.1),
                      lambda value: released_capacity.set())
    task = asyncio.create_task(relay.start())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert not released_capacity.is_set()
    assert relay._close_task is not None and not relay._close_task.done()
    release_spawn.set()
    async with asyncio.timeout(.5):
        await released_capacity.wait()
    assert process.reaped.is_set()
    assert relay.process is process
    await relay.aclose()


async def test_timeout_pending_spawn_does_not_release_unowned_resources(monkeypatch):
    release_spawn = asyncio.Event()
    capacity_released = asyncio.Event()
    process = Process()

    async def spawn(*args, **kwargs):
        await release_spawn.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    relay = ExecRelay("a" * 64, 8123, PreviewLimits(connect_seconds=.02),
                      lambda value: capacity_released.set())
    with pytest.raises(TimeoutError):
        await relay.start()
    assert not capacity_released.is_set()
    release_spawn.set()
    async with asyncio.timeout(.5):
        await capacity_released.wait()
    assert process.reaped.is_set()
    await relay.aclose()
