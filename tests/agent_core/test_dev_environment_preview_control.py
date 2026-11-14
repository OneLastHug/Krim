from __future__ import annotations

import asyncio

import pytest

from krim_agent.dev_environment.preview_control import PreviewControl, PreviewLimits


@pytest.mark.parametrize("args", [
    ("start", "a" * 64), ("exec", "a" * 64), ("rm", "--force", "a" * 64),
    ("container", "ls"), ("network", "connect", "a" * 64),
])
async def test_preview_control_rejects_mutating_or_noninspect_calls(args):
    with pytest.raises(ValueError):
        await PreviewControl().run(args)


class Stream:
    def __init__(self, data: bytes | None = None):
        self.data = data

    async def read(self, count):
        if self.data is None:
            await asyncio.Future()
        result, self.data = self.data, b""
        return result


class Process:
    def __init__(self, stdout=None):
        self.returncode = None
        self.stdout = Stream(stdout)
        self.stderr = Stream(b"")
        self.killed = False

    def kill(self):
        self.killed = True
        self.returncode = -9

    async def wait(self):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


async def test_preview_inspection_timeout_is_cancellable_and_reaps_cli(monkeypatch):
    process = Process()

    async def spawn(*args, **kwargs):
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    async with asyncio.timeout(.5):
        with pytest.raises(TimeoutError):
            await PreviewControl(.03).run(("container", "inspect", "a" * 64))
    assert process.killed


async def test_preview_inspect_returns_bounded_json(monkeypatch):
    process = Process(b"[]")

    async def spawn(*args, **kwargs):
        assert kwargs["env"] == {"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"}
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    reply = await PreviewControl().run(("container", "inspect", "a" * 64))
    assert reply.code == 0 and reply.stdout == "[]"


async def test_preview_inspect_cancellation_during_spawn_kills_late_process(monkeypatch):
    process = Process(b"[]")
    entered = asyncio.Event()
    complete = asyncio.Event()

    async def spawn(*args, **kwargs):
        entered.set()
        await complete.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(PreviewControl().run(("container", "inspect", "a" * 64)))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not process.killed
    complete.set()
    for _ in range(10):
        await asyncio.sleep(0)
        if process.killed:
            break
    assert process.killed


@pytest.mark.parametrize("values", [
    {"control_seconds": 2.1}, {"lease_seconds": 5.1}, {"heartbeat_seconds": 1.1},
    {"max_age_seconds": 3601}, {"idle_seconds": 121}, {"service_connections": 9},
    {"adapter_connections": 65}, {"service_connections": True}, {"control_seconds": float("nan")},
])
def test_preview_limits_reject_unbounded_values(values):
    with pytest.raises(ValueError):
        PreviewLimits(**values)
