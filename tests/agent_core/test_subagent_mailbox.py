from __future__ import annotations

import asyncio
from typing import Any

import pytest

from krim_agent.subagents.mailbox import MailboxWakeups


async def test_commit_between_subscription_and_read_cannot_lose_wakeup() -> None:
    wakeups = MailboxWakeups()
    rows: list[dict[str, Any]] = []
    reading = asyncio.Event()
    finish_read = asyncio.Event()
    calls = 0

    async def read(cursor: int) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        captured = [row for row in rows if row["seq"] > cursor]
        if calls == 1:
            reading.set()
            await finish_read.wait()
        return captured

    waiter = asyncio.create_task(wakeups.wait("agent", 0, read, seconds=1))
    await reading.wait()
    rows.append({"seq": 1, "message_id": "message-one"})
    await wakeups.publish("agent")
    finish_read.set()
    assert await asyncio.wait_for(waiter, 2) == rows
    assert calls == 2


async def test_existing_rows_are_visible_without_an_in_memory_signal() -> None:
    wakeups = MailboxWakeups()

    async def read(cursor: int) -> list[dict[str, Any]]:
        return [{"seq": 8}] if cursor < 8 else []

    assert await wakeups.wait("restored", 7, read, seconds=0) == [{"seq": 8}]
    assert await wakeups.wait("restored", 8, read, seconds=0) == []


async def test_timeout_only_finishes_wait_not_the_worker() -> None:
    wakeups = MailboxWakeups()
    alive = asyncio.Event()

    async def read(cursor: int) -> list[dict[str, Any]]:
        assert not alive.is_set()
        return []

    assert await wakeups.wait("agent", 0, read, seconds=0.01) == []
    assert not alive.is_set()


async def test_cancellation_removes_waiter_and_does_not_consume_rows() -> None:
    wakeups = MailboxWakeups()
    reading = asyncio.Event()
    rows: list[dict[str, Any]] = []

    async def read(cursor: int) -> list[dict[str, Any]]:
        reading.set()
        return rows

    waiter = asyncio.create_task(wakeups.wait("agent", 0, read, seconds=2))
    await reading.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    rows.append({"seq": 1})
    await wakeups.publish("agent")
    assert await wakeups.wait("agent", 0, read, seconds=0) == [{"seq": 1}]


async def test_host_close_wakes_all_waiters() -> None:
    wakeups = MailboxWakeups()
    calls = 0
    reading = asyncio.Event()

    async def read(cursor: int) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        if calls >= 2:
            reading.set()
        return []

    one = asyncio.create_task(wakeups.wait("one", 0, read, seconds=2))
    two = asyncio.create_task(wakeups.wait("two", 0, read, seconds=2))
    await reading.wait()
    await wakeups.close()
    assert await asyncio.wait_for(asyncio.gather(one, two), 1) == [[], []]
