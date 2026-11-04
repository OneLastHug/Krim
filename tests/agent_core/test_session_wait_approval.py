import asyncio
from types import SimpleNamespace

import pytest

import krim_agent.sessions.service as session_module
from krim_agent.sessions.service import SessionService


def waiter(monkeypatch, producer, read_turn):
    service = object.__new__(SessionService)
    service.settings = SimpleNamespace(session_queue_timeout=0.1, run_timeout=0.1)
    service.turn = read_turn

    async def wait_run(_run_id):
        await asyncio.shield(producer)

    service.runs = SimpleNamespace(tasks={"run-1": producer} if producer else {}, wait=wait_run)
    original_timeout = asyncio.timeout
    # 缩短只读消费者的旧墙钟上限，避免为此回归实际等待数分钟。
    monkeypatch.setattr(session_module, "asyncio", SimpleNamespace(
        timeout=lambda _seconds: original_timeout(0.05),
        sleep=asyncio.sleep,
        get_running_loop=asyncio.get_running_loop,
    ))
    return service


async def test_live_producer_outlasts_the_old_reader_wall_timeout(monkeypatch):
    producer = asyncio.create_task(asyncio.sleep(0.15))
    completed = SimpleNamespace(status="completed", run_id="run-1")

    async def read_turn(*_args):
        return completed if producer.done() else SimpleNamespace(status="running", run_id="run-1")

    service = waiter(monkeypatch, producer, read_turn)
    try:
        result = await service.wait(None, "local", "session-1", "turn-1")
        assert result is completed and producer.done() and not producer.cancelled()
    finally:
        if not producer.done():
            producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)


async def test_cancelling_wait_does_not_cancel_the_producer(monkeypatch):
    release, entered = asyncio.Event(), asyncio.Event()
    producer = asyncio.create_task(release.wait())

    async def read_turn(*_args):
        entered.set()
        return SimpleNamespace(status="running", run_id="run-1")

    service = waiter(monkeypatch, producer, read_turn)
    consumer = asyncio.create_task(service.wait(None, "local", "session-1", "turn-1"))
    try:
        await entered.wait()
        await asyncio.sleep(0.02)
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer
        assert not producer.done() and not producer.cancelled()
    finally:
        consumer.cancel()
        release.set()
        await asyncio.gather(consumer, producer, return_exceptions=True)


async def test_queue_without_a_producer_keeps_the_reader_timeout(monkeypatch):
    async def read_turn(*_args):
        return SimpleNamespace(status="queued", run_id=None)

    service = waiter(monkeypatch, None, read_turn)
    with pytest.raises(TimeoutError):
        await service.wait(None, "local", "session-1", "turn-1")
