from __future__ import annotations

import asyncio

import pytest

from krim_agent.subagents.scheduling import (
    CapacityUnavailable,
    RequestLimiter,
    ResidentCapacity,
    SchedulerClosed,
)


async def test_one_connection_progresses_root_and_workers_in_order() -> None:
    limiter = RequestLimiter(1)
    first = await limiter.acquire("root", kind="root")
    order: list[str] = []

    async def request(owner: str, kind: str) -> None:
        async with limiter.slot(owner, kind=kind):  # type: ignore[arg-type]
            order.append(owner)
            await asyncio.sleep(0)

    child = asyncio.create_task(request("child", "child"))
    await asyncio.sleep(0)
    root = asyncio.create_task(request("root-next", "root"))
    await asyncio.sleep(0)
    assert limiter.active == 1 and limiter.waiting == 2
    await first.release()
    await asyncio.wait_for(asyncio.gather(child, root), timeout=2)
    assert order == ["child", "root-next"]
    assert limiter.active == 0 and limiter.waiting == 0


async def test_cancelled_waiter_does_not_block_the_queue() -> None:
    limiter = RequestLimiter(1)
    first = await limiter.acquire("first")
    cancelled = asyncio.create_task(limiter.acquire("cancelled"))
    await asyncio.sleep(0)
    last = asyncio.create_task(limiter.acquire("last"))
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await first.release()
    permit = await asyncio.wait_for(last, timeout=2)
    assert permit.owner == "last"
    await permit.release()
    await permit.release()
    assert limiter.active == 0


async def test_close_rejects_waiters_but_preserves_active_accounting() -> None:
    limiter = RequestLimiter(1)
    first = await limiter.acquire("active")
    waiter = asyncio.create_task(limiter.acquire("queued"))
    await asyncio.sleep(0)
    await limiter.close()
    with pytest.raises(SchedulerClosed):
        await waiter
    with pytest.raises(SchedulerClosed):
        await limiter.acquire("late")
    assert limiter.active == 1 and limiter.waiting == 0
    await first.release()
    assert limiter.active == 0


async def test_cancelled_request_returns_its_permit() -> None:
    limiter = RequestLimiter(1)
    started = asyncio.Event()

    async def request() -> None:
        async with limiter.slot("child"):
            started.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(request())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert limiter.active == 0
    async with limiter.slot("next", kind="root"):
        assert limiter.active == 1


async def test_guard_is_rechecked_after_waiting() -> None:
    limiter = RequestLimiter(1)
    first = await limiter.acquire("first")
    allowed = True

    def guard() -> None:
        if not allowed:
            raise PermissionError("revoked")

    waiter = asyncio.create_task(limiter.acquire("child", guard=guard))
    await asyncio.sleep(0)
    allowed = False
    await first.release()
    with pytest.raises(PermissionError):
        await waiter
    assert limiter.active == 0 and limiter.waiting == 0


def test_resident_admission_is_fail_fast_and_identity_scoped() -> None:
    capacity = ResidentCapacity(1)
    capacity.reserve("activation-one")
    capacity.reserve("activation-one")
    with pytest.raises(CapacityUnavailable):
        capacity.reserve("activation-two")
    # A stale activation's repeated release cannot affect the next activation.
    capacity.release("activation-one")
    capacity.reserve("activation-two")
    capacity.release("activation-one")
    assert capacity.active == 1


@pytest.mark.parametrize("capacity", [0, -1, True, 1.5])
def test_invalid_capacities_are_rejected(capacity: int) -> None:
    with pytest.raises(ValueError):
        RequestLimiter(capacity)
    with pytest.raises(ValueError):
        ResidentCapacity(capacity)
