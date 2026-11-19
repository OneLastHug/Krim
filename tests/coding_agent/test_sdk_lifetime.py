"""Exact ownership of the pinned SDK's event-loop sentinel, not task sweeps."""

import asyncio

import pytest
from ai import util as sdk_util

from krim_agent.agent_sdk.host import sdk_lifetime


async def test_last_concurrent_host_joins_only_its_exact_sdk_sentinel():
    loop = asyncio.get_running_loop()
    entered, release = asyncio.Event(), asyncio.Event()

    async def concurrent():
        async with sdk_lifetime():
            entered.set()
            await release.wait()

    async with sdk_lifetime():
        sentinel = sdk_util._LOOP_CLOSING_MAP[loop]
        task = asyncio.create_task(concurrent())
        await entered.wait()
    assert not sentinel.done()  # another host still uses the SDK
    release.set()
    await task
    assert sentinel.cancelled() and sdk_util._LOOP_CLOSING_MAP.get(loop) is None


async def test_preexisting_sdk_sentinel_is_borrowed_not_cancelled():
    loop = asyncio.get_running_loop()
    sdk_util.get_loop_closing_checker()
    sentinel = sdk_util._LOOP_CLOSING_MAP[loop]
    try:
        async with sdk_lifetime():
            assert sdk_util._LOOP_CLOSING_MAP[loop] is sentinel
        assert not sentinel.done()
    finally:
        sentinel.cancel()
        await asyncio.gather(sentinel, return_exceptions=True)


async def test_cancelled_host_still_joins_its_sdk_sentinel():
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    owned = []

    async def run():
        async with sdk_lifetime():
            owned.append(sdk_util._LOOP_CLOSING_MAP[loop])
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(run())
    await entered.wait()
    task.cancel()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert owned[0].cancelled() and sdk_util._LOOP_CLOSING_MAP.get(loop) is None
