"""Bound in-flight authorization and failed lifecycle operations."""

import asyncio

import pytest
from test_dev_browser_session import ORIGIN, Check, Context, Route, session

from krim_agent.dev_browser import BrowserError, BrowserLimits, BrowserSession


@pytest.mark.asyncio
async def test_repeated_start_does_not_allocate_another_page_or_watcher():
    context, _, adapter = await session()
    watcher = adapter._watcher
    try:
        with pytest.raises(BrowserError, match="browser_already_started"):
            await adapter.start()
        assert adapter._watcher is watcher
        assert not context.closed.is_set()
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_missing_websocket_interception_fails_closed():
    context = Context()

    async def unavailable(*args):
        raise AttributeError("not supported by this browser version")

    context.route_web_socket = unavailable
    adapter = BrowserSession(context, ORIGIN, Check())
    with pytest.raises(BrowserError, match="browser_open_failed"):
        await adapter.start()
    assert context.closed.is_set()
    assert adapter._watcher is None


@pytest.mark.asyncio
async def test_revocation_blocks_business_request_before_continue():
    context, check, adapter = await session()
    check.revoked = True
    route = Route(context)
    await context.handler(route)
    assert route.aborted and not route.continued
    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        await adapter.click("#go")
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_authorization_check_timeout_closes_idle_context():
    context, _, adapter = await session(limits=BrowserLimits(poll_seconds=0.01, check_seconds=0.02))

    async def hang():
        await asyncio.Event().wait()

    adapter._check = hang
    await asyncio.wait_for(context.closed.wait(), 1)
    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        await adapter.click("#go")


@pytest.mark.asyncio
async def test_authorization_fanout_does_not_queue_unlimited_checks():
    context, _, adapter = await session(limits=BrowserLimits(poll_seconds=2))
    release, entered = asyncio.Event(), asyncio.Event()
    calls = 0

    async def blocked_check():
        nonlocal calls
        calls += 1
        if calls == 8:
            entered.set()
        await release.wait()

    adapter._check = blocked_check
    tasks = [asyncio.create_task(adapter._authorize()) for _ in range(8)]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        with pytest.raises(BrowserError, match="browser_check_capacity"):
            await adapter._authorize()
        assert calls == 8
        release.set()
        await asyncio.gather(*tasks)
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await adapter.close()
