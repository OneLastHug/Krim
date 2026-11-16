"""Resize reuses the existing action fence; no new context or arbitrary size."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from test_dev_browser_session import session

from krim_agent.dev_browser import BrowserError, BrowserLimits
from krim_agent.dev_browser.policy import viewport_preset

DEPLOY = Path(__file__).parents[2] / "deployment/development-browser"
sys.path.insert(0, str(DEPLOY))
from worker import dispatch  # noqa: E402


@pytest.mark.parametrize("preset,size", [("desktop", (1024, 768)), ("mobile", (375, 812))])
def test_only_fixed_dimensions(preset, size):
    assert viewport_preset(preset) == size


@pytest.mark.parametrize("preset", ["", "tablet", "Mobile", "375x812", None, 375, {}, []])
def test_unknown_preset_rejected(preset):
    with pytest.raises(BrowserError, match="^browser_viewport_invalid$"):
        viewport_preset(preset)


@pytest.mark.asyncio
async def test_resize_round_trip_uses_one_action_and_two_authorizations_each():
    context, check, adapter = await session()
    resized = []

    async def set_viewport(value):
        resized.append(value)

    context.page.set_viewport_size = set_viewport
    before = check.calls
    try:
        await adapter.resize("mobile")
        await adapter.resize("desktop")
        assert resized == [{"width": 375, "height": 812}, {"width": 1024, "height": 768}]
        assert check.calls == before + 4
        assert adapter.diagnostics()["actions"] == 2
        assert context.page.calls == []  # no implicit goto/reload or new page
        assert not context.closed.is_set()
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_invalid_resize_does_not_mutate_page_or_consume_action_budget():
    context, _, adapter = await session()
    context.page.set_viewport_size = AsyncMock()
    try:
        with pytest.raises(BrowserError, match="browser_viewport_invalid"):
            await adapter.resize("tablet")
        context.page.set_viewport_size.assert_not_called()
        assert adapter.diagnostics()["actions"] == 0
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_resize_preserves_action_budget():
    context, _, adapter = await session(limits=BrowserLimits(max_actions=1))
    context.page.set_viewport_size = AsyncMock()
    await adapter.resize("mobile")
    with pytest.raises(BrowserError, match="browser_action_limit"):
        await adapter.resize("desktop")
    assert context.page.set_viewport_size.await_count == 1
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_authorization_lost_before_resize_prevents_mutation():
    context, check, adapter = await session()
    context.page.set_viewport_size = AsyncMock()
    check.revoked = True
    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        await adapter.resize("mobile")
    context.page.set_viewport_size.assert_not_called()
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_authorization_lost_after_resize_withholds_success():
    context, check, adapter = await session()

    async def revoke_after_change(value):
        check.revoked = True

    context.page.set_viewport_size = revoke_after_change
    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        await adapter.resize("mobile")
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_resize_cancellation_closes_context_and_propagates():
    context, _, adapter = await session()
    entered = asyncio.Event()

    async def block(value):
        entered.set()
        await asyncio.Event().wait()

    context.page.set_viewport_size = block
    task = asyncio.create_task(adapter.resize("mobile"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_resize_timeout_and_unsafe_navigation_fail_closed():
    context, _, adapter = await session(limits=BrowserLimits(action_seconds=0.02))

    async def block(value):
        await asyncio.Event().wait()

    context.page.set_viewport_size = block
    with pytest.raises(BrowserError, match="browser_timeout"):
        await adapter.resize("mobile")
    assert context.closed.is_set()

    context, _, adapter = await session()

    async def leave_preview(value):
        context.page.url = "https://other.preview.example/"

    context.page.set_viewport_size = leave_preview
    with pytest.raises(BrowserError, match="browser_navigation_denied"):
        await adapter.resize("mobile")
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_resize_serialization_does_not_queue_concurrent_actions():
    context, _, adapter = await session()
    entered, release = asyncio.Event(), asyncio.Event()

    async def block(value):
        entered.set()
        await release.wait()

    context.page.set_viewport_size = block
    task = asyncio.create_task(adapter.resize("mobile"))
    try:
        await entered.wait()
        with pytest.raises(BrowserError, match="browser_busy"):
            await adapter.fill("#input", "value")
    finally:
        release.set()
        await task
        await adapter.close()


@pytest.mark.asyncio
async def test_worker_resize_only_delegates_preset():
    class Session:
        resize = AsyncMock()

    adapter = Session()
    assert await dispatch(adapter, {"op": "resize", "preset": "mobile"}) == {"done": True}
    adapter.resize.assert_awaited_once_with("mobile")
    for value in (
        {"op": "resize"},
        {"op": "resize", "preset": "mobile", "width": 10000},
        {"op": "resize", "preset": "mobile", "height": 10000},
        {"op": "resize", "preset": "mobile", "device": "arbitrary"},
    ):
        with pytest.raises(BrowserError, match="browser_invalid_input"):
            await dispatch(adapter, value)
    assert adapter.resize.await_count == 1
