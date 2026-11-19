"""Context factory tests with the same bounded fake Playwright surface."""

import asyncio
from types import SimpleNamespace

import pytest
from test_dev_browser_session import ORIGIN, Check, Context

from krim_agent.dev_browser import BrowserError, BrowserLimits
from krim_agent.dev_browser.context import open_context


class Browser:
    def __init__(self):
        self.context = Context()
        self.context.pages = []
        self.options = None

    async def new_context(self, **kwargs):
        self.options = kwargs
        return self.context


@pytest.mark.asyncio
async def test_fresh_context_never_uses_persistent_profiles_downloads_or_recording():
    browser, check = Browser(), Check()

    async def prepare(context):
        assert context == browser.context

    async with open_context(browser, ORIGIN, check, prepare) as adapter:
        await adapter.goto("/")
        assert browser.options == {
            "accept_downloads": False,
            "service_workers": "block",
            "viewport": {"width": 1024, "height": 768},
            "device_scale_factor": 1,
            "java_script_enabled": True,
            "ignore_https_errors": False,
            "permissions": [],
            "offline": False,
        }
    assert browser.context.closed.is_set()


@pytest.mark.asyncio
async def test_prepare_failure_does_not_leak_exception_or_leave_context():
    browser = Browser()

    async def prepare(context):
        raise RuntimeError("cookie=secret ticket=secret")

    with pytest.raises(BrowserError, match="^browser_open_failed$"):
        async with open_context(browser, ORIGIN, Check(), prepare):
            pytest.fail("must not yield")
    assert browser.context.closed.is_set()


@pytest.mark.asyncio
async def test_prepare_must_not_create_a_page():
    browser = Browser()

    async def prepare(context):
        context.pages = [SimpleNamespace(url="https://foreign.test")]

    with pytest.raises(BrowserError, match="browser_open_failed"):
        async with open_context(browser, ORIGIN, Check(), prepare):
            pytest.fail("must not yield")
    assert browser.context.closed.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_prepare_timeout_and_cancellation_close_context(cancel):
    browser = Browser()
    entered = asyncio.Event()

    async def prepare(context):
        entered.set()
        await asyncio.Event().wait()

    async def run():
        async with open_context(
            browser,
            ORIGIN,
            Check(),
            prepare,
            limits=BrowserLimits(action_seconds=0.02),
        ):
            pytest.fail("must not yield")

    task = asyncio.create_task(run())
    await entered.wait()
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else BrowserError):
        await task
    assert browser.context.closed.is_set()


@pytest.mark.asyncio
async def test_denied_check_does_not_create_context_or_prepare():
    browser, check = Browser(), Check()
    check.revoked = True

    async def prepare(context):
        pytest.fail("must not bootstrap denied viewer")

    with pytest.raises(BrowserError, match="browser_open_failed"):
        async with open_context(browser, ORIGIN, check, prepare):
            pytest.fail("must not yield")
    assert browser.options is None
