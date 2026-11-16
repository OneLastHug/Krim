"""Private download-context descriptors; no fetch or response body handling."""

import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from test_dev_browser_session import ORIGIN, session

from krim_agent.dev_browser import BrowserError
from krim_agent.dev_browser.session import _SESSION_COOKIE
from krim_agent.dev_preview.boundary import SESSION_COOKIE

DEPLOY = Path(__file__).parents[2] / "deployment/development-browser"
sys.path.insert(0, str(DEPLOY))
from worker import dispatch  # noqa: E402


def test_worker_infrastructure_cookie_name_matches_preview_boundary():
    assert _SESSION_COOKIE == SESSION_COOKIE


@pytest.mark.asyncio
async def test_context_mode_returns_only_url_scoped_application_cookies():
    context, _, adapter = await session()
    context.cookies = AsyncMock(return_value=[
        {"name": "app_session", "value": "abc=123"},
        {"name": SESSION_COOKIE, "value": "infrastructure-secret"},
    ])
    try:
        result = await adapter.download_context("/files/report.csv?version=2", "context")
        assert result == {
            "path": "/files/report.csv?version=2",
            "cookie_mode": "context",
            "cookie_header": "app_session=abc=123",
        }
        context.cookies.assert_awaited_once_with(ORIGIN + "/files/report.csv?version=2")
        assert "infrastructure-secret" not in repr(result)
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_preview_only_mode_never_reads_context_cookies():
    context, _, adapter = await session()
    context.cookies = AsyncMock(side_effect=AssertionError("cookies must not be read"))
    try:
        assert await adapter.download_context("/file", "preview_only") == {
            "path": "/file", "cookie_mode": "preview_only", "cookie_header": "",
        }
        context.cookies.assert_not_awaited()
    finally:
        await adapter.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cookies",
    [
        [{"name": "bad;name", "value": "x"}],
        [{"name": "app", "value": "x\r\nInjected: yes"}],
        [{"name": "app", "value": "x"}, {"name": "app", "value": "y"}],
        [{"name": "app", "value": "x"}] * 65,
        [{"name": "app", "value": "x" * 8192}],
        [{"name": "nonascii", "value": "caf\N{LATIN SMALL LETTER E WITH ACUTE}"}],
        [{"name": SESSION_COOKIE, "value": "bad\nvalue"}],
    ],
)
async def test_invalid_or_oversized_cookie_header_fails_closed(cookies):
    context, _, adapter = await session()
    context.cookies = AsyncMock(return_value=cookies)
    with pytest.raises(BrowserError, match="browser_cookie_invalid"):
        await adapter.download_context("/file", "context")
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_invalid_path_and_cookie_mode_are_rejected():
    context, _, adapter = await session()
    context.cookies = AsyncMock(return_value=[])
    with pytest.raises(BrowserError, match="browser_target_invalid"):
        await adapter.download_context("//other.example/file", "context")
    with pytest.raises(BrowserError, match="browser_cookie_invalid"):
        await adapter.download_context("/file", "all_cookies")
    context.cookies.assert_not_awaited()
    await adapter.close()


@pytest.mark.asyncio
async def test_authorization_failure_prevents_cookie_lookup():
    context, check, adapter = await session()
    context.cookies = AsyncMock(return_value=[{"name": "app", "value": "secret"}])
    check.revoked = True
    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        await adapter.download_context("/file", "context")
    context.cookies.assert_not_awaited()
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_authorization_revoked_during_cookie_lookup_withholds_descriptor():
    context, check, adapter = await session()

    async def cookies(url):
        check.revoked = True
        return [{"name": "app", "value": "secret"}]

    context.cookies = AsyncMock(side_effect=cookies)
    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        await adapter.download_context("/file", "context")
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_worker_dispatch_accepts_only_private_descriptor_fields():
    class Session:
        download_context = AsyncMock(return_value={
            "path": "/file", "cookie_mode": "preview_only", "cookie_header": "",
        })

    adapter = Session()
    command = {"op": "download_context", "path": "/file", "cookie_mode": "preview_only"}
    assert await dispatch(adapter, command) == {
        "path": "/file", "cookie_mode": "preview_only", "cookie_header": "",
    }
    adapter.download_context.assert_awaited_once_with("/file", "preview_only")
    for invalid in (
        {**command, "url": "https://example.com/file"},
        {**command, "data": "body"},
        {"op": "download_context", "path": "/file"},
    ):
        with pytest.raises(BrowserError, match="browser_invalid_input"):
            await dispatch(adapter, invalid)
    assert adapter.download_context.await_count == 1
