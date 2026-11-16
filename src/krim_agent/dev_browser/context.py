"""Allocate fresh contexts on an externally isolated Playwright browser.

This module neither installs nor launches a browser. The host supplies a
Playwright Browser connected to an approved, bounded worker deployment.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from typing import Any

from .policy import BrowserError, BrowserLimits, canonical_origin
from .session import BrowserSession, Check, DownloadReader

PrepareContext = Callable[[Any], Awaitable[None]]


@asynccontextmanager
async def open_context(
    browser: Any,
    origin: str,
    check: Check,
    prepare_context: PrepareContext,
    *,
    limits: BrowserLimits | None = None,
    read_download: DownloadReader | None = None,
) -> AsyncIterator[BrowserSession]:
    """Create no shared profile, persistent storage, recording or download dir.

    prepare_context is host-only and may only exchange the existing preview
    ticket for a cookie. It must not navigate or open pages. The original viewer
    is retained by check/prepare_context closures, never accepted per action.
    """
    limits = limits or BrowserLimits()
    canonical_origin(origin)
    context = None
    adapter = None
    try:
        async with asyncio.timeout(limits.check_seconds):
            await check()
        async with asyncio.timeout(limits.action_seconds):
            context = await browser.new_context(
                accept_downloads=False,
                service_workers="block",
                viewport={"width": 1024, "height": 768},
                device_scale_factor=1,
                java_script_enabled=True,
                ignore_https_errors=False,
                permissions=[],
                offline=False,
            )
            await prepare_context(context)
            if context.pages:
                raise BrowserError("browser_context_not_fresh")
            await check()
            adapter = BrowserSession(
                context, origin, check, limits=limits, read_download=read_download
            )
            await adapter.start()
    except asyncio.CancelledError:
        if adapter is not None:
            await adapter.close()
        elif context is not None:
            with suppress(Exception):
                async with asyncio.timeout(limits.check_seconds):
                    await context.close()
        raise
    except Exception:
        if adapter is not None:
            await adapter.close()
        elif context is not None:
            with suppress(Exception):
                async with asyncio.timeout(limits.check_seconds):
                    await context.close()
        raise BrowserError("browser_open_failed") from None
    try:
        yield adapter
    finally:
        await adapter.close()
