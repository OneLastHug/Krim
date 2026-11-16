"""Bind the local adapter to E's existing preview authorization and B lease."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from krim_agent.dev_preview.boundary import PreviewOrigins
from krim_agent.dev_preview.repository import PreviewRecord
from krim_agent.dev_preview.service import PreviewService
from krim_agent.development.contracts import PreviewHandle
from krim_agent.security import PrincipalContext

from .context import open_context
from .policy import BrowserError, BrowserLimits
from .session import BrowserSession, DownloadReader

PreparePreview = Callable[[Any, PreviewRecord, PrincipalContext], Awaitable[None]]


@asynccontextmanager
async def open_preview(
    browser: Any,
    service: PreviewService,
    origins: PreviewOrigins,
    handle: PreviewHandle,
    principal: PrincipalContext,
    *,
    project_id: str,
    prepare_context: PreparePreview,
    read_download: DownloadReader | None = None,
    limits: BrowserLimits | None = None,
) -> AsyncIterator[BrowserSession]:
    """Host-only entry: project_id comes from the authenticated run, not the tool.

    Use this context manager in one task so B lease entry/exit stay paired.
    The yielded session has no method to change project, preview or viewer.
    I owns bootstrap, tool admission, artifact persistence and worker lifecycle.
    """
    if principal is None or handle.environment.project_id != project_id:
        raise BrowserError("browser_preview_denied")
    limits = limits or BrowserLimits()
    route = service.route(handle.preview_id, principal)
    try:
        async with asyncio.timeout(limits.check_seconds):
            record, lease = await route.__aenter__()
    except asyncio.CancelledError:
        raise
    except Exception:
        raise BrowserError("browser_preview_denied") from None
    try:
        if (
            record.handle.preview_id != handle.preview_id
            or record.handle.environment != handle.environment
            or record.handle.service_id != handle.service_id
            or record.handle.port != handle.port
            or record.handle.environment.project_id != project_id
        ):
            raise BrowserError("browser_preview_denied")

        async def check() -> None:
            await service.recheck(record, principal, lease)

        async def prepare(context: Any) -> None:
            await prepare_context(context, record, principal)

        async with open_context(
            browser,
            origins.origin(handle.preview_id),
            check,
            prepare,
            limits=limits,
            read_download=read_download,
        ) as session:
            yield session
    finally:
        # No child task may enter/exit B's route context. Its own lease TTL and
        # relay reclamation remain necessary if control-plane cleanup fails.
        try:
            async with asyncio.timeout(limits.check_seconds):
                await route.__aexit__(None, None, None)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise BrowserError("browser_preview_cleanup_failed") from None
