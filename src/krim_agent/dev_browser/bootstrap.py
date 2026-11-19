"""Exchange E's single-use ticket in a fresh context; no credentials are returned."""

from __future__ import annotations

import asyncio
import re
from contextlib import suppress
from typing import Any
from urllib.parse import urlsplit

from krim_agent.dev_preview.app import PreviewApp
from krim_agent.dev_preview.boundary import BOOTSTRAP_PATH, SESSION_COOKIE
from krim_agent.dev_preview.repository import PreviewRecord
from krim_agent.security import PrincipalContext

from .policy import BrowserError, BrowserLimits, canonical_origin


class PreviewBootstrap:
    """Host-only callback for open_preview(prepare_context=PreviewBootstrap(app)).

    app must be the same trusted PreviewApp used by the dedicated TLS listener.
    The Playwright driver/APIRequestContext, not just Chromium, must share the
    worker's fixed-origin egress boundary. Never enable tracing or request logs.
    """

    def __init__(self, app: PreviewApp, *, limits: BrowserLimits | None = None) -> None:
        self._app = app
        self._limits = limits or BrowserLimits()

    async def __call__(
        self,
        context: Any,
        record: PreviewRecord,
        principal: PrincipalContext,
    ) -> None:
        response = None
        try:
            async with asyncio.timeout(self._limits.action_seconds):
                if principal is None or context.pages or await context.cookies():
                    raise BrowserError("browser_bootstrap_failed")
                preview_id = record.handle.preview_id
                origin = canonical_origin(self._app.origins.origin(preview_id))
                action = origin + BOOTSTRAP_PATH
                await self._app.service.check(
                    preview_id,
                    principal,
                    action="open",
                    expected=record,
                )
                bootstrap = await self._app.issue_bootstrap(preview_id, principal)
                if (
                    bootstrap.action != action
                    or not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", bootstrap.ticket)
                    or not 0 < bootstrap.expires_at - self._app.clock() <= 30
                ):
                    raise BrowserError("browser_bootstrap_failed")
                response = await context.request.post(
                    action,
                    form={"ticket": bootstrap.ticket},
                    headers={"Origin": self._app.origins.control_origin},
                    max_redirects=0,
                    max_retries=0,
                    ignore_https_errors=False,
                    fail_on_status_code=False,
                    timeout=self._limits.action_seconds * 1000,
                )
                if (
                    response.url != action
                    or response.status != 303
                    or response.headers.get("location") != "/"
                ):
                    raise BrowserError("browser_bootstrap_failed")
                cookies = await context.cookies()
                if len(cookies) != 1:
                    raise BrowserError("browser_bootstrap_failed")
                cookie = cookies[0]
                if (
                    cookie.get("name") != SESSION_COOKIE
                    or cookie.get("domain") != urlsplit(origin).hostname
                    or cookie.get("path") != "/"
                    or cookie.get("secure") is not True
                    or cookie.get("httpOnly") is not True
                    or cookie.get("sameSite") != "Lax"
                    or not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", cookie.get("value", ""))
                    or not 0
                    < cookie.get("expires", 0) - self._app.clock()
                    <= self._app.session_seconds + 1
                ):
                    raise BrowserError("browser_bootstrap_failed")
                await self._app.service.check(
                    preview_id,
                    principal,
                    action="open",
                    expected=record,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise BrowserError("browser_bootstrap_failed") from None
        finally:
            if response is not None:
                with suppress(Exception):
                    async with asyncio.timeout(self._limits.check_seconds):
                        await response.dispose()
