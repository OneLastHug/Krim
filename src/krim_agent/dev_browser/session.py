"""Thin async Playwright adapter. Host callbacks own preview credentials and IO."""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any, Literal, TypeVar
from urllib.parse import urlsplit

from krim_agent.browser.protocol import validate_png

from .policy import (
    BrowserArtifact,
    BrowserError,
    BrowserLimits,
    bounded_text,
    canonical_origin,
    relative_target,
    same_origin,
    selector,
    upload_payload,
    viewport_preset,
)

Check = Callable[[], Awaitable[None]]
DownloadReader = Callable[[str, int], Awaitable[bytes]]
_SESSION_COOKIE = "__Host-Krim-Preview"
T = TypeVar("T")


def _valid_cookie_name(value: str) -> bool:
    allowed = "!#$%&'*+-.^_`|~"
    return bool(value) and all(
        char.isascii() and (char.isalnum() or char in allowed) for char in value
    )


def _valid_cookie_value(value: str) -> bool:
    return all(
        char.isascii()
        and (char == "!" or "#" <= char <= "+" or "-" <= char <= ":"
             or "<" <= char <= "[" or "]" <= char <= "~")
        for char in value
    )


class BrowserSession:
    """One isolated context, one page, one original viewer and immutable preview.

    Constructed only by trusted host composition; never expose this object or
    its context to model tools. No arbitrary JS, URLs, headers or host paths.
    The browser process must additionally have a deployment egress boundary;
    Playwright interception is not a replacement for network isolation.
    """

    def __init__(
        self,
        context: Any,
        origin: str,
        check: Check,
        *,
        limits: BrowserLimits | None = None,
        read_download: DownloadReader | None = None,
    ) -> None:
        limits = limits or BrowserLimits()
        self._context = context
        self._origin = canonical_origin(origin)
        self._check = check
        self._limits = limits
        self._read_download = read_download
        self._page: Any = None
        self._lock = asyncio.Lock()
        self._checking = 0
        self._started = False
        self._watcher: asyncio.Task[None] | None = None
        self._failure: str | None = None
        self._closed = False
        self._actions = 0
        self._requests = 0
        self._events: Counter[str] = Counter()
        self._event_count = 0
        self._deadline = time.monotonic() + limits.session_seconds

    def _record(self, kind: str) -> None:
        # Raw messages, URLs, headers and request bodies can contain secrets.
        # Report bounded categories/counts only, not remote-controlled strings.
        if self._event_count < self._limits.max_events:
            self._events[kind] += 1
            self._event_count += 1

    def diagnostics(self) -> dict[str, object]:
        return {
            "events": dict(self._events),
            "truncated": self._event_count >= self._limits.max_events,
            "requests": self._requests,
            "actions": self._actions,
        }

    def _live(self) -> None:
        if self._failure:
            raise BrowserError(self._failure)
        if self._closed:
            raise BrowserError("browser_closed")
        if time.monotonic() >= self._deadline:
            raise BrowserError("browser_expired")

    async def _authorize(self) -> None:
        self._live()
        if self._checking >= 8:
            raise BrowserError("browser_check_capacity")
        self._checking += 1
        try:
            async with asyncio.timeout(self._limits.check_seconds):
                await self._check()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise BrowserError("browser_authorization_lost") from None
        finally:
            self._checking -= 1
        self._live()

    async def _guard_request(self, route: Any) -> None:
        try:
            self._requests += 1
            request = route.request
            if (
                self._requests > self._limits.max_requests
                or not same_origin(request.url, self._origin)
                or self._page is None
                or request.frame != self._page.main_frame
                or request.resource_type in {"serviceworker", "sharedworker", "worker"}
            ):
                self._record("request_blocked")
                await route.abort("blockedbyclient")
                return
            await self._authorize()
            await route.continue_()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._failure = "browser_authorization_lost"
            with suppress(Exception):
                await route.abort("blockedbyclient")

    async def _guard_websocket(self, route: Any) -> None:
        try:
            self._requests += 1
            if self._requests > self._limits.max_requests or not same_origin(
                route.url,
                self._origin,
                websocket=True,
            ):
                self._record("websocket_blocked")
                await route.close(code=1008, reason="browser_target_denied")
                return
            await self._authorize()
            route.connect_to_server()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._failure = "browser_authorization_lost"
            with suppress(Exception):
                await route.close(code=1008, reason="browser_authorization_lost")

    async def start(self) -> None:
        if self._started:
            raise BrowserError("browser_already_started")
        self._started = True
        try:
            await self._authorize()
            # route_web_socket (Playwright >=1.48) is mandatory, never fallback.
            await self._context.route("**/*", self._guard_request)
            await self._context.route_web_socket("**/*", self._guard_websocket)
            self._page = await self._context.new_page()
            self._page.set_default_timeout(self._limits.action_seconds * 1000)
            self._page.on(
                "console",
                lambda msg: self._record(
                    "console_" + msg.type if msg.type in {"error", "warning"} else "console_other",
                ),
            )
            self._page.on("pageerror", lambda _: self._record("page_error"))
            self._page.on("requestfailed", lambda _: self._record("request_failed"))
            self._page.on("response", self._response)
            self._page.on("dialog", self._dismiss_dialog)
            self._page.on("download", self._cancel_download)
            self._context.on("page", self._unexpected_page)
            self._page.on("framenavigated", self._navigated)
            self._watcher = asyncio.create_task(self._watch())
        except asyncio.CancelledError:
            await self.close()
            raise
        except Exception:
            await self.close()
            raise BrowserError("browser_open_failed") from None

    def _response(self, response: Any) -> None:
        if 400 <= response.status <= 599:
            self._record("http_" + str(response.status))

    async def _dismiss_dialog(self, dialog: Any) -> None:
        self._record("dialog_dismissed")
        with suppress(Exception):
            await dialog.dismiss()

    async def _cancel_download(self, download: Any) -> None:
        self._record("browser_download_blocked")
        with suppress(Exception):
            await download.cancel()

    async def _unexpected_page(self, page: Any) -> None:
        if page != self._page:
            self._record("popup_blocked")
            with suppress(Exception):
                await page.close()

    def _navigated(self, frame: Any) -> None:
        if frame != self._page.main_frame or not same_origin(frame.url, self._origin):
            self._failure = "browser_navigation_denied"

    async def _watch(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._limits.poll_seconds)
                await self._authorize()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failure = (
                exc.code if isinstance(exc, BrowserError) else "browser_authorization_lost"
            )
            await self.close()

    async def close(self) -> None:
        self._closed = True
        watcher, self._watcher = self._watcher, None
        if watcher is not None and watcher != asyncio.current_task():
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
        with suppress(Exception):
            async with asyncio.timeout(self._limits.check_seconds):
                await self._context.close()

    async def _run(self, operation: Callable[[], Awaitable[T]]) -> T:
        # Reject concurrency instead of queuing potentially unbounded callers.
        if self._lock.locked():
            raise BrowserError("browser_busy")
        async with self._lock:
            try:
                self._live()
                self._actions += 1
                if self._actions > self._limits.max_actions:
                    raise BrowserError("browser_action_limit")
                async with asyncio.timeout(
                    min(
                        self._limits.action_seconds,
                        max(0.001, self._deadline - time.monotonic()),
                    )
                ):
                    await self._authorize()
                    result = await operation()
                    await self._authorize()
                    if not same_origin(self._page.url, self._origin):
                        raise BrowserError("browser_navigation_denied")
                    return result
            except asyncio.CancelledError:
                await self.close()
                raise
            except BrowserError:
                await self.close()
                raise
            except TimeoutError:
                await self.close()
                raise BrowserError("browser_timeout") from None
            except Exception:
                await self.close()
                raise BrowserError("browser_action_failed") from None

    async def goto(self, path: str = "/") -> None:
        target = self._origin + relative_target(path)

        async def action() -> None:
            await self._page.goto(target, wait_until="domcontentloaded")

        await self._run(action)

    async def resize(self, preset: Literal["desktop", "mobile"]) -> None:
        width, height = viewport_preset(preset)

        async def action() -> None:
            await self._page.set_viewport_size({"width": width, "height": height})

        await self._run(action)

    async def click(self, css: str) -> None:
        target = selector(css)

        async def action() -> None:
            await self._page.locator(target).click(no_wait_after=False)

        await self._run(action)

    async def fill(self, css: str, value: str) -> None:
        target, text = selector(css), bounded_text(value)

        async def action() -> None:
            await self._page.locator(target).fill(text)

        await self._run(action)

    async def wait(
        self, css: str, state: Literal["visible", "hidden", "attached", "detached"] = "visible"
    ) -> None:
        target = selector(css)
        if state not in {"visible", "hidden", "attached", "detached"}:
            raise BrowserError("browser_wait_invalid")

        async def action() -> None:
            await self._page.locator(target).wait_for(state=state)

        await self._run(action)

    async def assert_dom(
        self, css: str, *, text: str | None = None, count: int | None = None
    ) -> None:
        target = selector(css)
        if text is not None:
            bounded_text(text)
        if count is not None and (
            isinstance(count, bool) or not isinstance(count, int) or not 0 <= count <= 1000
        ):
            raise BrowserError("browser_assertion_invalid")
        if text is None and count is None:
            raise BrowserError("browser_assertion_invalid")

        async def action() -> None:
            locator = self._page.locator(target)
            actual_count = await locator.count()
            if count is not None and actual_count != count:
                raise BrowserError("browser_assertion_failed")
            if text is not None:
                if actual_count != 1 or not await locator.is_visible():
                    raise BrowserError("browser_assertion_failed")
                # Compare in the browser; don't transfer unbounded DOM text.
                actual = await locator.evaluate(
                    "(element, expected) => element.textContent === expected", text
                )
                if actual is not True:
                    raise BrowserError("browser_assertion_failed")

        await self._run(action)

    async def screenshot(self) -> BrowserArtifact:
        async def action() -> BrowserArtifact:
            data = await self._page.screenshot(
                type="png",
                full_page=False,
                timeout=self._limits.action_seconds * 1000,
                animations="disabled",
                mask=[self._page.locator('input[type="password"]')],
            )
            if len(data) > self._limits.max_screenshot_bytes:
                raise BrowserError("browser_artifact_limit")
            validate_png(data)
            return BrowserArtifact("screenshot", "image/png", data)

        return await self._run(action)

    async def upload(self, css: str, *, name: str, media_type: str, data: bytes) -> None:
        target = selector(css)
        payload = upload_payload(name, media_type, data, self._limits.max_upload_bytes)

        async def action() -> None:
            await self._page.locator(target).set_input_files(payload)

        await self._run(action)

    async def click_download(self, css: str) -> BrowserArtifact:
        target = selector(css)

        async def action() -> BrowserArtifact:
            if self._read_download is None:
                raise BrowserError("browser_download_unavailable")
            async with self._page.expect_download(
                timeout=self._limits.action_seconds * 1000
            ) as pending:
                await self._page.locator(target).click()
            download = await pending.value
            await download.cancel()
            if not same_origin(download.url, self._origin):
                raise BrowserError("browser_download_denied")
            parsed = urlsplit(download.url)
            path = (parsed.path or "/") + ("?" + parsed.query if parsed.query else "")
            data = await self._read_download(path, self._limits.max_download_bytes)
            if not isinstance(data, bytes) or len(data) > self._limits.max_download_bytes:
                raise BrowserError("browser_artifact_limit")
            return BrowserArtifact("download", "application/octet-stream", data)

        return await self._run(action)

    async def download_context(
        self, path: str, cookie_mode: Literal["context", "preview_only"]
    ) -> dict[str, str]:
        target = relative_target(path)
        if not isinstance(cookie_mode, str) or cookie_mode not in {"context", "preview_only"}:
            raise BrowserError("browser_cookie_invalid")

        async def action() -> dict[str, str]:
            pairs: list[str] = []
            header_bytes = 0
            if cookie_mode == "context":
                cookies = await self._context.cookies(self._origin + target)
                if not isinstance(cookies, list) or len(cookies) > 64:
                    raise BrowserError("browser_cookie_invalid")
                names: set[str] = set()
                for cookie in cookies:
                    if not isinstance(cookie, dict):
                        raise BrowserError("browser_cookie_invalid")
                    name, value = cookie.get("name"), cookie.get("value")
                    if (
                        not isinstance(name, str)
                        or not name.isascii()
                        or not _valid_cookie_name(name)
                    ):
                        raise BrowserError("browser_cookie_invalid")
                    if name in names:
                        raise BrowserError("browser_cookie_invalid")
                    names.add(name)
                    if (
                        not isinstance(value, str)
                        or not value.isascii()
                        or not _valid_cookie_value(value)
                    ):
                        raise BrowserError("browser_cookie_invalid")
                    if name == _SESSION_COOKIE:
                        continue
                    pair_bytes = len(name) + 1 + len(value)
                    if pairs:
                        pair_bytes += 2
                    header_bytes += pair_bytes
                    if header_bytes > 8192:
                        raise BrowserError("browser_cookie_invalid")
                    pairs.append(f"{name}={value}")
            cookie_header = "; ".join(pairs)
            return {
                "path": target,
                "cookie_mode": cookie_mode,
                "cookie_header": cookie_header,
            }

        return await self._run(action)

    async def check_download(self, path: str) -> BrowserArtifact:
        target = relative_target(path)

        async def action() -> BrowserArtifact:
            if self._read_download is None:
                raise BrowserError("browser_download_unavailable")
            data = await self._read_download(target, self._limits.max_download_bytes)
            if not isinstance(data, bytes) or len(data) > self._limits.max_download_bytes:
                raise BrowserError("browser_artifact_limit")
            return BrowserArtifact("download", "application/octet-stream", data)

        return await self._run(action)
