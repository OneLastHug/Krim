"""Adapter contract tests; fake page, not real browser or application acceptance."""

from __future__ import annotations

import asyncio
import struct
import zlib
from types import SimpleNamespace

import pytest

from krim_agent.dev_browser import BrowserError, BrowserLimits, BrowserSession
from krim_agent.dev_browser.policy import (
    canonical_origin,
    relative_target,
    same_origin,
    selector,
    upload_payload,
)

ORIGIN = "https://p-fixed.preview.example"


def png():
    def chunk(kind, body):
        return (
            struct.pack(">I", len(body)) + kind + body + struct.pack(">I", zlib.crc32(kind + body))
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"\x00\xff\xff\xff"))
        + chunk(b"IEND", b"")
    )


class Locator:
    def __init__(self, page):
        self.page = page

    async def click(self, **kwargs):
        if self.page.block is not None:
            self.page.entered.set()
            await self.page.block.wait()
        if self.page.error is not None:
            raise self.page.error
        self.page.calls.append(("click", kwargs))

    async def fill(self, value):
        self.page.calls.append(("fill", value))

    async def wait_for(self, **kwargs):
        self.page.calls.append(("wait", kwargs))

    async def count(self):
        return self.page.count

    async def is_visible(self):
        return True

    async def evaluate(self, expression, value):
        assert expression == "(element, expected) => element.textContent === expected"
        return self.page.text == value

    async def set_input_files(self, payload):
        self.page.calls.append(("upload", payload))


class Page:
    def __init__(self):
        self.url = ORIGIN + "/"
        self.main_frame = SimpleNamespace(url=self.url)
        self.calls = []
        self.events = {}
        self.count = 1
        self.text = "done"
        self.block = None
        self.error = None
        self.entered = asyncio.Event()
        self.image = png()

    def on(self, event, handler):
        self.events[event] = handler

    def set_default_timeout(self, value):
        self.timeout = value

    async def goto(self, url, **kwargs):
        self.url = url
        self.main_frame.url = url
        self.calls.append(("goto", url))

    def locator(self, css):
        self.calls.append(("locator", css))
        return Locator(self)

    async def screenshot(self, **kwargs):
        self.calls.append(("screenshot", kwargs))
        return self.image


class Context:
    def __init__(self):
        self.page = Page()
        self.closed = asyncio.Event()
        self.events = {}

    async def route(self, pattern, handler):
        self.handler = handler

    async def route_web_socket(self, pattern, handler):
        self.ws_handler = handler

    async def new_page(self):
        return self.page

    def on(self, event, handler):
        self.events[event] = handler

    async def close(self):
        self.closed.set()


class Route:
    def __init__(self, context, url=ORIGIN + "/", *, frame=None, kind="document"):
        self.request = SimpleNamespace(
            url=url, frame=frame or context.page.main_frame, resource_type=kind
        )
        self.url = url
        self.continued = self.aborted = self.connected = False

    async def abort(self, reason):
        self.aborted = True

    async def continue_(self):
        self.continued = True

    async def close(self, **kwargs):
        self.aborted = True

    def connect_to_server(self):
        self.connected = True


class Check:
    def __init__(self):
        self.calls = 0
        self.revoked = False

    async def __call__(self):
        self.calls += 1
        if self.revoked:
            raise RuntimeError("secret viewer credential")


async def session(**kwargs):
    context, check = Context(), Check()
    adapter = BrowserSession(context, ORIGIN, check, **kwargs)
    await adapter.start()
    return context, check, adapter


@pytest.mark.parametrize(
    "value",
    [
        "https://evil.test/",
        "//evil.test/",
        "/%2fother.test",
        "/%252fother.test",
        "/a/../.krim-preview/session",
        "/.krim-preview/session",
        "/%2ekrim-preview/session",
        "/a\\b",
        "/%5cb",
        "/%0aX",
        "/#fragment",
        "",
    ],
)
def test_target_rejects_url_escape_reserved_paths(value):
    with pytest.raises(BrowserError):
        relative_target(value)


def test_policy_accepts_relative_paths_and_exact_origin_only():
    assert relative_target("/a?x=1") == "/a?x=1"
    assert same_origin(ORIGIN + "/a?x=1", ORIGIN)
    assert not same_origin("https://p-other.preview.example/a", ORIGIN)
    assert not same_origin(ORIGIN + ":443/a", ORIGIN)
    assert not same_origin("https://u:p@p-fixed.preview.example/a", ORIGIN)
    assert not same_origin("file:///etc/passwd", ORIGIN)
    assert not same_origin("data:text/html,x", ORIGIN)
    assert not same_origin("blob:" + ORIGIN + "/id", ORIGIN)
    assert same_origin(ORIGIN.replace("https:", "wss:") + "/hmr", ORIGIN, websocket=True)
    assert selector("#button") == "css=#button"
    with pytest.raises(BrowserError):
        selector("#a >> internal:control=enter-frame")
    with pytest.raises(BrowserError):
        canonical_origin("http://localhost")


@pytest.mark.parametrize(
    "options",
    [
        {"max_actions": 0},
        {"max_events": 1000},
        {"max_upload_bytes": True},
        {"poll_seconds": float("nan")},
        {"session_seconds": float("inf")},
        {"max_requests": 1.5},
    ],
)
def test_limits_reject_invalid_values(options):
    with pytest.raises(ValueError):
        BrowserLimits(**options)


@pytest.mark.parametrize(
    "name,mime,data",
    [
        ("../../secret", "text/plain", b"x"),
        ("C:\\secret", "text/plain", b"x"),
        ("ok", "text/plain\r\nX: secret", b"x"),
        ("ok", "text/plain", b"12345"),
    ],
)
def test_upload_rejects_paths_header_injection_and_excess_bytes(name, mime, data):
    with pytest.raises(BrowserError):
        upload_payload(name, mime, data, 4)


@pytest.mark.asyncio
async def test_actions_artifacts_and_safe_diagnostics():
    async def read(path, maximum):
        assert path == "/download" and maximum == 1048576
        return b"fixture bytes"

    context, check, adapter = await session(read_download=read)
    try:
        await adapter.goto("/form")
        await adapter.click("#submit")
        await adapter.fill("#name", "example")
        await adapter.wait("#done")
        await adapter.assert_dom("#done", text="done", count=1)
        await adapter.upload("#file", name="fixture.txt", media_type="text/plain", data=b"fixture")
        image = await adapter.screenshot()
        artifact = await adapter.check_download("/download")
        assert image.metadata["media_type"] == "image/png"
        assert image.metadata["size"] == len(png())
        assert len(image.metadata["sha256"]) == 64
        assert artifact.data == b"fixture bytes"
        assert "fixture bytes" not in repr(artifact)
        context.page.events["console"](SimpleNamespace(type="error", text="secret token"))
        context.page.events["pageerror"](RuntimeError("secret password"))
        context.page.events["response"](SimpleNamespace(status=503, url="https://secret"))
        report = adapter.diagnostics()
        assert report["events"] == {"console_error": 1, "page_error": 1, "http_503": 1}
        assert "secret" not in repr(report)
        assert check.calls >= 17
    finally:
        await adapter.close()
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_requests_and_websockets_are_fixed_to_same_preview_main_frame():
    context, _, adapter = await session()
    try:
        for url in ["https://evil.test/", "http://127.0.0.1/", ORIGIN + "/.krim-preview/session"]:
            route = Route(context, url)
            await context.handler(route)
            assert route.aborted and not route.continued
        for route in [Route(context, frame=object()), Route(context, kind="worker")]:
            await context.handler(route)
            assert route.aborted
        route = Route(context)
        await context.handler(route)
        assert route.continued
        foreign_ws = Route(context, "wss://other.preview.example/")
        await context.ws_handler(foreign_ws)
        assert foreign_ws.aborted and not foreign_ws.connected
        same_ws = Route(context, ORIGIN.replace("https:", "wss:") + "/hmr")
        await context.ws_handler(same_ws)
        assert same_ws.connected
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_idle_revocation_closes_context():
    context, check, adapter = await session(limits=BrowserLimits(poll_seconds=0.01))
    check.revoked = True
    await asyncio.wait_for(context.closed.wait(), 1)
    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        await adapter.click("#submit")


@pytest.mark.asyncio
async def test_cancellation_closes_context_and_propagates():
    context, _, adapter = await session()
    context.page.block = asyncio.Event()
    task = asyncio.create_task(adapter.click("#submit"))
    await context.page.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_action_timeout_and_no_exception_content_leak():
    context, _, adapter = await session(limits=BrowserLimits(action_seconds=0.02))
    context.page.block = asyncio.Event()
    with pytest.raises(BrowserError, match="^browser_timeout$"):
        await adapter.click("#submit")
    assert context.closed.is_set()
    context, _, adapter = await session()
    context.page.error = RuntimeError("token=secret http://private/secret")
    with pytest.raises(BrowserError, match="^browser_action_failed$"):
        await adapter.click("#submit")
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_concurrent_actions_rejected_without_queueing():
    context, _, adapter = await session()
    context.page.block = asyncio.Event()
    task = asyncio.create_task(adapter.click("#submit"))
    await context.page.entered.wait()
    with pytest.raises(BrowserError, match="browser_busy"):
        await adapter.fill("#field", "value")
    context.page.block.set()
    await task
    await adapter.close()


@pytest.mark.asyncio
async def test_bad_png_and_download_limit_close_context():
    context, _, adapter = await session()
    context.page.image = b"not a png"
    with pytest.raises(BrowserError):
        await adapter.screenshot()
    assert context.closed.is_set()

    async def too_large(path, maximum):
        return b"12345"

    context, _, adapter = await session(
        limits=BrowserLimits(max_download_bytes=4), read_download=too_large
    )
    with pytest.raises(BrowserError, match="browser_artifact_limit"):
        await adapter.check_download("/file")
    assert context.closed.is_set()


@pytest.mark.asyncio
async def test_expiry_actions_diagnostics_and_request_budgets():
    context, _, adapter = await session(
        limits=BrowserLimits(max_actions=1, max_requests=1, max_events=1)
    )
    await adapter.click("#one")
    first, second = Route(context), Route(context)
    await context.handler(first)
    await context.handler(second)
    assert first.continued and second.aborted
    adapter._record("raw_never_used")
    assert len(adapter.diagnostics()["events"]) == 1
    with pytest.raises(BrowserError, match="browser_action_limit"):
        await adapter.click("#two")
    assert context.closed.is_set()
    context, _, adapter = await session(
        limits=BrowserLimits(session_seconds=0.1, poll_seconds=0.01)
    )
    await asyncio.wait_for(context.closed.wait(), 1)
    with pytest.raises(BrowserError, match="browser_expired"):
        await adapter.click("#one")


@pytest.mark.asyncio
async def test_assertion_failure_and_navigation_escape():
    context, _, adapter = await session()
    with pytest.raises(BrowserError, match="browser_assertion_failed"):
        await adapter.assert_dom("#message", text="wrong")
    assert context.closed.is_set()
    context, _, adapter = await session()
    context.page.main_frame.url = "file:///etc/passwd"
    context.page.events["framenavigated"](context.page.main_frame)
    with pytest.raises(BrowserError, match="browser_navigation_denied"):
        await adapter.click("#one")
    assert context.closed.is_set()
