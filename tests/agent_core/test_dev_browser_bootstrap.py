"""Ticket bootstrap contract plus real E ASGI exchange, without Chromium."""

import asyncio
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx
import pytest
from test_dev_preview_protocol import rig as rig

from krim_agent.dev_browser import BrowserError, BrowserLimits
from krim_agent.dev_browser.bootstrap import PreviewBootstrap
from krim_agent.dev_preview.boundary import BOOTSTRAP_PATH, SESSION_COOKIE

ORIGIN = "https://p-fixed.preview.example"
PRINCIPAL = object()
RECORD = SimpleNamespace(handle=SimpleNamespace(preview_id="preview"))


class Response:
    def __init__(self):
        self.url = ORIGIN + BOOTSTRAP_PATH
        self.status = 303
        self.headers = {"location": "/"}
        self.disposed = False

    async def dispose(self):
        self.disposed = True


class Context:
    def __init__(self):
        self.pages = []
        self.cookie_jar = []
        self.response = Response()
        self.request = self
        self.post_args = None
        self.block = False
        self.entered = asyncio.Event()
        self.new_cookie = {
            "name": SESSION_COOKIE,
            "value": "s" * 43,
            "domain": urlsplit(ORIGIN).hostname,
            "path": "/",
            "secure": True,
            "httpOnly": True,
            "sameSite": "Lax",
            "expires": 3700,
        }

    async def cookies(self):
        return self.cookie_jar

    async def post(self, action, **kwargs):
        self.post_args = action, kwargs
        self.entered.set()
        if self.block:
            await asyncio.Event().wait()
        self.cookie_jar = [self.new_cookie]
        return self.response


class App:
    def __init__(self):
        self.origins = SimpleNamespace(
            origin=lambda _: ORIGIN,
            control_origin="https://control.example.org",
        )
        self.service = self
        self.session_seconds = 3600
        self.checks = []
        self.bootstrap = SimpleNamespace(
            action=ORIGIN + BOOTSTRAP_PATH,
            ticket="t" * 43,
            expires_at=130,
        )

    def clock(self):
        return 100

    async def check(self, preview_id, principal, **kwargs):
        self.checks.append((preview_id, principal, kwargs))

    async def issue_bootstrap(self, preview_id, principal):
        assert preview_id == "preview" and principal is PRINCIPAL
        return self.bootstrap


@pytest.mark.asyncio
async def test_bootstrap_only_posts_ticket_and_checks_cookie_without_redirecting():
    app, context = App(), Context()
    result = await PreviewBootstrap(app)(context, RECORD, PRINCIPAL)
    assert result is None
    assert context.post_args == (
        ORIGIN + BOOTSTRAP_PATH,
        {
            "form": {"ticket": "t" * 43},
            "headers": {"Origin": app.origins.control_origin},
            "max_redirects": 0,
            "max_retries": 0,
            "ignore_https_errors": False,
            "fail_on_status_code": False,
            "timeout": 10000,
        },
    )
    assert context.response.disposed
    assert len(app.checks) == 2
    assert all(call[1] is PRINCIPAL and call[2]["expected"] is RECORD for call in app.checks)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("domain", ".preview.example"),
        ("domain", "p-other.preview.example"),
        ("secure", False),
        ("httpOnly", False),
        ("path", "/nested"),
        ("sameSite", "None"),
        ("expires", -1),
        ("expires", 100000),
        ("name", "application-cookie"),
        ("value", "malformed\nvalue"),
    ],
)
async def test_malformed_or_cross_preview_cookie_is_rejected_and_response_disposed(field, value):
    app, context = App(), Context()
    context.new_cookie[field] = value
    with pytest.raises(BrowserError, match="^browser_bootstrap_failed$"):
        await PreviewBootstrap(app)(context, RECORD, PRINCIPAL)
    assert context.response.disposed


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["anonymous", "page", "cookies", "action", "expired"])
async def test_untrusted_bootstrap_or_nonfresh_context_is_rejected_before_post(mutation):
    app, context, principal = App(), Context(), PRINCIPAL
    if mutation == "anonymous":
        principal = None
    elif mutation == "page":
        context.pages = [object()]
    elif mutation == "cookies":
        context.cookie_jar = [context.new_cookie]
    elif mutation == "action":
        app.bootstrap.action = "https://foreign.example/session"
    else:
        app.bootstrap.expires_at = 99
    with pytest.raises(BrowserError, match="^browser_bootstrap_failed$"):
        await PreviewBootstrap(app)(context, RECORD, principal)
    assert context.post_args is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("url", "https://foreign.example/"),
        ("status", 200),
        ("headers", {"location": "https://foreign.example/"}),
    ],
)
async def test_response_must_be_exact_unfollowed_303(field, value):
    app, context = App(), Context()
    setattr(context.response, field, value)
    with pytest.raises(BrowserError, match="^browser_bootstrap_failed$"):
        await PreviewBootstrap(app)(context, RECORD, PRINCIPAL)
    assert context.response.disposed


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_ticket_post_timeout_and_cancel_propagation(cancel):
    app, context = App(), Context()
    context.block = True
    task = asyncio.create_task(
        PreviewBootstrap(
            app,
            limits=BrowserLimits(action_seconds=0.02),
        )(context, RECORD, PRINCIPAL)
    )
    await context.entered.wait()
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else BrowserError):
        await task


@pytest.mark.asyncio
async def test_bootstrap_exchanges_real_e_asgi_ticket_without_browser_or_listener(rig):
    # No actual TLS/browser process is exercised; this is the real E app in ASGI.
    rig.clock[0] = time.time()
    record = await rig.service.repository.get("preview")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=rig.app),
        follow_redirects=False,
        base_url=rig.origins.origin("preview"),
    ) as client:

        class Request:
            async def post(self, action, **kwargs):
                assert kwargs["max_redirects"] == 0 and kwargs["max_retries"] == 0
                response = await client.post(
                    action,
                    data=kwargs["form"],
                    headers=kwargs["headers"],
                    follow_redirects=False,
                )
                return SimpleNamespace(
                    url=str(response.url),
                    status=response.status_code,
                    headers=response.headers,
                    dispose=response.aclose,
                )

        class RealContext:
            pages = []
            request = Request()

            async def cookies(self):
                return [
                    {
                        "name": c.name,
                        "value": c.value,
                        "domain": c.domain,
                        "path": c.path,
                        "secure": c.secure,
                        "httpOnly": "HttpOnly" in c._rest,
                        "sameSite": c._rest.get("SameSite", "").capitalize(),
                        "expires": c.expires,
                    }
                    for c in client.cookies.jar
                ]

        await PreviewBootstrap(rig.app)(RealContext(), record, rig.principal)
        assert len(client.cookies.jar) == 1
        grant = await rig.app._grant(
            {
                "headers": [
                    (b"cookie", f"{SESSION_COOKIE}={next(iter(client.cookies.jar)).value}".encode())
                ]
            },
            "preview",
        )
        assert grant.principal == rig.principal and grant.preview_id == "preview"
