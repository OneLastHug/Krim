"""Preview bridge tests use existing shared handles and no new DTOs."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from test_dev_browser_context import Browser

from krim_agent.dev_browser import BrowserError
from krim_agent.dev_browser.preview import open_preview
from krim_agent.dev_preview.boundary import PreviewOrigins
from krim_agent.development.contracts import EnvironmentHandle, PreviewHandle
from krim_agent.security import PrincipalContext

ORIGINS = PreviewOrigins("https://krim.example.com", "example.com", "preview.example.net")
VIEWER = PrincipalContext("viewer", 7, ())


def handle():
    return PreviewHandle(
        preview_id="preview",
        environment=EnvironmentHandle(
            owner_id="owner",
            project_id="project",
            environment_id="env",
            generation=1,
            state="ready",
        ),
        service_id="process",
        port=3000,
        state="ready",
        access_path="/api/development/previews/preview/open",
    )


class Service:
    def __init__(self, expected):
        self.record = SimpleNamespace(handle=expected)
        self.lease = object()
        self.opened = self.closed = False
        self.revoked = False
        self.checks = []

    @asynccontextmanager
    async def route(self, preview_id, principal):
        assert preview_id == self.record.handle.preview_id and principal is VIEWER
        task = asyncio.current_task()
        self.opened = True
        try:
            yield self.record, self.lease
        finally:
            assert asyncio.current_task() is task
            self.closed = True

    async def recheck(self, record, principal, lease):
        assert record is self.record and principal is VIEWER and lease is self.lease
        self.checks.append(principal)
        if self.revoked:
            raise RuntimeError("secret account permission")


@pytest.mark.asyncio
async def test_route_is_held_and_original_viewer_rechecked_without_new_contracts():
    expected, browser = handle(), Browser()
    service = Service(expected)
    prepared = []

    async def prepare(context, record, principal):
        prepared.append((context, record, principal))

    async with open_preview(
        browser,
        service,
        ORIGINS,
        expected,
        VIEWER,
        project_id="project",
        prepare_context=prepare,
    ) as session:
        assert service.opened and not service.closed
        await session.goto("/fixture")
        assert browser.context.page.url == ORIGINS.origin("preview") + "/fixture"
        assert prepared == [(browser.context, service.record, VIEWER)]
        assert len(service.checks) >= 4
    assert service.closed and browser.context.closed.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("principal,project", [(None, "project"), (VIEWER, "other")])
async def test_no_anonymous_or_cross_project_preview(principal, project):
    expected, browser = handle(), Browser()
    service = Service(expected)

    async def prepare(*args):
        pytest.fail("denied preview must not bootstrap")

    with pytest.raises(BrowserError, match="browser_preview_denied"):
        async with open_preview(
            browser,
            service,
            ORIGINS,
            expected,
            principal,
            project_id=project,
            prepare_context=prepare,
        ):
            pytest.fail("denied preview must not yield")
    assert not service.opened and browser.options is None


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("service_id", "other"), ("port", 8080)])
async def test_forged_handle_binding_is_rejected_after_lookup(field, value):
    expected, browser = handle(), Browser()
    service = Service(expected.model_copy(update={field: value}))

    async def prepare(*args):
        pytest.fail("drift must not bootstrap")

    with pytest.raises(BrowserError, match="browser_preview_denied"):
        async with open_preview(
            browser,
            service,
            ORIGINS,
            expected,
            VIEWER,
            project_id="project",
            prepare_context=prepare,
        ):
            pytest.fail("drift must not yield")
    assert service.closed and browser.options is None


@pytest.mark.asyncio
async def test_revocation_and_cancellation_release_context_before_route():
    expected, browser = handle(), Browser()
    service = Service(expected)

    async def prepare(*args):
        pass

    with pytest.raises(BrowserError, match="browser_authorization_lost"):
        async with open_preview(
            browser,
            service,
            ORIGINS,
            expected,
            VIEWER,
            project_id="project",
            prepare_context=prepare,
        ) as session:
            await session.goto("/")
            service.revoked = True
            await session.click("#go")
    assert browser.context.closed.is_set() and service.closed

    browser, service = Browser(), Service(expected)
    entered = asyncio.Event()

    async def run():
        async with open_preview(
            browser,
            service,
            ORIGINS,
            expected,
            VIEWER,
            project_id="project",
            prepare_context=prepare,
        ) as session:
            await session.goto("/")
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(run())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert browser.context.closed.is_set() and service.closed
