"""E bootstrap/HTTP with real SQLite identity and credentials; route is socketpair."""

import hashlib
import time

import httpx
import pytest
from sqlalchemy import select

from krim_agent.contracts import ServiceError
from krim_agent.dev_preview.app import PreviewApp
from krim_agent.dev_preview.boundary import SESSION_COOKIE, PreviewOrigins
from krim_agent.dev_preview.service import PreviewService
from krim_agent.storage.preview_models import preview_credentials
from tests.agent_core.test_dev_preview_protocol import Routes, Sockets
from tests.coding_agent.test_preview_storage import setup


async def test_real_sqlite_ticket_exchange_proxy_and_revocation(tmp_path) -> None:
    store, development, db, record, actor, grant = await setup(tmp_path / "preview.sqlite")
    routes = Routes()
    routes.started_at = record.instance.started_at

    class Backend:
        async def inspect(self, process):
            return (await development.processes.get(process.process_id)).handle

    service = PreviewService(db.records, development.processes, development.environments,
                             Backend(), db.authority, routes)
    origins = PreviewOrigins("https://control.example", "control.example", "preview.example")
    app = PreviewApp(service, db.credentials, origins, Sockets())
    try:
        bootstrap = await app.issue_bootstrap(record.handle.preview_id, actor)
        assert bootstrap.expires_at > time.time()
        async with store.engine.connect() as conn:
            stored = (await conn.execute(select(preview_credentials))).mappings().one()
            assert stored["digest"] == hashlib.sha256(bootstrap.ticket.encode()).hexdigest()
            assert bootstrap.ticket not in repr(dict(stored))
        requests = []

        def fixture(request):
            requests.append(request)
            assert SESSION_COOKIE not in request.headers.get("cookie", "")
            assert "authorization" not in request.headers
            return httpx.Response(200, content=b"sqlite-preview-ok")

        routes.transport = httpx.MockTransport(fixture)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=origins.origin(record.handle.preview_id), follow_redirects=False,
        ) as client:
            denied = await client.get("/")
            assert denied.status_code == 401
            opened = await client.post(bootstrap.action, data={"ticket": bootstrap.ticket},
                                       headers={"Origin": origins.control_origin})
            assert opened.status_code == 303
            cookie = opened.headers["set-cookie"]
            assert "HttpOnly" in cookie and "Secure" in cookie and "Domain=" not in cookie
            response = await client.get("/")
            assert response.status_code == 200 and response.content == b"sqlite-preview-ok"
            repeated = await client.post(bootstrap.action, data={"ticket": bootstrap.ticket},
                                         headers={"Origin": origins.control_origin})
            assert repeated.status_code == 401
            await db.access.put(grant.model_copy(update={"revoked": True, "revision": 2}),
                                expected_revision=1)
            revoked = await client.get("/")
            assert revoked.status_code == 403
            assert len(requests) == 1
            with pytest.raises(ServiceError, match="permission_denied"):
                await app.issue_bootstrap(record.handle.preview_id, actor)
    finally:
        await store.close()
