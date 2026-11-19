"""Standalone manual-compaction transport; no production composition required."""

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from krim_agent.api.context import mount_context
from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL


class Context:
    def __init__(self):
        self.calls = []

    async def schedule_compaction(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return {"request_key": args[3], "status": "pending"}

    async def pending(self, *args):
        return [{"request_key": "manual-1", "status": "pending"}]


@pytest.fixture
async def client():
    app, service = FastAPI(), Context()
    app.state.application = SimpleNamespace(context_service=service)

    @app.middleware("http")
    async def actor(request: Request, call_next):
        request.state.principal = LOCAL
        return await call_next(request)

    @app.exception_handler(ServiceError)
    async def error(request: Request, exception: ServiceError):
        return JSONResponse({"code": exception.code}, status_code=exception.status)

    mount_context(app)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://context.test"
    ) as http:
        yield http, service


async def test_manual_compaction_transport_enqueues_and_lists(client):
    http, service = client
    path = "/v2/workspaces/local/sessions/session/context/compactions"
    response = await http.post(
        path,
        json={"request_key": "manual-1", "expected_revision": 2, "reason": "user clicked compact"},
    )
    assert response.status_code == 202
    assert response.json() == {"request_key": "manual-1", "status": "pending"}
    args, kwargs = service.calls[0]
    assert args == (LOCAL, "local", "session", "manual-1", 2)
    assert kwargs == {"reason": "user clicked compact"}
    response = await http.get(path)
    assert response.status_code == 200 and len(response.json()["items"]) == 1


@pytest.mark.parametrize(
    "body",
    [
        {"request_key": "x", "owner": "spoof"},
        {"request_key": "x", "expected_revision": -1},
        {"request_key": 42},
        {},
    ],
)
async def test_manual_compaction_transport_rejects_invalid_body(client, body):
    http, service = client
    response = await http.post(
        "/v2/workspaces/local/sessions/session/context/compactions", json=body
    )
    assert response.status_code == 422 and not service.calls
