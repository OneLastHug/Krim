from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from krim_agent.api.planning import mount_planning
from krim_agent.contracts import ServiceError
from krim_agent.planning.contracts import (
    PlanCounts,
    PlanRead,
    PlanSnapshot,
    PlanStep,
    PlanUpdate,
)
from krim_agent.security import LOCAL

WORKSPACE_ID = "workspace-test"
SESSION_ID = "11111111-1111-4111-8111-111111111111"


def snapshot(*, revision: int = 3, steps: tuple[PlanStep, ...] | None = None) -> PlanSnapshot:
    current = steps or (
        PlanStep(id="one", text="First", status="pending"),
        PlanStep(id="two", text="Second", status="blocked"),
    )
    counts = {
        status: sum(step.status == status for step in current)
        for status in ("pending", "in_progress", "completed", "blocked")
    }
    return PlanSnapshot(
        session_id=SESSION_ID,
        revision=revision,
        content_hash="a" * 64,
        updated_at=123.0,
        counts=PlanCounts(**counts),
        total=len(current),
        matched_total=len(current),
        steps=current,
        offset=0,
        limit=50,
        next_offset=None,
    )


class PlanningProbe:
    def __init__(self) -> None:
        self.read_call: tuple[Any, ...] | None = None
        self.update_call: tuple[Any, ...] | None = None

    async def read(
        self, actor: Any, workspace_id: str, session_id: str, query: PlanRead | None = None
    ) -> PlanSnapshot:
        self.read_call = (actor, workspace_id, session_id, query)
        return snapshot()

    async def update(
        self, actor: Any, workspace_id: str, session_id: str, body: PlanUpdate
    ) -> PlanSnapshot:
        self.update_call = (actor, workspace_id, session_id, body)
        return snapshot(revision=body.expected_revision + 1, steps=body.steps)


def make_api(planning: Any = None) -> FastAPI:
    app = FastAPI()
    app.state.application = SimpleNamespace(planning=planning)

    @app.middleware("http")
    async def bind_actor(request: Request, call_next: Any) -> Any:
        request.state.principal = LOCAL
        return await call_next(request)

    @app.exception_handler(ServiceError)
    async def service_error(_request: Request, error: ServiceError) -> JSONResponse:
        return JSONResponse({"error": {"code": error.code}}, status_code=error.status)

    mount_planning(app)
    return app


@pytest.mark.asyncio
async def test_get_forwards_actor_path_pagination_and_repeated_statuses() -> None:
    service = PlanningProbe()
    app = make_api(service)
    path = f"/v2/workspaces/{WORKSPACE_ID}/sessions/{SESSION_ID}/plan"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            path,
            params=[
                ("revision", "3"),
                ("offset", "10"),
                ("limit", "25"),
                ("statuses", "pending"),
                ("statuses", "blocked"),
            ],
        )

    assert response.status_code == 200, response.text
    assert response.json() == snapshot().model_dump(mode="json")
    assert service.read_call == (
        LOCAL,
        WORKSPACE_ID,
        SESSION_ID,
        PlanRead(revision=3, offset=10, limit=25, statuses=("pending", "blocked")),
    )


@pytest.mark.asyncio
async def test_get_rejects_invalid_query_values() -> None:
    service = PlanningProbe()
    app = make_api(service)
    path = f"/v2/workspaces/{WORKSPACE_ID}/sessions/{SESSION_ID}/plan"
    invalid_queries = (
        "revision=-1",
        "limit=201",
        "offset=1",
        "statuses=pending&statuses=pending",
        "unknown=value",
        "revision=1&revision=2",
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        for query in invalid_queries:
            response = await client.get(path + "?" + query)
            assert response.status_code == 422, (query, response.text)
            assert response.json()["error"]["code"] == "invalid_input"
    assert service.read_call is None


@pytest.mark.asyncio
async def test_put_parses_array_steps_and_forwards_actor_and_path() -> None:
    service = PlanningProbe()
    app = make_api(service)
    path = f"/v2/workspaces/{WORKSPACE_ID}/sessions/{SESSION_ID}/plan"
    body = {
        "request_key": "update-plan-1",
        "expected_revision": 3,
        "steps": [
            {"id": "one", "text": "First", "status": "in_progress"},
            {"id": "two", "text": "Second", "status": "completed"},
        ],
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(path, json=body)

    expected_steps = (
        PlanStep(id="one", text="First", status="in_progress"),
        PlanStep(id="two", text="Second", status="completed"),
    )
    assert response.status_code == 200, response.text
    assert response.json() == snapshot(revision=4, steps=expected_steps).model_dump(mode="json")
    assert service.update_call == (
        LOCAL,
        WORKSPACE_ID,
        SESSION_ID,
        PlanUpdate(request_key="update-plan-1", expected_revision=3, steps=expected_steps),
    )
    assert isinstance(service.update_call[3].steps, tuple)


@pytest.mark.asyncio
async def test_put_rejects_extra_fields_invalid_versions_and_invalid_json() -> None:
    service = PlanningProbe()
    app = make_api(service)
    path = f"/v2/workspaces/{WORKSPACE_ID}/sessions/{SESSION_ID}/plan"
    valid = {
        "request_key": "update-plan-invalid",
        "expected_revision": 0,
        "steps": [{"id": "one", "text": "First"}],
    }
    invalid_bodies = (
        valid | {"extra": True},
        valid | {"expected_revision": "0"},
        valid | {"steps": {"id": "one", "text": "First"}},
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        for body in invalid_bodies:
            response = await client.put(path, json=body)
            assert response.status_code == 422, response.text
            assert response.json()["error"]["code"] == "invalid_input"
        malformed = await client.put(path, content=b"{")
        assert malformed.status_code == 422
        assert malformed.json()["error"]["code"] == "invalid_input"
    assert service.update_call is None


@pytest.mark.asyncio
async def test_missing_planning_service_requires_schema_upgrade() -> None:
    app = make_api()
    path = f"/v2/workspaces/{WORKSPACE_ID}/sessions/{SESSION_ID}/plan"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        read = await client.get(path)
        update = await client.put(
            path,
            json={
                "request_key": "update-plan-missing",
                "expected_revision": 0,
                "steps": [],
            },
        )

    for response in (read, update):
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "schema_upgrade_required"
