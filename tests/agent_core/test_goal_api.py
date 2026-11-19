"""HTTP contracts for owner-scoped goal routes (thin transport, service does the work)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import update

from krim_agent.api.goals import mount_goals
from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.goals.contracts import GoalRead
from krim_agent.goals.service import GoalService
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.goal_models import GOAL_TABLES
from krim_agent.storage.models import metadata
from krim_agent.storage.sqlite import SQLiteStore

WORKSPACE = "alpha"
BASE = f"/v2/workspaces/{WORKSPACE}/sessions/{{session_id}}/goal"


def actor(principal_id: str) -> PrincipalContext:
    return PrincipalContext(
        principal_id,
        1,
        (
            Membership(
                workspace_id=WORKSPACE,
                role="admin" if principal_id == "admin" else "member",
            ),
        ),
    )


@asynccontextmanager
async def running_api(path: Path) -> AsyncIterator[tuple[httpx.AsyncClient, dict]]:
    store = await SQLiteStore.open(path)
    try:
        async with store.lock, store.engine.begin() as conn:
            await conn.run_sync(lambda sync: metadata.create_all(sync, tables=list(GOAL_TABLES)))
        session = await session_sql.create_session(
            store,
            WORKSPACE,
            "owner",
            SessionCreate(request_key="owner-session"),
            {"key": "demo"},
            ContextPolicy(),
            Settings(),
        )
        turn = await session_sql.submit(
            store,
            session.id,
            TurnRequest(request_key="user-1", text="任务", expected_revision=0),
            Settings(),
        )
        run, _ = await store.begin(
            RunRequest(request_key="run-1", text="run", target="demo"), {"key": "demo"}
        )
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(session_sql.turns)
                .where(session_sql.turns.c.id == turn.id)
                .values(run_id=run.id, status="completed")
            )
        api = FastAPI()
        api.state.application = SimpleNamespace(goals=GoalService(store))

        @api.middleware("http")
        async def identity(request, call_next):
            request.state.principal = actor(request.headers.get("x-principal", "owner"))
            return await call_next(request)

        api.add_exception_handler(
            ServiceError,
            lambda _request, error: JSONResponse(
                {"error": {"code": error.code}}, status_code=error.status
            ),
        )
        mount_goals(api)
        transport = httpx.ASGITransport(app=api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, {"store": store, "session": session, "turn": turn, "run": run}
    finally:
        await store.close()


def create_body(**values):
    body = {
        "request_key": "create",
        "expected_revision": 0,
        "objective": "完成目标",
        "completion_criteria": ["测试通过"],
    }
    body.update(values)
    return body


@pytest.mark.asyncio
async def test_goal_routes_round_trip(tmp_path):
    async with running_api(tmp_path / "api.sqlite") as (client, world):
        path = BASE.format(session_id=world["session"].id)
        created = await client.post(
            path,
            json=create_body(origin_turn_id=world["turn"].id, origin_run_id=world["run"].id),
        )
        assert created.status_code == 200, created.text
        snapshot = created.json()
        assert snapshot["status"] == "active" and snapshot["revision"] == 1

        latest = await client.get(path)
        assert latest.status_code == 200, latest.text
        assert latest.json()["revision"] == 1
        historical = await client.get(path, params={"revision": "1"})
        assert historical.json() == snapshot
        assert (await client.get(path, params={"revision": "9"})).status_code == 404

        paused = await client.post(
            path + "/pause", json={"request_key": "pause", "expected_revision": 1}
        )
        assert paused.status_code == 200, paused.text
        assert paused.json()["status"] == "paused"
        resumed = await client.post(
            path + "/resume", json={"request_key": "resume", "expected_revision": 2}
        )
        assert resumed.json()["status"] == "active"
        cancelled = await client.post(
            path + "/cancel",
            json={"request_key": "cancel", "expected_revision": 3, "reason": "用户停止"},
        )
        assert cancelled.json()["status"] == "cancelled"

        continuation = await client.get(path + "/continuation")
        assert continuation.status_code == 200
        assert continuation.json() is None


@pytest.mark.asyncio
async def test_legacy_workspace_session_rejects_goal_activation(tmp_path):
    async with running_api(tmp_path / "api-workspace.sqlite") as (client, world):
        path = BASE.format(session_id=world["session"].id)
        async with world["store"].lock, world["store"].engine.begin() as conn:
            await conn.execute(
                update(session_sql.sessions)
                .where(session_sql.sessions.c.id == world["session"].id)
                .values(target_key="target-workspace")
            )
        response = await client.post(
            path,
            json=create_body(origin_turn_id=world["turn"].id, origin_run_id=world["run"].id),
        )
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "workspace_contract_required"
        # Reads remain available on the legacy target.
        assert (await client.get(path)).status_code == 200


@pytest.mark.asyncio
async def test_goal_routes_reject_invalid_input_and_foreign_actors(tmp_path):
    async with running_api(tmp_path / "api-invalid.sqlite") as (client, world):
        path = BASE.format(session_id=world["session"].id)
        assert (await client.get(path, params={"revision": "abc"})).status_code == 422
        assert (await client.get(path, params={"revision": "0"})).status_code == 422
        assert (await client.get(path, params={"status": "active"})).status_code == 422
        assert (await client.post(path + "/pause", json={"request_key": "x"})).status_code == 422
        assert (await client.post(path, json={"objective": "缺字段"})).status_code == 422
        response = await client.post(
            path,
            headers={"x-principal": "admin"},
            json=create_body(origin_turn_id=world["turn"].id, origin_run_id=world["run"].id),
        )
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_missing_service_returns_schema_upgrade_required(tmp_path):
    api = FastAPI()
    api.state.application = SimpleNamespace(goals=None)

    @api.middleware("http")
    async def identity(request, call_next):
        request.state.principal = actor("owner")
        return await call_next(request)

    api.add_exception_handler(
        ServiceError,
        lambda _request, error: JSONResponse(
            {"error": {"code": error.code}}, status_code=error.status
        ),
    )
    mount_goals(api)
    transport = httpx.ASGITransport(app=api)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/v2/workspaces/alpha/sessions/00000000-0000-4000-8000-000000000000/goal"
        )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "schema_upgrade_required"


def test_goal_read_query_defaults():
    assert GoalRead().revision is None
