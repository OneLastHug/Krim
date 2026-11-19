"""HTTP contracts for owner-bound interaction state and requests."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from krim_agent.api.interaction import mount_interaction
from krim_agent.api.sessions import principal
from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.interaction.contracts import AskUserArgs, Question
from krim_agent.interaction.service import InteractionService
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.interaction_models import INTERACTION_TABLES
from krim_agent.storage.models import metadata
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.session import SessionToolScope


@asynccontextmanager
async def running_api(path):
    store = await SQLiteStore.open(path)
    try:
        async with store.engine.begin() as conn:
            await conn.run_sync(
                lambda sync: metadata.create_all(sync, tables=list(INTERACTION_TABLES))
            )
        session = await session_sql.create_session(
            store, "alpha", "owner", SessionCreate(request_key="owner-session"),
            {"key": "demo"}, ContextPolicy(), Settings(),
        )
        api = FastAPI()
        api.state.application = SimpleNamespace(interactions=InteractionService(store))

        @api.middleware("http")
        async def test_identity(request, call_next):
            actor_id = request.headers.get("x-test-principal", "owner")
            request.state.principal = PrincipalContext(
                actor_id,
                1,
                (
                    Membership(
                        workspace_id="alpha",
                        role="admin" if actor_id == "admin" else "member",
                    ),
                ),
            )
            return await call_next(request)

        api.add_exception_handler(
            ServiceError,
            lambda _request, error: JSONResponse(
                {"error": {"code": error.code}}, status_code=error.status
            ),
        )
        mount_interaction(api)
        yield api, session
    finally:
        await store.close()


@asynccontextmanager
async def client(api, actor_id):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api),
        base_url="http://test",
        headers={"x-test-principal": actor_id},
    ) as value:
        yield value


async def test_interaction_routes_read_answer_cancel_and_hide_foreign_requests(tmp_path):
    async with running_api(tmp_path / "interaction-api.sqlite") as (api, session):
        path = f"/v2/workspaces/alpha/sessions/{session.id}/interactions"
        async with client(api, "owner") as owner:
            initial = await owner.get(path)
            assert initial.status_code == 200
            assert initial.json() == {"mode": "execute", "revision": 0, "pending": []}

            service = api.state.application.interactions
            owner_actor = PrincipalContext("owner", 1, (Membership(workspace_id="alpha"),))
            request = await service.ask(
                SessionToolScope(owner_actor, "alpha", session.id, "turn", "run"),
                AskUserArgs(questions=[Question(id="q", prompt="Answer", mode="text")]),
                "api-request",
            )
            fetched = await owner.get(path + "/" + request["id"])
            assert fetched.status_code == 200 and fetched.json()["status"] == "pending"
            answered = await owner.post(
                path + "/" + request["id"] + "/answer",
                json={"request_key": "api-answer", "expected_revision": 1,
                      "answers": [{"question_id": "q", "text": "done"}]},
            )
            assert answered.status_code == 200, answered.text
            assert answered.json()["status"] == "answered"

            second = await service.ask(
                SessionToolScope(owner_actor, "alpha", session.id, "turn-2", "run-2"),
                AskUserArgs(questions=[Question(id="q", prompt="Cancel?", mode="text")]),
                "api-cancel-request",
            )
            cancelled = await owner.post(
                path + "/" + second["id"] + "/cancel",
                json={"request_key": "api-cancel", "expected_revision": 1,
                      "reason": "no longer needed"},
            )
            assert cancelled.status_code == 200
            assert cancelled.json()["status"] == "cancelled"

        async with client(api, "admin") as admin_client:
            hidden = await admin_client.get(path + "/" + request["id"])
            assert hidden.status_code == 404
            denied = await admin_client.post(
                path + "/" + second["id"] + "/answer",
                json={"request_key": "admin-answer", "expected_revision": 1,
                      "answers": [{"question_id": "q", "text": "denied"}]},
            )
            assert denied.status_code == 404


async def test_missing_service_returns_upgrade_error():
    api = FastAPI()
    api.state.application = SimpleNamespace(interactions=None)
    api.add_exception_handler(
        ServiceError,
        lambda _request, error: JSONResponse(
            {"error": {"code": error.code}}, status_code=error.status
        ),
    )
    mount_interaction(api)
    api.dependency_overrides[principal] = lambda: PrincipalContext(
        "owner", 1, (Membership(workspace_id="alpha"),)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://test"
    ) as client:
        unavailable = await client.get(
            "/v2/workspaces/alpha/sessions/session/interactions"
        )
        assert unavailable.status_code == 409
        assert unavailable.json()["error"]["code"] == "schema_upgrade_required"
