from __future__ import annotations

import base64
import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError
import pytest
from sqlalchemy import insert

from krim_agent.api.workspace import mount_workspace
from krim_agent.client import AsyncRunClient
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.sdk.workspace import WorkspaceSDK
from krim_agent.security import LOCAL, Membership, PrincipalContext
from krim_agent.storage import work as work_sql
from krim_agent.storage.models import sessions
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.workspace.artifacts import ArtifactStore
from krim_agent.workspace.types import (
    ExecutionAccess,
    InputFile,
    InputSnapshotRequest,
    WorkspaceProfile,
)


SESSION_ID = "11111111-1111-4111-8111-111111111111"


class SessionReadProbe:
    def __init__(self, session_id: str, owner_id: str) -> None:
        self.session_id, self.owner_id = session_id, owner_id

    async def get(
        self,
        principal_or_id: Any,
        workspace: str | None = None,
        session_id: str | None = None,
        *,
        write: bool = False,
    ) -> object:
        if session_id is None:
            requested = principal_or_id
            if requested != self.session_id:
                raise ServiceError("not_found", 404)
            return object()
        actor: PrincipalContext = principal_or_id
        if session_id != self.session_id or workspace != "local":
            raise ServiceError("not_found", 404)
        member = actor.member(workspace, write=write)
        if actor.principal_id != self.owner_id and member.role != "admin":
            raise ServiceError("not_found", 404)
        return object()


def _identity(token: str | None) -> PrincipalContext:
    if token == "Bearer viewer":
        return PrincipalContext("owner", 1, (Membership(workspace_id="local", role="viewer"),))
    if token == "Bearer peer":
        return PrincipalContext("peer", 1, (Membership(workspace_id="local"),))
    return LOCAL


def _api(backend: Any) -> FastAPI:
    app = FastAPI()
    app.state.application = backend

    @app.middleware("http")
    async def bind_identity(request: Request, call_next: Any) -> Any:
        request.state.principal = _identity(request.headers.get("authorization"))
        return await call_next(request)

    @app.exception_handler(ServiceError)
    async def service_error(_request: Request, error: ServiceError) -> JSONResponse:
        return JSONResponse({"error": {"code": error.code}}, status_code=error.status)

    mount_workspace(app)
    return app


async def _seed_head(store: SQLiteStore, session_id: str, owner_id: str) -> None:
    profile = WorkspaceProfile(max_tools_per_step=1, environment={})
    session = {"id": session_id, "workspace_id": "local", "owner_id": owner_id}
    target = {"workspace_profile": profile.model_dump(mode="json")}
    async with store.engine.begin() as connection:
        await connection.execute(
            insert(sessions).values(
                id=session_id,
                workspace_id="local",
                owner_id=owner_id,
                create_key="head-session",
                request_hash=fingerprint({"head-session": session_id}),
                title="head test",
                target_key="target-workspace",
                target={},
                context_policy={},
                revision=0,
                status="open",
                dispatch_paused=False,
                created_at=1.0,
                closed_at=None,
            )
        )
        await work_sql.initialize(connection, session, target)


def _input_request(expected_revision: int) -> InputSnapshotRequest:
    content = b"current head content"
    return InputSnapshotRequest(
        request_key="head-input",
        expected_head_revision=expected_revision,
        files=(InputFile(
            path="head.txt",
            data=base64.b64encode(content).decode("ascii"),
            sha256=hashlib.sha256(content).hexdigest(),
        ),),
    )


@pytest.mark.asyncio
async def test_head_sdk_and_asgi_read_the_current_artifact_without_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner_id = "owner"
    database, root = tmp_path / "head.sqlite", tmp_path / "objects"
    root.mkdir()
    store = await SQLiteStore.open(database)
    await _seed_head(store, SESSION_ID, owner_id)
    artifacts = ArtifactStore(store, root)
    await artifacts.head(ExecutionAccess(LOCAL, "local", SESSION_ID))

    sessions_probe = SessionReadProbe(SESSION_ID, owner_id)
    monkeypatch.setattr(WorkspaceSDK, "_sessions", lambda _self: sessions_probe)
    backend = SimpleNamespace(artifacts=artifacts, sessions=sessions_probe, work=object())
    app = _api(backend)
    base = f"/v3/workspaces/local/sessions/{SESSION_ID}"
    sdk = WorkspaceSDK(backend, principal=LOCAL)
    try:
        local_initial = await sdk.head(SESSION_ID)
        assert local_initial.revision == 0 and local_initial.manifest == ()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost") as client:
            initial = await client.get(base + "/artifacts/head")
            assert initial.status_code == 200
            assert initial.json() == local_initial.model_dump(mode="json")

            viewer = await client.get(base + "/artifacts/head", headers={"Authorization": "Bearer viewer"})
            assert viewer.status_code == 200 and viewer.json() == initial.json()
            denied = await client.get(base + "/artifacts/head", headers={"Authorization": "Bearer peer"})
            assert denied.status_code == 404
            rejected_write = await client.post(
                base + "/inputs",
                headers={"Authorization": "Bearer viewer"},
                json=_input_request(0).model_dump(mode="json"),
            )
            assert rejected_write.status_code == 403

            uploaded = await client.post(base + "/inputs", json=_input_request(0).model_dump(mode="json"))
            assert uploaded.status_code == 200, uploaded.text
            revision = uploaded.json()
            assert revision["revision"] == 1 and revision["parent_id"] == local_initial.revision_id

            database_hash = hashlib.sha256(database.read_bytes()).hexdigest()
            first = await client.get(base + "/artifacts/head")
            second = await client.get(base + "/artifacts/head")
            assert first.status_code == second.status_code == 200
            assert first.json() == second.json() == revision
            assert (await sdk.head(SESSION_ID)).model_dump(mode="json") == revision
            assert hashlib.sha256(database.read_bytes()).hexdigest() == database_hash
    finally:
        await sdk.close()
        await artifacts.close()
        await store.close()

    readonly = await SQLiteStore.open(database, readonly=True)
    readonly_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    readonly_sdk = WorkspaceSDK(readonly, principal=LOCAL, artifact_root=root)
    try:
        current = await readonly_sdk.head(SESSION_ID)
        assert current.revision == 1 and current.request_key == "head-input"
        assert not readonly.closed
        assert hashlib.sha256(database.read_bytes()).hexdigest() == readonly_hash
    finally:
        await readonly_sdk.close()
        await readonly.close()


@pytest.mark.asyncio
async def test_remote_sdk_head_uses_get_and_strict_artifact_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncRunClient("http://head.invalid")
    calls: list[tuple[str, str, Any]] = []
    value = {
        "revision_id": "22222222-2222-4222-8222-222222222222",
        "session_id": SESSION_ID,
        "parent_id": None,
        "revision": 0,
        "source_kind": "input",
        "request_key": "initial",
        "request_hash": "a" * 64,
        "manifest": [],
        "manifest_hash": "b" * 64,
        "producer_run_id": None,
        "producer_call_id": None,
    }

    async def respond(method: str, path: str, body: Any = None) -> Any:
        calls.append((method, path, body))
        return value

    monkeypatch.setattr(client, "request", respond)
    sdk = WorkspaceSDK(client, workspace="local")
    try:
        head = await sdk.head(SESSION_ID)
        assert head.revision == 0 and head.session_id == SESSION_ID
        assert calls == [(
            "GET",
            "/v3/workspaces/local/sessions/" + SESSION_ID + "/artifacts/head",
            None,
        )]
        async def invalid(_method: str, _path: str, _body: Any = None) -> Any:
            return value | {"unknown": True}
        monkeypatch.setattr(client, "request", invalid)
        with pytest.raises(ValidationError):
            await sdk.head(SESSION_ID)
    finally:
        await sdk.close()
        await client.close()
