"""Read-only routes for historical workspace records and artifacts."""

from fastapi import APIRouter, FastAPI, Query, Request
from pydantic import JsonValue

from krim_agent.contracts import ServiceError
from krim_agent.sdk.workspace import WorkspaceSDK
from krim_agent.sessions.types import TurnView
from krim_agent.workspace.types import ArtifactChunk, ArtifactRevision, WorkView

from .sessions import application, principal


def mount_workspace(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v3/workspaces/{workspace_id}/sessions/{session_id}"

    def sdk(request: Request, workspace: str) -> WorkspaceSDK:
        return WorkspaceSDK(application(request), principal=principal(request), workspace=workspace)

    @router.get("/v3/workspace/capabilities")
    async def capabilities() -> dict[str, JsonValue]:
        return {"schema_version": 1, "available": False, "profile": None}

    @router.get(base + "/work")
    async def current(workspace_id: str, session_id: str, request: Request) -> WorkView:
        return await sdk(request, workspace_id).get(session_id)

    @router.get(base + "/turns/{turn_id}/work")
    async def turn_work(
        workspace_id: str, session_id: str, turn_id: str, request: Request
    ) -> WorkView:
        return await sdk(request, workspace_id).get(session_id, turn_id)

    @router.post(base + "/turns", status_code=202)
    async def submit(workspace_id: str, session_id: str) -> TurnView:
        raise ServiceError("workspace_unavailable", 422)

    @router.get(base + "/turns/by-key/{key}")
    async def turn_by_key(
        workspace_id: str, session_id: str, key: str, request: Request
    ) -> WorkView:
        return await sdk(request, workspace_id).by_key(session_id, key)

    @router.post(base + "/inputs")
    async def upload(workspace_id: str, session_id: str) -> ArtifactRevision:
        raise ServiceError("workspace_unavailable", 422)

    @router.get(base + "/inputs/by-key/{key}")
    async def input_by_key(
        workspace_id: str, session_id: str, key: str, request: Request
    ) -> ArtifactRevision:
        return await sdk(request, workspace_id).input_by_key(session_id, key)

    @router.get(base + "/artifacts/head")
    async def artifact_head(
        workspace_id: str, session_id: str, request: Request
    ) -> ArtifactRevision:
        return await sdk(request, workspace_id).head(session_id)

    @router.get(base + "/artifacts/{revision_id}")
    async def artifact(
        workspace_id: str, session_id: str, revision_id: str, request: Request
    ) -> ArtifactRevision:
        return await sdk(request, workspace_id).artifact(session_id, revision_id)

    @router.get(base + "/artifacts/{revision_id}/files/{path:path}")
    async def chunk(
        workspace_id: str,
        session_id: str,
        revision_id: str,
        path: str,
        request: Request,
        offset: int = Query(default=0, ge=0),
        length: int = Query(default=16384, ge=1, le=16384),
    ) -> ArtifactChunk:
        return await sdk(request, workspace_id).chunk(
            session_id, revision_id, path, offset=offset, length=length
        )

    app.include_router(router)
