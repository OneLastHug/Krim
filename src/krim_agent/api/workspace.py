"""v3工作区薄路由；对象权限及业务沿原Session/Artifact服务。"""

from fastapi import APIRouter, FastAPI, Query, Request
from pydantic import JsonValue

from krim_agent.contracts import ServiceError, parse_json
from krim_agent.sdk.workspace import WorkspaceSDK
from krim_agent.sessions.types import TurnView
from krim_agent.workspace.types import (
    ArtifactChunk,
    ArtifactRevision,
    InputSnapshotRequest,
    WorkspaceTurnRequest,
    WorkView,
)

from .sessions import application, principal


def mount_workspace(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v3/workspaces/{workspace_id}/sessions/{session_id}"

    def sdk(request: Request, workspace: str) -> WorkspaceSDK:
        return WorkspaceSDK(application(request), principal=principal(request), workspace=workspace)

    @router.get("/v3/workspace/capabilities")
    async def capabilities(request: Request) -> dict[str, JsonValue]:
        actor = principal(request)
        active = application(request)
        # 认证由原LocalBoundary完成；仅投影该主体受权的有限可用性。
        allowed = bool(actor.memberships)
        available = allowed and active.work is not None and active.runner is not None
        return {
            "schema_version": 1,
            "available": available,
            "profile": {"name": "python-unittest/1"} if available else None,
        }

    @router.get(base + "/work")
    async def current(workspace_id: str, session_id: str, request: Request) -> WorkView:
        return await sdk(request, workspace_id).get(session_id)

    @router.get(base + "/turns/{turn_id}/work")
    async def turn_work(
        workspace_id: str, session_id: str, turn_id: str, request: Request
    ) -> WorkView:
        return await sdk(request, workspace_id).get(session_id, turn_id)

    @router.post(base + "/turns", status_code=202)
    async def submit(workspace_id: str, session_id: str, request: Request) -> TurnView:
        try:
            raw = await request.body()
            value = parse_json(raw.decode("utf-8"))
            if not isinstance(value, dict) or type(value.get("schema_version")) is not int:
                raise ValueError("schema_version必须是严格整数")
            body = WorkspaceTurnRequest.model_validate_json(raw)
        except ValueError:
            raise ServiceError("invalid_input", 422) from None
        return await sdk(request, workspace_id).start(session_id, body)

    @router.get(base + "/turns/by-key/{key}")
    async def turn_by_key(
        workspace_id: str, session_id: str, key: str, request: Request
    ) -> WorkView:
        return await sdk(request, workspace_id).by_key(session_id, key)

    @router.post(base + "/inputs")
    async def upload(workspace_id: str, session_id: str, request: Request) -> ArtifactRevision:
        try:
            raw = await request.body()
            value = parse_json(raw.decode("utf-8"))
            if not isinstance(value, dict) or type(value.get("schema_version")) is not int:
                raise ValueError("schema_version必须是严格整数")
            body = InputSnapshotRequest.model_validate_json(raw)
        except ValueError:
            raise ServiceError("invalid_input", 422) from None
        return await sdk(request, workspace_id).input(session_id, body)

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
