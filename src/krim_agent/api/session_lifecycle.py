"""Session archive and trash management routes."""

from typing import Literal, cast

from fastapi import APIRouter, FastAPI, Query, Request

from krim_agent.composition import Application
from krim_agent.contracts import ServiceError
from krim_agent.security import PrincipalContext
from krim_agent.sessions.service import SessionService
from krim_agent.sessions.types import SessionLifecycleRequest, SessionView
from krim_agent.storage.session_lifecycle import apply as apply_lifecycle


def _application(request: Request) -> Application:
    return cast(Application, request.app.state.application)


def _principal(request: Request) -> PrincipalContext:
    return cast(PrincipalContext, request.state.principal)


def _service(request: Request) -> SessionService:
    current = _application(request).sessions
    if current is None:
        raise ServiceError("schema_upgrade_required")
    return current


def mount_session_lifecycle(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v2/workspaces/{workspace_id}/sessions"

    @router.get(base + "/lifecycle/archived")
    async def archived(
        workspace_id: str,
        request: Request,
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> list[SessionView]:
        actor = _principal(request)
        return await _service(request).list(
            actor, workspace_id, offset, limit, view="archived"
        )

    @router.get(base + "/lifecycle/trash")
    async def trash(
        workspace_id: str,
        request: Request,
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> list[SessionView]:
        return await _service(request).list(
            _principal(request), workspace_id, offset, limit, view="trash"
        )

    @router.post(base + "/{session_id}/lifecycle/{action}")
    async def lifecycle(
        workspace_id: str,
        session_id: str,
        action: Literal["archive", "unarchive", "trash", "restore"],
        body: SessionLifecycleRequest,
        request: Request,
    ) -> SessionView:
        actor = _principal(request)
        await _service(request).get(actor, workspace_id, session_id, write=True)
        return await apply_lifecycle(
            _application(request).store,
            principal_id=actor.principal_id,
            workspace=workspace_id,
            session_id=session_id,
            request_key=body.request_key,
            expected_revision=body.expected_revision,
            action=action,
        )

    app.include_router(router)
