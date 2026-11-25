"""Owner-scoped model gateway settings and explicit model discovery."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse

from krim_agent.contracts import ServiceError
from krim_agent.model_gateway.contracts import GatewayUpdate
from krim_agent.model_gateway.discovery import discover_models
from krim_agent.model_gateway.selection import SessionModelUpdate, get_selection, set_selection
from krim_agent.model_gateway.service import ModelGatewayService
from krim_agent.security import PrincipalContext

from .sessions import application, principal


def _access(
    request: Request,
    *,
    write: bool = False,
) -> tuple[ModelGatewayService, PrincipalContext]:
    current = application(request)
    actor = principal(request)
    if current.identities is not None and current.identities.managed:
        actor = current.identities.validate(actor)
    if not actor.memberships or write and not any(m.role != "viewer" for m in actor.memberships):
        raise ServiceError("permission_denied", 403)
    if current.model_gateways is None:
        raise ServiceError("provider_not_configured", 503)
    return current.model_gateways, actor


def _response(value: Any) -> JSONResponse:
    body = value.model_dump(mode="json") if hasattr(value, "model_dump") else value
    return JSONResponse(body, headers={"Cache-Control": "no-store"})


def mount_model_gateways(app: FastAPI) -> None:
    router = APIRouter()

    @router.get("/v2/me/model-gateways")
    async def settings(request: Request) -> JSONResponse:
        service, actor = _access(request)
        return _response(await service.get(actor.principal_id))

    @router.put("/v2/me/model-gateways")
    async def update_settings(body: GatewayUpdate, request: Request) -> JSONResponse:
        service, actor = _access(request, write=True)
        return _response(await service.update(actor.principal_id, body))

    @router.post("/v2/me/model-gateways/{gateway_id}/models/discover")
    async def discover(gateway_id: str, request: Request) -> JSONResponse:
        service, actor = _access(request, write=True)
        config = await service.gateway_config(actor.principal_id, gateway_id)
        return _response({"models": await discover_models(config)})

    @router.get("/v2/workspaces/{workspace_id}/sessions/{session_id}/model-selection")
    async def selection(workspace_id: str, session_id: str, request: Request) -> JSONResponse:
        _, actor = _access(request)
        return _response(await get_selection(application(request), actor, workspace_id, session_id))

    @router.put("/v2/workspaces/{workspace_id}/sessions/{session_id}/model-selection")
    async def update_selection(
        workspace_id: str,
        session_id: str,
        body: SessionModelUpdate,
        request: Request,
    ) -> JSONResponse:
        _, actor = _access(request, write=True)
        return _response(
            await set_selection(
                application(request),
                actor,
                workspace_id,
                session_id,
                body,
            )
        )

    app.include_router(router)
