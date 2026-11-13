"""Personal MCP configuration API; credential input is write-only and never returned."""

from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse

from krim_agent.account_mcp.service import (
    AccountMCPService,
    ConnectionCreate,
    ConnectionDelete,
    ConnectionUpdate,
    SessionMCPSelection,
)
from krim_agent.contracts import ServiceError

from .sessions import application, principal


def _service(request: Request) -> AccountMCPService:
    result = application(request).account_mcp
    if result is None:
        raise ServiceError("schema_upgrade_required")
    return result


def _no_store(value: Any, status_code: int = 200) -> JSONResponse:
    return JSONResponse(value, status_code=status_code, headers={"Cache-Control": "no-store"})


def mount_account_mcp(app: FastAPI) -> None:
    router = APIRouter()

    @router.get("/v2/me/mcp-connections")
    async def list_connections(request: Request) -> JSONResponse:
        service = _service(request)
        actor = principal(request)
        service.actor(actor)
        return _no_store(await service.list_connections(actor))

    @router.post("/v2/me/mcp-connections", status_code=201)
    async def create_connection(body: ConnectionCreate, request: Request) -> JSONResponse:
        service = _service(request)
        actor = principal(request)
        service.actor(actor)
        return _no_store(await service.create_connection(actor, body), 201)

    @router.put("/v2/me/mcp-connections/{connection_id}")
    async def update_connection(
        connection_id: str, body: ConnectionUpdate, request: Request
    ) -> JSONResponse:
        service = _service(request)
        actor = principal(request)
        service.actor(actor)
        return _no_store(await service.update_connection(actor, connection_id, body))

    @router.delete("/v2/me/mcp-connections/{connection_id}")
    async def delete_connection(
        connection_id: str, body: ConnectionDelete, request: Request
    ) -> JSONResponse:
        service = _service(request)
        actor = principal(request)
        service.actor(actor)
        return _no_store(await service.delete_connection(actor, connection_id, body))

    base = "/v2/workspaces/{workspace_id}/sessions/{session_id}/mcp"

    @router.get(base)
    async def read_selection(
        workspace_id: str, session_id: str, request: Request
    ) -> JSONResponse:
        service = _service(request)
        actor = principal(request)
        service.actor(actor)
        return _no_store(await service.session_selection(actor, workspace_id, session_id))

    @router.put(base)
    async def write_selection(
        workspace_id: str,
        session_id: str,
        body: SessionMCPSelection,
        request: Request,
    ) -> JSONResponse:
        service = _service(request)
        actor = principal(request)
        service.actor(actor)
        return _no_store(await service.session_selection(actor, workspace_id, session_id, body))

    app.include_router(router)
