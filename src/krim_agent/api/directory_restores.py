"""目录恢复公开路由；只委托原AgentService，不接受私有围栏或材料。"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, FastAPI, Path, Query, Request

from krim_agent.application.agents import AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore import (
    IDENTIFIER,
    REQUEST_KEY,
    DirectoryRestoreExecute,
    DirectoryRestoreOperationView,
    DirectoryRestorePreview,
    DirectoryRestoreView,
)
from krim_agent.workspace_directory_restore_public import (
    DirectoryRestoreDiffPage,
    directory_restore_diff_page,
)

Identifier = Annotated[str, Path(min_length=1, max_length=36, pattern=IDENTIFIER)]


def _parameters(request: Request, allowed: set[str]) -> None:
    items = request.query_params.multi_items()
    names = {name for name, _ in items}
    if not names <= allowed or len(names) != len(items):
        raise ServiceError(
            "workspace_directory_restore_parameters", "目录恢复查询参数未知或重复", 422
        )


async def _empty_body(request: Request) -> None:
    async for chunk in request.stream():
        if chunk:
            raise ServiceError(
                "workspace_directory_restore_parameters", "此目录恢复动作不接受请求体", 422
            )


def register_directory_restore_routes(app: FastAPI, service: AgentService) -> None:
    router = APIRouter(prefix="/v1/agents/{agent_id}/workspace/directory-restores")

    @router.post("/preview", status_code=202)
    async def preview(
        request: Request,
        agent_id: Identifier,
        payload: DirectoryRestorePreview,
        confirmed: bool = Query(default=False),
    ) -> DirectoryRestoreView:
        _parameters(request, {"confirmed"})
        return await service.workspace_preview_directory_restore(
            agent_id, payload, confirmed=confirmed
        )

    @router.post("/start", status_code=202)
    async def start(
        request: Request,
        agent_id: Identifier,
        payload: DirectoryRestoreExecute,
        confirmed: bool = Query(default=False),
    ) -> DirectoryRestoreView:
        _parameters(request, {"confirmed"})
        return await service.workspace_start_directory_restore(
            agent_id, payload, confirmed=confirmed
        )

    @router.get("")
    async def history(
        request: Request,
        agent_id: Identifier,
        offset: int = Query(default=0, ge=0, le=9007199254740991),
        limit: int = Query(default=32, ge=1, le=32),
    ) -> list[DirectoryRestoreView]:
        _parameters(request, {"offset", "limit"})
        await _empty_body(request)
        return await service.workspace_directory_restore_list(agent_id, offset=offset, limit=limit)

    @router.get("/by-key")
    async def by_key(
        request: Request,
        agent_id: Identifier,
        request_key: str = Query(min_length=1, max_length=128, pattern=REQUEST_KEY),
    ) -> DirectoryRestoreView | None:
        _parameters(request, {"request_key"})
        await _empty_body(request)
        return await service.workspace_directory_restore_by_key(agent_id, request_key)

    @router.get("/operations/by-key")
    async def operation_by_key(
        request: Request,
        agent_id: Identifier,
        request_key: str = Query(min_length=1, max_length=128, pattern=REQUEST_KEY),
    ) -> DirectoryRestoreOperationView | None:
        _parameters(request, {"request_key"})
        await _empty_body(request)
        return await service.workspace_directory_restore_operation_by_key(agent_id, request_key)

    @router.get("/operations/{operation_id}")
    async def operation(
        request: Request, agent_id: Identifier, operation_id: Identifier
    ) -> DirectoryRestoreOperationView:
        _parameters(request, set())
        await _empty_body(request)
        return await service.workspace_directory_restore_operation(agent_id, operation_id)

    @router.post("/operations/{operation_id}/verify", status_code=202)
    async def verify(
        request: Request,
        agent_id: Identifier,
        operation_id: Identifier,
        confirmed: bool = Query(default=False),
    ) -> DirectoryRestoreView:
        _parameters(request, {"confirmed"})
        await _empty_body(request)
        return await service.workspace_verify_directory_restore(
            agent_id, operation_id, confirmed=confirmed
        )

    @router.get("/{plan_id}")
    async def get(
        request: Request, agent_id: Identifier, plan_id: Identifier
    ) -> DirectoryRestoreView:
        _parameters(request, set())
        await _empty_body(request)
        return await service.workspace_directory_restore(agent_id, plan_id)

    @router.get("/{plan_id}/diff")
    async def diff(
        request: Request,
        agent_id: Identifier,
        plan_id: Identifier,
        offset: int = Query(default=0, ge=0, le=9007199254740991),
        limit: int = Query(default=128, ge=1, le=128),
    ) -> DirectoryRestoreDiffPage:
        _parameters(request, {"offset", "limit"})
        await _empty_body(request)
        entries = await service.workspace_directory_restore_diff(
            agent_id, plan_id, offset=offset, limit=limit
        )
        plan = await service.workspace_directory_restore(agent_id, plan_id)
        return directory_restore_diff_page(plan, entries, offset, limit)

    @router.post("/{plan_id}/wait")
    async def wait(
        request: Request,
        agent_id: Identifier,
        plan_id: Identifier,
        timeout_seconds: float = Query(default=30, ge=0, le=300, allow_inf_nan=False),
    ) -> DirectoryRestoreView:
        _parameters(request, {"timeout_seconds"})
        await _empty_body(request)
        return await service.workspace_directory_restore_wait(
            agent_id, plan_id, timeout_seconds=timeout_seconds
        )

    @router.post("/{plan_id}/cancel")
    async def cancel(
        request: Request, agent_id: Identifier, plan_id: Identifier
    ) -> DirectoryRestoreView:
        _parameters(request, set())
        await _empty_body(request)
        return await service.workspace_directory_restore_cancel(agent_id, plan_id)

    app.include_router(router)
