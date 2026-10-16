"""缺失基线回退的公开路由；原Scope/版本/来源仍由原受控AgentService裁决。"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, FastAPI, Path, Query, Request

from krim_agent.application.agents import AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore import IDENTIFIER
from krim_agent.workspace_missing_directory_rollback import (
    MissingDirectoryRollbackAction,
    MissingDirectoryRollbackAttemptView,
    MissingDirectoryRollbackPreview,
    MissingDirectoryRollbackView,
)
from krim_agent.workspace_missing_directory_rollback_control import (
    MissingDirectoryRollbackCancel,
    MissingDirectoryRollbackQuery,
    MissingDirectoryRollbackQueryResult,
)
from krim_agent.workspace_tree_move import TreeMoveOperationRequest, TreeMoveQueryKind

Identifier = Annotated[str, Path(min_length=1, max_length=36, pattern=IDENTIFIER)]


def _parameters(request: Request, allowed: set[str]) -> None:
    items = request.query_params.multi_items()
    names = {name for name, _ in items}
    if not names <= allowed or len(names) != len(items):
        raise ServiceError(
            "workspace_missing_directory_rollback_parameters", "回退参数未知或重复", 422
        )


async def _empty_body(request: Request) -> None:
    async for chunk in request.stream():
        if chunk:
            raise ServiceError(
                "workspace_missing_directory_rollback_parameters", "此动作不接受请求体", 422
            )


def register_missing_directory_rollback_routes(app: FastAPI, service: AgentService) -> None:
    router = APIRouter(prefix="/v1/agents/{agent_id}/workspace/missing-directory-rollbacks")

    @router.post("/preview", status_code=202)
    async def preview(
        request: Request,
        agent_id: Identifier,
        payload: MissingDirectoryRollbackPreview,
        confirmed: bool = Query(default=False),
    ) -> MissingDirectoryRollbackView:
        _parameters(request, {"confirmed"})
        return await service.missing_directory_rollback_preview(
            agent_id, payload, confirmed=confirmed
        )

    @router.post("/change", status_code=202)
    async def change(
        request: Request,
        agent_id: Identifier,
        payload: MissingDirectoryRollbackAction,
        confirmed: bool = Query(default=False),
    ) -> MissingDirectoryRollbackAttemptView:
        _parameters(request, {"confirmed"})
        return await service.missing_directory_rollback_change(
            agent_id, payload, confirmed=confirmed
        )

    @router.get("/query")
    async def query(
        request: Request,
        agent_id: Identifier,
        kind: Annotated[TreeMoveQueryKind, Query()],
        identifier: str | None = Query(default=None),
        request_key: str | None = Query(default=None),
        after: str | None = Query(default=None),
        limit: int = Query(default=4, ge=1, le=4),
        confirmed: bool = Query(default=False),
    ) -> MissingDirectoryRollbackQueryResult:
        _parameters(request, {"kind", "identifier", "request_key", "after", "limit", "confirmed"})
        await _empty_body(request)
        try:
            selected = MissingDirectoryRollbackQuery(
                kind=kind, identifier=identifier, request_key=request_key, after=after, limit=limit
            )
        except (ValueError, TypeError):
            raise ServiceError(
                "workspace_missing_directory_rollback_parameters", "回退查询身份与分页不一致", 422
            ) from None
        return await service.missing_directory_rollback_query(
            agent_id, selected, confirmed=confirmed
        )

    @router.post("/operations/{operation_id}/reconcile", status_code=202)
    async def reconcile(
        request: Request,
        agent_id: Identifier,
        operation_id: Identifier,
        confirmed: bool = Query(default=False),
    ) -> MissingDirectoryRollbackAttemptView:
        _parameters(request, {"confirmed"})
        await _empty_body(request)
        return await service.missing_directory_rollback_reconcile(
            agent_id, TreeMoveOperationRequest(operation_id=operation_id), confirmed=confirmed
        )

    @router.post("/{proposal_id}/cancel", status_code=202)
    async def cancel(
        request: Request,
        agent_id: Identifier,
        proposal_id: Identifier,
        confirmed: bool = Query(default=False),
    ) -> MissingDirectoryRollbackView:
        _parameters(request, {"confirmed"})
        await _empty_body(request)
        return await service.missing_directory_rollback_cancel(
            agent_id, MissingDirectoryRollbackCancel(proposal_id=proposal_id), confirmed=confirmed
        )

    app.include_router(router)
