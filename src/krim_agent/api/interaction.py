"""Owner-scoped endpoints for durable questions and plan confirmation."""

from typing import Any, cast

from fastapi import APIRouter, FastAPI, Request

from krim_agent.contracts import ServiceError
from krim_agent.interaction.contracts import AnswerRequest, CancelRequest
from krim_agent.interaction.service import InteractionService

from .sessions import application, principal


def _interactions(request: Request) -> InteractionService:
    result = getattr(application(request), "interactions", None)
    if result is None:
        raise ServiceError("schema_upgrade_required")
    return cast(InteractionService, result)


def mount_interaction(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v2/workspaces/{workspace_id}/sessions/{session_id}/interactions"

    @router.get(base)
    async def state(workspace_id: str, session_id: str, request: Request) -> Any:
        return await _interactions(request).state(
            principal(request), workspace_id, session_id
        )

    @router.get(base + "/{request_id}")
    async def read_request(
        workspace_id: str, session_id: str, request_id: str, request: Request
    ) -> Any:
        return await _interactions(request).read_request(
            principal(request), workspace_id, session_id, request_id
        )

    @router.post(base + "/{request_id}/answer")
    async def answer(
        workspace_id: str,
        session_id: str,
        request_id: str,
        body: AnswerRequest,
        request: Request,
    ) -> Any:
        return await _interactions(request).answer(
            principal(request), workspace_id, session_id, request_id, body
        )

    @router.post(base + "/{request_id}/cancel")
    async def cancel(
        workspace_id: str,
        session_id: str,
        request_id: str,
        body: CancelRequest,
        request: Request,
    ) -> Any:
        return await _interactions(request).cancel(
            principal(request), workspace_id, session_id, request_id, body
        )

    app.include_router(router)
