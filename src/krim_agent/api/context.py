"""Manual context requests share the model-boundary compaction queue."""

from typing import Any

from fastapi import APIRouter, FastAPI, Request
from pydantic import Field

from krim_agent.contracts import ServiceError, StrictModel

from .sessions import application, principal


class ContextCompactionBody(StrictModel):
    request_key: str = Field(min_length=1, max_length=128)
    expected_revision: int | None = Field(default=None, ge=0)
    reason: str | None = None


def _context(request: Request) -> Any:
    result = getattr(application(request), "context_service", None)
    if result is None:
        raise ServiceError("schema_upgrade_required")
    return result


def mount_context(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v2/workspaces/{workspace_id}/sessions/{session_id}/context/compactions"

    @router.post(base, status_code=202)
    async def schedule(workspace_id: str, session_id: str, request: Request) -> Any:
        try:
            body = ContextCompactionBody.model_validate_json(await request.body())
        except ValueError:
            raise ServiceError("invalid_input", 422) from None
        return await _context(request).schedule_compaction(
            principal(request),
            workspace_id,
            session_id,
            body.request_key,
            body.expected_revision,
            reason=body.reason,
        )

    @router.get(base)
    async def pending(workspace_id: str, session_id: str, request: Request) -> Any:
        return {
            "items": await _context(request).pending(
                principal(request),
                workspace_id,
                session_id,
            )
        }

    app.include_router(router)
