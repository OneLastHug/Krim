"""Owner-scoped HTTP transport for durable goals.

Routes only parse and forward; authorization, CAS, request-key idempotency and
turn binding live in :class:`~krim_agent.goals.service.GoalService`. The
service is optional on the application, so a missing ``goals`` attribute
returns ``schema_upgrade_required`` like the other feature mounts.
"""

from __future__ import annotations

from typing import Any, cast

from fastapi import APIRouter, FastAPI, Request

from krim_agent.contracts import ServiceError
from krim_agent.goals.contracts import GoalControlRequest, GoalCreateRequest, GoalRead
from krim_agent.goals.service import GoalService

from .sessions import application, principal


def _goals(request: Request) -> GoalService:
    result = getattr(application(request), "goals", None)
    if result is None:
        raise ServiceError("schema_upgrade_required")
    return cast(GoalService, result)


def mount_goals(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v2/workspaces/{workspace_id}/sessions/{session_id}/goal"

    @router.get(base)
    async def read_goal(workspace_id: str, session_id: str, request: Request) -> Any:
        params = request.query_params
        if any(key != "revision" for key in params.keys()):
            raise ServiceError("invalid_input", 422)
        supplied = params.getlist("revision")
        if len(supplied) > 1:
            raise ServiceError("invalid_input", 422)
        revision = None
        if supplied:
            try:
                revision = int(supplied[0])
            except ValueError:
                raise ServiceError("invalid_input", 422) from None
            if revision < 1:
                raise ServiceError("invalid_input", 422)
        snapshot = await _goals(request).read(
            principal(request), workspace_id, session_id, query=GoalRead(revision=revision)
        )
        return snapshot.model_dump(mode="json")

    @router.get(base + "/continuation")
    async def read_continuation(workspace_id: str, session_id: str, request: Request) -> Any:
        view = await _goals(request).latest_continuation(
            principal(request), workspace_id, session_id
        )
        return None if view is None else view.model_dump(mode="json")

    @router.post(base)
    async def create_goal(workspace_id: str, session_id: str, request: Request) -> Any:
        body = await _body(request, GoalCreateRequest)
        snapshot = await _goals(request).create(principal(request), workspace_id, session_id, body)
        return snapshot.model_dump(mode="json")

    async def control(workspace_id: str, session_id: str, request: Request, action: str) -> Any:
        body = await _body(request, GoalControlRequest)
        snapshot = await _goals(request).control(
            principal(request), workspace_id, session_id, body, action=action
        )
        return snapshot.model_dump(mode="json")

    @router.post(base + "/pause")
    async def pause(workspace_id: str, session_id: str, request: Request) -> Any:
        return await control(workspace_id, session_id, request, "pause")

    @router.post(base + "/resume")
    async def resume(workspace_id: str, session_id: str, request: Request) -> Any:
        return await control(workspace_id, session_id, request, "resume")

    @router.post(base + "/cancel")
    async def cancel(workspace_id: str, session_id: str, request: Request) -> Any:
        return await control(workspace_id, session_id, request, "cancel")

    app.include_router(router)


async def _body[T: (GoalCreateRequest, GoalControlRequest)](request: Request, kind: type[T]) -> T:
    try:
        return kind.model_validate_json(await request.body())
    except ValueError:
        raise ServiceError("invalid_input", 422) from None
