"""Thin HTTP transport for session planning snapshots."""

from typing import Any

from fastapi import APIRouter, FastAPI, Request

from krim_agent.contracts import ServiceError
from krim_agent.planning.contracts import PlanRead, PlanSnapshot, PlanUpdate

from .sessions import application, principal


def _planning(request: Request) -> Any:
    result = getattr(application(request), "planning", None)
    if result is None:
        raise ServiceError("schema_upgrade_required")
    return result


def _read_query(request: Request) -> PlanRead:
    params = request.query_params
    allowed = {"revision", "offset", "limit", "statuses"}
    if any(key not in allowed for key in params.keys()):
        raise ServiceError("invalid_input", 422)

    values: dict[str, object] = {}
    try:
        for key in ("revision", "offset", "limit"):
            supplied = params.getlist(key)
            if len(supplied) > 1:
                raise ValueError("Repeated scalar query parameter")
            if supplied:
                values[key] = int(supplied[0])
        values["statuses"] = tuple(params.getlist("statuses"))
        return PlanRead.model_validate(values)
    except ValueError:
        raise ServiceError("invalid_input", 422) from None


def mount_planning(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v2/workspaces/{workspace_id}/sessions/{session_id}/plan"

    @router.get(base)
    async def read_plan(workspace_id: str, session_id: str, request: Request) -> Any:
        snapshot: PlanSnapshot = await _planning(request).read(
            principal(request), workspace_id, session_id, _read_query(request)
        )
        return snapshot.model_dump(mode="json")

    @router.put(base)
    async def update_plan(workspace_id: str, session_id: str, request: Request) -> Any:
        try:
            body = PlanUpdate.model_validate_json(await request.body())
        except ValueError:
            raise ServiceError("invalid_input", 422) from None
        snapshot: PlanSnapshot = await _planning(request).update(
            principal(request), workspace_id, session_id, body
        )
        return snapshot.model_dump(mode="json")

    app.include_router(router)
