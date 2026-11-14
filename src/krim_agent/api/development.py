"""Authenticated project preparation and authorization for the development target."""

from typing import Any

from fastapi import APIRouter, FastAPI, Request
from pydantic import Field
from sqlalchemy import select

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.development.contracts import EnvironmentHandle
from krim_agent.development.service import DevelopmentService
from krim_agent.storage.development_models import development_environments

from .accounts import _mutation
from .sessions import application, principal


class GrantBody(StrictModel):
    expected_revision: int = Field(ge=0)
    expires_at: float
    requires_approval: bool = True


def service(request: Request) -> DevelopmentService:
    value = application(request).development
    if value is None:
        raise ServiceError("sandbox_management_unavailable", 409)
    return value


async def project_environment(request: Request, project_id: str) -> EnvironmentHandle:
    selected = service(request)
    actor = principal(request)
    await selected.sandbox.project(actor, project_id)
    async with selected.store.lock, selected.store.engine.connect() as conn:
        raw = (await conn.execute(select(development_environments.c.record).where(
            development_environments.c.project_id == project_id,
            development_environments.c.owner_id == actor.principal_id,
        ))).scalar_one_or_none()
    if raw is None:
        raise ServiceError("not_found", 404)
    return EnvironmentHandle.model_validate(raw["environment"])


def mount_development(app: FastAPI) -> None:
    router = APIRouter(prefix="/v2/me/development/projects")

    @router.post("/{project_id}/prepare")
    async def prepare(project_id: str, body: dict[str, Any], request: Request) -> EnvironmentHandle:
        key, values = _mutation(body)
        if values:
            raise ServiceError("invalid_input", 422)
        return await service(request).prepare_project(
            principal(request), project_id, request_key=key,
        )

    @router.get("/{project_id}/environment")
    async def environment(project_id: str, request: Request) -> EnvironmentHandle:
        handle = await project_environment(request, project_id)
        return await service(request).environments.inspect(handle)

    @router.put("/{project_id}/grant")
    async def grant(project_id: str, body: GrantBody, request: Request) -> Any:
        return await service(request).grant_project(
            principal(request), project_id, **body.model_dump(),
        )

    @router.post("/{project_id}/stop")
    async def stop(project_id: str, request: Request) -> EnvironmentHandle:
        handle = await project_environment(request, project_id)
        return await service(request).environments.stop(handle)

    app.include_router(router)
