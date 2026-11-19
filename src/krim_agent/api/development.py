"""Authenticated project preparation and authorization for the development target."""

import re
from typing import Any

from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import Field, SecretStr, field_validator
from sqlalchemy import select

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.development.contracts import EnvironmentHandle, Identifier, PreviewHandle
from krim_agent.development.service import DevelopmentService
from krim_agent.project_access.git import GitAction, GitIdentity, canonical_repo, exact_ref
from krim_agent.project_access.types import Action
from krim_agent.storage.development_models import development_environments

from .accounts import _mutation
from .sessions import application, principal


def _default_actions() -> list[Action]:
    return ["shell.execute"]


class GrantBody(StrictModel):
    expected_revision: int = Field(ge=0)
    expires_at: float
    requires_approval: bool = True
    actions: list[Action] = Field(
        default_factory=_default_actions, min_length=1, max_length=7,
    )

    @field_validator("actions")
    @classmethod
    def unique_actions(cls, value: list[Action]) -> list[Action]:
        if len(set(value)) != len(value):
            raise ValueError("duplicate project action")
        return value


class GitGrantBody(StrictModel):
    expected_revision: int = Field(ge=0)
    repository: str = Field(min_length=1, max_length=512)
    refs: list[str] = Field(min_length=1, max_length=256)
    actions: list[GitAction] = Field(min_length=1, max_length=4)
    identity: GitIdentity
    expires_at: float
    network_policy_ref: Identifier | None = None
    credential_secret_id: Identifier | None = None
    credential_username: str = Field(default="oauth2", min_length=1, max_length=128)
    revoked: bool = False

    _repository = field_validator("repository")(canonical_repo)

    @field_validator("refs")
    @classmethod
    def validate_refs(cls, value: list[str]) -> list[str]:
        try:
            if len(set(value)) != len(value):
                raise ValueError
            return [exact_ref(item) for item in value]
        except ValueError:
            raise ValueError("invalid Git refs") from None

    @field_validator("actions")
    @classmethod
    def validate_actions(cls, value: list[GitAction]) -> list[GitAction]:
        if len(set(value)) != len(value):
            raise ValueError("duplicate Git action")
        return value

    @field_validator("credential_username")
    @classmethod
    def validate_credential_username(cls, value: str) -> str:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@+-]{0,127}", value) is None:
            raise ValueError("invalid credential username")
        return value


class SecretBody(StrictModel):
    expected_revision: int = Field(ge=0)
    expires_at: float
    value: SecretStr = Field(min_length=1, max_length=12000, repr=False, exclude=True)
    revoked: bool = False

    @field_validator("value")
    @classmethod
    def printable_token(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if not raw or not raw.isascii() or any(not 33 <= ord(char) <= 126 for char in raw):
            raise ValueError("credential token must be printable ASCII")
        return value


class ProjectSecretBody(StrictModel):
    expected_revision: int = Field(ge=0)
    expires_at: float
    value: SecretStr = Field(min_length=1, max_length=8192, repr=False, exclude=True)
    revoked: bool = False

    @field_validator("value")
    @classmethod
    def bounded_value(cls, value: SecretStr) -> SecretStr:
        if not 1 <= len(value.get_secret_value().encode("utf-8")) <= 8192:
            raise ValueError("project secret exceeds byte limit")
        return value


class DownloadPolicyBody(StrictModel):
    expected_revision: int = Field(ge=0)
    hosts: list[str] = Field(min_length=1, max_length=256)
    expires_at: float
    revoked: bool = False


class PreviewRegistrationBody(StrictModel):
    request_key: str = Field(min_length=1, max_length=128)
    process_id: Identifier
    port: int = Field(ge=1, le=65535, strict=True)


def preview_origin(request: Request, selected: DevelopmentService) -> None:
    configuration = selected.configuration.preview
    if configuration is None:
        raise ServiceError("preview_unavailable", 503)
    origins = request.headers.getlist("origin")
    hosts = request.headers.getlist("host")
    authorization = request.headers.getlist("authorization")
    fetch_site = request.headers.getlist("sec-fetch-site")
    if (
        origins != [configuration.control_origin]
        or len(hosts) != 1
        or request.scope["scheme"] + "://" + hosts[0] != configuration.control_origin
        or len(authorization) != 1
        or not authorization[0].startswith("Bearer ")
        or (fetch_site and fetch_site != ["same-origin"])
    ):
        raise ServiceError("origin_denied", 403)


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
            principal(request), project_id, expected_revision=body.expected_revision,
            expires_at=body.expires_at, requires_approval=body.requires_approval,
            actions=tuple(body.actions),
        )

    @router.put("/{project_id}/git-grants")
    async def git_grant(project_id: str, body: GitGrantBody, request: Request) -> Any:
        return await service(request).configure_git(
            principal(request), project_id,
            repository=body.repository, refs=tuple(body.refs), actions=tuple(body.actions),
            identity=body.identity, expected_revision=body.expected_revision,
            expires_at=body.expires_at, network_policy_ref=body.network_policy_ref,
            credential_secret_id=body.credential_secret_id,
            credential_username=body.credential_username, revoked=body.revoked,
        )

    @router.put("/{project_id}/git-secrets/{secret_id}")
    async def git_secret(
        project_id: str, secret_id: Identifier, body: SecretBody, request: Request,
    ) -> Any:
        return await service(request).configure_git_secret(
            principal(request), project_id, secret_id,
            value=body.value.get_secret_value(), expected_revision=body.expected_revision,
            expires_at=body.expires_at, revoked=body.revoked,
        )

    @router.put("/{project_id}/secrets/{secret_id}")
    async def project_secret(
        project_id: str, secret_id: Identifier, body: ProjectSecretBody, request: Request,
    ) -> Any:
        return await service(request).configure_project_secret(
            principal(request), project_id, secret_id,
            value=body.value.get_secret_value(), expected_revision=body.expected_revision,
            expires_at=body.expires_at, revoked=body.revoked,
        )

    @router.post("/{project_id}/stop")
    async def stop(project_id: str, request: Request) -> EnvironmentHandle:
        handle = await project_environment(request, project_id)
        return await service(request).environments.stop(handle)

    @router.put("/{project_id}/download-policies/{policy_id}")
    async def download_policy(
        project_id: str, policy_id: Identifier, body: DownloadPolicyBody, request: Request,
    ) -> Any:
        return await service(request).configure_download_policy(
            principal(request), project_id, policy_id,
            expected_revision=body.expected_revision, hosts=tuple(body.hosts),
            expires_at=body.expires_at, revoked=body.revoked,
        )

    @router.get("/{project_id}/browser-artifacts/{run_id}/{reference}")
    async def browser_artifact(
        project_id: str, run_id: str, reference: str, request: Request,
    ) -> Response:
        data, _ = await service(request).read_browser_artifact(
            principal(request), project_id, run_id, reference,
        )
        return Response(data, media_type="image/png", headers={
            "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
            "Content-Disposition": "inline; filename=screenshot.png",
        })

    @router.post("/{project_id}/previews")
    async def register_preview(
        project_id: str, body: PreviewRegistrationBody, request: Request,
    ) -> PreviewHandle:
        return await service(request).register_preview(
            principal(request), project_id, body.process_id, body.port,
            request_key=body.request_key,
        )

    @app.get("/v2/me/development/previews/{preview_id}")
    async def inspect_preview(
        preview_id: str, request: Request, wait_seconds: float = Query(default=0, ge=0, le=20),
    ) -> PreviewHandle:
        return await service(request).inspect_preview(
            principal(request), preview_id, wait_seconds=wait_seconds,
        )

    @app.post("/v2/me/development/previews/{preview_id}/stop")
    async def stop_preview(preview_id: str, request: Request) -> PreviewHandle:
        return await service(request).stop_preview(principal(request), preview_id)

    @app.post("/api/development/previews/{preview_id}/open")
    async def open_preview(preview_id: str, request: Request) -> JSONResponse:
        selected = service(request)
        preview_origin(request, selected)
        value = await selected.open_preview(principal(request), preview_id)
        return JSONResponse(
            {"action": value.action, "ticket": value.ticket, "expires_at": value.expires_at},
            headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
        )

    app.include_router(router)
