"""账号管理 HTTP 入口；身份、事务和密钥生命周期归 IdentityService。"""

from typing import Any, cast

from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, JsonValue

from krim_agent.accounts.service import IdentityService
from krim_agent.contracts import ServiceError, fingerprint, parse_json
from krim_agent.providers.openai_compatible import OpenAICompatibleProvider
from krim_agent.security import PrincipalContext

from .sessions import application, principal


def _identities(request: Request) -> IdentityService:
    identities = application(request).identities
    if identities is None or not identities.managed:
        raise ServiceError("account_management_unavailable", 409)
    return identities


def _body(value: Any) -> Any:
    return value.model_dump(mode="json") if isinstance(value, BaseModel) else value


def _mutation(body: dict[str, Any], *, expected: str | None = None) -> tuple[str, dict[str, Any]]:
    values = dict(body)
    key = values.pop("request_key", None)
    if not isinstance(key, str) or not 1 <= len(key) <= 128 or any(ord(c) < 33 for c in key):
        raise ServiceError("invalid_input", 422)
    if expected is not None and (type(values.get(expected)) is not int or values[expected] < 1):
        raise ServiceError("invalid_input", 422)
    return key, values


def _secret_response(value: Any) -> JSONResponse:
    return JSONResponse(_body(value), headers={"Cache-Control": "no-store"})


def mount_accounts(app: FastAPI) -> None:
    router = APIRouter()

    @router.get("/v2/me/profile")
    async def profile(request: Request) -> Any:
        return await _identities(request).profile(principal(request))

    @router.patch("/v2/me/profile")
    async def update_profile(body: dict[str, Any], request: Request) -> Any:
        actor = principal(request)
        # 普通账号不能通过个人资料入口修改平台角色或账号状态。
        if set(body) - {"display_name", "expected_revision", "request_key"}:
            raise ServiceError("invalid_input", 422)
        key, values = _mutation(body, expected="expected_revision")
        return await _identities(request).update_account(
            actor, actor.principal_id, values, request_key=key
        )

    @router.post("/v2/me/key/rotate")
    async def rotate(body: dict[str, Any], request: Request) -> JSONResponse:
        key, values = _mutation(body)
        return _secret_response(
            await _identities(request).rotate_key(principal(request), values, request_key=key)
        )

    @router.post("/v2/me/key/revoke")
    async def revoke_self(body: dict[str, Any], request: Request) -> Any:
        actor = principal(request)
        key, values = _mutation(body)
        return await _identities(request).revoke_key(
            actor, actor.principal_id, values, request_key=key
        )

    @router.get("/v2/admin/accounts")
    async def accounts(
        request: Request,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=20, ge=1, le=100),
        status: str | None = Query(default=None, pattern="^(active|disabled)$"),
        query: str | None = Query(default=None, max_length=64),
    ) -> Any:
        return await _identities(request).list_accounts(
            principal(request), offset=offset, limit=limit, status=status, query=query
        )

    @router.post("/v2/admin/accounts", status_code=201)
    async def create(body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body)
        return await _identities(request).create_account(
            principal(request), values, request_key=key
        )

    @router.get("/v2/admin/accounts/{principal_id}")
    async def account(principal_id: str, request: Request) -> Any:
        return await _identities(request).account(principal(request), principal_id)

    @router.patch("/v2/admin/accounts/{principal_id}")
    async def update(principal_id: str, body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body, expected="expected_revision")
        return await _identities(request).update_account(
            principal(request), principal_id, values, request_key=key
        )

    @router.post("/v2/admin/accounts/{principal_id}/key/issue")
    async def issue(principal_id: str, body: dict[str, Any], request: Request) -> JSONResponse:
        key, values = _mutation(body)
        return _secret_response(
            await _identities(request).issue_key(
                principal(request), principal_id, values, request_key=key
            )
        )

    @router.post("/v2/admin/accounts/{principal_id}/key/revoke")
    async def revoke(principal_id: str, body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body)
        return await _identities(request).revoke_key(
            principal(request), principal_id, values, request_key=key
        )

    @router.get("/v2/admin/accounts/{principal_id}/memberships")
    async def read_memberships(principal_id: str, request: Request) -> dict[str, JsonValue]:
        return await _identities(request).memberships(principal(request), principal_id)

    @router.put("/v2/admin/accounts/{principal_id}/memberships")
    async def memberships(principal_id: str, body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body, expected="expected_generation")
        return await _identities(request).set_memberships(
            principal(request), principal_id, values, request_key=key
        )

    @router.get("/v2/admin/account-events")
    async def events(
        request: Request,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=20, ge=1, le=100),
        principal_id: str | None = Query(default=None, max_length=64),
        request_key: str | None = Query(default=None, min_length=1, max_length=128),
    ) -> Any:
        return await _identities(request).events(
            principal(request),
            offset=offset,
            limit=limit,
            principal_id=principal_id,
            request_key=request_key,
        )

    @router.get("/v2/targets/{target_key}/connection-profile")
    async def connection_profile(target_key: str, request: Request) -> dict[str, JsonValue]:
        actor: PrincipalContext = principal(request)
        current = application(request)
        if current.identities is not None and current.identities.managed:
            actor = current.identities.validate(actor)
        if not actor.memberships:
            raise ServiceError("not_found", 404)
        binding = current.service.bindings.get(target_key)
        if binding is None:
            raise ServiceError("not_found", 404)
        binding.assert_current()
        remote = isinstance(binding.loop.provider, OpenAICompatibleProvider)
        return {
            "profile_id": "connection-" + target_key,
            "display_name": (
                current.settings.gateway_display_name or "API 网关" if remote else "本地模式"
            ),
            "mode": "api_gateway" if remote else "local",
            "target_key": target_key,
            "target_fingerprint": fingerprint(
                cast(dict[str, JsonValue], parse_json(binding.descriptor_json))
            ),
        }

    app.include_router(router)
