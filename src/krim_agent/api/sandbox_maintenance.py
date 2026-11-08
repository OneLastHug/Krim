"""项目恢复、检查点维护与在线额度接口；每次写操作都显式确认。"""

from typing import Any

from fastapi import APIRouter, FastAPI, Query, Request

from krim_agent.api.accounts import _mutation
from krim_agent.api.sandbox import _service
from krim_agent.api.sessions import principal
from krim_agent.contracts import ServiceError
from krim_agent.sandbox.management import project_action


def fields(body: dict[str, Any], allowed: set[str]) -> None:
    if set(body) != allowed:
        raise ServiceError("invalid_input", 422)


def mount_sandbox_maintenance(app: FastAPI) -> None:
    router = APIRouter()
    prefix = "/v2/me/sandbox/projects/{project_id}"

    @router.get(prefix + "/checkpoints")
    async def checkpoints(
        project_id: str,
        request: Request,
        offset: int = Query(default=0, ge=0, le=1000),
        limit: int = Query(default=20, ge=1, le=100),
    ) -> Any:
        return await project_action(
            _service(request),
            principal(request),
            project_id,
            "checkpoint_list",
            {"offset": offset, "limit": limit},
        )

    @router.post(prefix + "/checkpoints/{checkpoint_id}/restore-plans")
    async def preview(project_id: str, checkpoint_id: str, request: Request) -> Any:
        return await project_action(
            _service(request),
            principal(request),
            project_id,
            "restore_preview",
            {"checkpoint_id": checkpoint_id},
        )

    @router.post(prefix + "/restore-plans/{plan_id}/confirm")
    async def confirm(project_id: str, plan_id: str, body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body)
        fields(values, {"plan_digest", "expected_generation", "selected", "confirmed"})
        if values.pop("confirmed") is not True:
            raise ServiceError("invalid_input", 422)
        generation = values.pop("expected_generation")
        return await project_action(
            _service(request),
            principal(request),
            project_id,
            "restore_confirm",
            {"plan_id": plan_id, **values},
            request_key=key,
            expected_generation=generation,
        )

    @router.get(prefix + "/restores/{operation_id}")
    async def status(project_id: str, operation_id: str, request: Request) -> Any:
        return await project_action(
            _service(request),
            principal(request),
            project_id,
            "restore_status",
            {"operation_id": operation_id},
        )

    @router.post(prefix + "/restores/{operation_id}/{decision}")
    async def continuation(
        project_id: str, operation_id: str, decision: str, body: dict[str, Any], request: Request
    ) -> Any:
        if decision not in {"resume", "reconcile", "abandon"}:
            raise ServiceError("not_found", 404)
        key, values = _mutation(body)
        fields(values, {"expected_generation", "confirmed"})
        if values["confirmed"] is not True:
            raise ServiceError("invalid_input", 422)
        return await project_action(
            _service(request),
            principal(request),
            project_id,
            "restore_" + decision,
            {"operation_id": operation_id},
            request_key=key,
            expected_generation=values["expected_generation"],
        )

    @router.put(prefix + "/checkpoints/{checkpoint_id}/retention")
    async def retention(
        project_id: str, checkpoint_id: str, body: dict[str, Any], request: Request
    ) -> Any:
        key, values = _mutation(body)
        fields(values, {"expected_generation", "pinned"})
        if type(values["pinned"]) is not bool:
            raise ServiceError("invalid_input", 422)
        return await project_action(
            _service(request),
            principal(request),
            project_id,
            "checkpoint_retention",
            {"checkpoint_id": checkpoint_id, "pinned": values["pinned"]},
            request_key=key,
            expected_generation=values["expected_generation"],
        )

    @router.post(prefix + "/checkpoint-cleanup-plans")
    async def cleanup_preview(project_id: str, request: Request) -> Any:
        return await project_action(
            _service(request), principal(request), project_id, "checkpoint_cleanup_preview", {}
        )

    @router.post(prefix + "/checkpoint-cleanup-plans/{plan_id}/confirm")
    async def cleanup_confirm(
        project_id: str, plan_id: str, body: dict[str, Any], request: Request
    ) -> Any:
        key, values = _mutation(body)
        fields(values, {"expected_generation", "plan_digest", "confirmed"})
        if values["confirmed"] is not True:
            raise ServiceError("invalid_input", 422)
        return await project_action(
            _service(request),
            principal(request),
            project_id,
            "checkpoint_cleanup_confirm",
            {"plan_id": plan_id, "plan_digest": values["plan_digest"]},
            request_key=key,
            expected_generation=values["expected_generation"],
        )

    quota_prefix = "/v2/admin/accounts/{principal_id}/sandbox"

    @router.post(quota_prefix + "/quota-plans")
    async def quota_preview(principal_id: str, body: dict[str, Any], request: Request) -> Any:
        from krim_agent.sandbox.quota_control import preview

        return await preview(_service(request), principal(request), principal_id, body)

    @router.post(quota_prefix + "/quota-plans/{plan_id}/confirm")
    async def quota_confirm(
        principal_id: str, plan_id: str, body: dict[str, Any], request: Request
    ) -> Any:
        from krim_agent.sandbox.quota_control import confirm

        key, values = _mutation(body)
        fields(values, {"expected_revision", "plan_digest", "confirmed"})
        if values["confirmed"] is not True:
            raise ServiceError("invalid_input", 422)
        return await confirm(
            _service(request),
            principal(request),
            principal_id,
            plan_id,
            values["plan_digest"],
            values["expected_revision"],
            key,
        )

    @router.get(quota_prefix + "/quota-operations/{operation_id}")
    async def quota_status(principal_id: str, operation_id: str, request: Request) -> Any:
        from krim_agent.sandbox.quota_control import status

        return await status(_service(request), principal(request), principal_id, operation_id)

    @router.post(quota_prefix + "/quota-operations/{operation_id}/cancel")
    async def quota_cancel(
        principal_id: str, operation_id: str, body: dict[str, Any], request: Request
    ) -> Any:
        from krim_agent.sandbox.quota_control import status

        fields(body, {"confirmed"})
        if body["confirmed"] is not True:
            raise ServiceError("invalid_input", 422)
        return await status(
            _service(request), principal(request), principal_id, operation_id, cancel=True
        )

    app.include_router(router)
