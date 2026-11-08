"""沙箱管理 HTTP API；未启用时不回落宿主执行。"""

from typing import Any

from fastapi import APIRouter, FastAPI, Query, Request

from krim_agent.api.accounts import _mutation
from krim_agent.api.sessions import application, principal
from krim_agent.contracts import ServiceError
from krim_agent.sandbox.service import SandboxService


def _service(request: Request) -> SandboxService:
    service = application(request).sandbox
    if service is None:
        raise ServiceError("sandbox_management_unavailable", 409)
    return service


def mount_sandbox(app: FastAPI) -> None:
    router = APIRouter()

    @router.get("/v2/me/sandbox")
    async def environment(request: Request) -> Any:
        return await _service(request).environment(principal(request))

    @router.get("/v2/me/sandbox/projects")
    async def projects(
        request: Request,
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=20, ge=1, le=100),
    ) -> Any:
        return await _service(request).list_projects(principal(request), offset=offset, limit=limit)

    @router.post("/v2/me/sandbox/projects", status_code=201)
    async def create_project(body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body)
        return await _service(request).create_project(principal(request), values, request_key=key)

    @router.get("/v2/me/sandbox/projects/{project_id}")
    async def project(project_id: str, request: Request) -> Any:
        return await _service(request).project(principal(request), project_id)

    @router.get("/v2/me/sandbox/sessions/{session_id}/project")
    async def session_project(session_id: str, request: Request) -> Any:
        from krim_agent.sandbox.bindings import read_binding

        return await read_binding(_service(request), principal(request), session_id)

    @router.put("/v2/me/sandbox/sessions/{session_id}/project")
    async def bind_session_project(session_id: str, body: dict[str, Any], request: Request) -> Any:
        from krim_agent.sandbox.bindings import bind_project

        key, values = _mutation(body)
        return await bind_project(
            _service(request), principal(request), session_id, values, request_key=key
        )

    @router.get("/v2/me/sandbox/operations")
    async def operation(
        request: Request, request_key: str = Query(min_length=1, max_length=128)
    ) -> Any:
        return await _service(request).operation(principal(request), request_key)

    @router.post("/v2/me/sandbox/provision")
    async def provision(body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body)
        if values:
            raise ServiceError("invalid_input", 422)
        return await _service(request).provision(principal(request), request_key=key)

    @router.post("/v2/me/sandbox/projects/{project_id}/provision")
    async def provision_project(project_id: str, body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body)
        if values:
            raise ServiceError("invalid_input", 422)
        return await _service(request).provision(
            principal(request), request_key=key, project_id=project_id
        )

    @router.get("/v2/me/sandbox/projects/{project_id}/files")
    async def directory(
        project_id: str,
        request: Request,
        path: str = Query(default=".", max_length=512),
        limit: int = Query(default=100, ge=1, le=100),
        offset: int = Query(default=0, ge=0, le=4096),
    ) -> Any:
        return await _service(request).directory(
            principal(request), project_id, path=path, limit=limit, offset=offset
        )

    @router.get("/v2/me/sandbox/projects/{project_id}/search")
    async def search_files(
        project_id: str,
        request: Request,
        query: str = Query(min_length=1, max_length=256),
        path: str = Query(default=".", max_length=512),
        limit: int = Query(default=50, ge=1, le=100),
    ) -> Any:
        return await _service(request).search_files(
            principal(request), project_id, query=query, path=path, limit=limit
        )

    @router.get("/v2/me/sandbox/projects/{project_id}/file")
    async def read_file(
        project_id: str,
        request: Request,
        path: str = Query(max_length=512),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=65536, ge=1, le=65536),
    ) -> Any:
        return await _service(request).read_file(
            principal(request), project_id, path=path, offset=offset, limit=limit
        )

    @router.post("/v2/me/sandbox/projects/{project_id}/commands/{operation_id}/reconcile")
    async def reconcile_command(
        project_id: str, operation_id: str, body: dict[str, Any], request: Request
    ) -> Any:
        if set(body) != {"expected_generation", "confirmed"}:
            raise ServiceError("invalid_input", 422)
        return await _service(request).reconcile_command(
            principal(request),
            project_id,
            operation_id,
            expected_generation=body["expected_generation"],
            confirmed=body["confirmed"],
        )

    @router.get("/v2/admin/accounts/{principal_id}/sandbox")
    async def admin_environment(principal_id: str, request: Request) -> Any:
        return await _service(request).environment(
            principal(request), principal_id, require_admin=True
        )

    @router.put("/v2/admin/accounts/{principal_id}/sandbox/quota")
    async def configure(principal_id: str, body: dict[str, Any], request: Request) -> Any:
        key, values = _mutation(body)
        return await _service(request).configure_quota(
            principal(request), principal_id, values, request_key=key
        )

    app.include_router(router)
    from krim_agent.api.sandbox_maintenance import mount_sandbox_maintenance
    mount_sandbox_maintenance(app)
