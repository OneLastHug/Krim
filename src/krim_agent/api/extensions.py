"""个人Skills、私有记忆与会话扩展配置HTTP入口；校验、授权与幂等归ExtensionService。"""

from typing import Any

from fastapi import APIRouter, FastAPI, Query, Request

from krim_agent.contracts import ServiceError
from krim_agent.extensions import (
    ExtensionService,
    MemoryCreate,
    MemoryDelete,
    MemoryUpdate,
    Preference,
    SessionExtensions,
    SkillImport,
    SkillRemove,
    SkillUpdate,
)

from .sessions import application, principal


def _extensions(request: Request) -> ExtensionService:
    result = application(request).extensions
    if result is None:
        raise ServiceError("schema_upgrade_required")
    return result


def mount_extensions(app: FastAPI) -> None:
    router = APIRouter()

    @router.get("/v2/me/skills")
    async def skills(request: Request) -> Any:
        return await _extensions(request).catalog(principal(request))

    @router.get("/v2/me/skills/{skill_id}")
    async def skill(skill_id: str, request: Request) -> Any:
        return await _extensions(request).skill(principal(request), skill_id)

    @router.post("/v2/me/skills", status_code=201)
    async def import_skill(body: SkillImport, request: Request) -> Any:
        return await _extensions(request).import_skill(principal(request), body)

    @router.put("/v2/me/skills/{skill_id}")
    async def update_skill(skill_id: str, body: SkillUpdate, request: Request) -> Any:
        return await _extensions(request).import_skill(principal(request), body, skill_id)

    @router.delete("/v2/me/skills/{skill_id}")
    async def delete_skill(skill_id: str, body: SkillRemove, request: Request) -> Any:
        return await _extensions(request).delete_skill(principal(request), skill_id, body)

    @router.get("/v2/me/memories")
    async def memories(request: Request) -> Any:
        return await _extensions(request).list_memories(principal(request))

    @router.get("/v2/me/memories/page")
    async def memory_page(
        request: Request,
        offset: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=200),
        query: str = "",
    ) -> Any:
        return await _extensions(request).memory_page(
            principal(request), offset=offset, limit=limit, query=query
        )

    @router.get("/v2/me/memories/{memory_id}")
    async def read_memory(memory_id: str, request: Request) -> Any:
        return await _extensions(request).read_memory(principal(request), memory_id)

    @router.post("/v2/me/memories", status_code=201)
    async def create_memory(body: MemoryCreate, request: Request) -> Any:
        return await _extensions(request).memory(principal(request), body)

    @router.put("/v2/me/memories/{memory_id}")
    async def update_memory(memory_id: str, body: MemoryUpdate, request: Request) -> Any:
        return await _extensions(request).memory(principal(request), body, memory_id)

    @router.delete("/v2/me/memories/{memory_id}")
    async def delete_memory(memory_id: str, body: MemoryDelete, request: Request) -> Any:
        return await _extensions(request).memory(principal(request), body, memory_id)

    project_base = "/v2/workspaces/{workspace_id}/projects/{project_id}/memories"

    @router.get(project_base)
    async def project_memories(
        workspace_id: str,
        project_id: str,
        request: Request,
        offset: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=200),
        query: str = "",
    ) -> Any:
        return await _extensions(request).memory_page(
            principal(request),
            workspace_id=workspace_id,
            project_id=project_id,
            offset=offset,
            limit=limit,
            query=query,
        )

    @router.get(project_base + "/{memory_id}")
    async def project_memory(
        workspace_id: str, project_id: str, memory_id: str, request: Request
    ) -> Any:
        return await _extensions(request).read_memory(
            principal(request), memory_id, workspace_id=workspace_id, project_id=project_id
        )

    @router.post(project_base, status_code=201)
    async def create_project_memory(
        workspace_id: str, project_id: str, body: MemoryCreate, request: Request
    ) -> Any:
        return await _extensions(request).memory(
            principal(request), body, workspace_id=workspace_id, project_id=project_id
        )

    @router.put(project_base + "/{memory_id}")
    async def update_project_memory(
        workspace_id: str, project_id: str, memory_id: str, body: MemoryUpdate, request: Request
    ) -> Any:
        return await _extensions(request).memory(
            principal(request), body, memory_id, workspace_id=workspace_id, project_id=project_id
        )

    @router.delete(project_base + "/{memory_id}")
    async def delete_project_memory(
        workspace_id: str, project_id: str, memory_id: str, body: MemoryDelete, request: Request
    ) -> Any:
        return await _extensions(request).memory(
            principal(request), body, memory_id, workspace_id=workspace_id, project_id=project_id
        )

    @router.get("/v2/me/memory-preference")
    async def read_preference(request: Request) -> Any:
        return await _extensions(request).preference(principal(request))

    @router.put("/v2/me/memory-preference")
    async def write_preference(body: Preference, request: Request) -> Any:
        return await _extensions(request).preference(principal(request), body)

    base = "/v2/workspaces/{workspace_id}/sessions/{session_id}/extensions"

    @router.get(base)
    async def read_session_config(workspace_id: str, session_id: str, request: Request) -> Any:
        return await _extensions(request).session_config(
            principal(request), workspace_id, session_id
        )

    @router.put(base)
    async def write_session_config(workspace_id: str, session_id: str, request: Request) -> Any:
        try:
            body = SessionExtensions.model_validate_json(await request.body())
        except ValueError:
            raise ServiceError("invalid_input", 422) from None
        return await _extensions(request).session_config(
            principal(request), workspace_id, session_id, body
        )

    app.include_router(router)
