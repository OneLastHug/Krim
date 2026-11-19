"""v2会话侧栏呈现API；每主体每工作区一份布局，CAS修订覆盖写。"""

from typing import cast

from fastapi import APIRouter, FastAPI, Request

from krim_agent.composition import Application
from krim_agent.contracts import ServiceError
from krim_agent.security import PrincipalContext
from krim_agent.sessions.types import SessionSidebarDocument, SessionSidebarWrite
from krim_agent.storage.session_sidebar import get as read_sidebar
from krim_agent.storage.session_sidebar import put as write_sidebar


def _application(request: Request) -> Application:
    return cast(Application, request.app.state.application)


def _principal(request: Request) -> PrincipalContext:
    return cast(PrincipalContext, request.state.principal)


def _live(request: Request) -> PrincipalContext:
    """托管模式下对请求主体做当前授权复核；拒绝旧generation/已停用主体。"""
    identities = _application(request).identities
    principal = _principal(request)
    if identities is None or not getattr(identities, "managed", False):
        return principal
    try:
        return identities.validate(principal)
    except ServiceError:
        raise ServiceError("permission_denied", 403) from None


def mount_session_sidebar(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v2/workspaces/{workspace_id}/session-sidebar"

    @router.get(base)
    async def read(workspace_id: str, request: Request) -> SessionSidebarDocument:
        actor = _live(request)
        return await read_sidebar(
            _application(request).store, principal=actor, workspace=workspace_id
        )

    @router.put(base)
    async def write(workspace_id: str, request: Request) -> SessionSidebarDocument:
        # 列表字段走JSON解析；FastAPI python模式严格校验不接受JSON数组作tuple。
        try:
            body = SessionSidebarWrite.model_validate_json(await request.body())
        except ValueError:
            raise ServiceError("invalid_input", 422) from None
        actor = _live(request)
        return await write_sidebar(
            _application(request).store, principal=actor, workspace=workspace_id, body=body
        )

    app.include_router(router)
