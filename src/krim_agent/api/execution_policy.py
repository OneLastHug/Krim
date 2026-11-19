"""单会话执行模式与本会话允许规则的GET/PUT/DELETE：owner严格、CAS与请求幂等。"""

from typing import Literal, cast

from fastapi import APIRouter, FastAPI, Request
from pydantic import Field

from krim_agent.composition import Application
from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.security import PrincipalContext
from krim_agent.sessions.execution_policy import (
    SessionExecutionPolicyUpdate,
    SessionExecutionPolicyView,
    commit_mode,
    execution_gate,
    notify_mode_change,
)
from krim_agent.storage.execution_policy import (
    clear_session_allow_rules,
    count_allow_rules,
    get_policy,
    get_session_project,
)


class SessionAllowRulesView(StrictModel):
    session_id: str
    rule_count: int = Field(ge=0)


class SessionAllowRulesClear(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")


class SessionAllowRulesClearView(StrictModel):
    session_id: str
    cleared_count: int = Field(ge=0)
    rule_count: int = Field(ge=0)


class SessionProjectView(StrictModel):
    """已批准项目详情最小只读视图；sandbox_status为环境实际状态或unprovisioned。"""

    session_id: str
    project_id: str
    title: str
    owner_id: str
    project_access: Literal["read", "write"]
    is_creator: bool
    sandbox_status: str


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


def mount_execution_policy(app: FastAPI) -> None:
    router = APIRouter()
    base = "/v2/workspaces/{workspace_id}/sessions/{session_id}/execution-policy"

    @router.get(base)
    async def read_policy(
        workspace_id: str, session_id: str, request: Request
    ) -> SessionExecutionPolicyView:
        actor = _live(request)
        actor.member(workspace_id)
        return await get_policy(
            _application(request).store,
            principal=actor,
            workspace=workspace_id,
            session_id=session_id,
        )

    @router.put(base)
    async def write_policy(
        workspace_id: str,
        session_id: str,
        body: SessionExecutionPolicyUpdate,
        request: Request,
    ) -> SessionExecutionPolicyView:
        actor = _live(request)
        actor.member(workspace_id, write=True)
        result = await commit_mode(
            _application(request).store,
            principal=actor,
            workspace=workspace_id,
            session_id=session_id,
            request_key=body.request_key,
            expected_revision=body.expected_revision,
            mode=body.mode,
        )
        try:
            # 提交已完成，通知只是runtime重评的加速信号；订阅方异常不回滚结果。
            notify_mode_change(result)
        except Exception:
            pass
        return result

    @router.get(base + "/allow-rules")
    async def read_allow_rules(
        workspace_id: str, session_id: str, request: Request
    ) -> SessionAllowRulesView:
        actor = _live(request)
        actor.member(workspace_id)
        count = await count_allow_rules(
            _application(request).store,
            principal=actor,
            workspace=workspace_id,
            session_id=session_id,
        )
        return SessionAllowRulesView(session_id=session_id, rule_count=count)

    @router.delete(base + "/allow-rules")
    async def discard_allow_rules(
        workspace_id: str,
        session_id: str,
        body: SessionAllowRulesClear,
        request: Request,
    ) -> SessionAllowRulesClearView:
        actor = _live(request)
        actor.member(workspace_id, write=True)
        # 与模式PUT共用同一execution_gate；删除与幂等台账在存储层同一事务。
        async with execution_gate(session_id):
            result = await clear_session_allow_rules(
                _application(request).store,
                principal=actor,
                workspace=workspace_id,
                session_id=session_id,
                request_key=body.request_key,
            )
        return SessionAllowRulesClearView.model_validate(result)

    @router.get(base + "/project")
    async def read_session_project(
        workspace_id: str, session_id: str, request: Request
    ) -> SessionProjectView:
        actor = _live(request)
        actor.member(workspace_id)
        return SessionProjectView.model_validate(
            await get_session_project(
                _application(request).store,
                principal=actor,
                workspace=workspace_id,
                session_id=session_id,
            )
        )

    app.include_router(router)
