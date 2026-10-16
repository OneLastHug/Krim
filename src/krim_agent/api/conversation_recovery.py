"""会话恢复控制面；仅原Agent/Owner范围，不接受上传消息或工具回执。"""

from typing import Annotated

from fastapi import APIRouter, FastAPI, Path, Query, Request

from krim_agent.application.agents import AgentService
from krim_agent.conversation_recovery import (
    ID,
    KEY,
    ConversationRecoveryPreview,
    ConversationRecoveryRecord,
    ConversationRecoveryRequest,
)
from krim_agent.protocol.domain import ServiceError

Identifier = Annotated[str, Path(pattern=ID, max_length=36)]


def parameters(request: Request, allowed: set[str]) -> None:
    items = request.query_params.multi_items()
    names = {name for name, _ in items}
    if not names <= allowed or len(names) != len(items):
        raise ServiceError("conversation_recovery_parameters", "会话恢复参数未知或重复", 422)


async def empty_body(request: Request) -> None:
    async for chunk in request.stream():
        if chunk:
            raise ServiceError("conversation_recovery_parameters", "恢复只读查询不接受请求体", 422)


def register_conversation_recovery_routes(app: FastAPI, service: AgentService) -> None:
    router = APIRouter(prefix="/v1/agents/{agent_id}/conversation-recovery")

    @router.get("/preview")
    async def preview(request: Request, agent_id: Identifier) -> ConversationRecoveryPreview:
        parameters(request, set())
        await empty_body(request)
        return await service.conversation_recovery_preview(agent_id)

    @router.post("", status_code=202)
    async def apply(
        request: Request,
        agent_id: Identifier,
        payload: ConversationRecoveryRequest,
        confirmed: bool = Query(default=False),
    ) -> ConversationRecoveryRecord:
        parameters(request, {"confirmed"})
        return await service.conversation_recovery_apply(agent_id, payload, confirmed=confirmed)

    @router.get("/by-key")
    async def by_key(
        request: Request, agent_id: Identifier, request_key: str = Query(pattern=KEY)
    ) -> ConversationRecoveryRecord | None:
        parameters(request, {"request_key"})
        await empty_body(request)
        return await service.conversation_recovery_by_key(agent_id, request_key)

    app.include_router(router)
