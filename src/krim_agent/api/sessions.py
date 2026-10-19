"""v2薄会话API；认证主体由LocalBoundary设置，业务只调用原服务。"""

from collections.abc import AsyncIterator
from typing import cast

from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import JsonValue

from krim_agent.composition import Application
from krim_agent.contracts import ApprovalReply, RunEvent, ServiceError, encoded
from krim_agent.security import PrincipalContext
from krim_agent.sessions.service import SessionService
from krim_agent.sessions.types import (
    ControlRequest,
    ControlView,
    SessionCreate,
    SessionView,
    TurnRequest,
    TurnView,
)


def principal(request: Request) -> PrincipalContext:
    return cast(PrincipalContext, request.state.principal)


def application(request: Request) -> Application:
    return cast(Application, request.app.state.application)


def service(request: Request) -> SessionService:
    result = application(request).sessions
    if result is None:
        raise ServiceError("schema_upgrade_required")
    return result


async def run_id(request: Request, workspace_id: str, session_id: str, turn_id: str) -> str:
    turn = await service(request).turn(principal(request), workspace_id, session_id, turn_id)
    if turn.run_id is None:
        raise ServiceError("not_terminal")
    return turn.run_id


def mount_sessions(app: FastAPI) -> None:
    router = APIRouter()

    @router.get("/v2/me")
    async def me(request: Request) -> dict[str, JsonValue]:
        actor = principal(request)
        return {
            "principal_id": actor.principal_id,
            "generation": actor.generation,
            "legacy": actor.legacy,
            "target_keys": list(application(request).service.bindings),
            "workspaces": [
                {
                    "workspace_id": m.workspace_id,
                    "role": m.role,
                    "approve": m.approve and m.role != "viewer",
                }
                for m in actor.memberships
            ],
        }

    base = "/v2/workspaces/{workspace_id}/sessions"

    @router.get(base)
    async def listing(
        workspace_id: str,
        request: Request,
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> list[SessionView]:
        return await service(request).list(principal(request), workspace_id, offset, limit)

    @router.post(base, status_code=201)
    async def create(workspace_id: str, body: SessionCreate, request: Request) -> SessionView:
        return await service(request).create(principal(request), workspace_id, body)

    @router.get(base + "/by-key/{key}")
    async def by_key(workspace_id: str, key: str, request: Request) -> SessionView:
        return await service(request).by_key(principal(request), workspace_id, key)

    @router.get(base + "/{session_id}")
    async def get(workspace_id: str, session_id: str, request: Request) -> SessionView:
        return await service(request).get(principal(request), workspace_id, session_id)

    @router.get(base + "/{session_id}/controls/by-key/{key}")
    async def control_by_key(
        workspace_id: str, session_id: str, key: str, request: Request
    ) -> ControlView:
        from krim_agent.storage.sessions import control_by_key as read_control

        await service(request).get(principal(request), workspace_id, session_id)
        return await read_control(application(request).store, session_id, key)

    @router.post(base + "/{session_id}/rename")
    async def rename(
        workspace_id: str, session_id: str, body: ControlRequest, request: Request
    ) -> ControlView:
        return await service(request).control(
            principal(request), workspace_id, session_id, "rename", body
        )

    @router.post(base + "/{session_id}/close")
    async def close(
        workspace_id: str, session_id: str, body: ControlRequest, request: Request
    ) -> ControlView:
        return await service(request).control(
            principal(request), workspace_id, session_id, "close", body
        )

    @router.post(base + "/{session_id}/resume")
    async def resume(
        workspace_id: str, session_id: str, body: ControlRequest, request: Request
    ) -> ControlView:
        return await service(request).control(
            principal(request), workspace_id, session_id, "resume", body
        )

    @router.get(base + "/{session_id}/turns")
    async def history(
        workspace_id: str,
        session_id: str,
        request: Request,
        offset: int = Query(default=0, ge=0, le=100000),
        limit: int = Query(default=20, ge=1, le=50),
    ) -> list[TurnView]:
        return await service(request).turns(
            principal(request), workspace_id, session_id, offset, limit
        )

    @router.post(base + "/{session_id}/turns", status_code=202)
    async def submit(
        workspace_id: str, session_id: str, body: TurnRequest, request: Request
    ) -> TurnView:
        return await service(request).submit(principal(request), workspace_id, session_id, body)

    @router.get(base + "/{session_id}/turns/by-key/{key}")
    async def turn_by_key(
        workspace_id: str, session_id: str, key: str, request: Request
    ) -> TurnView:
        return await service(request).turn_by_key(principal(request), workspace_id, session_id, key)

    @router.get(base + "/{session_id}/turns/{turn_id}")
    async def turn(workspace_id: str, session_id: str, turn_id: str, request: Request) -> TurnView:
        return await service(request).turn(principal(request), workspace_id, session_id, turn_id)

    @router.post(base + "/{session_id}/turns/{turn_id}/cancel")
    async def cancel(
        workspace_id: str, session_id: str, turn_id: str, body: ControlRequest, request: Request
    ) -> ControlView:
        return await service(request).control(
            principal(request), workspace_id, session_id, "cancel_turn", body, turn_id=turn_id
        )

    @router.get(base + "/{session_id}/turns/{turn_id}/events")
    async def events(
        workspace_id: str,
        session_id: str,
        turn_id: str,
        request: Request,
        from_seq: int = Query(default=0, ge=0),
        limit: int = Query(default=64, ge=1, le=64),
    ) -> list[RunEvent]:
        value = await service(request).turn(principal(request), workspace_id, session_id, turn_id)
        if value.run_id is None:
            return []
        return await application(request).service.events(value.run_id, from_seq, limit)

    @router.get(base + "/{session_id}/turns/{turn_id}/result")
    async def result(
        workspace_id: str, session_id: str, turn_id: str, request: Request
    ) -> dict[str, JsonValue]:
        value = await application(request).service.result(
            await run_id(request, workspace_id, session_id, turn_id)
        )
        return cast(dict[str, JsonValue], value.model_dump(mode="json"))

    @router.get(base + "/{session_id}/turns/{turn_id}/trace")
    async def trace(
        workspace_id: str, session_id: str, turn_id: str, request: Request
    ) -> dict[str, JsonValue]:
        value = await application(request).service.trace(
            await run_id(request, workspace_id, session_id, turn_id)
        )
        return {
            "session_id": session_id,
            "turn_id": turn_id,
            "trace": value.model_dump(mode="json"),
        }

    @router.get(base + "/{session_id}/turns/{turn_id}/approvals")
    async def approvals(
        workspace_id: str, session_id: str, turn_id: str, request: Request
    ) -> list[dict[str, JsonValue]]:
        value = await service(request).turn(principal(request), workspace_id, session_id, turn_id)
        if value.run_id is None:
            return []
        return await application(request).service.approvals(value.run_id)

    @router.post(base + "/{session_id}/turns/{turn_id}/approvals/{request_id}")
    async def reply(
        workspace_id: str,
        session_id: str,
        turn_id: str,
        request_id: str,
        body: ApprovalReply,
        request: Request,
    ) -> ControlView:
        session = await service(request).get(
            principal(request), workspace_id, session_id, approve=True
        )
        return await service(request).control(
            principal(request),
            workspace_id,
            session_id,
            "approval",
            ControlRequest(request_key=body.request_key, expected_revision=session.revision),
            turn_id=turn_id,
            approval=body,
            request_id=request_id,
        )

    @router.get(base + "/{session_id}/turns/{turn_id}/stream")
    async def stream(
        workspace_id: str,
        session_id: str,
        turn_id: str,
        request: Request,
        from_seq: int = Query(default=0, ge=0),
    ) -> StreamingResponse:
        runs = application(request).service
        current_id = await run_id(request, workspace_id, session_id, turn_id)
        await runs.events(current_id, from_seq, 1)

        async def frames() -> AsyncIterator[str]:
            try:
                async for event in runs.stream(current_id, from_seq):
                    yield (
                        ": heartbeat\n\n"
                        if event is None
                        else "id: "
                        + str(event.seq)
                        + "\nevent: "
                        + event.kind
                        + "\ndata: "
                        + encoded(event.model_dump(mode="json"))
                        + "\n\n"
                    )
            except Exception:
                yield 'event: error\ndata: {"code":"storage_unavailable"}\n\n'

        return StreamingResponse(
            frames(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    app.include_router(router)
