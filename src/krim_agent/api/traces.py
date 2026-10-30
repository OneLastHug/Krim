"""只读工作区会话Trace索引。"""

from fastapi import APIRouter, FastAPI, Query, Request

from krim_agent.contracts import RunStatus
from krim_agent.sessions.types import TraceIndexPage

from .sessions import principal as request_principal
from .sessions import service as session_service


def mount_traces(app: FastAPI) -> None:
    router = APIRouter()

    @router.get(
        "/v2/workspaces/{workspace_id}/traces", response_model=TraceIndexPage
    )
    async def listing(
        workspace_id: str,
        request: Request,
        limit: int = Query(default=20, ge=1, le=50),
        cursor: str | None = None,
        status: RunStatus | None = None,
        session_id: str | None = None,
        turn_id: str | None = None,
    ) -> TraceIndexPage:
        return await session_service(request).traces(
            request_principal(request),
            workspace_id,
            limit=limit,
            cursor=cursor,
            status=status,
            session_id=session_id,
            turn_id=turn_id,
        )

    app.include_router(router)
