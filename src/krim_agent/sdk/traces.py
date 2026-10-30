"""工作区会话Trace索引SDK；只提供分页读取。"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote, urlencode

from pydantic import ValidationError

from krim_agent.client import AsyncRunClient
from krim_agent.contracts import RunStatus, ServiceError
from krim_agent.security import LOCAL, PrincipalContext
from krim_agent.sessions.service import SessionService, read_trace_index
from krim_agent.sessions.types import TraceIndexItem, TraceIndexPage
from krim_agent.storage.sqlite import SQLiteStore

_TRACE_STATUSES = frozenset(
    {"queued", "running", "completed", "failed", "aborted", "interrupted"}
)


class TraceSDK:
    """在本地服务、SQLite读口或AsyncRunClient上分页读取Trace索引。"""

    def __init__(
        self,
        backend: SessionService | AsyncRunClient | SQLiteStore,
        *,
        principal: PrincipalContext = LOCAL,
        workspace: str = "local",
    ) -> None:
        self.backend, self.principal, self.workspace = backend, principal, workspace

    @property
    def path(self) -> str:
        return "/v2/workspaces/" + quote(self.workspace, safe="") + "/traces"

    async def list(
        self,
        limit: int = 20,
        cursor: str | None = None,
        status: RunStatus | None = None,
        session_id: str | None = None,
        turn_id: str | None = None,
    ) -> TraceIndexPage:
        if type(limit) is not int or not 1 <= limit <= 50:
            raise ServiceError("invalid_input", 422)
        if status is not None and (not isinstance(status, str) or status not in _TRACE_STATUSES):
            raise ServiceError("invalid_input", 422)
        if session_id is not None and (
            not isinstance(session_id, str) or not session_id or len(session_id) > 128
        ):
            raise ServiceError("invalid_input", 422)
        if turn_id is not None and (
            not isinstance(turn_id, str) or not turn_id or len(turn_id) > 128
        ):
            raise ServiceError("invalid_input", 422)
        if isinstance(self.backend, SessionService):
            return await self.backend.traces(
                self.principal,
                self.workspace,
                limit=limit,
                cursor=cursor,
                status=status,
                session_id=session_id,
                turn_id=turn_id,
            )
        if isinstance(self.backend, SQLiteStore):
            return await read_trace_index(
                self.backend,
                self.principal,
                self.workspace,
                limit=limit,
                cursor=cursor,
                status=status,
                session_id=session_id,
                turn_id=turn_id,
            )

        query: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            query["cursor"] = cursor
        if status is not None:
            query["status"] = status
        if session_id is not None:
            query["session_id"] = session_id
        if turn_id is not None:
            query["turn_id"] = turn_id
        value = await self.backend.request("GET", self.path + "?" + urlencode(query))
        try:
            return TraceIndexPage.model_validate(value)
        except ValidationError:
            raise ServiceError("facts_corrupt", 502) from None


__all__ = ["TraceSDK", "TraceIndexItem", "TraceIndexPage"]
