"""Session SDK：同服务或同HTTP；只读模式不创建Application。"""

from __future__ import annotations

import asyncio
import builtins
from typing import Any, cast
from urllib.parse import quote

from krim_agent.client import AsyncRunClient
from krim_agent.contracts import RunFacts, ServiceError
from krim_agent.runtime.facts import project_facts
from krim_agent.security import LOCAL, PrincipalContext
from krim_agent.sessions.service import SessionService
from krim_agent.sessions.types import (
    ControlRequest,
    ControlView,
    SessionCreate,
    SessionView,
    TurnRequest,
    TurnView,
)
from krim_agent.storage import sessions as sql
from krim_agent.storage.sqlite import SQLiteStore


class SessionSDK:
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
        return "/v2/workspaces/" + quote(self.workspace, safe="") + "/sessions"

    @staticmethod
    def ident(value: str) -> str:
        return quote(value, safe="")

    def writer(self) -> SessionService:
        if not isinstance(self.backend, SessionService):
            raise ServiceError("readonly")
        return self.backend

    async def create(self, request: SessionCreate) -> SessionView:
        if isinstance(self.backend, AsyncRunClient):
            return SessionView.model_validate(
                await self.backend.request("POST", self.path, request.model_dump(mode="json"))
            )
        return await self.writer().create(self.principal, self.workspace, request)

    async def get(self, session_id: str) -> SessionView:
        if isinstance(self.backend, AsyncRunClient):
            return SessionView.model_validate(
                await self.backend.request("GET", self.path + "/" + self.ident(session_id))
            )
        if isinstance(self.backend, SessionService):
            return await self.backend.get(self.principal, self.workspace, session_id)
        result = await sql.get_session(self.backend, session_id)
        if result.workspace_id != self.workspace:
            raise ServiceError("not_found", 404)
        self.principal.owns(self.workspace, result.owner_id)
        return result

    async def list(self, offset: int = 0, limit: int = 20) -> list[SessionView]:
        if not 0 <= offset <= 100000 or not 1 <= limit <= 50:
            raise ServiceError("invalid_input", 422)
        if isinstance(self.backend, AsyncRunClient):
            return [
                SessionView.model_validate(v)
                for v in await self.backend.request(
                    "GET", f"{self.path}?offset={offset}&limit={limit}"
                )
            ]
        if isinstance(self.backend, SessionService):
            return await self.backend.list(self.principal, self.workspace, offset, limit)
        membership = self.principal.member(self.workspace)
        return await sql.list_sessions(
            self.backend,
            self.workspace,
            self.principal.principal_id,
            membership.role == "admin",
            offset,
            limit,
        )

    async def by_key(self, key: str) -> SessionView:
        if isinstance(self.backend, AsyncRunClient):
            return SessionView.model_validate(
                await self.backend.request("GET", self.path + "/by-key/" + self.ident(key))
            )
        if isinstance(self.backend, SessionService):
            return await self.backend.by_key(self.principal, self.workspace, key)
        self.principal.member(self.workspace)
        return await sql.by_key(self.backend, self.workspace, self.principal.principal_id, key)

    async def submit(self, session_id: str, request: TurnRequest) -> TurnView:
        if isinstance(self.backend, AsyncRunClient):
            return TurnView.model_validate(
                await self.backend.request(
                    "POST",
                    self.path + "/" + self.ident(session_id) + "/turns",
                    request.model_dump(mode="json"),
                )
            )
        return await self.writer().submit(self.principal, self.workspace, session_id, request)

    async def turn(self, session_id: str, turn_id: str) -> TurnView:
        if isinstance(self.backend, AsyncRunClient):
            return TurnView.model_validate(
                await self.backend.request(
                    "GET",
                    self.path + "/" + self.ident(session_id) + "/turns/" + self.ident(turn_id),
                )
            )
        if isinstance(self.backend, SessionService):
            return await self.backend.turn(self.principal, self.workspace, session_id, turn_id)
        await self.get(session_id)
        value = await sql.get_turn(self.backend, turn_id)
        if value.session_id != session_id:
            raise ServiceError("not_found", 404)
        return value

    async def turn_by_key(self, session_id: str, key: str) -> TurnView:
        if isinstance(self.backend, AsyncRunClient):
            return TurnView.model_validate(
                await self.backend.request(
                    "GET",
                    self.path + "/" + self.ident(session_id) + "/turns/by-key/" + self.ident(key),
                )
            )
        if isinstance(self.backend, SessionService):
            return await self.backend.turn_by_key(self.principal, self.workspace, session_id, key)
        await self.get(session_id)
        return await sql.turn_by_key(self.backend, session_id, key)

    async def history(
        self, session_id: str, offset: int = 0, limit: int = 20
    ) -> builtins.list[TurnView]:
        if not 0 <= offset <= 100000 or not 1 <= limit <= 50:
            raise ServiceError("invalid_input", 422)
        if isinstance(self.backend, AsyncRunClient):
            return [
                TurnView.model_validate(v)
                for v in await self.backend.request(
                    "GET",
                    f"{self.path}/{self.ident(session_id)}/turns?offset={offset}&limit={limit}",
                )
            ]
        if isinstance(self.backend, SessionService):
            return await self.backend.turns(
                self.principal, self.workspace, session_id, offset, limit
            )
        await self.get(session_id)
        return await sql.list_turns(self.backend, session_id, offset, limit)

    async def wait(self, session_id: str, turn_id: str) -> TurnView:
        if isinstance(self.backend, SQLiteStore):
            raise ServiceError("readonly")
        if isinstance(self.backend, SessionService):
            return await self.backend.wait(self.principal, self.workspace, session_id, turn_id)
        while True:
            value = await self.turn(session_id, turn_id)
            if value.status not in {"queued", "running"}:
                return value
            await asyncio.sleep(0.1)

    async def control(
        self, session_id: str, operation: str, request: ControlRequest, turn_id: str | None = None
    ) -> ControlView:
        if operation not in {"rename", "resume", "close", "cancel_turn"}:
            raise ServiceError("invalid_input", 422)
        if isinstance(self.backend, AsyncRunClient):
            suffix = (
                "/turns/" + self.ident(turn_id) + "/cancel"
                if turn_id is not None
                else "/" + operation
            )
            return ControlView.model_validate(
                await self.backend.request(
                    "POST",
                    self.path + "/" + self.ident(session_id) + suffix,
                    request.model_dump(mode="json"),
                )
            )
        from typing import Literal, cast

        return await self.writer().control(
            self.principal,
            self.workspace,
            session_id,
            cast(Literal["rename", "resume", "close", "cancel_turn"], operation),
            request,
            turn_id=turn_id,
        )

    async def result(self, session_id: str, turn_id: str) -> RunFacts:
        if isinstance(self.backend, AsyncRunClient):
            return RunFacts.model_validate(
                await self.backend.request(
                    "GET",
                    self.path
                    + "/"
                    + self.ident(session_id)
                    + "/turns/"
                    + self.ident(turn_id)
                    + "/result",
                )
            )
        view = await self.turn(session_id, turn_id)
        if view.run_id is None:
            raise ServiceError("not_terminal")
        if isinstance(self.backend, SessionService):
            return await self.backend.runs.result(view.run_id)
        snapshot = await self.backend.snapshot(view.run_id)
        if snapshot.run.status in {"queued", "running"}:
            raise ServiceError("not_terminal")
        return project_facts(snapshot)

    async def trace(self, session_id: str, turn_id: str) -> dict[str, Any]:
        if isinstance(self.backend, AsyncRunClient):
            return cast(
                dict[str, Any],
                await self.backend.request(
                    "GET",
                    self.path
                    + "/"
                    + self.ident(session_id)
                    + "/turns/"
                    + self.ident(turn_id)
                    + "/trace",
                ),
            )
        from krim_agent.observability import project_trace

        view = await self.turn(session_id, turn_id)
        if view.run_id is None:
            raise ServiceError("not_terminal")
        store = self.backend.store if isinstance(self.backend, SessionService) else self.backend
        return {
            "session_id": session_id,
            "turn_id": turn_id,
            "trace": project_trace(await store.snapshot(view.run_id)).model_dump(mode="json"),
        }
