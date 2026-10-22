"""工作区薄SDK；执行借原Application，冷读只借readonly Store。"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, TypeVar, cast
from urllib.parse import quote, urlencode

from sqlalchemy import select

from krim_agent.client import AsyncRunClient
from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.runtime.contracts import settled
from krim_agent.security import LOCAL, PrincipalContext
from krim_agent.sessions.types import TurnView
from krim_agent.storage import work
from krim_agent.storage.models import compactions, tool_effects, turns, work_turns
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.workspace.artifacts import ArtifactStore
from krim_agent.workspace.types import (
    ArtifactChunk,
    ArtifactRevision,
    ExecutionAccess,
    InputSnapshotRequest,
    WorkspaceTurnRequest,
    WorkView,
)

from .sessions import SessionSDK

T = TypeVar("T")


class WorkspaceSDK:
    def __init__(
        self,
        backend: Any,
        *,
        principal: PrincipalContext = LOCAL,
        workspace: str = "local",
        artifact_root: Path | None = None,
    ) -> None:
        self.backend, self.principal, self.workspace = backend, principal, workspace
        self.artifact_root = artifact_root
        self._reads: set[asyncio.Task[Any]] = set()
        self._artifacts: set[ArtifactStore] = set()
        self._close_task: asyncio.Task[None] | None = None
        self._closing = False

    async def _read(self, operation: Coroutine[Any, Any, T]) -> T:
        if self._closing:
            operation.close()
            raise ServiceError("closed")
        task = asyncio.create_task(operation)
        self._reads.add(task)
        try:
            return await task
        finally:
            if not task.done():
                task.cancel()
                await settled(asyncio.gather(task, return_exceptions=True))
            self._reads.discard(task)

    def path(self, session_id: str) -> str:
        return (
            "/v3/workspaces/"
            + quote(self.workspace, safe="")
            + "/sessions/"
            + quote(session_id, safe="")
        )

    def _store(self) -> SQLiteStore:
        if isinstance(self.backend, SQLiteStore):
            return self.backend
        if isinstance(getattr(self.backend, "store", None), SQLiteStore):
            return cast(SQLiteStore, self.backend.store)
        raise ServiceError("readonly")

    def _sessions(self) -> SessionSDK:
        if isinstance(self.backend, (SQLiteStore, AsyncRunClient)):
            return SessionSDK(self.backend, principal=self.principal, workspace=self.workspace)
        if self.backend.sessions is None:
            raise ServiceError("schema_upgrade_required")
        return SessionSDK(self.backend.sessions, principal=self.principal, workspace=self.workspace)

    def _writer(self) -> Any:
        if isinstance(self.backend, (SQLiteStore, AsyncRunClient)):
            raise ServiceError("readonly")
        if self.backend.work is None or self.backend.artifacts is None:
            raise ServiceError("workspace_unavailable")
        return self.backend

    async def start(self, session_id: str, request: WorkspaceTurnRequest) -> TurnView:
        frozen = WorkspaceTurnRequest.model_validate_json(request.model_dump_json())
        if isinstance(self.backend, AsyncRunClient):
            return TurnView.model_validate_json(
                encoded(
                    await self.backend.request(
                        "POST", self.path(session_id) + "/turns", frozen.model_dump(mode="json")
                    )
                )
            )
        app = self._writer()
        return cast(
            TurnView, await app.sessions.submit(self.principal, self.workspace, session_id, frozen)
        )

    async def input(self, session_id: str, request: InputSnapshotRequest) -> ArtifactRevision:
        frozen = InputSnapshotRequest.model_validate_json(request.model_dump_json())
        if isinstance(self.backend, AsyncRunClient):
            return ArtifactRevision.model_validate_json(
                encoded(
                    await self.backend.request(
                        "POST", self.path(session_id) + "/inputs", frozen.model_dump(mode="json")
                    )
                )
            )
        app = self._writer()
        await app.sessions.get(self.principal, self.workspace, session_id, write=True)
        return cast(
            ArtifactRevision,
            await app.artifacts.input(
                ExecutionAccess(self.principal, self.workspace, session_id), frozen
            ),
        )

    async def by_key(self, session_id: str, key: str) -> WorkView:
        async def read() -> WorkView:
            if isinstance(self.backend, AsyncRunClient):
                return WorkView.model_validate_json(
                    encoded(
                        await self.backend.request(
                            "GET", self.path(session_id) + "/turns/by-key/" + quote(key, safe="")
                        )
                    )
                )
            turn = await self._sessions().turn_by_key(session_id, key)
            return await self.get(session_id, turn.id)

        return await self._read(read())

    async def get(self, session_id: str, turn_id: str | None = None) -> WorkView:
        async def read() -> WorkView:
            if isinstance(self.backend, AsyncRunClient):
                suffix = "/turns/" + quote(turn_id, safe="") + "/work" if turn_id else "/work"
                return WorkView.model_validate_json(
                    encoded(await self.backend.request("GET", self.path(session_id) + suffix))
                )
            await self._sessions().get(session_id)
            store = self._store()
            access = ExecutionAccess(self.principal, self.workspace, session_id)

            async def read(conn: Any) -> WorkView:
                await work.row(conn, session_id, access)
                query = (
                    select(work_turns)
                    .join(turns, turns.c.id == work_turns.c.turn_id)
                    .where(work_turns.c.session_id == session_id)
                )
                if turn_id is not None:
                    query = query.where(work_turns.c.turn_id == turn_id)
                row = (
                    (await conn.execute(query.order_by(turns.c.ordinal.desc()).limit(1)))
                    .mappings()
                    .first()
                )
                if row is None:
                    raise ServiceError("not_found", 404)
                turn = (
                    (await conn.execute(select(turns).where(turns.c.id == row["turn_id"])))
                    .mappings()
                    .one()
                )
                if (
                    fingerprint(row["request_json"]) != row["request_hash"]
                    or row["request_hash"] != turn["request_hash"]
                    or row["request_json"]["request_key"] != turn["request_key"]
                    or row["request_json"]["text"] != turn["text"]
                    or row["request_json"]["expected_revision"] != turn["accepted_revision"]
                    or row["request_json"]["input"] != row["requested_input"]
                ):
                    raise ServiceError("facts_corrupt")
                if row["run_id"] is not None:
                    await work.load(conn, row["run_id"])
                effects = []
                summaries = []
                if row["run_id"] is not None:
                    for effect in (
                        await conn.execute(
                            select(tool_effects)
                            .where(tool_effects.c.run_id == row["run_id"])
                            .order_by(tool_effects.c.created_at)
                        )
                    ).mappings():
                        receipt = effect["receipt"] or {
                            "executor_started": False if effect["phase"] == "intent" else None,
                            "exit_code": None,
                            "output_complete": None,
                            "cleanup": "pending",
                            "code": None,
                            "artifact_revision_id": None,
                        }
                        effects.append(
                            receipt
                            | {
                                "call_id": effect["call_id"],
                                "kind": effect["kind"],
                                "phase": effect["phase"],
                                "result_available": True
                                if receipt.get("artifact_revision_id")
                                else None,
                            }
                        )
                    for summary in (
                        await conn.execute(
                            select(compactions)
                            .where(compactions.c.run_id == row["run_id"])
                            .order_by(compactions.c.pass_index)
                        )
                    ).mappings():
                        summaries.append(
                            {
                                "pass_index": summary["pass_index"],
                                "status": {"reserved": "pending", "complete": "completed"}.get(
                                    summary["status"], summary["status"]
                                ),
                                "source_digest": summary["source_hash"],
                                "model_step": summary["model_step"],
                                "summary_hash": summary["output_hash"],
                                "summary": summary["output"]
                                if summary["status"] == "complete"
                                else None,
                            }
                        )
                return WorkView.model_validate_json(
                    encoded(
                        {
                            "schema_version": 1,
                            "session_id": session_id,
                            "turn_id": row["turn_id"],
                            "run_id": row["run_id"],
                            "request_key": row["request_json"]["request_key"],
                            "original_request": row["request_json"],
                            "requested_input": row["requested_input"],
                            "resolved_input_revision": row["resolved_input_revision"],
                            "phase": row["phase"],
                            "plan": row["plan"] | {"revision": row["plan_revision"]},
                            "compactions": summaries,
                            "effects": effects,
                            "verification": row["verification"],
                            "effective_fingerprint": row["effective_hash"],
                        }
                    )
                )

            return await store.work_read(read)

        return await self._read(read())

    async def artifact(self, session_id: str, revision_id: str) -> ArtifactRevision:
        async def read() -> ArtifactRevision:
            if isinstance(self.backend, AsyncRunClient):
                return ArtifactRevision.model_validate_json(
                    encoded(
                        await self.backend.request(
                            "GET",
                            self.path(session_id) + "/artifacts/" + quote(revision_id, safe=""),
                        )
                    )
                )
            return cast(ArtifactRevision, await self._artifact_read(session_id, "get", revision_id))

        return await self._read(read())

    async def input_by_key(self, session_id: str, key: str) -> ArtifactRevision:
        async def read() -> ArtifactRevision:
            if isinstance(self.backend, AsyncRunClient):
                return ArtifactRevision.model_validate_json(
                    encoded(
                        await self.backend.request(
                            "GET", self.path(session_id) + "/inputs/by-key/" + quote(key, safe="")
                        )
                    )
                )
            return cast(ArtifactRevision, await self._artifact_read(session_id, "by_key", key))

        return await self._read(read())

    async def chunk(
        self, session_id: str, revision_id: str, path: str, *, offset: int = 0, length: int = 16384
    ) -> ArtifactChunk:
        async def read() -> ArtifactChunk:
            if isinstance(self.backend, AsyncRunClient):
                return ArtifactChunk.model_validate_json(
                    encoded(
                        await self.backend.request(
                            "GET",
                            self.path(session_id)
                            + "/artifacts/"
                            + quote(revision_id, safe="")
                            + "/files/"
                            + quote(path, safe="/")
                            + "?"
                            + urlencode({"offset": offset, "length": length}),
                        )
                    )
                )
            return cast(
                ArtifactChunk,
                await self._artifact_read(
                    session_id, "chunk", revision_id, path, offset=offset, length=length
                ),
            )

        return await self._read(read())

    async def _artifact_read(self, session_id: str, method: str, *args: Any, **kwargs: Any) -> Any:
        await self._sessions().get(session_id)
        owned = isinstance(self.backend, SQLiteStore)
        if owned:
            if not self.backend.readonly:
                raise ServiceError("readonly")
            if self.artifact_root is None or not self.artifact_root.is_absolute():
                raise ServiceError("invalid_input", 422)
            artifacts = ArtifactStore(self.backend, self.artifact_root)
            self._artifacts.add(artifacts)
        else:
            artifacts = self.backend.artifacts
            if artifacts is None:
                raise ServiceError("workspace_unavailable")
        try:
            return await getattr(artifacts, method)(
                ExecutionAccess(self.principal, self.workspace, session_id), *args, **kwargs
            )
        finally:
            if owned:
                await settled(artifacts.close())
                if artifacts.closed:
                    self._artifacts.discard(artifacts)

    async def verify(self, session_id: str, turn_id: str | None = None) -> Any:
        async def read() -> Any:
            return (await self.get(session_id, turn_id)).verification

        return await self._read(read())

    async def close(self) -> None:
        # SDK只拥有读者和自己构造的短生命周期ArtifactStore，不拥有借用backend。
        if not self._reads and not self._artifacts:
            return
        if self._close_task is None or self._close_task.done():
            self._closing = True

            async def close_reads() -> None:
                tasks = tuple(self._reads)
                for task in tasks:
                    task.cancel()
                outcomes = await asyncio.gather(*tasks, return_exceptions=True)
                self._reads.difference_update(tasks)
                owned = tuple(self._artifacts)
                cleanup = await asyncio.gather(
                    *(value.close() for value in owned), return_exceptions=True
                )
                self._artifacts.difference_update(value for value in owned if value.closed)
                if not self._artifacts:
                    self._closing = False
                for failure in [*outcomes, *cleanup]:
                    if isinstance(failure, BaseException) and not isinstance(
                        failure, asyncio.CancelledError
                    ):
                        raise failure

            self._close_task = asyncio.create_task(close_reads())
        await settled(self._close_task)
