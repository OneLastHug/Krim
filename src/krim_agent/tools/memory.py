"""Private memory tools over ExtensionService and host-resolved session authority.

Mutations use the ordinary tool approval policy. The tool schema cannot enable
memory, choose another owner/project, or claim an external source is a preference.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field, JsonValue, model_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, StrictModel, fingerprint
from krim_agent.extensions import (
    ExtensionService,
    MemoryCreate,
    MemoryDelete,
    MemoryUpdate,
    body_hash,
)
from krim_agent.storage.extension_models import memories
from krim_agent.storage.models import turns
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.session import SessionScopeResolver, SessionToolScope, ToolBundle

if TYPE_CHECKING:
    from krim_agent.accounts.service import IdentityService
    from krim_agent.storage.sqlite import SQLiteStore


class MemorySearch(StrictModel):
    query: str = Field(min_length=1)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=8, ge=1, le=100)
    max_chars: int = Field(default=8192, ge=1, le=65536)


class MemoryRead(StrictModel):
    memory_id: str = Field(min_length=1, max_length=64)
    scope: Literal["account", "project"] = "account"
    offset: int = Field(default=0, ge=0)
    length: int = Field(default=4096, ge=1, le=65536)
    expected_revision: int | None = Field(default=None, ge=1)
    expected_content_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class MemoryWrite(StrictModel):
    scope: Literal["account", "project"] = "account"
    memory_id: str | None = Field(default=None, min_length=1, max_length=64)
    body: str = Field(min_length=1)
    source_kind: Literal["user", "web", "mcp", "task"] = "user"
    source_ref: str | None = Field(default=None, max_length=256)
    memory_kind: Literal["note", "preference"] = "note"
    expected_revision: int | None = Field(default=None, ge=1)
    expected_content_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def valid_write(self) -> MemoryWrite:
        editing = self.memory_id is not None
        if editing != (self.expected_revision is not None) or editing != (
            self.expected_content_hash is not None
        ):
            raise ValueError("更新记忆须提供已读取的版本和哈希")
        if self.memory_kind == "preference" and self.source_kind != "user":
            raise ValueError("用户偏好不能来自外部内容")
        if self.source_kind in {"web", "mcp"} and not self.source_ref:
            raise ValueError("外部内容须保留来源")
        return self


class MemoryRemove(StrictModel):
    memory_id: str = Field(min_length=1, max_length=64)
    scope: Literal["account", "project"] = "account"
    expected_revision: int = Field(ge=1)
    expected_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


async def unavailable(parameters: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required", 409)


class MemoryToolService:
    def __init__(
        self,
        store: SQLiteStore,
        identities: IdentityService | None = None,
        *,
        extensions: ExtensionService | None = None,
    ) -> None:
        self.store = store
        self.extensions = extensions or ExtensionService(store, identities)

    async def _access(
        self, conn: AsyncConnection, scope: SessionToolScope, *, write: bool = False
    ) -> dict[str, Any]:
        scope.check()
        access = await self.extensions.memory_access(
            conn,
            scope.actor,
            scope.workspace_id,
            scope.session_id,
            project_id=scope.project_id,
            write=write,
        )
        scope.check()
        return access

    @staticmethod
    def _selected(scope: SessionToolScope, selected: str, access: dict[str, Any]) -> dict[str, Any]:
        if selected == "project":
            if scope.project_id is None:
                raise ServiceError("workspace_contract_required", 409)
            return dict(workspace_id=scope.workspace_id, project_id=scope.project_id)
        if not access["account_allowed"]:
            raise ServiceError("private_memory_workspace", 403)
        return dict(workspace_id=None, project_id=None)

    async def recall(
        self,
        scope: SessionToolScope,
        query: str,
        *,
        limit: int = 8,
        max_chars: int = 8192,
        offset: int = 0,
    ) -> dict[str, Any]:
        if limit < 1 or max_chars < 1 or offset < 0:
            raise ServiceError("invalid_input", 422)

        async def read(conn: AsyncConnection) -> dict[str, Any]:
            access = await self._access(conn, scope)
            page = (
                await self.extensions.memory_recall(
                    conn,
                    scope.actor.principal_id,
                    scope.workspace_id,
                    scope.project_id,
                    query,
                    account_allowed=access["account_allowed"],
                    limit=limit,
                    max_chars=max_chars,
                    offset=offset,
                )
                if access["enabled"]
                else dict(items=[], next_offset=None)
            )
            snapshot = dict(
                version="memory-recall/1",
                status="ok" if access["enabled"] else "disabled",
                owner_id=scope.actor.principal_id,
                workspace_id=scope.workspace_id,
                session_id=scope.session_id,
                project_id=scope.project_id,
                preference_revision=access["preference_revision"],
                session_extension_revision=access["session_extension_revision"],
                query_hash=body_hash(query),
                offset=offset,
                **page,
            )
            scope.check()
            return snapshot | {"fingerprint": fingerprint(snapshot)}

        return await self.extensions._read(scope.actor, read)

    async def read(self, scope: SessionToolScope, parameters: MemoryRead) -> dict[str, Any]:
        async def read(conn: AsyncConnection) -> dict[str, Any]:
            access = await self._access(conn, scope)
            if not access["enabled"]:
                return dict(status="disabled")
            selected = self._selected(scope, parameters.scope, access)
            columns = [column for column in memories.c if column.name != "body"]
            row = (
                (
                    await conn.execute(
                        select(
                            *columns,
                            func.substr(
                                memories.c.body, parameters.offset + 1, parameters.length
                            ).label("body"),
                            func.length(memories.c.body).label("total_chars"),
                        ).where(
                            memories.c.id == parameters.memory_id,
                            self.extensions.memory_filter(scope.actor.principal_id, **selected),
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            if (
                parameters.expected_revision is not None
                and row["revision"] != parameters.expected_revision
                or parameters.expected_content_hash is not None
                and row["content_hash"] != parameters.expected_content_hash
            ):
                raise ServiceError("revision_conflict", 409)
            if parameters.offset and (
                parameters.expected_revision is None or parameters.expected_content_hash is None
            ):
                raise ServiceError("invalid_input", 422)
            if parameters.offset > row["total_chars"]:
                raise ServiceError("invalid_input", 422)
            if parameters.offset == 0 and len(row["body"]) == row["total_chars"]:
                self.extensions._memory(row)
            end = parameters.offset + len(row["body"])
            result = dict(row) | dict(
                status="ok",
                scope=parameters.scope,
                offset=parameters.offset,
                next_offset=end if end < row["total_chars"] else None,
                excerpt_hash=body_hash(row["body"]),
            )
            scope.check()
            return result

        return await self.extensions._read(scope.actor, read)

    async def _user_turn(self, conn: AsyncConnection, scope: SessionToolScope) -> Any:
        row = (
            (
                await conn.execute(
                    select(turns).where(
                        turns.c.id == scope.turn_id,
                        turns.c.session_id == scope.session_id,
                        turns.c.workspace_id == scope.workspace_id,
                        turns.c.owner_id == scope.actor.principal_id,
                        turns.c.run_id == scope.run_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("workspace_contract_required", 409)
        return row

    async def mutate(
        self, scope: SessionToolScope, parameters: MemoryWrite | MemoryRemove, *, request_key: str
    ) -> dict[str, Any]:
        # The same guard runs before an idempotency receipt is read and in the write transaction.
        async def guard(conn: AsyncConnection) -> None:
            access = await self._access(conn, scope, write=True)
            if not access["enabled"]:
                raise ServiceError("permission_denied", 409)
            self._selected(scope, parameters.scope, access)
            turn = await self._user_turn(conn, scope)
            if isinstance(parameters, MemoryWrite) and parameters.memory_kind == "preference":
                # A web/tool result cannot be relabelled by the model as the user's preference.
                if parameters.body not in turn["text"]:
                    raise ServiceError("invalid_input", 422)
            scope.check()

        initial = await self.extensions._read(
            scope.actor, lambda conn: self._access(conn, scope, write=True)
        )
        if not initial["enabled"]:
            return dict(status="disabled")
        selected = self._selected(scope, parameters.scope, initial)
        if isinstance(parameters, MemoryRemove):
            body: MemoryCreate | MemoryDelete = MemoryDelete(
                request_key=request_key,
                expected_revision=parameters.expected_revision,
                expected_content_hash=parameters.expected_content_hash,
            )
        else:
            values: dict[str, Any] = dict(
                request_key=request_key,
                body=parameters.body,
                source_kind=parameters.source_kind,
                memory_kind=parameters.memory_kind,
                source_ref=(
                    f"turn:{scope.turn_id}"
                    if parameters.source_kind == "user"
                    else parameters.source_ref or f"turn:{scope.turn_id}"
                ),
            )
            if parameters.memory_id is None:
                body = MemoryCreate(**values)
            else:
                assert parameters.expected_revision is not None
                assert parameters.expected_content_hash is not None
                body = MemoryUpdate(
                    **values,
                    expected_revision=parameters.expected_revision,
                    expected_content_hash=parameters.expected_content_hash,
                )
        result = await self.extensions.memory(
            scope.actor,
            body,
            parameters.memory_id,
            **selected,
            authorization_turn_id=scope.turn_id,
            guard=guard,
        )
        return dict(status="ok", **result)

    def bundle(self, resolve: SessionScopeResolver) -> ToolBundle:
        definitions: dict[str, tuple[type[BaseModel], str]] = {
            "memory_search": (
                MemorySearch,
                "按需搜索已启用的私有记忆。返回来源、版本、片段及续页位置。",
            ),
            "memory_read": (MemoryRead, "分页读取记忆；续读时传回版本和完整内容哈希。"),
            "memory_write": (
                MemoryWrite,
                "经用户批准保存有复用价值的记忆。外部内容只能记为带来源的笔记；用户偏好须直接来自当前用户原文。不会开启记忆。",
            ),
            "memory_delete": (MemoryRemove, "经用户批准删除已读取版本的记忆；关闭记忆时不可执行。"),
        }

        async def policy(invocation: Invocation) -> Decision:
            if invocation.name not in definitions:
                return "deny"
            scope = await resolve(invocation)
            access = await self.extensions._read(
                scope.actor,
                lambda conn: self._access(
                    conn, scope, write=invocation.name in {"memory_write", "memory_delete"}
                ),
            )
            if not access["enabled"] and invocation.name in {"memory_write", "memory_delete"}:
                return "deny"
            # Existing ToolExecutor approvals authorize persistence, including external notes.
            return "ask" if invocation.name in {"memory_write", "memory_delete"} else "allow"

        async def execute(
            invocation: Invocation, parameters: BaseModel, context: object
        ) -> JsonValue:
            scope = await resolve(invocation)
            scope.check()
            if invocation.name == "memory_search" and isinstance(parameters, MemorySearch):
                return await self.recall(scope, **parameters.model_dump())
            if invocation.name == "memory_read" and isinstance(parameters, MemoryRead):
                return await self.read(scope, parameters)
            if (invocation.name == "memory_write" and isinstance(parameters, MemoryWrite)) or (
                invocation.name == "memory_delete" and isinstance(parameters, MemoryRemove)
            ):
                key = "memory-tool:" + fingerprint(
                    dict(session=scope.session_id, turn=scope.turn_id, call=invocation.call_id)
                )
                return await self.mutate(scope, parameters, request_key=key)
            raise ServiceError("invalid_input", 422)

        digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        return ToolBundle(
            tuple(
                ToolSpec(
                    name=name,
                    parameters=model,
                    executor=unavailable,
                    output_schema={"type": "object"},
                    version="memory/1",
                    implementation_digest=digest,
                    scoped_executor=execute,
                    capability="session_write"
                    if name in {"memory_write", "memory_delete"}
                    else "session_read",
                    risk="write" if name in {"memory_write", "memory_delete"} else "read",
                    concurrency=("parallel" if name in {"memory_search", "memory_read"}
                                 else "exclusive"),
                    description=description,
                )
                for name, (model, description) in definitions.items()
            ),
            policy,
        )


Service = MemoryToolService
