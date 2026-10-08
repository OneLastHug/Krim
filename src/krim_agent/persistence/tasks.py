"""持久任务账本：作用域/owner校验、SQL CAS、幂等和操作事实。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pydantic import JsonValue
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.task_state import (
    TaskItemView,
    TaskKind,
    TaskMutation,
    TaskOperationView,
    TaskSnapshot,
)

from .agent_sessions import assert_session_open, assert_session_owner
from .database import (
    AgentSessionRow,
    AgentTaskHeadRow,
    AgentTaskItemRow,
    AgentTaskOperationRow,
    Database,
    RunRow,
    TopicRow,
)

if TYPE_CHECKING:
    from krim_agent.tools.core import ToolContext


class AgentTaskStore:
    def __init__(self, database: Database, *, owner_id: str | None = None) -> None:
        self.database, self.owner_id = database, owner_id

    async def _scope(
        self,
        session: AsyncSession,
        scope_id: str,
        *,
        writing: bool,
        run_id: str | None = None,
    ) -> None:
        if scope_id.startswith("session:"):
            identifier = scope_id.removeprefix("session:")
            row = await session.get(AgentSessionRow, identifier)
            if row is None:
                raise ServiceError("agent_session_not_found", "任务会话不存在", 404)
            if writing:
                await assert_session_owner(session, row, self.owner_id)
                await assert_session_open(session, row)
            topic_id = row.topic_id
        elif scope_id.startswith("topic:"):
            topic_id = scope_id.removeprefix("topic:")
            if await session.get(TopicRow, topic_id) is None:
                raise ServiceError("topic_not_found", "任务话题不存在", 404)
            managed = await session.scalar(
                select(AgentSessionRow.session_id).where(AgentSessionRow.topic_id == topic_id)
            )
            if managed is not None:
                raise ServiceError("task_scope_managed", "Agent任务须使用自身会话作用域", 403)
        else:
            raise ServiceError("task_scope_invalid", "任务作用域无效", 422)
        if run_id is not None:
            run = await session.get(RunRow, run_id)
            if run is None or run.topic_id != topic_id:
                raise ServiceError("task_run_mismatch", "任务操作的Run不属于该作用域", 403)
            if writing and (run.status not in {"queued", "running"} or run.stop_requested):
                raise ServiceError("task_run_closed", "任务所属Run已关闭", 409)

    @staticmethod
    def _view(
        scope_id: str, kind: TaskKind, revision: int, rows: list[AgentTaskItemRow]
    ) -> TaskSnapshot:
        return TaskSnapshot(
            scope_id=scope_id,
            kind=kind,
            revision=revision,
            items=[
                TaskItemView(
                    id=row.item_id,
                    content=row.content,
                    completed=row.completed,
                    position=row.position,
                )
                for row in rows
            ],
        )

    @staticmethod
    async def _items(
        session: AsyncSession, scope_id: str, kind: TaskKind
    ) -> list[AgentTaskItemRow]:
        return list(
            await session.scalars(
                select(AgentTaskItemRow)
                .where(AgentTaskItemRow.scope_id == scope_id, AgentTaskItemRow.kind == kind)
                .order_by(AgentTaskItemRow.position, AgentTaskItemRow.item_id)
            )
        )

    async def snapshot(self, scope_id: str, kind: TaskKind) -> TaskSnapshot:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=False)
            head = await session.get(AgentTaskHeadRow, (scope_id, kind))
            return self._view(
                scope_id,
                kind,
                head.revision if head else 0,
                await self._items(session, scope_id, kind),
            )

    async def history(
        self, scope_id: str, kind: TaskKind, *, offset: int = 0, limit: int = 100
    ) -> list[TaskOperationView]:
        if offset < 0 or not 1 <= limit <= 100:
            raise ServiceError("task_pagination", "任务历史分页无效", 422)
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope_id, writing=False)
            rows = await session.scalars(
                select(AgentTaskOperationRow)
                .where(
                    AgentTaskOperationRow.scope_id == scope_id, AgentTaskOperationRow.kind == kind
                )
                .order_by(AgentTaskOperationRow.revision)
                .offset(offset)
                .limit(limit)
            )
            return [
                TaskOperationView(
                    request_key=row.request_key,
                    revision=row.revision,
                    operation=row.operation,
                    run_id=row.run_id,
                    call_id=row.call_id,
                    created_at=row.created_at,
                )
                for row in rows
            ]

    async def mutate(
        self,
        scope_id: str,
        kind: TaskKind,
        mutation: TaskMutation,
        *,
        run_id: str | None = None,
        call_id: str | None = None,
        tool_request_hash: str | None = None,
    ) -> TaskSnapshot:
        request = TaskMutation.model_validate(mutation.model_dump())
        if kind not in {"plan", "todo"}:
            raise ServiceError("task_kind_invalid", "任务类型无效", 422)
        payload = request.model_dump(mode="json", exclude={"request_key", "expected_revision"})
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        async with self.database.write_lock, self.database.sessions.begin() as session:
            # 首条写语句取得SQLite写锁；跨连接不能依赖本进程的asyncio锁。
            await session.execute(
                insert(AgentTaskHeadRow)
                .values(scope_id=scope_id, kind=kind, revision=0, updated_at=utc_now())
                .on_conflict_do_nothing()
            )
            await self._scope(session, scope_id, writing=True, run_id=run_id)
            previous = await session.get(
                AgentTaskOperationRow, (scope_id, kind, request.request_key)
            )
            if previous is not None:
                if previous.request_hash != digest:
                    raise ServiceError(
                        "task_idempotency_conflict", "任务幂等键不能用于不同操作", 409
                    )
                return TaskSnapshot.model_validate(previous.result)
            head = await session.get(AgentTaskHeadRow, (scope_id, kind))
            assert head is not None
            current_revision = head.revision
            expected = (
                request.expected_revision
                if request.expected_revision is not None
                else current_revision
            )
            if current_revision != expected:
                raise ServiceError("task_revision_conflict", "计划/待办版本已变化", 409)
            if current_revision >= 2000:
                raise ServiceError("task_history_limit", "该作用域任务操作已达上限", 429)
            updated_revision = await session.scalar(
                update(AgentTaskHeadRow)
                .where(
                    AgentTaskHeadRow.scope_id == scope_id,
                    AgentTaskHeadRow.kind == kind,
                    AgentTaskHeadRow.revision == expected,
                )
                .values(revision=expected + 1, updated_at=utc_now())
                .returning(AgentTaskHeadRow.revision)
            )
            if updated_revision is None:
                raise ServiceError("task_revision_conflict", "计划/待办版本已变化", 409)
            rows = await self._items(session, scope_id, kind)
            if request.operation == "replace":
                await session.execute(
                    delete(AgentTaskItemRow).where(
                        AgentTaskItemRow.scope_id == scope_id, AgentTaskItemRow.kind == kind
                    )
                )
                for position, content in enumerate(request.items):
                    session.add(
                        AgentTaskItemRow(
                            scope_id=scope_id,
                            kind=kind,
                            item_id=str(uuid4()),
                            content=content,
                            completed=False,
                            position=position,
                            updated_at=utc_now(),
                        )
                    )
            elif request.operation == "add":
                if len(rows) >= 100:
                    raise ServiceError("task_item_limit", "计划/待办条目已达100项", 429)
                session.add(
                    AgentTaskItemRow(
                        scope_id=scope_id,
                        kind=kind,
                        item_id=request.item_id or str(uuid4()),
                        content=request.content,
                        completed=False,
                        position=max((row.position for row in rows), default=-1) + 1,
                        updated_at=utc_now(),
                    )
                )
            else:
                row = next((row for row in rows if row.item_id == request.item_id), None)
                if row is None:
                    raise ServiceError("task_item_not_found", "计划/待办项不存在", 404)
                if request.operation == "remove":
                    await session.delete(row)
                else:
                    if request.operation == "complete":
                        row.completed = True
                    else:
                        if request.content:
                            row.content = request.content
                        if request.completed is not None:
                            row.completed = request.completed
                    row.updated_at = utc_now()
            await session.flush()
            result = self._view(
                scope_id, kind, updated_revision, await self._items(session, scope_id, kind)
            )
            if len(result.model_dump_json().encode()) > 48000:
                raise ServiceError("task_snapshot_limit", "任务总内容超过48KiB", 413)
            session.add(
                AgentTaskOperationRow(
                    scope_id=scope_id,
                    kind=kind,
                    request_key=request.request_key,
                    request_hash=digest,
                    result=result.model_dump(mode="json"),
                    revision=updated_revision,
                    operation=request.operation,
                    run_id=run_id,
                    call_id=call_id,
                    tool_request_hash=tool_request_hash,
                    created_at=utc_now(),
                )
            )
            return result

    async def tool_operation(
        self, kind: TaskKind, payload: Mapping[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        from krim_agent.tools.core import ToolError

        scope_id = (
            f"session:{context.session_id}" if context.session_id else f"topic:{context.topic_id}"
        )
        operation = str(payload["operation"])
        try:
            key = hashlib.sha256(
                f"tool:{context.run_id}:{context.call_id}:{kind}".encode()
            ).hexdigest()
            tool_hash = hashlib.sha256(
                json.dumps(dict(payload), sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()
            if operation not in {"get", "list"}:
                async with self.database.write_lock, self.database.sessions() as session:
                    await self._scope(session, scope_id, writing=False, run_id=context.run_id)
                    previous = await session.get(AgentTaskOperationRow, (scope_id, kind, key))
                    if previous is not None:
                        if previous.tool_request_hash != tool_hash:
                            raise ServiceError(
                                "task_idempotency_conflict", "相同工具调用不能修改任务参数", 409
                            )
                        return self._tool_result(
                            TaskSnapshot.model_validate(previous.result), operation
                        )
            snapshot = await self.snapshot(scope_id, kind)
            if operation not in {"get", "list"}:
                item_id = str(payload.get("item_id", ""))
                content = str(payload.get("item", ""))
                if kind == "plan" and operation == "complete":
                    matched = next(
                        (
                            item
                            for item in snapshot.items
                            if not item.completed and item.content == content
                        ),
                        None,
                    )
                    if matched is None:
                        raise ServiceError("plan_item_not_found", "计划项不存在", 404)
                    item_id = matched.id
                snapshot = await self.mutate(
                    scope_id,
                    kind,
                    TaskMutation.model_validate(
                        {
                            "operation": operation,
                            "items": list(payload.get("items", [])),
                            "item_id": item_id,
                            "content": content,
                            "expected_revision": snapshot.revision,
                            "request_key": key,
                        }
                    ),
                    run_id=context.run_id,
                    call_id=context.call_id,
                    tool_request_hash=tool_hash,
                )
            return self._tool_result(snapshot, operation)
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    @staticmethod
    def _tool_result(snapshot: TaskSnapshot, operation: str) -> dict[str, JsonValue]:
        if snapshot.kind == "plan":
            return {
                "items": [item.content for item in snapshot.items if not item.completed],
                "operation": operation,
                "revision": snapshot.revision,
            }
        return {
            "items": [
                {"id": item.id, "item": item.content, "completed": item.completed}
                for item in snapshot.items
            ],
            "revision": snapshot.revision,
        }
