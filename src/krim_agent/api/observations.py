"""只读观测API；授权沿用会话服务入口与Trace索引查询，聚合仅覆盖返回页。

列表不经 /traces 的服务端字面量校验（其不含awaiting），改用同一存储查询与
同scope游标，保证真实停靠记录可见；单run投影失败降级为unavailable，不扩散。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, FastAPI, Query, Request
from sqlalchemy import func, select

from krim_agent.contracts import RunStatus, ServiceError
from krim_agent.observation_views import (
    CompactionRecord,
    ObservationItem,
    ObservationPage,
    ObservationScope,
    TurnObservation,
    project_compaction_row,
    project_detail,
    project_summary,
)
from krim_agent.sessions.service import (
    SessionService,
    _decode_trace_cursor,
    _encode_trace_cursor,
    _trace_cursor_scope,
)
from krim_agent.sessions.types import TraceIndexItem
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.models import compactions
from krim_agent.storage.sqlite import SQLiteStore

from .sessions import application as request_application
from .sessions import principal as request_principal
from .sessions import service as session_service

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection


def _admit(sessions: SessionService, store: SQLiteStore) -> None:
    """与SessionService.traces相同的准入检查（公开属性复核）。"""
    if sessions.closed:
        raise ServiceError("closed", 503)
    if not store.healthy or sessions.failure:
        raise ServiceError("storage_unavailable", 503)


async def _compaction_pass_counts(store: SQLiteStore, run_ids: Sequence[str]) -> dict[str, int]:
    if not run_ids or store.schema_version < 4:
        return {}

    async def operation(conn: AsyncConnection) -> dict[str, int]:
        rows = (
            await conn.execute(
                select(compactions.c.run_id, func.count())
                .where(compactions.c.run_id.in_(run_ids))
                .group_by(compactions.c.run_id)
            )
        ).all()
        return {str(row[0]): int(row[1]) for row in rows}

    return await store.work_read(operation)


async def _compaction_rows(
    store: SQLiteStore, run_id: str, session_id: str
) -> list[CompactionRecord]:
    if store.schema_version < 4:
        return []

    async def operation(conn: AsyncConnection) -> list[dict[str, object]]:
        rows = (
            (
                await conn.execute(
                    select(compactions)
                    .where(compactions.c.run_id == run_id)
                    .order_by(compactions.c.pass_index)
                )
            )
            .mappings()
            .all()
        )
        return [dict(row) for row in rows]

    return [project_compaction_row(row, session_id) for row in await store.work_read(operation)]


def mount_observations(app: FastAPI) -> None:
    router = APIRouter()

    @router.get(
        "/v2/workspaces/{workspace_id}/observations", response_model=ObservationPage
    )
    async def listing(
        workspace_id: str,
        request: Request,
        limit: int = Query(default=20, ge=1, le=50),
        cursor: str | None = None,
        status: RunStatus | None = None,
        session_id: str | None = None,
    ) -> ObservationPage:
        sessions = session_service(request)
        principal = request_principal(request)
        membership = principal.member(workspace_id)
        store = request_application(request).store
        _admit(sessions, store)
        if session_id is not None and (not session_id or len(session_id) > 128):
            raise ServiceError("invalid_input", 422)
        scope = _trace_cursor_scope(
            principal, workspace_id, membership.role, limit, status, session_id, None
        )
        before = _decode_trace_cursor(cursor, scope)
        rows = await session_sql.list_trace_index(
            store,
            workspace_id,
            principal.principal_id,
            membership.role == "admin",
            limit,
            status=status,
            session_id=session_id,
            before=before,
        )
        has_more = len(rows) > limit
        rows = rows[:limit]
        counts = await _compaction_pass_counts(store, [str(row["run_id"]) for row in rows])
        items: list[ObservationItem] = []
        for row in rows:
            try:
                snapshot = await store.snapshot(str(row["run_id"]))
                context = snapshot.session_context
                if (context is None or context.workspace_id != workspace_id
                        or context.session_id != row["session_id"]
                        or context.turn_id != row["turn_id"]):
                    raise ServiceError("facts_corrupt")
            except ServiceError as error:
                # 单条run事实损坏/缺失不拖垮整页；存储整体故障仍按503上抛。
                if error.code in {"facts_corrupt", "not_found"} or (
                    error.code == "storage_unavailable" and store.healthy
                ):
                    items.append(
                        project_summary(
                            row,
                            None,
                            compaction_passes=counts.get(str(row["run_id"]), 0),
                            error=error.code,
                        )
                    )
                    continue
                raise
            items.append(
                project_summary(row, snapshot, compaction_passes=counts.get(str(row["run_id"]), 0))
            )
        next_cursor = (
            # The shared cursor encoder only reads run_id and created_at.
            _encode_trace_cursor(scope, cast(TraceIndexItem, items[-1]))
            if has_more and rows
            else None
        )
        return ObservationPage(
            scope=ObservationScope(
                workspace_id=workspace_id,
                role=membership.role,
                status=status,
                session_id=session_id,
                limit=limit,
            ),
            items=items,
            next_cursor=next_cursor,
        )

    @router.get(
        "/v2/workspaces/{workspace_id}/sessions/{session_id}/turns/{turn_id}/observations",
        response_model=TurnObservation,
    )
    async def detail(
        workspace_id: str, session_id: str, turn_id: str, request: Request
    ) -> TurnObservation:
        sessions = session_service(request)
        principal = request_principal(request)
        await sessions.get(principal, workspace_id, session_id)
        turn = await sessions.turn(principal, workspace_id, session_id, turn_id)
        if turn.run_id is None:
            raise ServiceError("not_terminal", 409)
        store = request_application(request).store
        snapshot = await store.snapshot(turn.run_id)
        context = snapshot.session_context
        if (
            context is None
            or context.session_id != session_id
            or context.turn_id != turn_id
            or context.workspace_id != workspace_id
        ):
            raise ServiceError("facts_corrupt")
        rows = await _compaction_rows(store, turn.run_id, session_id)
        history_turns = (
            len(snapshot.work.source_refs) if snapshot.work is not None else None
        )
        return project_detail(
            snapshot,
            session_id=session_id,
            turn_id=turn_id,
            compactions=rows,
            history_turns=history_turns,
        )

    app.include_router(router)
