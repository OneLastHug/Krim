"""模型树原调用准入；元数据来源与执行资格分开，来源不进入公共响应。"""

from __future__ import annotations

from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_tree import TreePlanModelSource

from .database import AgentSessionRow, RunRow, WorkspaceTreeModelSourceRow, WorkspaceTreePlanRow

TreeModelKind = Literal["prepare", "start"]


def validate(source: TreePlanModelSource) -> None:
    if (
        not isinstance(source.run_id, str)
        or not source.run_id
        or source.run_id == "local"
        or not isinstance(source.call_id, str)
        or not 1 <= len(source.call_id) <= 128
        or any(ord(char) < 32 or ord(char) == 127 for char in source.call_id)
    ):
        raise ServiceError("workspace_tree_model_context", "模型树须绑定原Run/call", 403)


async def run_scope(sql: AsyncSession, scope: str, run_id: str, *, live: bool) -> None:
    if not scope.startswith("session:"):
        raise ServiceError("workspace_tree_model_context", "模型树须有原Agent会话", 403)
    session = await sql.get(AgentSessionRow, scope.removeprefix("session:"))
    run = await sql.get(RunRow, run_id)
    if session is None or run is None or run.topic_id != session.topic_id:
        raise ServiceError("workspace_tree_model_run_mismatch", "模型树Run不属于原会话", 403)
    if live and (run.status not in {"queued", "running"} or run.stop_requested):
        raise ServiceError("workspace_tree_model_run_closed", "模型树原Run已关闭或停止", 409)


async def get(
    sql: AsyncSession, plan: WorkspaceTreePlanRow, kind: TreeModelKind
) -> TreePlanModelSource | None:
    row = await sql.get(WorkspaceTreeModelSourceRow, (plan.plan_id, kind))
    if row is None:
        return None
    if kind == "start" and plan.producer_status == "planned":
        raise ServiceError("workspace_tree_corrupt", "模型启动来源缺少原父准入", 409)
    source = TreePlanModelSource(row.run_id, row.call_id)
    validate(source)
    await run_scope(sql, plan.scope_id, source.run_id, live=False)
    return source


async def assert_retry(
    sql: AsyncSession, plan: WorkspaceTreePlanRow, kind: TreeModelKind, source: TreePlanModelSource
) -> None:
    if await get(sql, plan, kind) != source:
        raise ServiceError(
            "workspace_tree_model_source_conflict", "原树已绑定其他来源Run/call", 409
        )


async def assert_new(sql: AsyncSession, scope: str, source: TreePlanModelSource) -> None:
    validate(source)
    await run_scope(sql, scope, source.run_id, live=True)
    previous = await sql.scalar(
        select(WorkspaceTreeModelSourceRow).where(
            WorkspaceTreeModelSourceRow.run_id == source.run_id,
            WorkspaceTreeModelSourceRow.call_id == source.call_id,
        )
    )
    if previous is not None:
        raise ServiceError(
            "workspace_tree_model_call_conflict", "同一模型调用不能换树/阶段准入", 409
        )


async def bind(
    sql: AsyncSession, plan: WorkspaceTreePlanRow, kind: TreeModelKind, source: TreePlanModelSource
) -> None:
    sql.add(
        WorkspaceTreeModelSourceRow(
            plan_id=plan.plan_id,
            kind=kind,
            run_id=source.run_id,
            call_id=source.call_id,
        )
    )
    await sql.flush()


async def assert_execution(sql: AsyncSession, plan: WorkspaceTreePlanRow) -> None:
    source = await get(sql, plan, "start")
    if source is not None:
        await run_scope(sql, plan.scope_id, source.run_id, live=True)


async def assert_invocation(
    sql: AsyncSession,
    plan: WorkspaceTreePlanRow,
    expected: TreePlanModelSource | None,
    run_id: str | None,
) -> None:
    source = await get(sql, plan, "start")
    if source != expected:
        raise ServiceError("workspace_tree_model_source_conflict", "树围栏未保留原模型来源", 403)
    if (source is not None and source.run_id != run_id) or (source is None and run_id is not None):
        raise ServiceError("workspace_tree_model_run_mismatch", "原树动作Run未保留冻结来源", 403)
    await assert_execution(sql, plan)
