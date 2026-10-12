"""树步骤在原preview/reserve/Writer资格中的同事务关联与父回执见证。"""

from __future__ import annotations

import json
import secrets
import time
from typing import Literal

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.execution.directory_publication import CommandDirectoryProposalRequest
from krim_agent.execution.publication import CommandProposalRequest
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_change import WorkspaceChangeAction
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeReceipt,
    DirectorySnapshot,
    PreparedDirectoryChange,
)
from krim_agent.workspace_tree import TreePlanFence, TreePlanRequest
from krim_agent.workspace_writer import PreparedFileChange

from . import tree_model_sources
from .database import (
    AgentSessionRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceTreePlanRow,
    WorkspaceTreeStepRow,
)

TREE_PREVIEW_PREFIX = "krim-tree-preview:"
TREE_ACTION_PREFIX = "krim-tree-action:"


def step_key(plan_id: str, position: int, phase: Literal["preview", "change"]) -> str:
    return (
        f"{TREE_PREVIEW_PREFIX if phase == 'preview' else TREE_ACTION_PREFIX}{plan_id}:{position}"
    )


def assert_live(plan: WorkspaceTreePlanRow, owner_id: str) -> None:
    if (
        plan.producer_status != "running"
        or plan.owner_id != owner_id
        or (plan.lease_expires_at or 0) <= time.time()
    ):
        raise ServiceError("workspace_tree_fenced", "目录树原执行资格已失效", 409)


async def lookup(
    sql: AsyncSession, fence: TreePlanFence, scope: str, resource: str, source: str, phase: str
) -> tuple[WorkspaceTreePlanRow, WorkspaceTreeStepRow]:
    from .workspace_tree_plans import WorkspaceTreePlanStore

    if type(fence.position) is not int or not 0 <= fence.position < 16384:
        raise ServiceError("workspace_tree_fence_mismatch", "目录树步骤位置无效", 403)
    plan = await sql.get(WorkspaceTreePlanRow, fence.plan_id)
    step = await sql.get(WorkspaceTreeStepRow, (fence.plan_id, fence.position))
    if (
        plan is None
        or step is None
        or plan.scope_id != scope
        or plan.resource_fingerprint != resource
        or (
            source != fence.source
            or phase != fence.phase
            or source != step.source
            or not secrets.compare_digest(plan.token, fence.token)
        )
    ):
        raise ServiceError("workspace_tree_fence_mismatch", "目录树原步骤凭据不匹配", 403)
    frozen = WorkspaceTreePlanStore._frozen(plan)
    if fence.model_source != await tree_model_sources.get(sql, plan, "start"):
        raise ServiceError("workspace_tree_model_source_conflict", "树凭据未保留原模型来源", 403)
    if (
        step.position >= len(frozen)
        or {key: getattr(step, key) for key in frozen[step.position]} != frozen[step.position]
    ):
        raise ServiceError("workspace_tree_corrupt", "目录树原步骤冻结字段损坏", 409)
    await WorkspaceTreePlanStore._step_view(sql, plan, step)
    return plan, step


async def assert_source(sql: AsyncSession, plan: WorkspaceTreePlanRow) -> None:
    from .command_proposals import load_source
    from .workspace_tree_plans import WorkspaceTreePlanStore

    WorkspaceTreePlanStore._frozen(plan)
    source = await load_source(sql, plan.scope_id, plan.command_id)
    if (
        source.source_hash != plan.source_hash
        or source.workspace != plan.frozen["workspace"]
        or source.snapshot.resource.model_dump(mode="json") != plan.frozen["execution"]
    ):
        raise ServiceError("workspace_tree_source_conflict", "树原命令/资源来源已变化", 409)
    if plan.scope_id.startswith("session:"):
        session = await sql.get(AgentSessionRow, plan.scope_id.removeprefix("session:"))
        if (
            session is None
            or session.permissions.get("workspace") != plan.frozen["workspace"]
            or session.permissions.get("execution") != plan.frozen["execution"]
        ):
            raise ServiceError(
                "workspace_tree_resource_conflict", "树原会话工作区/执行资源已变化", 403
            )


async def parent_identity(
    sql: AsyncSession, plan: WorkspaceTreePlanRow, step: WorkspaceTreeStepRow
) -> tuple[int, int]:
    from .workspace_directories import decode

    if step.position == 0:
        value = plan.frozen["root_prepared"]
        if not isinstance(value, dict):
            raise ServiceError("workspace_tree_corrupt", "树根准备材料缺失", 409)
        return decode(value).parent_identity
    parent = await sql.get(WorkspaceTreeStepRow, (step.plan_id, step.parent_position))
    if parent is None or parent.source != "directory" or parent.directory_operation_id is None:
        raise ServiceError("workspace_tree_parent_unresolved", "原父目录尚未有完成回执", 409)
    attempt = await sql.get(WorkspaceDirectoryAttemptRow, parent.directory_operation_id)
    proposal = await sql.get(WorkspaceDirectoryProposalRow, parent.directory_proposal_id)
    if attempt is None or attempt.status != "done":
        raise ServiceError("workspace_tree_parent_unresolved", "原父目录结果未确认", 409)
    from .workspace_tree_plans import WorkspaceTreePlanStore

    frozen = WorkspaceTreePlanStore._frozen(plan)
    if (
        parent.position >= len(frozen)
        or {key: getattr(parent, key) for key in frozen[parent.position]} != frozen[parent.position]
    ):
        raise ServiceError("workspace_tree_corrupt", "树父冻结字段损坏", 409)
    await WorkspaceTreePlanStore._step_view(sql, plan, parent)
    if proposal is None or (
        attempt.scope_id != plan.scope_id
        or attempt.proposal_id != parent.directory_proposal_id
        or attempt.action != "apply"
        or attempt.expected_revision != parent.expected_revision
        or attempt.request_key != step_key(plan.plan_id, parent.position, "change")
        or proposal.path != parent.path
        or proposal.workspace_key != plan.workspace_key
        or proposal.resource_fingerprint != plan.resource_fingerprint
    ):
        raise ServiceError("workspace_tree_corrupt", "树父原proposal/attempt关联不符", 409)
    from .command_directory_proposals import assert_directory_source

    await assert_directory_source(sql, proposal)
    try:
        receipt = TypeAdapter(DirectoryChangeReceipt).validate_json(json.dumps(attempt.receipt))
        observed = TypeAdapter(DirectorySnapshot).validate_json(json.dumps(attempt.observed_after))
        original = decode(attempt.prepared)
        if (
            receipt.status != "done"
            or not receipt.changed
            or receipt.path != parent.path
            or receipt.before_fingerprint is not None
            or receipt.after_fingerprint != observed.fingerprint
            or receipt.operation_token != original.token
            or observed.mode != parent.mode
        ):
            raise ValueError()
        return observed.identity
    except (TypeError, ValueError):
        raise ServiceError("workspace_tree_corrupt", "树父原成功观察回执损坏", 409) from None


async def assert_preview(
    sql: AsyncSession,
    owner_id: str,
    scope: str,
    resource: str,
    source: Literal["file", "directory"],
    request: CommandProposalRequest | CommandDirectoryProposalRequest,
    fence: TreePlanFence | None,
    *,
    prepared: PreparedFileChange | PreparedDirectoryChange | None = None,
    existing_proposal_id: str | None = None,
    run_id: str | None = None,
) -> WorkspaceTreeStepRow | None:
    if fence is None:
        if existing_proposal_id is None and request.request_key.startswith(TREE_PREVIEW_PREFIX):
            raise ServiceError("workspace_tree_request_reserved", "内部树预览键禁止公开准入", 403)
        return None
    plan, step = await lookup(sql, fence, scope, resource, source, "preview")
    original_request = TreePlanRequest.model_validate(plan.frozen["request"])
    if (
        request.command_id != plan.command_id
        or request.path != step.path
        or request.request_key != step_key(plan.plan_id, step.position, "preview")
        or request.accept_failed != original_request.accept_failed
        or request.ttl_seconds != 3600
    ):
        raise ServiceError("workspace_tree_fence_mismatch", "树原来源预览参数被替换", 403)
    associated = step.file_proposal_id if source == "file" else step.directory_proposal_id
    if existing_proposal_id is not None:
        if associated != existing_proposal_id:
            raise ServiceError("workspace_tree_proposal_conflict", "树不能认领其他原提案", 409)
        return step
    await tree_model_sources.assert_invocation(sql, plan, fence.model_source, run_id)
    assert_live(plan, owner_id)
    await assert_source(sql, plan)
    identity = await parent_identity(sql, plan, step)
    if not step.admission_open or associated is not None or step.status != "pending":
        raise ServiceError("workspace_tree_step_closed", "树步骤原准入已关闭", 409)
    if prepared is not None and (
        prepared.parent_identity != identity
        or prepared.spec.path != step.path
        or prepared.before is not None
    ):
        raise ServiceError("workspace_tree_parent_mismatch", "树原父身份或目标条件不匹配", 409)
    return step


def bind_proposal(step: WorkspaceTreeStepRow | None, source: str, proposal_id: str) -> None:
    if step is None:
        return
    if source == "file":
        step.file_proposal_id = proposal_id
    else:
        step.directory_proposal_id = proposal_id
    step.expected_revision = 0


async def assert_request(
    sql: AsyncSession,
    owner_id: str,
    scope: str,
    resource: str,
    source: Literal["file", "directory"],
    request: WorkspaceChangeAction | DirectoryChangeAction,
    fence: TreePlanFence | None,
    existing_operation_id: str | None = None,
    *,
    run_id: str | None = None,
) -> WorkspaceTreeStepRow | None:
    proposal_column = (
        WorkspaceTreeStepRow.file_proposal_id
        if source == "file"
        else WorkspaceTreeStepRow.directory_proposal_id
    )
    if fence is None:
        if existing_operation_id is None and (
            request.request_key.startswith(TREE_ACTION_PREFIX)
            or await sql.scalar(
                select(WorkspaceTreeStepRow.plan_id).where(proposal_column == request.proposal_id)
            )
            is not None
        ):
            raise ServiceError(
                "workspace_tree_request_reserved", "树提案只能经原树执行凭据准入", 403
            )
        return None
    plan, step = await lookup(sql, fence, scope, resource, source, "change")
    proposal = step.file_proposal_id if source == "file" else step.directory_proposal_id
    associated = step.file_operation_id if source == "file" else step.directory_operation_id
    if (
        proposal != request.proposal_id
        or request.expected_revision != step.expected_revision
        or request.action != "apply"
        or request.request_key != step_key(plan.plan_id, step.position, "change")
    ):
        raise ServiceError(
            "workspace_tree_fence_mismatch", "树原动作/proposal/revision/key被替换", 403
        )
    if existing_operation_id is not None:
        if associated != existing_operation_id:
            raise ServiceError("workspace_tree_operation_conflict", "树不能认领其他原attempt", 409)
        return step
    await tree_model_sources.assert_invocation(sql, plan, fence.model_source, run_id)
    assert_live(plan, owner_id)
    await assert_source(sql, plan)
    await parent_identity(sql, plan, step)
    if not step.admission_open or associated is not None or step.status != "pending":
        raise ServiceError("workspace_tree_step_closed", "树原步骤准入已关闭", 409)
    return step


def bind_operation(step: WorkspaceTreeStepRow | None, source: str, operation_id: str) -> None:
    if step is None:
        return
    if source == "file":
        step.file_operation_id = operation_id
    else:
        step.directory_operation_id = operation_id
    step.admission_open = False


async def assert_execution(
    sql: AsyncSession, owner_id: str, source: Literal["file", "directory"], operation_id: str
) -> None:
    column = (
        WorkspaceTreeStepRow.file_operation_id
        if source == "file"
        else WorkspaceTreeStepRow.directory_operation_id
    )
    step = await sql.scalar(select(WorkspaceTreeStepRow).where(column == operation_id))
    if step is None:
        return
    plan = await sql.get(WorkspaceTreePlanRow, step.plan_id)
    if plan is None:
        raise ServiceError("workspace_tree_corrupt", "树原父计划缺失", 409)
    attempt = (
        await sql.get(WorkspaceEditAttemptRow, operation_id)
        if source == "file"
        else await sql.get(WorkspaceDirectoryAttemptRow, operation_id)
    )
    if (
        attempt is None
        or attempt.scope_id != plan.scope_id
        or attempt.proposal_id
        != (step.file_proposal_id if source == "file" else step.directory_proposal_id)
        or attempt.action != "apply"
        or attempt.expected_revision != step.expected_revision
        or attempt.request_key != step_key(plan.plan_id, step.position, "change")
    ):
        raise ServiceError("workspace_tree_corrupt", "树原attempt关联损坏", 409)
    model_source = await tree_model_sources.get(sql, plan, "start")
    if model_source is not None and (attempt.run_id, attempt.call_id) != (
        model_source.run_id,
        model_source.call_id,
    ):
        raise ServiceError("workspace_tree_corrupt", "树原attempt模型Run/call关联损坏", 409)
    await tree_model_sources.assert_execution(sql, plan)
    assert_live(plan, owner_id)
    await assert_source(sql, plan)
    await lookup(
        sql,
        TreePlanFence(plan.plan_id, step.position, source, "change", plan.token, model_source),
        plan.scope_id,
        plan.resource_fingerprint,
        source,
        "change",
    )
    from .workspace_directories import decode
    from .workspace_edits import decode_prepared

    if isinstance(attempt, WorkspaceEditAttemptRow):
        prepared: PreparedFileChange | PreparedDirectoryChange = decode_prepared(
            attempt.prepared, attempt.before_content, attempt.after_content
        )
    else:
        prepared = decode(attempt.prepared)
    if prepared.parent_identity != await parent_identity(sql, plan, step):
        raise ServiceError("workspace_tree_parent_mismatch", "树父成功身份与原操作不匹配", 409)


async def attempt_call(
    sql: AsyncSession, step: WorkspaceTreeStepRow | None, call_id: str | None
) -> str | None:
    if step is None:
        return call_id
    plan = await sql.get(WorkspaceTreePlanRow, step.plan_id)
    if plan is None:
        raise ServiceError("workspace_tree_corrupt", "树原父计划缺失", 409)
    source = await tree_model_sources.get(sql, plan, "start")
    if source is not None and call_id != source.call_id:
        raise ServiceError("workspace_tree_model_source_conflict", "树动作未携带原start call", 403)
    return call_id
