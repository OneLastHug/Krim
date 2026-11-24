"""root_merge：阶段 6 的根显式整合助手（plan §9.2，真实 baseline/CAS）。

caller 与 invocation 都由工具层从域解析后传入（模型不填任何授权字段）：
仅 root 角色允许；目标任务必须属同一棵树、已终态（settle 结束）且
task.input 携带 ``baseline``/``workspace``（spawn 隔离工作区的基线与
分配）。补丁从子 run 的 ``workspace_patch`` 事件读取（payload 兼容顶层
model_dump 与 ``{"patch": …}`` 两种形状），``expected_patch_fingerprint``
必须与事件指纹一致（CAS）。

稳定 key = fingerprint(invocation.run_id, call_id, task_id)：整合前先写
``integrate-intent:<hash>`` intent 事件；成功后写同前缀
``integrate:<hash>`` receipt 事件（含 state/patch/post_fingerprint 与
``tests_pending``——整合后测试由根模型用既有工具自行执行，助手不造测试
结果）。同 key 重试只读返回原 receipt；只有 intent 没有 receipt 说明上次
结果未知——一律拒 execution_unknown 交用户核对，即使之前尚未发生第一个
写入也不重新应用、不覆盖已应用部分。port 由宿主提供（授权沙箱面），本
助手不构造命令、不调模型；测试通过不等于真实容器验证。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.storage.models import run_events
from krim_agent.storage.subagent_models import subagent_members, subagent_tasks
from krim_agent.tools.core import Invocation

from .errors import SubagentError
from .types import TERMINAL_TASK_STATUS
from .workspace import WorkspaceAssignment, WorkspaceBaseline, WorkspaceIsolation, WorkspacePatch

if TYPE_CHECKING:
    from .service import Caller

INTENT_PREFIX = "integrate-intent:"
RESULT_PREFIX = "integrate:"


def _event_ids(invocation: Invocation, task_id: str) -> tuple[str, str]:
    digest = fingerprint(
        {"run": invocation.run_id, "call": invocation.call_id, "task": task_id}
    )
    return INTENT_PREFIX + digest, RESULT_PREFIX + digest


def _patch_from_event(payload: dict[str, Any]) -> WorkspacePatch:
    nested = payload.get("patch")
    data = nested if isinstance(nested, dict) else payload
    # process 事件键 patch_fingerprint；WorkspacePatch.model_dump 键 fingerprint。
    fingerprint_value = data.get("patch_fingerprint") or data.get("fingerprint")
    for field in ("baseline_fingerprint", "source_project_id", "directory"):
        if not isinstance(data.get(field), str) or not data[field]:
            raise ServiceError("facts_corrupt")
    if not isinstance(fingerprint_value, str) or not fingerprint_value:
        raise ServiceError("facts_corrupt")
    return WorkspacePatch(
        mode=data.get("mode") or "isolated_write",
        source_project_id=str(data["source_project_id"]),
        workspace_project_id=str(data.get("workspace_project_id") or data["source_project_id"]),
        directory=str(data["directory"]),
        input_version=int(data.get("input_version") or 1),
        baseline_fingerprint=str(data["baseline_fingerprint"]),
        changes=(),
        truncated=bool(data.get("truncated")),
        created_at=float(data.get("created_at") or 0.0),
        fingerprint=fingerprint_value,
    )


async def root_merge(
    store: Any,
    caller: Caller,
    invocation: Invocation,
    target_task_id: str,
    expected_patch_fingerprint: str,
    port: Any,
) -> dict[str, Any]:
    """root 显式整合一个已终态委派任务的隔离工作区补丁。

    key 幂等以宿主 exclusive 串行入口为前提（workspace_integrate 工具默认
    exclusive）；并发同 key 调用需外层串行，本助手不做跨调用加锁。
    """
    if caller.member.get("role") != "root":
        raise SubagentError("permission_denied", accepted=False,
                            action="only the root coordinator integrates patches")
    if invocation.run_id != caller.scope.run_id:
        # 工具层绑定错误：invocation 必须就是 caller 的根 run。
        raise SubagentError("permission_denied", accepted=False)
    tree_id = caller.tree["tree_id"]
    run_id = caller.scope.run_id
    intent_id, result_id = _event_ids(invocation, target_task_id)

    async def load(conn: Any) -> Any:
        task = (
            (
                await conn.execute(
                    select(subagent_tasks).where(subagent_tasks.c.id == target_task_id)
                )
            )
            .mappings()
            .first()
        )
        producer = (
            (
                await conn.execute(
                    select(subagent_members).where(
                        subagent_members.c.id == task["agent_id"]
                    )
                )
            )
            .mappings()
            .first()
            if task is not None
            else None
        )
        events = (
            await conn.execute(
                select(run_events.c.event_id, run_events.c.data).where(
                    run_events.c.run_id == run_id,
                    run_events.c.event_id.in_([intent_id, result_id]),
                )
            )
        ).all()
        return task, producer, {event_id: data for event_id, data in events}

    task, producer, existing = await store.work_read(load)

    def receipt(data: dict[str, Any], *, replayed: bool) -> dict[str, Any]:
        return {
            "key": result_id,
            "task_id": target_task_id,
            "tests_pending": True,
            "replayed": replayed,
            **data,
        }

    if task is None or task["tree_id"] != tree_id:
        raise ServiceError("not_found", 404)
    if producer is None or producer["tree_id"] != tree_id or producer["role"] != "agent":
        # 来源必须是本树 root 后代的 worker 成员，不收宿主/根自身的补丁。
        raise ServiceError("facts_corrupt")
    if task["status"] not in TERMINAL_TASK_STATUS:
        raise SubagentError("invalid_input", accepted=False,
                            action="settle the delegated task before integrating its patch")
    if task["run_id"] is None:
        raise ServiceError("facts_corrupt")
    document: Any = task["input"]
    raw_baseline = document.get("baseline") if isinstance(document, dict) else None
    raw_assignment = document.get("workspace") if isinstance(document, dict) else None
    if not isinstance(raw_baseline, dict) or not isinstance(raw_assignment, dict):
        raise SubagentError("invalid_input", accepted=False,
                            action="the task carries no isolated workspace to integrate")
    # strict 模型不接受 list 形状：按 JSON 语义从存储文档重建。
    baseline = WorkspaceBaseline.model_validate_json(encoded(raw_baseline))
    assignment = WorkspaceAssignment.model_validate_json(encoded(raw_assignment))
    if caller.tree.get("project_id") is not None and (
        caller.tree["project_id"] != baseline.project_id
    ):
        raise ServiceError("facts_corrupt")
    snapshot = await store.snapshot(str(task["run_id"]))
    event = next(
        (item for item in reversed(snapshot.events) if item.kind == "workspace_patch"), None
    )
    if event is None:
        raise ServiceError("facts_corrupt")
    patch = _patch_from_event(dict(event.data))
    if patch.fingerprint != expected_patch_fingerprint:
        raise ServiceError("input_revision_conflict")
    if (
        patch.baseline_fingerprint != baseline.fingerprint
        or patch.source_project_id != baseline.project_id
        or patch.source_project_id != assignment.source_project_id
        or patch.directory != assignment.directory
        or patch.input_version != assignment.input_version
    ):
        raise ServiceError("facts_corrupt")

    # 重放也按本次 expected 指纹核对：同 key 换指纹视为新的 CAS 冲突。
    if result_id in existing:
        prior = dict(existing[result_id])
        if prior.get("patch_fingerprint") != expected_patch_fingerprint:
            raise ServiceError("input_revision_conflict")
        return receipt(prior, replayed=True)
    if intent_id in existing:
        raise SubagentError(
            "execution_unknown",
            action="A previous integration started but its receipt is unknown; "
            "inspect the source tree with the user instead of retrying.",
        )

    intent_data: dict[str, Any] = {
        "task_id": target_task_id,
        "tree_id": tree_id,
        "patch_fingerprint": patch.fingerprint,
        "baseline_fingerprint": baseline.fingerprint,
        "source_project_id": patch.source_project_id,
        "directory": patch.directory,
    }
    await store.append(run_id, "workspace_integrate_intent", intent_data, event_id=intent_id)
    outcome = await WorkspaceIsolation(port).integrate(patch, baseline)
    result_data: dict[str, Any] = {
        "state": outcome.state,
        "code": outcome.code,
        "patch_fingerprint": outcome.patch_fingerprint,
        "baseline_fingerprint": baseline.fingerprint,
        "post_fingerprint": outcome.post_fingerprint,
        "applied": list(outcome.applied),
        "skipped": list(outcome.skipped),
        "pending": list(outcome.pending),
        "task_id": target_task_id,
        "child_run_id": str(task["run_id"]),
    }
    await store.append(run_id, "workspace_integrated", result_data, event_id=result_id)
    return receipt(result_data, replayed=False)


__all__ = ["root_merge"]
