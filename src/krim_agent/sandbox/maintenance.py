"""受限维护协议路由；不接受宿主路径、任意Git参数或文件内容。"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, cast

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.checkpoints import ShadowCheckpoints
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.xfs import internal_id

if TYPE_CHECKING:
    from krim_agent.sandbox.broker import StorageBroker


FIELDS = {
    "restore_preview": {"project_id", "checkpoint_id"},
    "restore_confirm": {
        "project_id",
        "plan_id",
        "plan_digest",
        "operation_id",
        "generation",
        "selected",
    },
    "restore_status": {"project_id", "operation_id"},
    "restore_reconcile": {"project_id", "operation_id"},
    "restore_resume": {"project_id", "operation_id", "action_id"},
    "restore_abandon": {"project_id", "operation_id"},
    "checkpoint_list": {"project_id", "offset", "limit"},
    "checkpoint_retention": {"project_id", "checkpoint_id", "pinned"},
    "checkpoint_cleanup_preview": {"project_id"},
    "checkpoint_cleanup_confirm": {"project_id", "plan_id", "plan_digest", "request_key"},
    "quota_preview": {"limits"},
    "quota_confirm": {"plan_id", "plan_digest", "operation_id"},
    "quota_status": {"operation_id"},
    "quota_cancel": {"operation_id"},
    "quota_retirement_confirm": {"plan_id", "plan_digest", "operation_id"},
    "quota_retirement_status": {"operation_id"},
    "quota_retirement_cancel": {"operation_id"},
}


def dispatch_maintenance(
    broker: StorageBroker, action: str, values: dict[str, Any]
) -> dict[str, Any]:
    fields = FIELDS.get(action)
    if fields is None or set(values) != fields | {"environment_id"}:
        raise ServiceError("invalid_input", 422)
    env = internal_id(values["environment_id"])
    environment = broker._environment(env)
    args = {k: v for k, v in values.items() if k != "environment_id"}
    if action.startswith("quota_retirement_"):
        result = getattr(
            broker.quota_retirements, action.removeprefix("quota_retirement_")
        )(env, **args)
        return {
            **result,
            "retirement": True,
            "quota_state": "applied"
            if result["retirement_state"] == "retired"
            else result["retirement_state"],
        }
    if action == "quota_preview":
        limits = QuotaLimits.model_validate(args["limits"])
        if limits.checkpoint_bytes == 0 and environment.get("checkpoint_number") is not None:
            return {**broker.quota_retirements.preview(env, args["limits"]), "retirement": True}
    if action.startswith("quota_"):
        return cast(
            dict[str, Any],
            getattr(broker.quota_changes, action.removeprefix("quota_"))(env, **args),
        )
    project = internal_id(args.pop("project_id"))
    broker._project(env, project)
    if action.startswith("restore_"):
        # 改额维护中可读取旧操作，但不能开始/继续文件修改。
        if environment.get("quota_operation") and action not in {
            "restore_status",
            "restore_reconcile",
        }:
            raise ServiceError("workspace_busy")
        if action == "restore_resume":
            from krim_agent.contracts import fingerprint

            request = internal_id(args.pop("action_id"))
            name = "restore-resume-" + request + ".json"
            digest = fingerprint({"environment": env, "project": project, "arguments": args})
            prior = broker._read(name)
            if prior is not None:
                if prior["digest"] != digest:
                    raise ServiceError("key_conflict")
                return prior.get("response") or broker.recovery.status(
                    env, project, args["operation_id"]
                )
            broker._save(name, {"digest": digest, "status": "intent"})
            response = broker.recovery.resume(env, project, args["operation_id"])
            broker._save(name, {"digest": digest, "status": "completed", "response": response})
            return response
        remap = {
            "checkpoint_id": "checkpoint",
            "plan_digest": "digest",
            "operation_id": "operation",
        }
        args = {remap.get(k, k): v for k, v in args.items()}
        return cast(
            dict[str, Any],
            getattr(broker.recovery, action.removeprefix("restore_"))(env, project, **args),
        )
    if action == "checkpoint_list" and (
        type(args["offset"]) is not int or not 0 <= args["offset"] <= 1000
        or type(args["limit"]) is not int or not 1 <= args["limit"] <= 100
    ):
        raise ServiceError("invalid_input", 422)
    if (
        environment.get("checkpoint_retired_number") is not None
        and environment.get("checkpoint_number") is None
    ):
        if action == "checkpoint_list":
            return {"items": [], "total": 0, "offset": args["offset"], "limit": args["limit"]}
        # 查询/旧清理计划不能通过构造ShadowCheckpoints重建已退役目录。
        raise ServiceError("not_found", 404)
    checkpoints = ShadowCheckpoints(broker.environments / env / "checkpoints")
    if action == "checkpoint_list":
        offset, limit = args["offset"], args["limit"]
        rows = checkpoints.list(env, project)
        # 文件manifest单独按有界摘要，不把所有文件内容塞入分页响应。
        items = [
            {k: v for k, v in r.items() if k not in {"files", "excluded"}}
            | {"file_count": len(r["files"]), "excluded_count": len(r["excluded"])}
            for r in rows[offset : offset + limit]
        ]
        return {"items": items, "total": len(rows), "offset": offset, "limit": limit}
    if action == "checkpoint_retention":
        checkpoints.pin(env, project, args["checkpoint_id"], pinned=args["pinned"])
        return {"checkpoint_id": args["checkpoint_id"], "pinned": args["pinned"]}
    if action == "checkpoint_cleanup_preview":
        return checkpoints.cleanup_preview(
            env, project, QuotaLimits.model_validate(environment["limits"])
        )
    return checkpoints.cleanup_confirm(env, project, **args)


def release_completed_restore_roots(broker: StorageBroker) -> None:
    """只释放已确认结束操作的24h保护；unknown/paused永远不按TTL解除。"""
    import os

    count = 0
    with os.scandir(broker.state) as records:
        for entry in records:
            count += 1
            if count > 8192:
                break
            if not entry.name.startswith("restore-") or entry.name.startswith("restore-plan-"):
                continue
            if not entry.name.endswith(".json"):
                continue
            row = broker._read(entry.name)
            if (
                row is None
                or row.get("status") not in {"completed", "abandoned_partial"}
                or (
                    row.get("before_protected_until") is None
                    or row["before_protected_until"] > time.time()
                )
                or not row.get("before_checkpoint")
            ):
                continue
            checkpoint = broker.recovery.store(row["environment_id"])
            checkpoint.release_reference(
                row["environment_id"],
                row["project_id"],
                row["before_checkpoint"],
                "restore-before",
                row["operation_id"],
            )
            row["before_protected_until"] = None
            broker._save(entry.name, row)


def scheduled_cleanup(broker: StorageBroker) -> dict[str, Any]:
    """每次仅维护一个项目；其他账号不长期等待，活动恢复优先。"""
    import os

    release_completed_restore_roots(broker)
    count = 0
    with os.scandir(broker.state) as records:
        names = []
        for entry in records:
            count += 1
            if count > 8192:
                raise ServiceError("sandbox_limit")
            if entry.name.startswith("project-") and entry.name.endswith(".json"):
                names.append(entry.name)
    if not names:
        return {"projects": 0}
    cursor = broker._read("maintenance-cursor.json") or {"index": 0}
    index = cursor["index"] % len(names)
    broker._save("maintenance-cursor.json", {"index": index + 1})
    row = broker._read(sorted(names)[index])
    if row is None:
        raise ServiceError("facts_corrupt")
    env = row["environment_id"]
    project = row["project_id"]
    due_name = "gc-due-" + project + ".json"
    due = broker._read(due_name) or {"next_gc_at": 0}
    if due["next_gc_at"] > time.time():
        return {"skipped": "not_due"}
    broker._save(due_name, {"next_gc_at": time.time() + 900})
    if broker.jobs is not None and any(
        s["environment_id"] == env for s in broker.jobs.active.values()
    ):
        return {"skipped": "job_active"}
    if broker.recovery.active:
        return {"skipped": "restore_active"}
    state = broker._environment(env)
    if state.get("quota_operation"):
        return {"skipped": "quota_maintenance"}
    root = broker.environments / env / "checkpoints"
    if not root.is_dir():
        return {"skipped": "not_provisioned"}
    checkpoints = ShadowCheckpoints(root)
    plan = checkpoints.cleanup_preview(env, project, QuotaLimits.model_validate(state["limits"]))
    if not plan["candidates"]:
        return {"skipped": "no_candidates"}
    return checkpoints.cleanup_confirm(
        env, project, plan["plan_id"], plan["plan_digest"], "automatic:" + plan["plan_id"]
    )
