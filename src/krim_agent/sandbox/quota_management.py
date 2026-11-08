"""Broker在线配额调整：预览、确认、状态与取消；目标配置与内核实际值全程分开。

协议要点（docs/proposals/sandbox-recovery-gc-quota-review.md 第10-11节）：
- 预览零内核写入，15分钟有效；确认先保存操作意图，再设置账号维护门
  （env["quota_operation"] 与 quota_state），正常命令准入由控制面检查该门。
- 应用前等待全部活跃写者收束（项目租约、命令作业、恢复作业），不按期限抢锁；
  维护资格是 env/checkpoints/<env>/maintenance.lock 的非阻塞flock，不跨请求持有。
- 多区调整先收缩后扩张，任何收缩失败停止后续扩容；目标低于当前用量的收缩
  不写内核，标记 pending_reclaim，读取与明确清理始终可用，绝不自动删除文件。
- xfs limit 值 0 表示 unlimited；任何读回与比较都不得把它当成 0 硬限额。
- 每步先持久化"即将执行"，再写内核并读回实际值；回包丢失按原操作核实内核
  实际值，不盲写补偿。desired_revision 与鉴权由控制面负责，不在本模块。
"""

from __future__ import annotations

import fcntl
import os
import stat
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.xfs import internal_id, private_directory

if TYPE_CHECKING:
    from krim_agent.sandbox.broker import StorageBroker

PREVIEW_SECONDS = 900
RESERVATION_CEILING = 0.85
CHECKPOINT_NUMBER_OFFSET = 0x40000000
_PLAN_PREFIX = "quota-plan-"
_CHANGE_PREFIX = "quota-change-"
_HEX = frozenset("0123456789abcdef")
_STEP_RANK = {"shrink": 0, "initialize": 1, "grow": 2}


class QuotaChanges:
    """账号沙箱配额的在线调整协议；只改已登记project ID的限额，不递归重标记目录。"""

    def __init__(self, broker: StorageBroker) -> None:
        self.broker = broker

    # ---------- 入口 ----------

    def preview(self, environment_id: str, limits: dict[str, Any]) -> dict[str, Any]:
        """无副作用预览：目标、旧实际值、layout、usage与节点容量分开返回。"""
        environment_id = internal_id(environment_id)
        target = self._limits(limits)
        record = self.broker._environment(environment_id)
        current = self._snapshot(record)
        normalized = self._normalized(target)
        target_layout = "split" if normalized["checkpoint_bytes"] > 0 else "unsplit"
        usage = self._usage_view(record)
        node = self._node_view(environment_id, target)
        busy = self._active_writers(environment_id) or {
            "jobs": [],
            "recovery": [],
            "leases": [],
            "maintenance_lock": False,
        }
        reclaim = self._reclaim(record, normalized)
        now = time.time()
        plan_id = str(uuid.uuid4())
        target_limits = target.model_dump(mode="json")
        digest = fingerprint(
            {
                "environment_id": environment_id,
                "limits": target_limits,
                "normalized": normalized,
                "layout": target_layout,
            }
        )
        self.broker._save(
            _PLAN_PREFIX + plan_id + ".json",
            {
                "plan_id": plan_id,
                "environment_id": environment_id,
                "digest": digest,
                "created_at": now,
                "expires_at": now + PREVIEW_SECONDS,
                "limits": target_limits,
                "normalized": normalized,
                "layout": target_layout,
                "current": current,
            },
        )
        idle = not (busy["jobs"] or busy["recovery"] or busy["leases"])
        return {
            "environment_id": environment_id,
            "plan_id": plan_id,
            "plan_digest": digest,
            "expires_at": now + PREVIEW_SECONDS,
            "preview_seconds": PREVIEW_SECONDS,
            "current": current,
            "target": {
                "layout": target_layout,
                "limits": target_limits,
                "checkpoint_enabled": normalized["checkpoint_bytes"] > 0,
                "normalized": normalized,
                "zone_actions": self._zone_actions(current, normalized),
            },
            "usage": usage,
            "node": node,
            "busy": busy,
            "reclaim": reclaim,
            "applicable_now": node["target_fits"] and idle and not reclaim["required"],
        }

    def confirm(
        self, environment_id: str, plan_id: str, plan_digest: str, operation_id: str
    ) -> dict[str, Any]:
        """接受预览并启动在线应用；同plan同op重复确认只核实原操作。"""
        environment_id = internal_id(environment_id)
        plan_id = internal_id(plan_id)
        operation_id = internal_id(operation_id)
        plan_digest = self._digest(plan_digest)
        existing = self.broker._read(_CHANGE_PREFIX + operation_id + ".json")
        if existing is not None:
            if existing.get("environment_id") != environment_id:
                raise ServiceError("not_found", 404)
            if existing.get("plan_id") != plan_id or existing.get("plan_digest") != plan_digest:
                raise ServiceError("key_conflict")
            return self._advance(existing)
        plan = self.broker._read(_PLAN_PREFIX + plan_id + ".json")
        if plan is None or plan.get("environment_id") != environment_id:
            raise ServiceError("not_found", 404)
        if plan.get("digest") != plan_digest:
            raise ServiceError("key_conflict")
        if time.time() >= plan["expires_at"]:
            raise ServiceError("approval_expired")
        target = self._limits(plan["limits"])
        normalized = self._normalized(target)
        if not self._node_view(environment_id, target)["target_fits"]:
            # 节点预留不足：拒绝扩容，不创建操作记录，也不设置维护门。
            raise ServiceError("sandbox_limit")
        record = self.broker._environment(environment_id)
        if record.get("quota_operation") not in {None, operation_id}:
            raise ServiceError("workspace_busy")
        steps, blocked = self._build_steps(environment_id, record, target)
        state = "pending_reclaim" if blocked else "waiting_idle"
        now = time.time()
        target_limits = target.model_dump(mode="json")
        operation: dict[str, Any] = {
            "operation_id": operation_id,
            "plan_id": plan_id,
            "plan_digest": plan_digest,
            "environment_id": environment_id,
            "quota_state": state,
            "blocked_reason": blocked,
            "confirmed_at": now,
            "updated_at": now,
            "desired": {
                "limits": target_limits,
                "normalized": normalized,
                "layout": "split" if normalized["checkpoint_bytes"] > 0 else "unsplit",
            },
            "old_applied": self._snapshot(record),
            "steps": steps,
            "reservation": self._reservation(environment_id, target),
            "waiting_on": None,
        }
        self.broker._save(_CHANGE_PREFIX + operation_id + ".json", operation)
        self._gate(environment_id, operation_id, state)
        return self._advance(operation)

    def status(self, environment_id: str, operation_id: str) -> dict[str, Any]:
        """读取并可推进原操作：等待空闲后继续应用，pending_reclaim复核用量。"""
        environment_id = internal_id(environment_id)
        operation_id = internal_id(operation_id)
        operation = self.broker._read(_CHANGE_PREFIX + operation_id + ".json")
        if operation is None or operation.get("environment_id") != environment_id:
            raise ServiceError("not_found", 404)
        return self._advance(operation)

    def cancel(self, environment_id: str, operation_id: str) -> dict[str, Any]:
        """只允许撤销尚未修改内核的waiting_idle/pending_reclaim操作。"""
        environment_id = internal_id(environment_id)
        operation_id = internal_id(operation_id)
        operation = self.broker._read(_CHANGE_PREFIX + operation_id + ".json")
        if operation is None or operation.get("environment_id") != environment_id:
            raise ServiceError("not_found", 404)
        if operation["quota_state"] not in {"waiting_idle", "pending_reclaim"} or any(
            step["state"] not in {"prepared", "pending_reclaim"} for step in operation["steps"]
        ):
            raise ServiceError("invalid_input", 422)
        operation["quota_state"] = "cancelled"
        operation["waiting_on"] = None
        operation["updated_at"] = time.time()
        self._save_operation(operation)
        record = self.broker._read("env-" + environment_id + ".json")
        if record is not None and record.get("quota_operation") == operation_id:
            cleared = {
                key: value
                for key, value in record.items()
                if key not in {"quota_operation", "quota_state"}
            }
            self.broker._save("env-" + environment_id + ".json", cleared)
            self._reserve(environment_id, record["limits"], larger_only=False)
        return self._view(operation)

    # ---------- 应用推进 ----------

    def _advance(self, operation: dict[str, Any]) -> dict[str, Any]:
        environment_id = operation["environment_id"]
        if operation["quota_state"] in {"applied", "cancelled"}:
            return self._view(operation)
        record = self.broker._environment(environment_id)
        if not self._resume_blocked(operation, record):
            self._save_operation(operation)
            self._gate(environment_id, operation["operation_id"], operation["quota_state"])
            return self._view(operation)
        busy = self._active_writers(environment_id)
        if busy is None:
            busy = self._apply_under_lock(operation, record)
        if busy is not None:
            operation["quota_state"] = "waiting_idle"
            operation["waiting_on"] = busy
            operation["updated_at"] = time.time()
            self._save_operation(operation)
            self._gate(environment_id, operation["operation_id"], "waiting_idle")
        return self._view(operation)

    def _apply_under_lock(
        self, operation: dict[str, Any], record: dict[str, Any]
    ) -> dict[str, Any] | None:
        """取得检查点维护锁后复核租约并逐项应用；锁不可得时waiting_idle。"""
        environment_id = operation["environment_id"]
        try:
            with self._maintenance(environment_id):
                stuck = [
                    project_id
                    for project_id in self._environment_projects(environment_id)
                    if not self._idle_or_paused(environment_id, project_id)
                ]
                if stuck:
                    return {"jobs": [], "recovery": [], "leases": stuck, "maintenance_lock": False}
                if operation["quota_state"] != "applying":
                    operation["quota_state"] = "applying"
                    self._gate(environment_id, operation["operation_id"], "applying")
                self._run_steps(operation, record)
        except ServiceError as error:
            if error.code != "workspace_busy":
                raise
            return {"jobs": [], "recovery": [], "leases": [], "maintenance_lock": True}
        return None

    def _resume_blocked(self, operation: dict[str, Any], record: dict[str, Any]) -> bool:
        """复核blocked原因；目录已清空时按原目标重建步骤，其余保持pending_reclaim。"""
        blocked = operation.get("blocked_reason")
        if blocked is None:
            return True
        operation["quota_state"] = "pending_reclaim"
        if blocked == "checkpoint_directory_not_empty":
            environment_id = operation["environment_id"]
            if self._checkpoint_dir_empty(environment_id):
                steps, still = self._build_steps(
                    environment_id, record, self._limits(operation["desired"]["limits"])
                )
                if still is None:
                    operation["blocked_reason"] = None
                    operation["steps"] = steps
                    return True
        return False

    def _run_steps(self, operation: dict[str, Any], record: dict[str, Any]) -> None:
        """按"先缩后扩"顺序执行；每步先落盘再写内核并读回核实，失败即停。"""
        environment_id = operation["environment_id"]
        for step in operation["steps"]:
            if step["state"] == "verified":
                continue
            # pending_reclaim/failed步骤每轮复核：清理降低用量或瞬态故障消失后按原计划继续。
            if step["kind"] != "init":
                used = self.broker.quota.usage(step["project_number"])
                if (
                    used["environment_bytes"] > step["byte_limit"]
                    or used["environment_inodes"] > step["inode_limit"]
                ):
                    step["state"] = "pending_reclaim"
                    step["reason"] = "usage_above_target"
                    self._halt(operation)
                    return
            if step["kind"] == "init" and not self._checkpoint_dir_empty(environment_id):
                step["state"] = "pending_reclaim"
                step["reason"] = "checkpoint_directory_not_empty"
                self._halt(operation)
                return
            if step["state"] == "executing" and self._matches(step):
                # 回包丢失：内核已是目标值，核实而不重写。
                step["state"] = "verified"
                step["reason"] = None
                step["error"] = None
                self._save_operation(operation)
                continue
            step["state"] = "executing"
            step["reason"] = None
            step["error"] = None
            operation["quota_state"] = "applying"
            operation["updated_at"] = time.time()
            self._save_operation(operation)
            try:
                self._apply_step(step, environment_id)
                actual = self.broker.quota.usage(step["project_number"])
            except ServiceError as error:
                step["error"] = error.code
                operation["quota_state"] = "reconcile_required"
                operation["updated_at"] = time.time()
                self._gate(environment_id, operation["operation_id"], "reconcile_required")
                self._save_operation(operation)
                return
            if (
                actual["byte_limit"] != step["byte_limit"]
                or actual["inode_limit"] != step["inode_limit"]
            ):
                step["state"] = "failed"
                step["reason"] = "kernel_limit_mismatch"
                step["actual"] = {
                    "byte_limit": actual["byte_limit"],
                    "inode_limit": actual["inode_limit"],
                }
                self._halt(operation)
                return
            step["state"] = "verified"
            self._save_operation(operation)
        self._finalize(operation, record)

    def _halt(self, operation: dict[str, Any]) -> None:
        operation["updated_at"] = time.time()
        self._save_operation(operation)
        self._finalize(operation, self.broker._environment(operation["environment_id"]))

    def _finalize(self, operation: dict[str, Any], record: dict[str, Any]) -> None:
        steps = operation["steps"]
        if not steps:
            state = "pending_reclaim" if operation.get("blocked_reason") else "applied"
        elif all(step["state"] == "verified" for step in steps):
            state = "applied"
        elif any(step["state"] in {"verified", "failed"} for step in steps):
            state = "partial_apply"
        else:
            state = "pending_reclaim"
        environment_id = operation["environment_id"]
        if state == "applied":
            self._commit_applied(operation, record)
            self._open_gate(environment_id, "applied")
        else:
            # 失败保留较大节点预留；未生效的收缩不释放预算。
            self._reserve(environment_id, operation["desired"]["limits"], larger_only=True)
            self._gate(environment_id, operation["operation_id"], state)
        operation["quota_state"] = state
        operation["updated_at"] = time.time()
        self._save_operation(operation)

    def _commit_applied(self, operation: dict[str, Any], record: dict[str, Any]) -> None:
        """仅在全部核实后更新env limits、layout与节点预留，并解除维护门。"""
        environment_id = operation["environment_id"]
        updated = {key: value for key, value in record.items() if key != "quota_operation"}
        updated["limits"] = operation["desired"]["limits"]
        updated["quota_state"] = "applied"
        for step in operation["steps"]:
            if step["zone"] == "checkpoint" and step["kind"] == "init":
                updated["checkpoint_number"] = step["project_number"]
        if updated.get("checkpoint_number") is not None:
            checkpoint = self.broker.quota.usage(updated["checkpoint_number"])
            data = self.broker.quota.usage(record["project_number"])
            updated["checkpoint_bytes"] = checkpoint["byte_limit"]
            updated["data_bytes"] = data["byte_limit"]
        self.broker._save("env-" + environment_id + ".json", updated)
        self._reserve(environment_id, operation["desired"]["limits"], larger_only=False)

    def _apply_step(self, step: dict[str, Any], environment_id: str) -> None:
        if step["kind"] == "init":
            path = self.broker.environments / environment_id / "checkpoints"
            os.makedirs(path, mode=0o700, exist_ok=True)
            private_directory(path)
            self.broker.quota.command(f"project -s -p {path} {step['project_number']}")
        self.broker.quota.command(
            f"limit -p bhard={step['byte_limit'] // 1024}k"
            f" ihard={step['inode_limit']} {step['project_number']}"
        )

    def _matches(self, step: dict[str, Any]) -> bool:
        try:
            actual = self.broker.quota.usage(step["project_number"])
        except ServiceError:
            return False
        return bool(
            actual["byte_limit"] == step["byte_limit"]
            and actual["inode_limit"] == step["inode_limit"]
        )

    # ---------- 计划与步骤构建 ----------

    def _build_steps(
        self, environment_id: str, record: dict[str, Any], target: QuotaLimits
    ) -> tuple[list[dict[str, Any]], str | None]:
        """返回(有序步骤, blocked原因)；unsplit目标先缩data再登记空检查点区。"""
        number = record["project_number"]
        checkpoint_number = record.get("checkpoint_number")
        if checkpoint_number is not None:
            checkpoint_number = int(checkpoint_number)
        normalized = self._normalized(target)
        enabled = normalized["checkpoint_bytes"] > 0
        if not enabled and checkpoint_number is not None:
            # 关闭检查点：保留旧区applied直到无受保护内容并被明确清理；本协议不做GC。
            return [], "checkpoint_disable_requires_reclaim"
        entries = []
        data_step = self._maybe_step(
            "data",
            "limit",
            number,
            self._zone(number),
            normalized["data_bytes"],
            normalized["data_inodes"],
        )
        if data_step is not None:
            entries.append(data_step)
        if not enabled:
            return self._ordered(entries), None
        if checkpoint_number is None:
            if not self._checkpoint_dir_empty(environment_id):
                # 未知非空检查点目录交reconcile，不递归重标记租户文件。
                return [], "checkpoint_directory_not_empty"
            entries.append(
                {
                    "step_id": 0,
                    "zone": "checkpoint",
                    "kind": "init",
                    "project_number": number + CHECKPOINT_NUMBER_OFFSET,
                    "byte_limit": normalized["checkpoint_bytes"],
                    "inode_limit": normalized["checkpoint_inodes"],
                    "action": "initialize",
                    "state": "prepared",
                    "reason": None,
                    "error": None,
                }
            )
        else:
            assert checkpoint_number is not None
            checkpoint_step = self._maybe_step(
                "checkpoint",
                "limit",
                checkpoint_number,
                self._zone(checkpoint_number),
                normalized["checkpoint_bytes"],
                normalized["checkpoint_inodes"],
            )
            if checkpoint_step is not None:
                entries.append(checkpoint_step)
        return self._ordered(entries), None

    @staticmethod
    def _ordered(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """先缩后扩；同级保持data→checkpoint→init的稳定顺序。"""
        expanded = []
        for step in entries:
            before = step.get("before")
            if before is not None and step["action"] == "shrink":
                low_bytes = (
                    step["byte_limit"]
                    if before["byte_limit_unlimited"]
                    else min(before["byte_limit"], step["byte_limit"])
                )
                low_inodes = (
                    step["inode_limit"]
                    if before["inode_limit_unlimited"]
                    else min(before["inode_limit"], step["inode_limit"])
                )
                if low_bytes != step["byte_limit"] or low_inodes != step["inode_limit"]:
                    expanded.append(
                        {
                            **step,
                            "byte_limit": low_bytes,
                            "inode_limit": low_inodes,
                            "action": "shrink",
                        }
                    )
                    expanded.append({**step, "action": "grow"})
                    continue
            expanded.append(step)
        ordered = sorted(expanded, key=lambda step: _STEP_RANK[step["action"]])
        for index, step in enumerate(ordered, start=1):
            step["step_id"] = index
        return ordered

    @staticmethod
    def _maybe_step(
        zone: str,
        kind: str,
        number: int,
        current: dict[str, Any],
        byte_limit: int,
        inode_limit: int,
    ) -> dict[str, Any] | None:
        action = QuotaChanges._direction(current, byte_limit, inode_limit)
        if action == "none":
            return None
        return {
            "step_id": 0,
            "zone": zone,
            "kind": kind,
            "project_number": number,
            "byte_limit": byte_limit,
            "inode_limit": inode_limit,
            "action": action,
            "before": current,
            "state": "prepared",
            "reason": None,
            "error": None,
        }

    @staticmethod
    def _direction(current: dict[str, Any], byte_limit: int, inode_limit: int) -> str:
        """unlimited按无穷大处理：任何分量收紧即视为收缩，排在扩容之前。"""
        shrink = (current["byte_limit_unlimited"] or byte_limit < current["byte_limit"]) or (
            current["inode_limit_unlimited"] or inode_limit < current["inode_limit"]
        )
        if shrink:
            return "shrink"
        if byte_limit > current["byte_limit"] or inode_limit > current["inode_limit"]:
            return "grow"
        return "none"

    # ---------- 状态与视图 ----------

    def _view(self, operation: dict[str, Any]) -> dict[str, Any]:
        record = self.broker._environment(operation["environment_id"])
        applied = self._snapshot(record)
        return {
            "operation_id": operation["operation_id"],
            "plan_id": operation["plan_id"],
            "environment_id": operation["environment_id"],
            "quota_state": operation["quota_state"],
            "blocked_reason": operation.get("blocked_reason"),
            "desired": operation["desired"],
            "old_applied": operation["old_applied"],
            "applied": applied,
            "usage": self._usage_view(record),
            "zones": self._zone_statuses(operation, applied),
            "steps": operation["steps"],
            "gate": {
                "quota_operation": record.get("quota_operation"),
                "new_writes_allowed": record.get("quota_operation") is None,
                "reads_allowed": True,
                "cleanup_allowed": True,
            },
            "reservation": operation["reservation"],
            "waiting_on": operation.get("waiting_on"),
            "confirmed_at": operation["confirmed_at"],
            "updated_at": operation["updated_at"],
        }

    def _zone_statuses(self, operation: dict[str, Any], applied: dict[str, Any]) -> dict[str, Any]:
        desired = operation["desired"]["normalized"]
        blocked = operation.get("blocked_reason")
        result: dict[str, Any] = {}
        for zone, actual in (("data", applied["data"]), ("checkpoint", applied["checkpoint"])):
            steps = [step for step in operation["steps"] if step["zone"] == zone]
            if steps:
                states = {step["state"] for step in steps}
                if "pending_reclaim" in states:
                    status = "pending_reclaim"
                elif "failed" in states:
                    status = "failed"
                elif states == {"verified"}:
                    status = "applied"
                else:
                    status = "pending"
            elif blocked == "checkpoint_disable_requires_reclaim":
                status = "kept_applied"
            elif blocked == "checkpoint_directory_not_empty" and zone == "checkpoint":
                status = "unavailable"
            elif zone == "checkpoint":
                if desired["checkpoint_bytes"] == 0 and actual is not None:
                    status = "kept_applied"
                else:
                    status = "not_provisioned" if actual is None else "applied"
            else:
                status = "applied"
            result[zone] = {
                "status": status,
                "action": steps[0]["action"] if steps else "none",
            }
        return result

    def _usage_view(self, record: dict[str, Any]) -> dict[str, Any]:
        def used(row: dict[str, Any]) -> dict[str, int]:
            return {
                "environment_bytes": row["environment_bytes"],
                "environment_inodes": row["environment_inodes"],
            }

        data = self.broker.quota.usage(record["project_number"])
        checkpoint_number = record.get("checkpoint_number")
        checkpoint = (
            None if checkpoint_number is None else self.broker.quota.usage(checkpoint_number)
        )
        account = dict(data)
        if checkpoint is not None:
            account = {key: account[key] + checkpoint[key] for key in account}
        return {
            "data": used(data),
            "checkpoint": None if checkpoint is None else used(checkpoint),
            "account": used(account),
            "measured_at": time.time(),
        }

    def _snapshot(self, record: dict[str, Any]) -> dict[str, Any]:
        checkpoint_number = record.get("checkpoint_number")
        return {
            "layout": "split" if checkpoint_number is not None else "unsplit",
            "limits": record.get("limits"),
            "storage_state": {
                "data": "provisioned",
                "checkpoint": "provisioned" if checkpoint_number is not None else "not_provisioned",
            },
            "data": self._zone(record["project_number"]),
            "checkpoint": None if checkpoint_number is None else self._zone(checkpoint_number),
        }

    def _zone(self, number: int) -> dict[str, Any]:
        row = self.broker.quota.usage(number)
        return {
            "project_number": number,
            "byte_limit": row["byte_limit"],
            "inode_limit": row["inode_limit"],
            "byte_limit_unlimited": row["byte_limit"] == 0,
            "inode_limit_unlimited": row["inode_limit"] == 0,
        }

    def _reclaim(self, record: dict[str, Any], normalized: dict[str, int]) -> dict[str, Any]:
        reasons: list[str] = []
        data = self.broker.quota.usage(record["project_number"])
        if (
            data["environment_bytes"] > normalized["data_bytes"]
            or data["environment_inodes"] > normalized["data_inodes"]
        ):
            reasons.append("data_usage_above_target")
        checkpoint_number = record.get("checkpoint_number")
        if normalized["checkpoint_bytes"] == 0 and checkpoint_number is not None:
            reasons.append("checkpoint_disable_requires_reclaim")
        elif checkpoint_number is not None:
            checkpoint = self.broker.quota.usage(checkpoint_number)
            if (
                checkpoint["environment_bytes"] > normalized["checkpoint_bytes"]
                or checkpoint["environment_inodes"] > normalized["checkpoint_inodes"]
            ):
                reasons.append("checkpoint_usage_above_target")
        return {"required": bool(reasons), "reasons": reasons}

    @staticmethod
    def _zone_actions(current: dict[str, Any], normalized: dict[str, int]) -> dict[str, str]:
        actions = {
            "data": QuotaChanges._direction(
                current["data"], normalized["data_bytes"], normalized["data_inodes"]
            )
        }
        checkpoint = current["checkpoint"]
        if normalized["checkpoint_bytes"] == 0:
            actions["checkpoint"] = "not_provisioned" if checkpoint is None else "pending_reclaim"
        elif checkpoint is None:
            actions["checkpoint"] = "initialize"
        else:
            actions["checkpoint"] = QuotaChanges._direction(
                checkpoint, normalized["checkpoint_bytes"], normalized["checkpoint_inodes"]
            )
        return actions

    @staticmethod
    def _normalized(target: QuotaLimits) -> dict[str, int]:
        """按内核KiB粒度规范化并显式拆分；规范化值在预览中明示，确认按它应用。"""
        checkpoint_bytes = target.checkpoint_bytes // 1024 * 1024
        if checkpoint_bytes:
            checkpoint_inodes = min(
                target.environment_inodes // 4, max(128, target.checkpoint_count * 64)
            )
            data_bytes = (target.environment_bytes - target.checkpoint_bytes) // 1024 * 1024
        else:
            checkpoint_inodes = 0
            data_bytes = target.environment_bytes // 1024 * 1024
        return {
            "environment_bytes": target.environment_bytes,
            "environment_inodes": target.environment_inodes,
            "data_bytes": data_bytes,
            "data_inodes": target.environment_inodes - checkpoint_inodes,
            "checkpoint_bytes": checkpoint_bytes,
            "checkpoint_inodes": checkpoint_inodes,
        }

    def _node_view(self, environment_id: str, target: QuotaLimits) -> dict[str, Any]:
        info = os.statvfs(self.broker.storage)
        capacity = info.f_blocks * info.f_frsize
        registry = self.broker._read("accounts.json") or {"allocations": {}}
        allocations = registry.get("allocations", {})
        reserved = 0
        mine = 0
        for key, allocation in allocations.items():
            value = allocation.get("limits", {}).get("environment_bytes", 0)
            if key == environment_id:
                mine = value
            else:
                reserved += value
        ceiling = int(capacity * RESERVATION_CEILING)
        target_reserved = reserved + target.environment_bytes
        return {
            "capacity_bytes": capacity,
            "ceiling_bytes": ceiling,
            "reserved_bytes": reserved + mine,
            "target_reserved_bytes": target_reserved,
            "target_fits": target_reserved <= ceiling,
        }

    def _reservation(self, environment_id: str, target: QuotaLimits) -> dict[str, int]:
        registry = self.broker._read("accounts.json") or {"allocations": {}}
        allocation = registry.get("allocations", {}).get(environment_id) or {}
        previous = allocation.get("limits", {}).get("environment_bytes", 0)
        new = target.environment_bytes
        return {
            "previous_bytes": previous,
            "target_bytes": new,
            "retained_bytes": max(previous, new),
        }

    def _reserve(self, environment_id: str, limits: dict[str, Any], *, larger_only: bool) -> None:
        registry = self.broker._read("accounts.json")
        if registry is None or environment_id not in registry.get("allocations", {}):
            return
        allocation = registry["allocations"][environment_id]
        current = allocation.get("limits", {}).get("environment_bytes", 0)
        if larger_only and limits["environment_bytes"] <= current:
            return
        allocation["limits"] = dict(limits)
        self.broker._save("accounts.json", registry)

    # ---------- 门、锁与写者核查 ----------

    def _gate(self, environment_id: str, operation_id: str, quota_state: str) -> None:
        record = self.broker._read("env-" + environment_id + ".json")
        if record is None or (
            record.get("quota_operation") == operation_id
            and record.get("quota_state") == quota_state
        ):
            return
        self.broker._save(
            "env-" + environment_id + ".json",
            {**record, "quota_operation": operation_id, "quota_state": quota_state},
        )

    def _open_gate(self, environment_id: str, quota_state: str) -> None:
        record = self.broker._read("env-" + environment_id + ".json")
        if record is None:
            return
        updated = {key: value for key, value in record.items() if key != "quota_operation"}
        if updated.get("quota_state") == quota_state and "quota_operation" not in record:
            return
        updated["quota_state"] = quota_state
        self.broker._save("env-" + environment_id + ".json", updated)

    def _active_writers(self, environment_id: str) -> dict[str, Any] | None:
        """同环境活跃写者快照；暂停恢复的blocked租约不算写者（豁免见_paused_recovery）。"""
        jobs: list[str] = []
        if self.broker.jobs is not None:
            for key, state in self.broker.jobs.active.items():
                record = state.get("record", {})
                if record.get("environment_id") == environment_id:
                    jobs.append(key)
        recovery: list[str] = []
        active_obj = getattr(self.broker, "recovery", None)
        active = getattr(active_obj, "active", None) if active_obj is not None else None
        if isinstance(active, dict):
            for key, state in active.items():
                if isinstance(state, dict):
                    value = state.get("environment_id")
                else:
                    value = getattr(state, "environment_id", None)
                if value == environment_id:
                    recovery.append(str(key))
        elif active:
            # set/list形式只含操作ID、无归属信息：保守视为影响本环境。
            recovery = sorted(str(key) for key in active)
        leases = [
            project_id
            for project_id in self._environment_projects(environment_id)
            if not self._idle_or_paused(environment_id, project_id)
        ]
        if jobs or recovery or leases:
            return {"jobs": jobs, "recovery": recovery, "leases": leases, "maintenance_lock": False}
        return None

    def _idle_or_paused(self, environment_id: str, project_id: str) -> bool:
        lease = self.broker.leases.status(project_id)
        if lease["status"] == "idle":
            return True
        return self._paused_recovery(environment_id, project_id, lease["lease_id"])

    def _paused_recovery(self, environment_id: str, project_id: str, lease_id: str) -> bool:
        """暂停恢复占用租约时放行调额：journal为该env/project、paused/blocked且不在active。

        豁免只让调额继续，不释放租约，也不碰恢复引用；无法核实的journal保持waiting。
        """
        journal = self.broker._read("restore-" + lease_id + ".json")
        if journal is None:
            return False
        if (
            journal.get("environment_id") != environment_id
            or journal.get("project_id") != project_id
        ):
            return False
        if journal.get("status") not in {"paused", "blocked"}:
            return False
        operation = journal.get("operation_id")
        if type(operation) is not str:
            return False
        return operation not in self._recovery_active()

    def _recovery_active(self) -> set[str]:
        """Broker恢复作业的active标识集合；容器为dict（状态表）或set（操作ID）。"""
        active_obj = getattr(self.broker, "recovery", None)
        active = getattr(active_obj, "active", None) if active_obj is not None else None
        if isinstance(active, dict):
            return {str(key) for key in active}
        if isinstance(active, (set, frozenset, list, tuple)):
            return {str(key) for key in active}
        return set()

    def _environment_projects(self, environment_id: str) -> list[str]:
        try:
            names = os.listdir(self.broker.state)
        except OSError:
            raise ServiceError("sandbox_backend_unavailable", 503) from None
        result = []
        for name in names:
            if name.startswith("project-") and name.endswith(".json"):
                record = self.broker._read(name)
                if record is not None and record.get("environment_id") == environment_id:
                    result.append(name[len("project-") : -len(".json")])
        return sorted(result)

    @contextmanager
    def _maintenance(self, environment_id: str) -> Iterator[None]:
        """账号检查点维护锁（与ShadowCheckpoints同一文件）；非阻塞、不跨请求。"""
        root = self.broker.environments / environment_id / "checkpoints"
        os.makedirs(root, mode=0o700, exist_ok=True)
        private_directory(root)
        account = root / environment_id
        os.makedirs(account, mode=0o700, exist_ok=True)
        private_directory(account)
        if account.stat().st_mode & 0o077 or account.stat().st_uid != os.geteuid():
            raise ServiceError("artifact_invalid")
        fd = os.open(account / "maintenance.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ServiceError("facts_corrupt")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError("workspace_busy") from None
            yield
        finally:
            os.close(fd)

    # ---------- 记录与校验 ----------

    def _save_operation(self, operation: dict[str, Any]) -> None:
        self.broker._save(_CHANGE_PREFIX + operation["operation_id"] + ".json", operation)

    def _checkpoint_dir_empty(self, environment_id: str) -> bool:
        """检查点区可登记为空区：缺失，或仅含本环境受管目录且其中只有maintenance.lock。

        自己的维护锁不能让空区判定永久失败；objects.git或任何未知文件仍视为非空。
        """
        path = self.broker.environments / environment_id / "checkpoints"
        if not path.exists():
            return True
        private_directory(path)
        with os.scandir(path) as entries:
            managed = next(iter(entries), None)
            if managed is None:
                return True
            if managed.name != environment_id or not managed.is_dir(follow_symlinks=False):
                return False
            if next(iter(entries), None) is not None:
                return False
        with os.scandir(path / environment_id) as entries:
            for entry in entries:
                if entry.name == "maintenance.lock" and entry.is_file(follow_symlinks=False):
                    continue
                return False
        return True

    @staticmethod
    def _limits(limits: dict[str, Any]) -> QuotaLimits:
        if not isinstance(limits, dict):
            raise ServiceError("invalid_input", 422)
        try:
            return QuotaLimits.model_validate(limits)
        except ValidationError:
            raise ServiceError("invalid_input", 422) from None

    @staticmethod
    def _digest(value: object) -> str:
        if type(value) is not str or len(value) != 64 or not set(value) <= _HEX:
            raise ServiceError("invalid_input", 422)
        return value
