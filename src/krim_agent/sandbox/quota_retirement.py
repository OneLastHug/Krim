"""检查点退役（C=0收尾）：旧检查点区隔离、预算释放与data扩容的关闭协议。

与配额协议（quota_management）的衔接：QuotaChanges把C=0目标停在pending_reclaim
（checkpoint_disable_requires_reclaim），旧区保持applied；本模块在显式GC完成、
受保护历史清空后，经注入的RetirementPort完成收尾，不编辑GC与配额模块。

协议要点：
- 默认不删除历史：存在user/job/restore/legacy引用、对象内容或未确认GC记录时，
  操作保持pending_reclaim并列出protected；C=0目标本身不是清理授权，本模块不执行
  GC、不删引用、不删文件；清理只能经checkpoints的两阶段GC由调用方显式完成。
- 顺序固定：先隔离旧检查点空区（归档内容与维护记录到有界管理区、不再挂给租户）、
  核实project id已无inode，再释放checkpoint预算，最后才扩data；任何一步失败停止
  后续步骤，保留实际limits向量与维护门供核实，不做自动补偿。
- XFS limit值0表示unlimited：仅当退役project id已隔离、无写路径且inode为0，才允许
  经release_quota_allocation(kernel_zero=True)清除内核旧限额，否则拒绝；默认保留
  内核旧限额、只停止分配，不强迫清0，也不把0读回当成0硬限额。
- 同一operation_id只释放一次预留：先落盘意图再动作，回包丢失按verify()核实实际值，
  不由调用方代码重放；仅清除DB checkpoint_number不构成完成，完成必须四项证据齐备
  （data限额、隔离与0 inode、预留已释放、登记已注销）。
- C=0后检查点写入继续fail_closed（capture在checkpoint_bytes=0时拒绝），本模块不提供
  无保护写入路径，也不顺手放开历史配额。
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Protocol

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.sandbox.quota_management import QuotaChanges
from krim_agent.sandbox.xfs import internal_id

PREVIEW_SECONDS = 900
_PLAN_PREFIX = "quota-retire-plan-"
_OPERATION_PREFIX = "quota-retire-"


class RetirementPort(Protocol):
    """主会话适配的存储端口；实际存储动作只经此端口，适配方持有维护锁与维护门。

    适配约定：
    - inspect返回完整快照（record、usage、refs、objects、gc、active、stopped_proof、
      checkpoint_zone、actual、reservation）；verify是廉价权威读回（actual、
      checkpoint_zone、reservation、record），供逐步核实与最终完成证明。
    - journal_get/journal_put持久化plan与operation记录（键由本模块给出）；记录必须
      是有界JSON安全对象。
    - quarantine_empty_checkpoint_root只允许隔离"仅剩维护锁"的空检查点区：适配方先
      把内容与维护记录归档进有界管理区，再持检查点维护锁隔离，隔离后该区不挂给租户。
    - release_quota_allocation按operation_id幂等：同一操作只释放一次节点预留，不同
      操作重复释放必须拒绝；kernel_zero仅在重新核实已隔离且inode为0后执行，否则拒绝。
      预算释放与checkpoint_number注销只在证据齐备后提交。
    - apply_data_limit是本协议最后一个存储动作：写内核data区限额并读回实际值，成功
      读回后适配方解除维护门。
    - 适配方应在首个变更动作前把env维护门绑定operation_id，使半途失败可核实；confirm
      检查门：被其他操作持有时拒绝（workspace_busy），主会话需先撤回/完结持有者。
    """

    def inspect(self, environment_id: str) -> dict[str, Any]: ...
    def verify(self, environment_id: str) -> dict[str, Any]: ...
    def journal_get(self, key: str) -> dict[str, Any] | None: ...
    def journal_put(self, key: str, record: dict[str, Any]) -> None: ...
    def quarantine_empty_checkpoint_root(
        self, environment_id: str, operation_id: str
    ) -> dict[str, Any]: ...
    def release_quota_allocation(
        self, environment_id: str, operation_id: str, *, kernel_zero: bool
    ) -> dict[str, Any]: ...
    def apply_data_limit(
        self, environment_id: str, operation_id: str, byte_limit: int, inode_limit: int
    ) -> dict[str, Any]: ...


def _map(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


class QuotaRetirement:
    """检查点关闭收尾协议：预览、确认、状态推进与撤回；默认不清理受保护历史。"""

    def __init__(self, port: RetirementPort) -> None:
        self._port = port

    # ---------- 入口 ----------

    def preview(
        self, environment_id: str, limits: dict[str, Any], *, kernel_zero: bool = False
    ) -> dict[str, Any]:
        """无副作用预览：保护根、写者、节点容量与内核实际值分开返回；只落盘计划。"""
        environment_id = internal_id(environment_id)
        target = QuotaChanges._limits(limits)
        normalized = QuotaChanges._normalized(target)
        if normalized["checkpoint_bytes"] != 0:
            # 本模块只收尾C=0目标；开启检查点的调整仍走QuotaChanges。
            raise ServiceError("invalid_input", 422)
        snapshot = self._port.inspect(environment_id)
        record = _map(snapshot.get("record"))
        if record.get("checkpoint_number") is None:
            raise ServiceError("invalid_input", 422)
        protection = self._protection(snapshot)
        busy = self._busy(snapshot)
        node = self._node_view(snapshot, normalized)
        now = time.time()
        plan_id = str(uuid.uuid4())
        target_limits = target.model_dump(mode="json")
        digest = fingerprint(
            {
                "environment_id": environment_id,
                "limits": target_limits,
                "normalized": normalized,
                "layout": "unsplit",
                "kernel_zero": kernel_zero,
            }
        )
        plan = {
            "plan_id": plan_id,
            "environment_id": environment_id,
            "digest": digest,
            "created_at": now,
            "expires_at": now + PREVIEW_SECONDS,
            "limits": target_limits,
            "normalized": normalized,
            "layout": "unsplit",
            "kernel_zero": kernel_zero,
            "checkpoint_policy": "fail_closed",
        }
        self._port.journal_put(_PLAN_PREFIX + plan_id, plan)
        return {
            "environment_id": environment_id,
            "plan_id": plan_id,
            "plan_digest": digest,
            "expires_at": plan["expires_at"],
            "preview_seconds": PREVIEW_SECONDS,
            "target": {
                "layout": "unsplit",
                "limits": target_limits,
                "normalized": normalized,
                "checkpoint_enabled": False,
                "checkpoint_policy": "fail_closed",
            },
            "kernel_zero_requested": kernel_zero,
            "kernel_zero_eligible": self._zone_zero_ready(snapshot),
            "protection": {
                "required": bool(protection),
                "reasons": protection,
                "protected": self._protected_view(snapshot, protection),
            },
            "active": _map(snapshot.get("active")),
            "stopped_proof": _map(snapshot.get("stopped_proof")),
            "usage": _map(snapshot.get("usage")),
            "node": node,
            "applicable_now": node["target_fits"] and busy is None and not protection,
        }

    def confirm(
        self, environment_id: str, plan_id: str, plan_digest: str, operation_id: str
    ) -> dict[str, Any]:
        """接受预览并启动收尾；同plan同op重复确认只推进原操作，不重复释放预留。"""
        environment_id = internal_id(environment_id)
        plan_id = internal_id(plan_id)
        operation_id = internal_id(operation_id)
        digest = QuotaChanges._digest(plan_digest)
        existing = self._port.journal_get(_OPERATION_PREFIX + operation_id)
        if existing is not None:
            if existing.get("environment_id") != environment_id:
                raise ServiceError("not_found", 404)
            if existing.get("plan_id") != plan_id or existing.get("plan_digest") != digest:
                raise ServiceError("key_conflict")
            return self._advance(existing)
        plan = self._port.journal_get(_PLAN_PREFIX + plan_id)
        if plan is None or plan.get("environment_id") != environment_id:
            raise ServiceError("not_found", 404)
        if plan.get("digest") != digest:
            raise ServiceError("key_conflict")
        if time.time() >= plan["expires_at"]:
            raise ServiceError("approval_expired")
        snapshot = self._port.inspect(environment_id)
        record = _map(snapshot.get("record"))
        if record.get("checkpoint_number") is None:
            raise ServiceError("invalid_input", 422)
        if not self._node_view(snapshot, _map(plan.get("normalized")))["target_fits"]:
            # 节点预留不足：拒绝创建操作记录，也不绑定维护门。
            raise ServiceError("sandbox_limit")
        if record.get("quota_operation") not in {None, operation_id}:
            raise ServiceError("workspace_busy")
        protection = self._protection(snapshot)
        now = time.time()
        kernel_zero = bool(plan.get("kernel_zero"))
        operation: dict[str, Any] = {
            "operation_id": operation_id,
            "plan_id": plan_id,
            "plan_digest": digest,
            "environment_id": environment_id,
            "retirement_state": "pending_reclaim" if protection else "waiting_idle",
            "blocked_reason": "protected_history_present" if protection else None,
            "protected": self._protected_view(snapshot, protection),
            "confirmed_at": now,
            "updated_at": now,
            "desired": {
                "limits": plan["limits"],
                "normalized": plan["normalized"],
                "layout": "unsplit",
                "kernel_zero": kernel_zero,
            },
            "steps": self._build_steps(kernel_zero, _map(plan.get("normalized"))),
            "stopped_proof": None,
            "waiting_on": None,
        }
        self._port.journal_put(_OPERATION_PREFIX + operation_id, operation)
        return self._advance(operation)

    def status(self, environment_id: str, operation_id: str) -> dict[str, Any]:
        """读取并可推进原操作：清理完成后继续隔离与释放，失败保留现场供核实。"""
        environment_id = internal_id(environment_id)
        operation_id = internal_id(operation_id)
        operation = self._port.journal_get(_OPERATION_PREFIX + operation_id)
        if operation is None or operation.get("environment_id") != environment_id:
            raise ServiceError("not_found", 404)
        return self._advance(operation)

    def cancel(self, environment_id: str, operation_id: str) -> dict[str, Any]:
        """只允许撤销尚未触碰存储的pending_reclaim/waiting_idle操作。"""
        environment_id = internal_id(environment_id)
        operation_id = internal_id(operation_id)
        operation = self._port.journal_get(_OPERATION_PREFIX + operation_id)
        if operation is None or operation.get("environment_id") != environment_id:
            raise ServiceError("not_found", 404)
        if operation["retirement_state"] not in {"pending_reclaim", "waiting_idle"}:
            raise ServiceError("invalid_input", 422)
        operation["retirement_state"] = "cancelled"
        operation["waiting_on"] = None
        self._save(operation)
        return self._view(operation)

    # ---------- 推进 ----------

    def _advance(self, operation: dict[str, Any]) -> dict[str, Any]:
        if operation["retirement_state"] in {"retired", "cancelled"}:
            return self._view(operation)
        environment_id = operation["environment_id"]
        snapshot = self._port.inspect(environment_id)
        record = _map(snapshot.get("record"))
        if record.get("quota_operation") not in {None, operation["operation_id"]}:
            # 维护门被其他操作持有：保持原状并可核实，不推进也不抢锁。
            operation["waiting_on"] = {"gate": record.get("quota_operation")}
            self._save(operation)
            return self._view(operation)
        if operation["retirement_state"] == "pending_reclaim":
            protection = self._protection(snapshot)
            operation["protected"] = self._protected_view(snapshot, protection)
            if protection:
                # 受保护历史仍在：保持pending_reclaim，展示protected，不做任何清理。
                operation["blocked_reason"] = "protected_history_present"
                self._save(operation)
                return self._view(operation)
            operation["blocked_reason"] = None
            operation["retirement_state"] = "waiting_idle"
        busy = self._busy(snapshot)
        if busy is not None:
            operation["retirement_state"] = "waiting_idle"
            operation["waiting_on"] = busy
            self._save(operation)
            return self._view(operation)
        operation["retirement_state"] = "applying"
        operation["waiting_on"] = None
        proof = snapshot.get("stopped_proof")
        if isinstance(proof, dict) and not operation.get("stopped_proof"):
            operation["stopped_proof"] = proof
        self._save(operation)
        self._run_steps(operation, snapshot)
        return self._view(operation)

    def _run_steps(self, operation: dict[str, Any], snapshot: dict[str, Any]) -> None:
        """按隔离→释放→扩data顺序执行；每步先落盘意图再动作并读回核实，失败即停。"""
        environment_id = operation["environment_id"]
        for step in operation["steps"]:
            if step["state"] == "verified":
                continue
            if step["state"] == "executing" and self._step_done(step, environment_id):
                # 回包丢失：按verify()核实实际已完成，不重放存储动作。
                step["state"] = "verified"
                step["reason"] = None
                step["error"] = None
                self._save(operation)
                continue
            if (
                step["kind"] == "release"
                and step.get("kernel_zero")
                and not self._zero_ready(operation, environment_id)
            ):
                self._fail_step(operation, step, "kernel_zero_requires_quarantined_zero_inode")
                return
            step["state"] = "executing"
            step["reason"] = None
            step["error"] = None
            self._save(operation)
            try:
                receipt = self._apply_step(step, operation)
            except ServiceError as error:
                step["error"] = error.code
                operation["retirement_state"] = "reconcile_required"
                self._save(operation)
                return
            if not self._step_done(step, environment_id):
                step["state"] = "failed"
                step["reason"] = "retirement_step_mismatch"
                operation["retirement_state"] = "reconcile_required"
                self._save(operation)
                return
            step["state"] = "verified"
            step["receipt"] = receipt
            self._save(operation)
        self._finalize(operation)

    def _apply_step(self, step: dict[str, Any], operation: dict[str, Any]) -> dict[str, Any]:
        environment_id = operation["environment_id"]
        operation_id = operation["operation_id"]
        kind = step["kind"]
        if kind == "quarantine":
            return self._port.quarantine_empty_checkpoint_root(environment_id, operation_id)
        if kind == "release":
            return self._port.release_quota_allocation(
                environment_id, operation_id, kernel_zero=bool(step.get("kernel_zero"))
            )
        if kind == "data_grow":
            return self._port.apply_data_limit(
                environment_id,
                operation_id,
                int(step["byte_limit"]),
                int(step["inode_limit"]),
            )
        raise ServiceError("invalid_input", 422)

    def _finalize(self, operation: dict[str, Any]) -> None:
        """全部步骤核实后做完成证明；仅清DB字段不算完成，缺任一证据保持可核实。"""
        state = self._port.verify(operation["environment_id"])
        gap = self._retirement_gap(operation, state)
        if gap is None:
            operation["retirement_state"] = "retired"
            operation["blocked_reason"] = None
        else:
            operation["retirement_state"] = "reconcile_required"
            operation["blocked_reason"] = gap
        self._save(operation)

    # ---------- 核实 ----------

    def _step_done(self, step: dict[str, Any], environment_id: str) -> bool:
        state = self._port.verify(environment_id)
        kind = step["kind"]
        if kind == "quarantine":
            zone = _map(state.get("checkpoint_zone"))
            return bool(zone.get("quarantined")) and zone.get("inode_count") == 0
        if kind == "release":
            reservation = _map(state.get("reservation"))
            return bool(reservation.get("checkpoint_released"))
        if kind == "data_grow":
            data = _map(_map(state.get("actual")).get("data"))
            record = _map(state.get("record"))
            # data_grow是最后一个存储动作：内核达标且维护门已放才算完成；
            # 放门落盘失败时判未完成，重试按同操作幂等补齐账本与放门。
            return bool(
                data.get("byte_limit") == step["byte_limit"]
                and data.get("inode_limit") == step["inode_limit"]
                and record.get("quota_operation") is None
            )
        return False

    def _zero_ready(self, operation: dict[str, Any], environment_id: str) -> bool:
        """kernel_zero仅在隔离步骤已核实且读回0 inode后允许；否则拒绝清0。"""
        quarantined = any(
            step["kind"] == "quarantine" and step["state"] == "verified"
            for step in operation["steps"]
        )
        return quarantined and QuotaRetirement._zone_zero_ready(self._port.verify(environment_id))

    @staticmethod
    def _zone_zero_ready(snapshot: dict[str, Any]) -> bool:
        zone = _map(snapshot.get("checkpoint_zone"))
        return bool(zone.get("quarantined")) and zone.get("inode_count") == 0

    @staticmethod
    def _retirement_gap(operation: dict[str, Any], state: dict[str, Any]) -> str | None:
        desired = _map(operation.get("desired"))
        normalized = _map(desired.get("normalized"))
        data = _map(_map(state.get("actual")).get("data"))
        if (
            data.get("byte_limit") != normalized.get("data_bytes")
            or data.get("inode_limit") != normalized.get("data_inodes")
        ):
            return "data_limit_mismatch"
        zone = _map(state.get("checkpoint_zone"))
        if not zone.get("quarantined") or zone.get("inode_count") != 0:
            return "checkpoint_zone_not_quarantined"
        if not _map(state.get("reservation")).get("checkpoint_released"):
            return "checkpoint_budget_not_released"
        record = _map(state.get("record"))
        if record.get("checkpoint_number") is not None:
            return "checkpoint_number_still_registered"
        if record.get("quota_operation") is not None:
            # 四项证据齐备但门未放开：放门落盘未完成，不构成可放写的完成。
            return "maintenance_gate_not_released"
        return None

    @staticmethod
    def _protection(snapshot: dict[str, Any]) -> list[str]:
        """受保护历史清单：引用、legacy、对象内容与未确认GC；任一存在即不释放。"""
        refs = _map(snapshot.get("refs"))
        objects = _map(snapshot.get("objects"))
        gc = _map(snapshot.get("gc"))
        reasons: list[str] = []
        if refs.get("protected"):
            reasons.append("checkpoint_refs_held")
        if refs.get("legacy_retained"):
            reasons.append("legacy_retained_refs")
        if objects.get("physical_bytes"):
            reasons.append("checkpoint_objects_present")
        if gc.get("pending"):
            reasons.append("gc_reclaim_pending")
        if gc.get("intents"):
            reasons.append("gc_intent_unconfirmed")
        return reasons

    @staticmethod
    def _busy(snapshot: dict[str, Any]) -> dict[str, Any] | None:
        """活跃写者快照；无法给出停写证明也视为忙碌，不按期限抢锁。"""
        active = _map(snapshot.get("active"))
        if (
            active.get("jobs")
            or active.get("recovery")
            or active.get("leases")
            or active.get("maintenance_lock")
        ):
            return active
        proof = _map(snapshot.get("stopped_proof"))
        if not proof.get("idle"):
            return {
                "jobs": [],
                "recovery": [],
                "leases": [],
                "maintenance_lock": False,
                "stopped_proof_missing": True,
            }
        return None

    @staticmethod
    def _node_view(snapshot: dict[str, Any], normalized: dict[str, Any]) -> dict[str, Any]:
        reservation = _map(snapshot.get("reservation"))
        ceiling = int(reservation.get("ceiling_bytes") or 0)
        reserved = int(reservation.get("reserved_bytes") or 0)
        mine = int(reservation.get("mine_bytes") or 0)
        target_reserved = reserved - mine + int(normalized["environment_bytes"])
        return {
            "ceiling_bytes": ceiling,
            "reserved_bytes": reserved,
            "mine_bytes": mine,
            "target_reserved_bytes": target_reserved,
            "target_fits": target_reserved <= ceiling,
        }

    @staticmethod
    def _protected_view(snapshot: dict[str, Any], reasons: list[str]) -> list[str]:
        if not reasons:
            return []
        refs = _map(snapshot.get("refs"))
        held = [f"checkpoint:{item}" for item in refs.get("protected") or []]
        return sorted(set(reasons) | set(held))

    @staticmethod
    def _build_steps(kernel_zero: bool, normalized: dict[str, Any]) -> list[dict[str, Any]]:
        """固定顺序：隔离旧区→释放预算（可选清0）→扩data；先缩/隔离后扩容。"""

        def blank(step_id: int, kind: str, zone: str) -> dict[str, Any]:
            return {
                "step_id": step_id,
                "kind": kind,
                "zone": zone,
                "state": "prepared",
                "reason": None,
                "error": None,
                "receipt": None,
            }

        return [
            blank(1, "quarantine", "checkpoint"),
            {
                **blank(2, "release", "checkpoint"),
                "kernel_zero": kernel_zero,
            },
            {
                **blank(3, "data_grow", "data"),
                "byte_limit": normalized["data_bytes"],
                "inode_limit": normalized["data_inodes"],
            },
        ]

    # ---------- 记录与视图 ----------

    def _fail_step(self, operation: dict[str, Any], step: dict[str, Any], reason: str) -> None:
        step["state"] = "failed"
        step["reason"] = reason
        operation["retirement_state"] = "reconcile_required"
        operation["blocked_reason"] = reason
        self._save(operation)

    def _save(self, operation: dict[str, Any]) -> None:
        operation["updated_at"] = time.time()
        self._port.journal_put(_OPERATION_PREFIX + str(operation["operation_id"]), operation)

    def _view(self, operation: dict[str, Any]) -> dict[str, Any]:
        state = self._port.verify(operation["environment_id"])
        record = _map(state.get("record"))
        gate_operation = record.get("quota_operation")
        return {
            "operation_id": operation["operation_id"],
            "plan_id": operation["plan_id"],
            "environment_id": operation["environment_id"],
            "retirement_state": operation["retirement_state"],
            "blocked_reason": operation.get("blocked_reason"),
            "desired": operation["desired"],
            "protected": operation.get("protected") or [],
            "steps": operation["steps"],
            "stopped_proof": operation.get("stopped_proof"),
            "waiting_on": operation.get("waiting_on"),
            "actual": _map(state.get("actual")),
            "checkpoint_zone": _map(state.get("checkpoint_zone")),
            "reservation": _map(state.get("reservation")),
            "checkpoint_policy": "fail_closed",
            "gate": {
                "quota_operation": gate_operation,
                "held_by_retirement": gate_operation == operation["operation_id"],
                "new_writes_allowed": gate_operation is None,
            },
            "confirmed_at": operation["confirmed_at"],
            "updated_at": operation["updated_at"],
        }
