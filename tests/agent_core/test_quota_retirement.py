"""检查点退役协议合成测试：保护根阻塞、等待空闲、清0拒绝与回包丢失核实。

FakeRetirementPort 模拟Broker侧端口动作并记录全部变更调用；真实隔离、inode核实与
节点预留释放由独立宿主实验覆盖，这里只验证退役状态机与端口边界。
"""

import time
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.quota_retirement import QuotaRetirement

MIB = 1024**2
ENV = "e1000000-0000-4000-8000-000000000001"
TARGET_LIMITS = {
    "environment_bytes": 500 * MIB,
    "environment_inodes": 100_000,
    "checkpoint_bytes": 0,
    "checkpoint_count": 0,
}


class FakeRetirementPort:
    """内存版RetirementPort：记录变更动作，支持保护注入、直接失败与回包丢失模拟。

    回包丢失语义：动作的全部提交效果（含登记注销与放门）先落定，再抛出
    sandbox_backend_unavailable，模拟"实际已完成但调用方未收到回执"。
    """

    def __init__(self, environment_id: str, *, ceiling_bytes: int = 850 * MIB) -> None:
        self.environment_id = environment_id
        self.journal: dict[str, dict] = {}
        self.record = {
            "project_number": 100_000,
            "checkpoint_number": 100_000 + 0x40000000,
            "limits": {
                "environment_bytes": 500 * MIB,
                "environment_inodes": 100_000,
                "checkpoint_bytes": 100 * MIB,
                "checkpoint_count": 20,
                "checkpoint_retention_seconds": 7 * 86400,
                "frozen_bytes": None,
            },
            "quota_operation": None,
            "quota_state": None,
        }
        self.usage = {
            "data": {"environment_bytes": 40 * MIB, "environment_inodes": 5_000},
            "checkpoint": {"environment_bytes": 0, "environment_inodes": 0},
            "account": {"environment_bytes": 40 * MIB, "environment_inodes": 5_000},
        }
        self.refs = {"protected": [], "legacy_retained": 0, "hold_kinds": {}}
        self.objects = {"physical_bytes": 0}
        self.gc = {"pending": [], "intents": 0}
        self.active = {"jobs": [], "recovery": [], "leases": [], "maintenance_lock": False}
        self.checkpoint_zone = {"quarantined": False, "inode_count": 0, "managed_path": None}
        self.actual = {
            "data": {"byte_limit": 400 * MIB, "inode_limit": 98_720},
            "checkpoint": {"byte_limit": 100 * MIB, "inode_limit": 1_280},
        }
        self.reservation = {
            "ceiling_bytes": ceiling_bytes,
            "reserved_bytes": 500 * MIB,
            "mine_bytes": 500 * MIB,
            "checkpoint_budget_bytes": 100 * MIB,
            "checkpoint_released": False,
            "released_by_operation": None,
        }
        self.calls: list[str] = []
        self.zero_requests: list[bool] = []
        self.refuse: dict[str, str] = {}
        self.applied_then_fail: set[str] = set()

    # ---------- 只读 ----------

    def _snapshot(self) -> dict:
        busy = (
            self.active["jobs"]
            or self.active["recovery"]
            or self.active["leases"]
            or self.active["maintenance_lock"]
        )
        return {
            "environment_id": self.environment_id,
            "record": dict(self.record),
            "usage": {key: dict(value) for key, value in self.usage.items()},
            "refs": dict(self.refs),
            "objects": dict(self.objects),
            "gc": dict(self.gc),
            "active": dict(self.active),
            "stopped_proof": {"idle": not busy, "checked_at": time.time()},
            "checkpoint_zone": dict(self.checkpoint_zone),
            "actual": {key: dict(value) for key, value in self.actual.items()},
            "reservation": dict(self.reservation),
        }

    def inspect(self, environment_id: str) -> dict:
        assert environment_id == self.environment_id
        return self._snapshot()

    def verify(self, environment_id: str) -> dict:
        assert environment_id == self.environment_id
        return self._snapshot()

    def journal_get(self, key: str) -> dict | None:
        return self.journal.get(key)

    def journal_put(self, key: str, record: dict) -> None:
        self.journal[key] = record

    # ---------- 变更 ----------

    def _bind_gate(self, operation_id: str) -> None:
        if self.record["quota_operation"] is None:
            self.record["quota_operation"] = operation_id
            self.record["quota_state"] = "retiring"

    def quarantine_empty_checkpoint_root(self, environment_id: str, operation_id: str) -> dict:
        self.calls.append("quarantine")
        if "quarantine" in self.refuse:
            raise ServiceError(self.refuse["quarantine"], 503)
        if (
            self.usage["checkpoint"]["environment_bytes"]
            or self.usage["checkpoint"]["environment_inodes"]
            or self.objects["physical_bytes"]
            or self.gc["pending"]
            or self.gc["intents"]
            or self.refs["protected"]
            or self.refs["legacy_retained"]
        ):
            raise ServiceError("artifact_conflict", 409)
        self.checkpoint_zone = {
            "quarantined": True,
            "inode_count": 0,
            "managed_path": "maintenance/retired/" + self.environment_id,
        }
        self._bind_gate(operation_id)
        if "quarantine" in self.applied_then_fail:
            raise ServiceError("sandbox_backend_unavailable", 503)
        return {"quarantined": True, "inode_count": 0}

    def release_quota_allocation(
        self, environment_id: str, operation_id: str, *, kernel_zero: bool
    ) -> dict:
        self.calls.append("release")
        if "release" in self.refuse:
            raise ServiceError(self.refuse["release"], 503)
        if self.reservation["checkpoint_released"]:
            if self.reservation["released_by_operation"] != operation_id:
                raise ServiceError("key_conflict", 409)
        else:
            self.reservation["checkpoint_released"] = True
            self.reservation["released_by_operation"] = operation_id
            self.reservation["reserved_bytes"] -= self.reservation["checkpoint_budget_bytes"]
            self.record["checkpoint_number"] = None
        if kernel_zero:
            self.zero_requests.append(kernel_zero)
            if not self.checkpoint_zone["quarantined"] or self.checkpoint_zone["inode_count"] != 0:
                raise ServiceError("sandbox_limit", 409)
            self.actual["checkpoint"] = {"byte_limit": 0, "inode_limit": 0}
        self._bind_gate(operation_id)
        if "release" in self.applied_then_fail:
            raise ServiceError("sandbox_backend_unavailable", 503)
        return {"checkpoint_released": True, "kernel_zero": kernel_zero}

    def apply_data_limit(
        self, environment_id: str, operation_id: str, byte_limit: int, inode_limit: int
    ) -> dict:
        self.calls.append("apply_data_limit")
        if "apply_data_limit" in self.refuse:
            raise ServiceError(self.refuse["apply_data_limit"], 503)
        self.actual["data"] = {"byte_limit": byte_limit, "inode_limit": inode_limit}
        self._bind_gate(operation_id)
        # 本协议最后一个存储动作：提交效果（含放门）落定后才可能丢失回执。
        self.record["quota_operation"] = None
        self.record["quota_state"] = "retired"
        if "apply_data_limit" in self.applied_then_fail:
            raise ServiceError("sandbox_backend_unavailable", 503)
        return {"byte_limit": byte_limit, "inode_limit": inode_limit}

    # ---------- 测试辅助 ----------

    def protect(self) -> None:
        self.refs = {
            "protected": ["cp-held-1"],
            "legacy_retained": 2,
            "hold_kinds": {"user": 1, "restore-target": 1},
        }
        self.objects = {"physical_bytes": 5 * MIB}
        self.gc = {"pending": ["receipt-x"], "intents": 1}
        self.usage["checkpoint"] = {"environment_bytes": 3 * MIB, "environment_inodes": 200}
        self.checkpoint_zone = {"quarantined": False, "inode_count": 200, "managed_path": None}

    def clear_history(self) -> None:
        self.refs = {"protected": [], "legacy_retained": 0, "hold_kinds": {}}
        self.objects = {"physical_bytes": 0}
        self.gc = {"pending": [], "intents": 0}
        self.usage["checkpoint"] = {"environment_bytes": 0, "environment_inodes": 0}
        self.usage["account"] = {
            "environment_bytes": self.usage["data"]["environment_bytes"],
            "environment_inodes": self.usage["data"]["environment_inodes"],
        }
        self.checkpoint_zone = {"quarantined": False, "inode_count": 0, "managed_path": None}

    def operation_key(self, operation_id: str) -> str:
        return "quota-retire-" + operation_id

    def plan_key(self, plan_id: str) -> str:
        return "quota-retire-plan-" + plan_id


def retired(view: dict) -> bool:
    return view["retirement_state"] == "retired"


def test_preview_reports_protection_without_storage_writes() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    port.protect()
    view = service.preview(ENV, TARGET_LIMITS)
    assert port.calls == []
    assert view["protection"]["required"] is True
    assert set(view["protection"]["reasons"]) == {
        "checkpoint_refs_held",
        "legacy_retained_refs",
        "checkpoint_objects_present",
        "gc_reclaim_pending",
        "gc_intent_unconfirmed",
    }
    assert "checkpoint:cp-held-1" in view["protection"]["protected"]
    assert view["applicable_now"] is False
    assert view["kernel_zero_eligible"] is False
    assert view["target"]["checkpoint_enabled"] is False
    assert view["target"]["checkpoint_policy"] == "fail_closed"
    assert view["preview_seconds"] == 900
    assert view["node"]["target_fits"] is True
    assert any(key.startswith("quota-retire-plan-") for key in port.journal)


def test_pending_reclaim_blocks_release_until_history_cleared() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    port.protect()
    plan = service.preview(ENV, TARGET_LIMITS)
    view = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert view["retirement_state"] == "pending_reclaim"
    assert view["blocked_reason"] == "protected_history_present"
    assert port.calls == []
    assert port.reservation["checkpoint_released"] is False
    # 清理完成（显式GC、引用解除）后，同一操作自动推进到完成。
    port.clear_history()
    view = service.status(ENV, view["operation_id"])
    assert retired(view)
    assert port.calls == ["quarantine", "release", "apply_data_limit"]
    assert port.actual["data"] == {"byte_limit": 500 * MIB, "inode_limit": 100_000}
    # 默认保留内核旧限额：只停止分配，不清0也不误当0硬限额。
    assert port.actual["checkpoint"] == {"byte_limit": 100 * MIB, "inode_limit": 1_280}
    assert port.zero_requests == []
    assert port.record["checkpoint_number"] is None
    assert port.reservation["checkpoint_released"] is True
    assert port.reservation["reserved_bytes"] == 400 * MIB
    assert view["gate"]["new_writes_allowed"] is True


def test_waits_for_active_writers_before_touching_storage() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    plan = service.preview(ENV, TARGET_LIMITS)
    port.active["leases"] = ["project-a"]
    view = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert view["retirement_state"] == "waiting_idle"
    assert view["waiting_on"]["leases"] == ["project-a"]
    assert port.calls == []
    port.active["leases"] = []
    view = service.status(ENV, view["operation_id"])
    assert retired(view)
    assert view["waiting_on"] is None
    assert view["stopped_proof"]["idle"] is True


def test_kernel_zero_applies_only_after_quarantine_with_zero_inodes() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    plan = service.preview(ENV, TARGET_LIMITS, kernel_zero=True)
    assert plan["kernel_zero_eligible"] is False
    view = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert retired(view)
    assert port.zero_requests == [True]
    assert port.actual["checkpoint"] == {"byte_limit": 0, "inode_limit": 0}


def test_kernel_zero_refused_when_quarantine_proof_is_missing() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    port.protect()
    plan = service.preview(ENV, TARGET_LIMITS, kernel_zero=True)
    view = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    operation_id = view["operation_id"]
    assert view["retirement_state"] == "pending_reclaim"
    # 隔离证明缺失却标记已核实：release前拒绝清0，不发出任何存储动作。
    record = port.journal[port.operation_key(operation_id)]
    record["steps"][0]["state"] = "verified"
    port.clear_history()
    view = service.status(ENV, operation_id)
    assert view["retirement_state"] == "reconcile_required"
    assert view["blocked_reason"] == "kernel_zero_requires_quarantined_zero_inode"
    assert view["steps"][1]["reason"] == "kernel_zero_requires_quarantined_zero_inode"
    assert "release" not in port.calls
    assert "apply_data_limit" not in port.calls
    assert port.zero_requests == []
    assert port.record["checkpoint_number"] is not None


def test_confirm_rejects_capacity_conflicts_expiry_and_busy_gate() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    plan = service.preview(ENV, TARGET_LIMITS)
    operation_id = str(uuid.uuid4())
    with pytest.raises(ServiceError) as wrong_digest:
        service.confirm(ENV, plan["plan_id"], "0" * 64, operation_id)
    assert wrong_digest.value.code == "key_conflict"
    port.reservation["ceiling_bytes"] = 300 * MIB
    with pytest.raises(ServiceError) as capacity:
        service.confirm(ENV, plan["plan_id"], plan["plan_digest"], operation_id)
    assert capacity.value.code == "sandbox_limit"
    assert port.operation_key(operation_id) not in port.journal
    port.reservation["ceiling_bytes"] = 850 * MIB
    port.record["quota_operation"] = "other-operation"
    with pytest.raises(ServiceError) as busy:
        service.confirm(ENV, plan["plan_id"], plan["plan_digest"], operation_id)
    assert busy.value.code == "workspace_busy"
    port.record["quota_operation"] = None
    port.journal[port.plan_key(plan["plan_id"])]["expires_at"] = time.time() - 1
    with pytest.raises(ServiceError) as expired:
        service.confirm(ENV, plan["plan_id"], plan["plan_digest"], operation_id)
    assert expired.value.code == "approval_expired"
    with pytest.raises(ServiceError) as missing:
        service.confirm(ENV, str(uuid.uuid4()), plan["plan_digest"], operation_id)
    assert missing.value.code == "not_found"
    assert port.calls == []


def test_duplicate_operation_id_is_idempotent_and_gate_blocks_second_operation() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    plan = service.preview(ENV, TARGET_LIMITS)
    operation_id = str(uuid.uuid4())
    first = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], operation_id)
    assert retired(first)
    again = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], operation_id)
    assert again["retirement_state"] == "retired"
    assert port.calls.count("release") == 1
    assert port.reservation["reserved_bytes"] == 400 * MIB
    # 退役完成后同一计划换操作号不再可确认：没有可退役的旧区。
    with pytest.raises(ServiceError) as done:
        service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert done.value.code == "invalid_input"
    with pytest.raises(ServiceError) as preview_done:
        service.preview(ENV, TARGET_LIMITS)
    assert preview_done.value.code == "invalid_input"


def test_lost_reply_resumes_from_actual_without_replay() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    port.applied_then_fail.add("apply_data_limit")
    plan = service.preview(ENV, TARGET_LIMITS)
    view = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert view["retirement_state"] == "reconcile_required"
    assert port.actual["data"]["byte_limit"] == 500 * MIB  # 实际已生效
    assert port.calls.count("apply_data_limit") == 1
    view = service.status(ENV, view["operation_id"])
    assert retired(view)
    assert port.calls.count("apply_data_limit") == 1  # 核实实际值，不重放


def test_release_lost_reply_never_double_counts_reservation() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    port.applied_then_fail.add("release")
    plan = service.preview(ENV, TARGET_LIMITS)
    view = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert view["retirement_state"] == "reconcile_required"
    assert port.reservation["checkpoint_released"] is True
    assert port.reservation["reserved_bytes"] == 400 * MIB
    view = service.status(ENV, view["operation_id"])
    assert retired(view)
    assert port.calls.count("release") == 1
    assert port.reservation["reserved_bytes"] == 400 * MIB  # 不二次扣减


def test_step_failure_preserves_actual_vector_and_maintenance_gate() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    port.refuse["apply_data_limit"] = "sandbox_backend_unavailable"
    plan = service.preview(ENV, TARGET_LIMITS)
    view = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert view["retirement_state"] == "reconcile_required"
    assert view["steps"][2]["error"] == "sandbox_backend_unavailable"
    assert view["actual"]["data"] == {"byte_limit": 400 * MIB, "inode_limit": 98_720}
    assert view["gate"]["quota_operation"] == view["operation_id"]
    assert view["gate"]["new_writes_allowed"] is False
    port.refuse.clear()
    view = service.status(ENV, view["operation_id"])
    assert retired(view)
    assert port.actual["data"] == {"byte_limit": 500 * MIB, "inode_limit": 100_000}


def test_checkpoint_number_deletion_alone_is_not_completion() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    port.record["checkpoint_number"] = None
    port.reservation["checkpoint_released"] = True
    port.reservation["released_by_operation"] = "seeded"
    port.actual["data"] = {"byte_limit": 500 * MIB, "inode_limit": 100_000}
    operation_id = str(uuid.uuid4())
    seeded = {
        "operation_id": operation_id,
        "plan_id": "seeded-plan",
        "plan_digest": "0" * 64,
        "environment_id": ENV,
        "retirement_state": "applying",
        "blocked_reason": None,
        "protected": [],
        "confirmed_at": time.time(),
        "updated_at": time.time(),
        "desired": {
            "limits": TARGET_LIMITS,
            "normalized": {
                "environment_bytes": 500 * MIB,
                "environment_inodes": 100_000,
                "data_bytes": 500 * MIB,
                "data_inodes": 100_000,
                "checkpoint_bytes": 0,
                "checkpoint_inodes": 0,
            },
            "layout": "unsplit",
            "kernel_zero": False,
        },
        "steps": [
            {
                "step_id": index,
                "kind": kind,
                "zone": zone,
                "state": "verified",
                "reason": None,
                "error": None,
                "receipt": None,
            }
            for index, kind, zone in (
                (1, "quarantine", "checkpoint"),
                (2, "release", "checkpoint"),
                (3, "data_grow", "data"),
            )
        ],
        "stopped_proof": {"idle": True, "checked_at": time.time()},
        "waiting_on": None,
    }
    port.journal[port.operation_key(operation_id)] = seeded
    view = service.status(ENV, operation_id)
    # DB登记已注销、预留已释放，但隔离证据缺失：不构成完成。
    assert view["retirement_state"] == "reconcile_required"
    assert view["blocked_reason"] == "checkpoint_zone_not_quarantined"
    port.checkpoint_zone = {"quarantined": True, "inode_count": 0, "managed_path": "m/retired"}
    view = service.status(ENV, operation_id)
    assert retired(view)


def test_cancel_allowed_only_before_storage_mutation() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    port.protect()
    plan = service.preview(ENV, TARGET_LIMITS)
    operation_id = str(uuid.uuid4())
    service.confirm(ENV, plan["plan_id"], plan["plan_digest"], operation_id)
    cancelled = service.cancel(ENV, operation_id)
    assert cancelled["retirement_state"] == "cancelled"
    assert port.calls == []
    with pytest.raises(ServiceError) as twice:
        service.cancel(ENV, operation_id)
    assert twice.value.code == "invalid_input"
    assert service.status(ENV, operation_id)["retirement_state"] == "cancelled"
    port.clear_history()
    assert service.status(ENV, operation_id)["retirement_state"] == "cancelled"
    with pytest.raises(ServiceError) as applied:
        service.cancel(ENV, str(uuid.uuid4()))
    assert applied.value.code == "not_found"


def test_missing_stopped_proof_is_busy_with_consistent_shape() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    original = port._snapshot

    def no_proof() -> dict:
        snapshot = original()
        snapshot.pop("stopped_proof")
        return snapshot

    port._snapshot = no_proof  # type: ignore[method-assign]
    plan = service.preview(ENV, TARGET_LIMITS)
    assert plan["applicable_now"] is False
    view = service.confirm(ENV, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert view["retirement_state"] == "waiting_idle"
    assert view["waiting_on"]["stopped_proof_missing"] is True
    assert "maintenance_lock" in view["waiting_on"]  # 写者快照形状一致
    assert port.calls == []


def test_preview_rejects_non_c0_targets_and_completed_retirement() -> None:
    port = FakeRetirementPort(ENV)
    service = QuotaRetirement(port)
    with pytest.raises(ServiceError) as not_c0:
        service.preview(ENV, {**TARGET_LIMITS, "checkpoint_bytes": 100 * MIB})
    assert not_c0.value.code == "invalid_input"
    with pytest.raises(ServiceError) as bad_env:
        service.preview("not-a-uuid", TARGET_LIMITS)
    assert bad_env.value.code == "invalid_input"
    port.record["checkpoint_number"] = None
    with pytest.raises(ServiceError) as nothing:
        service.preview(ENV, TARGET_LIMITS)
    assert nothing.value.code == "invalid_input"
