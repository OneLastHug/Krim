"""BrokerRetirementPort生产适配测试：真实Broker状态持久化与隔离证据。

FakeXfs按真实目录树推导project占用：flag物化在inode上、rename保留、新建继承
最近父目录，quota.usage()读回即权威inode/字节读数——隔离0 inode证明只能由
真实文件系统状态（归档重标记+移除空壳目录）满足，不能靠字段伪造。检查点引用、
对象与两阶段GC走真实ShadowCheckpoints（真实git对象库）。真实XFS prjquota内核
（dquot持久化、bhard=0=unlimited语义、维护锁竞争）由独立宿主实验覆盖。
"""

import fcntl
import hashlib
import os
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import broker as module
from krim_agent.sandbox.broker import StorageBroker
from krim_agent.sandbox.checkpoints import ShadowCheckpoints
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.quota_retirement import QuotaRetirement
from krim_agent.sandbox.quota_retirement_adapter import BrokerRetirementPort

MIB = 1024**2
RETENTION = 1  # 保留期1秒：显式GC可立刻回收未固定检查点


class FakeXfs:
    """以目录树物化flag模拟XFS project quota；usage由真实文件推导。"""

    def __init__(self, mount: Path, root: Path) -> None:
        self.mount = mount
        self.root = root
        self.flags: dict[tuple[int, int], tuple[int, Path]] = {}
        self.rows: dict[int, dict[str, int]] = {}
        self.commands: list[str] = []

    def current(self) -> None:
        return None

    @staticmethod
    def project_number(environment_id: str) -> int:
        return 100_000 + int(hashlib.sha256(environment_id.encode()).hexdigest()[:7], 16)

    def _prune_stale(self) -> None:
        """原路径已消失的登记随查询失效：rmdir释放的inode号可能被复用，不能串到新树。

        真内核flag存于inode本身，新建inode不带旧flag；本fake以登记路径是否
        仍解析到同一inode近似该语义。代价是rename后的树在重标记前不再计入
        原project（重标记命令序列由测试另行断言）。
        """
        for key in list(self.flags):
            _, origin = self.flags[key]
            try:
                info = origin.lstat()
            except OSError:
                del self.flags[key]
                continue
            if (info.st_dev, info.st_ino) != key:
                del self.flags[key]

    def _key(self, path: Path) -> tuple[int, int] | None:
        try:
            info = path.lstat()
        except OSError:
            return None
        return (info.st_dev, info.st_ino)

    def _flag_tree(self, path: Path, number: int) -> None:
        key = self._key(path)
        if key is not None:
            self.flags[key] = (number, path)
        for directory, folders, files in os.walk(path):
            for name in (*folders, *files):
                current = Path(directory) / name
                key = self._key(current)
                if key is not None:
                    self.flags[key] = (number, current)

    def _owner(self, path: Path) -> int:
        current = path
        while True:
            key = self._key(current)
            if key is not None and key in self.flags:
                return self.flags[key][0]
            if current == current.parent:
                return 0
            current = current.parent

    def command(self, expression: str) -> str:
        self.commands.append(expression)
        parts = expression.split()
        if parts[:2] == ["limit", "-p"]:
            bhard, ihard = 0, 0
            for token in parts[2:-1]:
                name, _, value = token.partition("=")
                if name == "bhard":
                    bhard = int(value.rstrip("k"))
                elif name == "ihard":
                    ihard = int(value)
            self.rows[int(parts[-1])] = {"byte_limit": bhard * 1024, "inode_limit": ihard}
            return ""
        if parts[:3] == ["project", "-s", "-p"]:
            self._flag_tree(Path(parts[3]), int(parts[4]))
            return ""
        raise AssertionError("意外命令：" + expression)

    def usage(self, number: int) -> dict[str, int]:
        row = self.rows.get(number)
        if row is None:
            raise ServiceError("sandbox_backend_unavailable", 503)
        self._prune_stale()
        used_bytes = 0
        used_inodes = 0
        for directory, folders, files in os.walk(self.mount):
            for name in (*folders, *files):
                path = Path(directory) / name
                if self._owner(path) == number:
                    used_inodes += 1
                    try:
                        used_bytes += path.lstat().st_size
                    except OSError:
                        pass
        return {
            "environment_bytes": used_bytes,
            "byte_limit": row["byte_limit"],
            "environment_inodes": used_inodes,
            "inode_limit": row["inode_limit"],
        }

    def provision(self, environment_id: str, limits: QuotaLimits) -> dict[str, Any]:
        number = self.project_number(environment_id)
        path = self.root / environment_id
        path.mkdir(mode=0o700, exist_ok=True)
        kib = limits.environment_bytes // 1024
        self.command(f"project -s -p {path} {number}")
        self.command(f"limit -p bhard={kib}k ihard={limits.environment_inodes} {number}")
        return {
            "project_number": number,
            "limits": limits.model_dump(mode="json"),
            "usage": self.usage(number),
            "enforcement": "xfs_project_quota",
        }

    def checkpoint_partition(self, environment_id: str, limits: QuotaLimits) -> dict[str, Any]:
        number = self.project_number(environment_id)
        checkpoint = number + 0x40000000
        path = self.root / environment_id / "checkpoints"
        path.mkdir(mode=0o700, exist_ok=True)
        if not limits.checkpoint_bytes or not limits.checkpoint_count:
            raise ServiceError("sandbox_limit")
        ck_inodes = min(limits.environment_inodes // 4, max(128, limits.checkpoint_count * 64))
        data_bytes = (limits.environment_bytes - limits.checkpoint_bytes) // 1024 * 1024
        data_inodes = limits.environment_inodes - ck_inodes
        ck_kib = limits.checkpoint_bytes // 1024
        self.command(f"project -s -p {path} {checkpoint}")
        self.command(f"limit -p bhard={ck_kib}k ihard={ck_inodes} {checkpoint}")
        self.command(f"limit -p bhard={data_bytes // 1024}k ihard={data_inodes} {number}")
        return {
            "checkpoint_number": checkpoint,
            "checkpoint_bytes": ck_kib * 1024,
            "data_bytes": data_bytes,
        }

    def account_usage(self, record: dict[str, Any]) -> dict[str, int]:
        used = self.usage(int(record["project_number"]))
        registered = record.get("checkpoint_number")
        if registered is not None:
            checkpoint = self.usage(int(registered))
            used = {key: used[key] + checkpoint[key] for key in used}
        return used


@pytest.fixture
def storage(tmp_path, monkeypatch):
    mount, state = tmp_path / "disk", tmp_path / "state"
    mount.mkdir(mode=0o700)
    state.mkdir(mode=0o700)
    monkeypatch.setattr(module, "XfsAccounts", FakeXfs)
    monkeypatch.setattr(module.os, "chown", lambda *a, **k: None)
    value = StorageBroker(mount, state)
    try:
        yield value
    finally:
        value.close()


def limits(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "environment_bytes": 8 * MIB,
        "environment_inodes": 4000,
        "checkpoint_bytes": 2 * MIB,
        "checkpoint_count": 20,
        "checkpoint_retention_seconds": RETENTION,
        "frozen_bytes": None,
    }
    values.update(overrides)
    return values


def prepare_split(storage: StorageBroker, environment_id: str, **overrides: Any) -> int:
    """登记环境并拆分检查点区（与jobs首次capture相同的partition路径）。"""
    base = limits(**overrides)
    storage.dispatch("provision_environment", {"environment_id": environment_id, "limits": base})
    partition = storage.quota.checkpoint_partition(environment_id, QuotaLimits(**base))
    record = storage._environment(environment_id)
    record.update(partition)
    storage._save("env-" + environment_id + ".json", record)
    storage.quota.commands.clear()
    return storage.quota.project_number(environment_id)


def store_of(storage: StorageBroker, environment_id: str) -> ShadowCheckpoints:
    return ShadowCheckpoints(storage.environments / environment_id / "checkpoints")


def capture_checkpoint(
    storage: StorageBroker, environment_id: str, project_id: str, *, pin: bool = True
) -> str:
    store = store_of(storage, environment_id)
    captured = storage.files.snapshot(environment_id, project_id)
    row = store.capture(
        environment_id, project_id, captured["files"], captured["excluded"], QuotaLimits(**limits())
    )
    if pin:
        store.pin(environment_id, project_id, row["checkpoint_id"], pinned=True)
    return str(row["checkpoint_id"])


def clear_history(
    storage: StorageBroker, environment_id: str, project_id: str, checkpoint_id: str
) -> None:
    """显式两阶段GC：解除用户引用→过期→cleanup_preview/confirm删引用并物理回收。"""
    store = store_of(storage, environment_id)
    store.pin(environment_id, project_id, checkpoint_id, pinned=False)
    time.sleep(RETENTION + 0.05)
    plan = store.cleanup_preview(environment_id, project_id, QuotaLimits(**limits()))
    receipt = store.cleanup_confirm(
        environment_id,
        project_id,
        plan["plan_id"],
        plan["plan_digest"],
        "retire:" + plan["plan_id"],
    )
    assert receipt["status"] == "completed"


def retirement(storage: StorageBroker) -> QuotaRetirement:
    return QuotaRetirement(BrokerRetirementPort(storage))


TARGET = {
    "environment_bytes": 8 * MIB,
    "environment_inodes": 4000,
    "checkpoint_bytes": 0,
    "checkpoint_count": 0,
}


def test_inspect_reports_kernel_state_and_protection(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    snapshot = BrokerRetirementPort(storage).inspect(env)

    assert snapshot["record"]["checkpoint_number"] == storage.quota.project_number(env) + 0x40000000
    assert snapshot["actual"]["checkpoint"]["byte_limit"] == 2 * MIB
    assert snapshot["usage"]["checkpoint"]["environment_inodes"] > 0  # 内核权威读回
    assert snapshot["refs"]["protected"] == [checkpoint_id]
    assert snapshot["refs"]["live_checkpoints"] == 1
    assert snapshot["objects"]["physical_bytes"] > 0
    assert snapshot["checkpoint_zone"]["quarantined"] is False
    assert snapshot["checkpoint_zone"]["inode_count"] > 0
    assert snapshot["reservation"]["checkpoint_budget_bytes"] == 2 * MIB
    assert snapshot["reservation"]["checkpoint_released"] is False
    assert snapshot["stopped_proof"]["idle"] is True

    preview = retirement(storage).preview(env, TARGET)
    assert preview["protection"]["required"] is True
    assert "checkpoint_refs_held" in preview["protection"]["reasons"]
    assert "checkpoint_objects_present" in preview["protection"]["reasons"]
    assert f"checkpoint:{checkpoint_id}" in preview["protection"]["protected"]
    assert preview["applicable_now"] is False
    assert preview["kernel_zero_eligible"] is False
    assert "quota_operation" not in storage._environment(env)  # 预览不设维护门


def test_retirement_completes_after_explicit_gc_with_persisted_state(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = prepare_split(storage, env)
    checkpoint_number = number + 0x40000000
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    service = retirement(storage)
    operation_id = str(uuid.uuid4())

    plan = service.preview(env, TARGET)
    view = service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
    assert view["retirement_state"] == "pending_reclaim"
    record = storage._environment(env)
    assert record["quota_operation"] == operation_id  # 门随confirm落盘
    assert record["quota_state"] == "retiring"
    stored = storage._read("quota-retire-" + operation_id + ".json")
    assert stored is not None and stored["retirement_state"] == "pending_reclaim"
    with pytest.raises(ServiceError, match="workspace_busy"):
        storage.dispatch("provision_project", {"environment_id": env, "project_id": project})

    clear_history(storage, env, project, checkpoint_id)
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "retired"
    assert view["gate"]["new_writes_allowed"] is True

    record = storage._environment(env)
    assert "checkpoint_number" not in record
    assert record["checkpoint_released_by"] == operation_id
    assert record["quota_state"] == "retired"
    assert "quota_operation" not in record
    assert record["limits"]["checkpoint_bytes"] == 0
    assert record["limits"]["environment_bytes"] == 8 * MIB
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["checkpoint_bytes"] == 0
    # 内核权威读回：data扩到全额，旧检查点project id为0 inode，旧限额保留
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB
    zone = storage.quota.usage(checkpoint_number)
    assert zone["environment_inodes"] == 0 and zone["environment_bytes"] == 0
    assert zone["byte_limit"] == 2 * MIB  # 默认keep模式不清0
    # 归档有界管理区：账户目录连同维护锁整体移入，checkpoints空壳移除
    assert not (storage.environments / env / "checkpoints").exists()
    archived = storage.environments / env / "maintenance-archives"
    entries = list(archived.iterdir())
    assert len(entries) == 1 and entries[0].name == "checkpoint-retirement-" + operation_id
    assert (entries[0] / "maintenance.lock").is_file()
    stored = storage._read("quota-retire-" + operation_id + ".json")
    assert stored is not None
    assert all(step["state"] == "verified" for step in stored["steps"])
    assert stored["stopped_proof"]["idle"] is True


def test_kernel_zero_clears_limits_only_after_quarantine_proof(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = prepare_split(storage, env)
    checkpoint_number = number + 0x40000000
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    service = retirement(storage)
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET, kernel_zero=True)
    pending = service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
    assert pending["retirement_state"] == "pending_reclaim"
    clear_history(storage, env, project, checkpoint_id)
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "retired"
    assert view["actual"]["checkpoint"] == {"byte_limit": 0, "inode_limit": 0}  # unlimited语义
    assert storage.quota.usage(checkpoint_number)["environment_inodes"] == 0
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB
    commands = storage.quota.commands
    flag = next(index for index, item in enumerate(commands) if item.startswith("project -s -p"))
    cleared = next(
        index
        for index, item in enumerate(commands)
        if item.startswith("limit -p") and item.endswith(f" {checkpoint_number}")
    )
    assert flag < cleared  # 清0只在归档重标记之后
    assert f"limit -p bhard=0 ihard=0 {checkpoint_number}" in commands


def test_out_of_order_and_foreign_port_calls_are_refused(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    port = BrokerRetirementPort(storage)
    service = QuotaRetirement(port)
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET)
    service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
    clear_history(storage, env, project, checkpoint_id)
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "retired"

    with pytest.raises(ServiceError, match="key_conflict"):
        port.release_quota_allocation(env, str(uuid.uuid4()), kernel_zero=False)
    with pytest.raises(ServiceError, match="key_conflict"):
        port.apply_data_limit(env, str(uuid.uuid4()), 4 * MIB, 2000)
    again = port.release_quota_allocation(env, operation_id, kernel_zero=False)
    assert again["checkpoint_released"] is True  # 同操作幂等读回
    assert storage._environment(env)["checkpoint_released_by"] == operation_id
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB

    other = str(uuid.uuid4())
    storage.dispatch("provision_environment", {"environment_id": other, "limits": limits()})
    with pytest.raises(ServiceError, match="invalid_input"):
        port.release_quota_allocation(other, operation_id, kernel_zero=False)


def test_quarantine_refuses_protected_or_unknown_content(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    port = BrokerRetirementPort(storage)
    service = QuotaRetirement(port)
    plan = service.preview(env, TARGET)
    operation_id = str(uuid.uuid4())
    service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)

    with pytest.raises(ServiceError, match="artifact_conflict"):
        port.quarantine_empty_checkpoint_root(env, operation_id)
    assert storage.quota.commands == []  # 拒绝时无内核写入
    assert not (storage.environments / env / "maintenance-archives").exists()

    clear_history(storage, env, project, checkpoint_id)
    foreign = storage.environments / env / "checkpoints" / env / "foreign.txt"
    foreign.write_text("x")
    with pytest.raises(ServiceError, match="artifact_conflict"):
        port.quarantine_empty_checkpoint_root(env, operation_id)
    assert foreign.exists()  # 未知内容不归档不删除
    foreign.unlink()
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "retired"

    with pytest.raises(ServiceError, match="invalid_input"):
        port.journal_get("../../env-evil")
    with pytest.raises(ServiceError, match="invalid_input"):
        port.journal_put("quota-retire-not-a-uuid", {})
    port.journal_put("quota-retire-plan-" + str(uuid.uuid4()), {"digest": "x"})


def test_gate_binds_at_confirm_and_cancel_clears_it(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    capture_checkpoint(storage, env, project)
    service = retirement(storage)
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET)
    service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
    assert storage._environment(env)["quota_operation"] == operation_id

    cancelled = service.cancel(env, operation_id)
    assert cancelled["retirement_state"] == "cancelled"
    record = storage._environment(env)
    assert "quota_operation" not in record and "quota_state" not in record
    with pytest.raises(ServiceError, match="invalid_input"):
        service.cancel(env, operation_id)
    assert service.preview(env, TARGET)["plan_id"]  # 撤回后控制面可正常预览
    assert service.status(env, operation_id)["retirement_state"] == "cancelled"


def test_waiting_idle_waits_for_lease_then_completes(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    lease = str(uuid.uuid4())
    storage.dispatch(
        "claim",
        {
            "environment_id": env,
            "project_id": project,
            "lease_id": lease,
            "deadline": time.time() + 60,
        },
    )
    service = retirement(storage)
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET)
    view = service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
    assert view["retirement_state"] == "waiting_idle"
    assert view["waiting_on"]["leases"] == [project]
    assert storage.quota.commands == []  # 等待期间零内核写入

    storage.dispatch(
        "release",
        {"environment_id": env, "project_id": project, "lease_id": lease, "clean": True},
    )
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "retired"
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB
    assert not (storage.environments / env / "checkpoints").exists()


def test_maintenance_lock_holding_blocks_start(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    service = retirement(storage)
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET)
    lock = storage.environments / env / "checkpoints" / env / "maintenance.lock"
    os.makedirs(lock.parent, mode=0o700, exist_ok=True)
    held = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(held, fcntl.LOCK_EX)
        snapshot = BrokerRetirementPort(storage).inspect(env)
        assert snapshot["active"]["maintenance_lock"] is True
        assert snapshot["stopped_proof"]["idle"] is False
        view = service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
        assert view["retirement_state"] == "waiting_idle"
        assert view["waiting_on"]["maintenance_lock"] is True
        assert storage.quota.commands == []
    finally:
        fcntl.flock(held, fcntl.LOCK_UN)
        os.close(held)
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "retired"
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB


def test_lost_release_reply_resumes_without_double_release(storage):
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)

    class FlakyPort(BrokerRetirementPort):
        def __init__(self, broker: StorageBroker) -> None:
            super().__init__(broker)
            self.fail_after = False

        def release_quota_allocation(
            self, environment_id: str, operation_id: str, *, kernel_zero: bool
        ) -> dict[str, Any]:
            result = super().release_quota_allocation(
                environment_id, operation_id, kernel_zero=kernel_zero
            )
            if self.fail_after:
                raise ServiceError("sandbox_backend_unavailable", 503)
            return result

    port = FlakyPort(storage)
    service = QuotaRetirement(port)
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET)
    service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
    clear_history(storage, env, project, checkpoint_id)
    port.fail_after = True
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "reconcile_required"
    assert view["steps"][1]["error"] == "sandbox_backend_unavailable"
    record = storage._environment(env)
    assert record["checkpoint_released_by"] == operation_id  # 释放已提交
    assert record["quota_operation"] == operation_id  # 门保持可核实

    port.fail_after = False
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "retired"
    assert storage._environment(env)["checkpoint_released_by"] == operation_id  # 无二次释放
    assert view["steps"][1]["state"] == "verified"


def test_checkpoint_flock_excludes_in_flight_gc_from_quarantine_and_release(storage):
    """在途GC持锁期间隔离与释放都必须被flock拒绝，不能只靠quota_operation门。"""
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    port = BrokerRetirementPort(storage)
    service = QuotaRetirement(port)
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET)
    service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
    clear_history(storage, env, project, checkpoint_id)
    lock = storage.environments / env / "checkpoints" / env / "maintenance.lock"
    held = os.open(lock, os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(ServiceError, match="workspace_busy"):
            port.quarantine_empty_checkpoint_root(env, operation_id)
        with pytest.raises(ServiceError, match="workspace_busy"):
            port.release_quota_allocation(env, operation_id, kernel_zero=False)
        assert storage.quota.commands == []  # 被拒期间零内核写入
        assert not (storage.environments / env / "maintenance-archives").exists()
    finally:
        fcntl.flock(held, fcntl.LOCK_UN)
        os.close(held)
    view = service.status(env, operation_id)  # 锁释放后同操作继续并完成
    assert view["retirement_state"] == "retired"
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB


def test_apply_failure_injections_keep_gate_until_reservation_and_gate_persist(storage):
    """账本与放门落盘失败都保持维护门：预留先行，同操作幂等补齐，门不提前放开。"""
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    service = QuotaRetirement(BrokerRetirementPort(storage))
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET)
    service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)
    clear_history(storage, env, project, checkpoint_id)

    original_reserve = storage.quota_changes._reserve

    def failing_reserve(environment_id: str, limits: dict, *, larger_only: bool) -> None:
        raise ServiceError("sandbox_limit")

    storage.quota_changes._reserve = failing_reserve
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "reconcile_required"
    assert view["steps"][2]["error"] == "sandbox_limit"
    record = storage._environment(env)
    assert record["quota_operation"] == operation_id and record["quota_state"] == "retiring"
    assert storage.quota.usage(number)["byte_limit"] == 6 * MIB  # 账本先行失败：内核未动
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["checkpoint_bytes"] == 2 * MIB  # 预留未更新

    storage.quota_changes._reserve = original_reserve
    original_save = storage._save
    state = {"fail_env_save": True}

    def flaky_save(name: str, value: dict) -> None:
        if name == "env-" + env + ".json" and state["fail_env_save"]:
            raise ServiceError("sandbox_limit")
        original_save(name, value)

    storage._save = flaky_save
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "reconcile_required"
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB  # 内核已生效
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["checkpoint_bytes"] == 0  # 预留已在放门之前完成
    assert storage._environment(env)["quota_operation"] == operation_id  # 门仍保持

    storage._save = original_save
    view = service.status(env, operation_id)
    assert view["retirement_state"] == "retired"
    record = storage._environment(env)
    assert "quota_operation" not in record and record["quota_state"] == "retired"
    assert storage._read("accounts.json")["allocations"][env]["limits"]["checkpoint_bytes"] == 0


def test_exists_treats_only_missing_as_absent_and_failures_fail_closed(storage, monkeypatch):
    """不可读不得当缺失参与隔离证明：仅FileNotFoundError返回False，其余OSError报503。"""
    from krim_agent.sandbox import quota_retirement_adapter as adapter

    missing = storage.environments / "no-such-env"
    assert adapter._exists(missing) is False
    assert adapter._exists(storage.environments) is True

    original = os.lstat

    def denied(path: object, *args: object, **kwargs: object) -> object:
        raise PermissionError(13, "denied")

    monkeypatch.setattr(adapter.os, "lstat", denied)
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        adapter._exists(storage.environments)

    # 集成面：检查点账户不可读时inspect/verify/隔离/释放全部fail-closed
    monkeypatch.setattr(adapter.os, "lstat", original)
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    prepare_split(storage, env)
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    capture_checkpoint(storage, env, project)
    port = BrokerRetirementPort(storage)
    account = storage.environments / env / "checkpoints" / env

    def deny_account(path: object, *args: object, **kwargs: object) -> object:
        target = str(path)
        if target == str(account) or target.startswith(str(account) + os.sep):
            raise PermissionError(13, "denied")
        return original(path, *args, **kwargs)

    real_stat = os.stat

    def deny_stat(path: object, *args: object, **kwargs: object) -> object:
        # Path.lstat()走os.stat(follow_symlinks=False)：同样拦截才能覆盖目录遍历
        target = str(path)
        if not kwargs.get("follow_symlinks", True) and (
            target == str(account) or target.startswith(str(account) + os.sep)
        ):
            raise PermissionError(13, "denied")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", deny_account)
    monkeypatch.setattr(os, "stat", deny_stat)
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        port.inspect(env)
    # verify读回非零（内核读数不受枚举受阻影响）时不做内容枚举：
    # 返回quarantined=False即安全方向，不可读不构成隔离证据
    state = port.verify(env)
    assert state["checkpoint_zone"]["quarantined"] is False
    probe = str(uuid.uuid4())  # 同一操作号：先绑门再撞不可读，避免误报他人持门
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        port.quarantine_empty_checkpoint_root(env, probe)
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        port.release_quota_allocation(env, probe, kernel_zero=False)


def test_verify_matches_inspect_and_zone_proof_follows_evidence(storage):
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
    checkpoint_number = number + 0x40000000
    port = BrokerRetirementPort(storage)
    service = QuotaRetirement(port)
    operation_id = str(uuid.uuid4())
    plan = service.preview(env, TARGET)
    assert service.confirm(env, plan["plan_id"], plan["plan_digest"], operation_id)[
        "retirement_state"
    ] == "retired"  # 空分区区：无内容直接隔离

    state = port.verify(env)
    snapshot = port.inspect(env)
    for key in ("record", "actual", "checkpoint_zone", "reservation"):
        assert state[key] == snapshot[key]
    assert set(state) == {"record", "actual", "checkpoint_zone", "reservation"}
    assert state["checkpoint_zone"]["quarantined"] is True
    assert storage.quota.usage(checkpoint_number)["environment_inodes"] == 0

    # 隔离证明基于证据：空目录重建不破坏判定（新inode未带旧flag）
    repo = storage.environments / env / "checkpoints" / env / "objects.git"
    repo.mkdir(parents=True, mode=0o700)
    store = store_of(storage, env)
    store._git(repo, ["init", "--bare", "--template=", str(repo)], initializing=True)
    assert port.verify(env)["checkpoint_zone"]["quarantined"] is True
    blob = store._git(repo, ["hash-object", "-w", "--stdin"], b"x").decode().strip()
    tree = store._git(repo, ["mktree"], f"100644 blob {blob}\tx\n".encode()).decode().strip()
    commit = store._git(repo, ["commit-tree", tree, "-m", "x"]).decode().strip()
    store._git(repo, ["update-ref", f"refs/krim/{uuid.uuid4()}/{uuid.uuid4()}", commit])
    assert port.verify(env)["checkpoint_zone"]["quarantined"] is False  # 新引用如实翻转
    store._git(repo, ["update-ref", "refs/krim/not-a-uuid/also-bad", commit])
    with pytest.raises(ServiceError, match="facts_corrupt"):
        port.verify(env)  # 未知形状拒绝：无法证明为空就不隔离

    # 旧project id被重新分配（如重新启用检查点的init重标记同一号）：
    # 内核读回如实变非零，隔离证据翻转，不由字段维持假象
    root = storage.environments / env / "checkpoints"
    root.mkdir(mode=0o700, exist_ok=True)
    storage.quota.command(f"project -s -p {root / env} {checkpoint_number}")
    state = port.verify(env)
    assert state["checkpoint_zone"]["quarantined"] is False
    assert state["checkpoint_zone"]["inode_count"] > 0
