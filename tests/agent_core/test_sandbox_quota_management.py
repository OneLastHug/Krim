"""在线配额调整协议合成测试：预览零写入、等待空闲、部分应用与内核实际值核实。

FakeQuota 用命令字符串模拟 XFS project quota 的写入与读回，并记录全部写命令；
真实 XFS 行为（含"降至已用以下"）由独立实验覆盖，这里只验证协议与状态机。
"""

import hashlib
import time
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import broker as module
from krim_agent.sandbox.broker import StorageBroker
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.quota_management import QuotaChanges

MIB = 1024**2


class FakeQuota:
    """模拟XFS：limit/project命令写入行值，usage读回；记录写命令供零写入断言。"""

    def __init__(self, mount, root) -> None:
        self.rows = {}
        self.projects = {}
        self.commands = []
        self.hijack = set()  # 命令生效后写回错误值，模拟内核部分应用/拒绝
        self.fail_after_apply = set()  # 命令生效后报错，模拟回包丢失
        self.refuse = set()  # 命令直接失败

    def current(self) -> None:
        pass

    @staticmethod
    def project_number(environment_id):
        return 100_000 + int(hashlib.sha256(environment_id.encode()).hexdigest()[:7], 16)

    def command(self, expression):
        parts = expression.split()
        if parts[:2] == ["limit", "-p"]:
            bhard, ihard = 0, 0
            for token in parts[2:-1]:
                key, _, value = token.partition("=")
                if key == "bhard":
                    bhard = int(value.rstrip("k"))
                elif key == "ihard":
                    ihard = int(value)
            number = int(parts[-1])
            self.commands.append(expression)
            if number in self.refuse:
                raise ServiceError("sandbox_backend_unavailable", 503)
            row = self.rows.setdefault(
                number,
                {"environment_bytes": 0, "environment_inodes": 0},
            )
            row["byte_limit"] = bhard * 1024
            row["inode_limit"] = ihard
            if number in self.hijack:
                row["byte_limit"] += 1024
            if number in self.fail_after_apply:
                raise ServiceError("sandbox_backend_unavailable", 503)
            return ""
        if parts[:3] == ["project", "-s", "-p"]:
            number = int(parts[4])
            self.commands.append(expression)
            if number in self.refuse:
                raise ServiceError("sandbox_backend_unavailable", 503)
            self.projects[number] = parts[3]
            return ""
        raise AssertionError("意外命令：" + expression)

    def usage(self, number):
        row = self.rows.get(number)
        if row is None:
            raise ServiceError("sandbox_backend_unavailable", 503)
        return dict(row)

    def provision(self, environment_id, limits):
        number = self.project_number(environment_id)
        self.rows[number] = {
            "environment_bytes": 0,
            "environment_inodes": 0,
            "byte_limit": limits.environment_bytes // 1024 * 1024,
            "inode_limit": limits.environment_inodes,
        }
        return {
            "project_number": number,
            "limits": limits.model_dump(mode="json"),
            "usage": dict(self.rows[number]),
            "enforcement": "xfs_project_quota",
        }

    def checkpoint_partition(self, environment_id, limits):
        number = self.project_number(environment_id)
        checkpoint = number + 0x40000000
        ck_inodes = min(limits.environment_inodes // 4, max(128, limits.checkpoint_count * 64))
        data_bytes = (limits.environment_bytes - limits.checkpoint_bytes) // 1024 * 1024
        data_inodes = limits.environment_inodes - ck_inodes
        self.command(
            f"limit -p bhard={limits.checkpoint_bytes // 1024}k ihard={ck_inodes} {checkpoint}"
        )
        self.command(f"limit -p bhard={data_bytes // 1024}k ihard={data_inodes} {number}")
        return {
            "checkpoint_number": checkpoint,
            "checkpoint_bytes": limits.checkpoint_bytes // 1024 * 1024,
            "data_bytes": data_bytes,
        }

    def account_usage(self, record):
        used = self.usage(record["project_number"])
        if record.get("checkpoint_number") is not None:
            checkpoint = self.usage(record["checkpoint_number"])
            used = {key: used[key] + checkpoint[key] for key in used}
        return used


class FakeJobs:
    def __init__(self) -> None:
        self.active = {}

    def close(self) -> None:
        self.active = {}


class FakeRecovery:
    def __init__(self, active) -> None:
        self.active = active


def blocked_lease(storage, project_id):
    lease_id = str(uuid.uuid4())
    storage.leases._save(
        project_id, {"status": "blocked", "lease_id": lease_id, "deadline": time.time() + 300}
    )
    return lease_id


def limits(**overrides) -> dict:
    values = {
        "environment_bytes": 8 * MIB,
        "environment_inodes": 4000,
        "checkpoint_bytes": 2 * MIB,
        "checkpoint_count": 20,
        "checkpoint_retention_seconds": 7 * 86400,
        "frozen_bytes": None,
    }
    values.update(overrides)
    return values


@pytest.fixture
def storage(tmp_path, monkeypatch):
    mount, state = tmp_path / "disk", tmp_path / "state"
    mount.mkdir(mode=0o700)
    state.mkdir(mode=0o700)
    monkeypatch.setattr(module, "XfsAccounts", FakeQuota)
    monkeypatch.setattr(module.os, "chown", lambda *a, **k: None)
    value = StorageBroker(mount, state)
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def changes(storage):
    return QuotaChanges(storage)


def prepare(storage, environment_id, **overrides):
    storage.dispatch(
        "provision_environment", {"environment_id": environment_id, "limits": limits(**overrides)}
    )
    storage.quota.commands.clear()
    return storage.quota.project_number(environment_id)


def prepare_split(storage, environment_id, **overrides):
    number = prepare(storage, environment_id, **overrides)
    base = QuotaLimits(**limits(**overrides))
    partition = storage.quota.checkpoint_partition(environment_id, base)
    record = storage._environment(environment_id)
    record.update(partition)
    storage._save("env-" + environment_id + ".json", record)
    storage.quota.commands.clear()
    return number


def test_preview_is_read_only_and_reports_unsplit_checkpoint_as_null(storage, changes):
    env = str(uuid.uuid4())
    prepare(storage, env)
    plan = changes.preview(env, limits(environment_bytes=16 * MIB))

    assert storage.quota.commands == []  # 预览零内核写入
    assert plan["current"]["layout"] == "unsplit"
    assert plan["current"]["checkpoint"] is None
    assert plan["current"]["storage_state"]["checkpoint"] == "not_provisioned"
    assert plan["target"]["checkpoint_enabled"] is True
    assert plan["target"]["normalized"]["data_bytes"] == 14 * MIB  # (16MiB-2MiB)
    assert plan["target"]["normalized"]["checkpoint_bytes"] == 2 * MIB
    assert plan["target"]["zone_actions"]["data"] == "shrink"  # 拆分区后data让出inode
    assert plan["target"]["zone_actions"]["checkpoint"] == "initialize"
    assert len(plan["plan_digest"]) == 64
    stored = storage._read("quota-plan-" + plan["plan_id"] + ".json")
    assert stored is not None and stored["digest"] == plan["plan_digest"]
    assert stored["expires_at"] <= time.time() + 900
    record = storage._environment(env)
    assert "quota_operation" not in record  # 预览不设维护门
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["environment_bytes"] == 8 * MIB  # 预览不动预留


def test_grow_applies_and_reads_back_kernel_values(storage, changes):
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
    checkpoint_number = number + 0x40000000
    plan = changes.preview(env, limits(environment_bytes=16 * MIB))
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))

    assert result["quota_state"] == "applied"
    data = storage.quota.usage(number)
    checkpoint = storage.quota.usage(checkpoint_number)
    assert data["byte_limit"] == 14 * MIB and data["inode_limit"] == 3000
    assert checkpoint["byte_limit"] == 2 * MIB and checkpoint["inode_limit"] == 1000
    assert result["applied"]["data"]["byte_limit"] == 14 * MIB
    assert result["usage"]["account"]["environment_bytes"] == 0
    record = storage._environment(env)
    assert record["limits"]["environment_bytes"] == 16 * MIB
    assert record["checkpoint_number"] == checkpoint_number
    assert record["data_bytes"] == 14 * MIB
    assert "quota_operation" not in record and record["quota_state"] == "applied"
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["environment_bytes"] == 16 * MIB
    assert result["gate"]["new_writes_allowed"] is True
    assert result["gate"]["reads_allowed"] and result["gate"]["cleanup_allowed"]


def test_shrink_below_usage_pending_reclaim_without_deletion(storage, changes):
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
    storage.quota.rows[number]["environment_bytes"] = 5 * MIB
    storage.quota.rows[number]["environment_inodes"] = 500
    operation = str(uuid.uuid4())
    plan = changes.preview(env, limits(environment_bytes=6 * MIB))
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)

    assert result["quota_state"] == "pending_reclaim"
    assert storage.quota.commands == []  # 低于用量不写内核
    row = storage.quota.usage(number)
    assert row["byte_limit"] == 6 * MIB  # 旧实际值保留
    assert row["environment_bytes"] == 5 * MIB  # 不删除数据
    step = result["steps"][0]
    assert step["state"] == "pending_reclaim" and step["reason"] == "usage_above_target"
    assert result["zones"]["data"]["status"] == "pending_reclaim"
    assert result["gate"]["quota_operation"] == operation
    assert result["gate"]["new_writes_allowed"] is False
    assert result["gate"]["reads_allowed"] and result["gate"]["cleanup_allowed"]
    again = changes.status(env, operation)
    assert again["quota_state"] == "pending_reclaim"  # 用量未降，复核后保持


def test_active_writers_wait_idle_then_status_applies(storage, changes):
    env = str(uuid.uuid4())
    number = prepare(storage, env)
    project = str(uuid.uuid4())
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    storage.jobs = FakeJobs()
    storage.jobs.active["job-1"] = {"record": {"environment_id": env}}
    plan = changes.preview(env, limits(environment_bytes=16 * MIB, checkpoint_bytes=0))
    operation = str(uuid.uuid4())
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)

    assert result["quota_state"] == "waiting_idle"
    assert result["waiting_on"]["jobs"] == ["job-1"]
    assert storage.quota.commands == []  # 等待期间不改内核
    assert storage._environment(env)["quota_operation"] == operation

    del storage.jobs.active["job-1"]
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
    result = changes.status(env, operation)
    assert result["quota_state"] == "waiting_idle"
    assert result["waiting_on"]["leases"] == [project]

    storage.dispatch(
        "release",
        {"environment_id": env, "project_id": project, "lease_id": lease, "clean": True},
    )
    result = changes.status(env, operation)
    assert result["quota_state"] == "applied"
    assert storage.quota.usage(number)["byte_limit"] == 16 * MIB
    assert "quota_operation" not in storage._environment(env)


def test_partial_apply_second_failure_keeps_gate_and_shows_actual(storage, changes):
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
    checkpoint_number = number + 0x40000000
    storage.quota.hijack = {checkpoint_number}
    # C 2MiB→4MiB 扩容，D 6MiB→4MiB 收缩：先缩data再扩checkpoint
    plan = changes.preview(env, limits(checkpoint_bytes=4 * MIB))
    operation = str(uuid.uuid4())
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)

    assert result["quota_state"] == "partial_apply"
    assert storage.quota.usage(number)["byte_limit"] == 4 * MIB  # 收缩已生效
    assert storage.quota.usage(checkpoint_number)["byte_limit"] == 4 * MIB + 1024
    failed = result["steps"][1]
    assert failed["zone"] == "checkpoint" and failed["state"] == "failed"
    assert failed["reason"] == "kernel_limit_mismatch"
    assert failed["actual"]["byte_limit"] == 4 * MIB + 1024  # 内核已写但核实不一致
    assert result["zones"]["data"]["status"] == "applied"
    assert result["zones"]["checkpoint"]["status"] == "failed"
    assert result["gate"]["new_writes_allowed"] is False  # 维护门不放写
    assert result["applied"]["checkpoint"]["byte_limit"] == 4 * MIB + 1024
    assert len(result["steps"]) == 2  # 失败后未继续扩容

    with pytest.raises(ServiceError, match="invalid_input"):
        changes.cancel(env, operation)
    storage.quota.hijack = set()
    resumed = changes.status(env, operation)
    assert resumed["quota_state"] == "applied"
    assert storage.quota.usage(checkpoint_number)["byte_limit"] == 4 * MIB
    assert "quota_operation" not in storage._environment(env)


def test_lost_reply_is_reconciled_from_kernel_without_rewrite(storage, changes):
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
    storage.quota.fail_after_apply = {number}
    plan = changes.preview(env, limits(environment_bytes=16 * MIB))
    operation = str(uuid.uuid4())
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)

    assert result["quota_state"] == "reconcile_required"
    assert storage.quota.usage(number)["byte_limit"] == 14 * MIB  # 内核已生效
    writes = len(storage.quota.commands)

    resumed = changes.status(env, operation)
    assert resumed["quota_state"] == "applied"
    assert len(storage.quota.commands) == writes  # 按实际值核实，没有重写
    assert resumed["steps"][0]["state"] == "verified"
    assert "quota_operation" not in storage._environment(env)


def test_repeated_confirm_returns_status_without_double_release(storage, changes):
    env = str(uuid.uuid4())
    number = prepare_split(storage, env, environment_bytes=16 * MIB)
    plan = changes.preview(env, limits(environment_bytes=8 * MIB))
    operation = str(uuid.uuid4())
    first = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    assert first["quota_state"] == "applied"
    assert (
        storage._read("accounts.json")["allocations"][env]["limits"]["environment_bytes"] == 8 * MIB
    )
    writes = len(storage.quota.commands)

    second = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    third = changes.status(env, operation)
    assert second["quota_state"] == third["quota_state"] == "applied"
    assert len(storage.quota.commands) == writes  # 不重复执行
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["environment_bytes"] == 8 * MIB  # 预算只释放一次
    assert storage.quota.usage(number)["byte_limit"] == 6 * MIB


def test_unknown_plan_and_parameter_conflicts_are_rejected(storage, changes):
    env = str(uuid.uuid4())
    prepare(storage, env)
    with pytest.raises(ServiceError, match="not_found"):
        changes.confirm(env, str(uuid.uuid4()), "0" * 64, str(uuid.uuid4()))
    plan = changes.preview(env, limits(environment_bytes=16 * MIB))
    with pytest.raises(ServiceError, match="key_conflict"):
        changes.confirm(env, plan["plan_id"], "f" * 64, str(uuid.uuid4()))
    with pytest.raises(ServiceError, match="invalid_input"):
        changes.confirm(env, plan["plan_id"], "g" * 64, str(uuid.uuid4()))
    operation = str(uuid.uuid4())
    changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    other = changes.preview(env, limits(environment_bytes=16 * MIB, checkpoint_bytes=4 * MIB))
    with pytest.raises(ServiceError, match="key_conflict"):
        changes.confirm(env, other["plan_id"], other["plan_digest"], operation)


def test_capacity_shortage_refuses_growth_without_records(storage, changes):
    env = str(uuid.uuid4())
    prepare(storage, env)
    storage._save(
        "accounts.json",
        {
            "allocations": {
                "other": {"project_number": 999999, "limits": {"environment_bytes": 10**18}}
            }
        },
    )
    plan = changes.preview(env, limits(environment_bytes=16 * MIB))
    assert plan["node"]["target_fits"] is False
    assert plan["applicable_now"] is False
    operation = str(uuid.uuid4())
    with pytest.raises(ServiceError, match="sandbox_limit"):
        changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    assert storage._read("quota-change-" + operation + ".json") is None
    assert "quota_operation" not in storage._environment(env)


def test_unsplit_normalizes_to_split_then_disable_keeps_old_zone(storage, changes):
    env = str(uuid.uuid4())
    number = prepare(storage, env)
    checkpoint_number = number + 0x40000000
    plan = changes.preview(env, limits())
    operation = str(uuid.uuid4())
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)

    assert result["quota_state"] == "applied"  # 空检查点区在此维护中初始化
    assert storage.quota.usage(number)["byte_limit"] == 6 * MIB  # 先缩data
    assert storage.quota.usage(checkpoint_number)["inode_limit"] == 1000
    assert storage._environment(env)["checkpoint_number"] == checkpoint_number
    lock = storage.environments / env / "checkpoints" / env / "maintenance.lock"
    assert lock.exists()  # 维护锁文件在ShadowCheckpoints同一布局下

    disabled = changes.preview(env, limits(checkpoint_bytes=0, checkpoint_count=0))
    assert disabled["target"]["checkpoint_enabled"] is False
    assert disabled["reclaim"]["required"] is True
    operation = str(uuid.uuid4())
    result = changes.confirm(env, disabled["plan_id"], disabled["plan_digest"], operation)
    assert result["quota_state"] == "pending_reclaim"
    assert result["blocked_reason"] == "checkpoint_disable_requires_reclaim"
    assert result["zones"]["checkpoint"]["status"] == "kept_applied"
    assert storage.quota.usage(checkpoint_number)["byte_limit"] == 2 * MIB  # 旧区保留
    assert checkpoint_number in storage.quota.projects  # 不解除登记、不清文件
    assert result["gate"]["new_writes_allowed"] is False
    assert changes.cancel(env, operation)["quota_state"] == "cancelled"
    assert storage.quota.usage(checkpoint_number)["byte_limit"] == 2 * MIB


def test_cancel_only_waiting_idle_and_clears_gate(storage, changes):
    env = str(uuid.uuid4())
    prepare(storage, env)
    project = str(uuid.uuid4())
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
    plan = changes.preview(env, limits(environment_bytes=16 * MIB, checkpoint_bytes=0))
    operation = str(uuid.uuid4())
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    assert result["quota_state"] == "waiting_idle"

    cancelled = changes.cancel(env, operation)
    assert cancelled["quota_state"] == "cancelled"
    record = storage._environment(env)
    assert "quota_operation" not in record and "quota_state" not in record
    number = storage.quota.project_number(env)
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB

    with pytest.raises(ServiceError, match="invalid_input"):
        changes.cancel(env, operation)
    storage.dispatch(
        "release",
        {"environment_id": env, "project_id": project, "lease_id": lease, "clean": True},
    )
    result = changes.status(env, operation)
    assert result["quota_state"] == "cancelled"  # 已撤销的操作不再推进
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB


def test_paused_recovery_lease_allows_quota_change_without_release(storage, changes):
    env = str(uuid.uuid4())
    number = prepare(storage, env)
    project = str(uuid.uuid4())
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    lease = blocked_lease(storage, project)
    storage.recovery = FakeRecovery(active=set())
    operation = str(uuid.uuid4())
    journal = {
        "environment_id": env,
        "project_id": project,
        "status": "paused",
        "operation_id": operation,
    }
    plan = changes.preview(env, limits(environment_bytes=16 * MIB, checkpoint_bytes=0))

    # 无journal的blocked租约：无法核实，保持waiting
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    assert result["quota_state"] == "waiting_idle"
    assert result["waiting_on"]["leases"] == [project]

    # journal归属不符或状态未暂停：保持waiting
    storage._save("restore-" + lease + ".json", {**journal, "project_id": str(uuid.uuid4())})
    assert changes.status(env, operation)["quota_state"] == "waiting_idle"
    storage._save("restore-" + lease + ".json", {**journal, "status": "applying"})
    assert changes.status(env, operation)["quota_state"] == "waiting_idle"

    # 暂停恢复且不在active：放行调额，但不释放租约与恢复引用
    storage._save("restore-" + lease + ".json", journal)
    result = changes.status(env, operation)
    assert result["quota_state"] == "applied"
    assert storage.quota.usage(number)["byte_limit"] == 16 * MIB
    assert storage.leases.status(project)["status"] == "blocked"  # 租约原样保留
    assert storage._read("restore-" + lease + ".json")["status"] == "paused"

    # 恢复重新活跃（operation出现在active）：后续调额回到waiting
    storage.recovery = FakeRecovery(active={operation})
    shrink = changes.preview(env, limits())
    storage.quota.commands.clear()
    result = changes.confirm(env, shrink["plan_id"], shrink["plan_digest"], str(uuid.uuid4()))
    assert result["quota_state"] == "waiting_idle"
    assert result["waiting_on"]["recovery"] == [operation]
    assert storage.quota.commands == []  # 活跃恢复期间不改内核


def test_no_kernel_step_policy_change_and_same_limits_can_complete(storage, changes):
    env = str(uuid.uuid4())
    prepare_split(storage, env)
    target = limits(checkpoint_retention_seconds=86400)
    plan = changes.preview(env, target)
    storage.quota.commands.clear()
    response = changes.confirm(env, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert response["quota_state"] == "applied"
    assert storage.quota.commands == []
    assert storage._environment(env)["limits"]["checkpoint_retention_seconds"] == 86400
    assert storage.quota.account_usage(storage._environment(env))["byte_limit"] == 8 * MIB


def test_mixed_byte_and_inode_reallocation_never_temporarily_exceeds_total(
    storage, changes, monkeypatch
):
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
    checkpoint = storage._environment(env)["checkpoint_number"]
    original = storage.quota.command
    totals = []

    def measured(expression):
        result = original(expression)
        if expression.startswith("limit "):
            data, other = storage.quota.usage(number), storage.quota.usage(checkpoint)
            totals.append(
                (
                    data["byte_limit"] + other["byte_limit"],
                    data["inode_limit"] + other["inode_limit"],
                )
            )
        return result

    monkeypatch.setattr(storage.quota, "command", measured)
    # 数据区字节扩、inode缩；检查点区字节缩、inode扩，不能两个都标shrink一次写完。
    plan = changes.preview(env, limits(checkpoint_bytes=MIB, checkpoint_count=50))
    response = changes.confirm(env, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert response["quota_state"] == "applied"
    assert totals and all(b <= 8 * MIB and i <= 4000 for b, i in totals)


def test_disable_waiting_for_history_cleanup_can_be_withdrawn_before_kernel_changes(
    storage, changes
):
    env = str(uuid.uuid4())
    prepare_split(storage, env)
    original = storage._environment(env)["limits"]
    plan = changes.preview(env, limits(checkpoint_bytes=0, checkpoint_count=0))
    operation = str(uuid.uuid4())
    pending = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    assert pending["quota_state"] == "pending_reclaim"
    assert storage._environment(env)["limits"] == original
    response = changes.cancel(env, operation)
    assert response["quota_state"] == "cancelled"
    assert not storage._environment(env).get("quota_operation")
    assert storage._environment(env)["limits"] == original
