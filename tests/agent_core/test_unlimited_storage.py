"""unlimited存储策略：显式资源策略、内核限额清零迁移与能力禁用语义保留。

有限额协议（先缩后扩、pending_reclaim、退役keep模式）由既有quota测试以显式
limited覆盖；本文件验证：
- QuotaLimits默认resource_policy=unlimited；旧记录面（缺该字段）按limited解读，
  已审批的有限预算不会被静默变成无限；
- unlimited评估只保留禁用规则：checkpoint 0/count 0与frozen None报任何用量，
  正数预算表示启用但无容量上限，不产生超额项；
- unlimited不保留容量比例校验（limited保留），实例数量没有准入上限；
- provision与checkpoint分区在unlimited下写0/0（XFS 0=unlimited）并读回核实；
- 已存有限额的账号经授权调额操作（preview/confirm）安全清零账号与检查点两个
  project ID并读回校验，等待租约空闲，内核未核实前不假称已应用。
"""

import os
import time
import uuid

import pytest
from pydantic import ValidationError
from test_quota_retirement_adapter import (
    TARGET as LIMITED_RETIRE_TARGET,
)
from test_quota_retirement_adapter import (
    capture_checkpoint,
    clear_history,
    retirement,
)
from test_quota_retirement_adapter import (
    limits as adapter_limits,
)
from test_quota_retirement_adapter import (
    prepare_split as xfs_prepare_split,
)
from test_sandbox_quota_management import FakeQuota, prepare_split

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import broker as module
from krim_agent.sandbox.broker import StorageBroker
from krim_agent.sandbox.quota import (
    QuotaLimits,
    QuotaOverage,
    QuotaOverrides,
    QuotaPolicy,
    QuotaUsage,
    limits_from_record,
)
from krim_agent.sandbox.quota_management import RESERVATION_CEILING, QuotaChanges

MIB = 1024**2


def _overage(field, used, limit):
    return QuotaOverage(field=field, used=used, limit=limit)


def unlimited_limits(**overrides) -> dict:
    values = {
        "resource_policy": "unlimited",
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
def xfs_storage(tmp_path, monkeypatch):
    from test_quota_retirement_adapter import FakeXfs

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


def prepare_unlimited(storage, environment_id, **overrides):
    storage.dispatch(
        "provision_environment",
        {"environment_id": environment_id, "limits": unlimited_limits(**overrides)},
    )
    storage.quota.commands.clear()
    return storage.quota.project_number(environment_id)


# ---------- 模型：显式策略与旧记录解读 ----------


def test_default_policy_is_unlimited_and_explicit_limited_is_accepted():
    assert QuotaLimits().resource_policy == "unlimited"
    assert QuotaLimits(resource_policy="limited").resource_policy == "limited"
    with pytest.raises(ValidationError):
        QuotaLimits(resource_policy="open")


def test_legacy_record_without_policy_reads_as_limited():
    """缺resource_policy的旧记录按limited解读：内核当时确实是有限额，不静默变无限。"""
    record = {
        "environment_bytes": 8 * MIB,
        "environment_inodes": 4000,
        "checkpoint_bytes": 2 * MIB,
        "checkpoint_count": 20,
        "checkpoint_retention_seconds": 7 * 86400,
        "frozen_bytes": None,
    }
    parsed = limits_from_record(record)
    assert parsed.resource_policy == "limited"
    assert parsed.environment_bytes == 8 * MIB
    # 显式携带策略的记录按字段值解读；直接model_validate是模型默认（unlimited），
    # 记录面必须经limits_from_record。
    assert limits_from_record({**record, "resource_policy": "unlimited"}).resource_policy == (
        "unlimited"
    )
    assert QuotaLimits.model_validate(record).resource_policy == "unlimited"


def test_evaluate_unlimited_only_disable_rules_no_capacity_caps():
    """unlimited只保留禁用规则：正数预算（count 20/大字节）超限不报超额；
    checkpoint bytes/count为0与frozen None仍对任何用量报超额。"""
    unlimited = QuotaLimits()
    assert unlimited.resource_policy == "unlimited"
    assert unlimited.checkpoint_count == 20  # 默认正数预算：启用但无容量上限
    assert unlimited.evaluate(QuotaUsage(environment_bytes=9007199254740991)) == ()
    assert unlimited.evaluate(QuotaUsage(environment_inodes=9007199254740991)) == ()
    assert unlimited.evaluate(QuotaUsage(checkpoint_bytes=4 * 1024**3)) == ()
    assert unlimited.evaluate(QuotaUsage(checkpoint_count=21)) == ()
    assert unlimited.evaluate(QuotaUsage(checkpoint_count=21, checkpoint_bytes=4 * 1024**3)) == ()

    disabled = QuotaLimits(checkpoint_bytes=0, checkpoint_count=0)
    assert disabled.evaluate(QuotaUsage(checkpoint_bytes=1)) == (
        _overage("checkpoint_bytes", 1, 0),
    )
    assert disabled.evaluate(QuotaUsage(checkpoint_count=1)) == (
        _overage("checkpoint_count", 1, 0),
    )
    assert disabled.evaluate(QuotaUsage(frozen_bytes=2)) == (
        _overage("frozen_bytes", 2, 0),
    )
    # 禁用与启用维度彼此独立：bytes禁用、count启用（正数）时只报bytes
    mixed = QuotaLimits(checkpoint_bytes=0)  # checkpoint_count默认20：启用无上限
    assert mixed.evaluate(QuotaUsage(checkpoint_bytes=1, checkpoint_count=999)) == (
        _overage("checkpoint_bytes", 1, 0),
    )


def test_capacity_ratio_validation_applies_only_to_limited():
    """unlimited没有容量天花板：检查点预算超过环境总量一半也合法；limited保留旧校验。"""
    enabled_big = QuotaLimits(
        environment_bytes=1 * 1024**3, checkpoint_bytes=600 * MIB, checkpoint_count=20
    )
    assert enabled_big.resource_policy == "unlimited"
    assert enabled_big.checkpoint_bytes == 600 * MIB
    with pytest.raises(ValidationError):
        QuotaLimits(
            resource_policy="limited",
            environment_bytes=1 * 1024**3,
            checkpoint_bytes=600 * MIB,
            checkpoint_count=20,
        )
    edge = QuotaLimits(
        resource_policy="limited", environment_bytes=1 * 1024**3, checkpoint_bytes=512 * MIB
    )
    assert edge.checkpoint_bytes == 512 * MIB


def test_evaluate_limited_keeps_full_storage_protocol():
    limits = QuotaLimits(
        resource_policy="limited",
        environment_bytes=1 * MIB,
        environment_inodes=1000,
        checkpoint_bytes=0,
        checkpoint_count=0,
    )
    usage = QuotaUsage(environment_bytes=2 * MIB, environment_inodes=2000)
    assert limits.evaluate(usage) == (
        _overage("environment_bytes", 2 * MIB, 1 * MIB),
        _overage("environment_inodes", 2000, 1000),
    )


def test_policy_merge_can_pin_policy_per_profile_and_account():
    policy = QuotaPolicy(
        defaults=QuotaLimits(),
        profiles={"boxed": QuotaOverrides(resource_policy="limited")},
    )
    assert policy.resolve().resource_policy == "unlimited"
    assert policy.resolve("boxed").resource_policy == "limited"
    assert policy.resolve("boxed", {"resource_policy": "unlimited"}).resource_policy == (
        "unlimited"
    )
    with pytest.raises(ValidationError):
        QuotaOverrides(resource_policy=None)
    with pytest.raises(ValidationError):
        QuotaOverrides(resource_policy="open")


# ---------- Broker：provision与分区 ----------


def test_provision_unlimited_writes_unlimited_kernel_limits(storage):
    env = str(uuid.uuid4())
    result = storage.dispatch(
        "provision_environment", {"environment_id": env, "limits": unlimited_limits()}
    )
    assert result["status"] == "ready"
    number = storage.quota.project_number(env)
    row = storage.quota.usage(number)
    assert row["byte_limit"] == 0 and row["inode_limit"] == 0  # XFS 0=unlimited
    record = storage._environment(env)
    assert record["limits"]["resource_policy"] == "unlimited"
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["resource_policy"] == "unlimited"


def test_unlimited_target_ignores_reservation_but_limited_does_not(storage):
    env = str(uuid.uuid4())
    prepare_unlimited(storage, env)
    changes = QuotaChanges(storage)
    info = os.statvfs(storage.storage)
    ceiling = int(info.f_blocks * info.f_frsize * RESERVATION_CEILING)
    storage._save(
        "accounts.json",
        {
            "allocations": {
                "other": {
                    "project_number": 999999,
                    "limits": {"environment_bytes": ceiling - 4 * MIB},
                }
            }
        },
    )
    limited_plan = changes.preview(env, {**unlimited_limits(), "resource_policy": "limited"})
    unlimited_plan = changes.preview(env, unlimited_limits())
    assert limited_plan["node"]["target_fits"] is False
    assert unlimited_plan["node"]["target_fits"] is True
    assert unlimited_plan["node"]["target_reserved_bytes"] == ceiling - 4 * MIB


def test_provision_does_not_claim_unlimited_while_kernel_still_finite(storage):
    """旧账号未迁移时provision如实拒绝：预约账本与内核读回都必须先到位。"""
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
    fresh = unlimited_limits()
    with pytest.raises(ServiceError, match="revision_conflict"):
        storage.provision_environment(env, fresh)
    registry = storage._read("accounts.json")
    registry["allocations"][env] = {
        "project_number": number,
        "limits": fresh,
    }
    storage._save("accounts.json", registry)
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        storage.provision_environment(env, fresh)
    # 内核未被静默清零：不假称已应用
    assert storage.quota.usage(number)["byte_limit"] == 6 * MIB


def test_provision_has_no_instance_count_admission_cap(storage):
    """实例数量没有准入上限：登记簿已有超过128条记录仍可继续登记新实例。"""
    storage._save(
        "accounts.json",
        {
            "allocations": {
                f"legacy-{index:03d}": {
                    "project_number": 500_000 + index,
                    "limits": unlimited_limits(),
                }
                for index in range(130)
            }
        },
    )
    env = str(uuid.uuid4())
    result = storage.dispatch(
        "provision_environment", {"environment_id": env, "limits": unlimited_limits()}
    )
    assert result["status"] == "ready"
    assert len(storage._read("accounts.json")["allocations"]) == 131


def test_state_records_have_no_size_cap_roundtrip_over_64kib(storage):
    """登记簿超过64KiB：原子落盘后分块读回完整一致；同记录可再次覆盖保存。"""
    big = {
        "allocations": {
            f"bulk-{index:05d}": {
                "project_number": 600_000 + index,
                "limits": {
                    "resource_policy": "unlimited",
                    "environment_bytes": 8 * MIB,
                    "environment_inodes": 4000,
                },
            }
            for index in range(800)
        }
    }
    storage._save("accounts.json", big)
    assert storage._read("accounts.json") == big
    shrunk = {"allocations": dict(list(big["allocations"].items())[:10])}
    storage._save("accounts.json", shrunk)
    assert storage._read("accounts.json") == shrunk


def test_state_dir_has_no_file_count_cap(storage):
    """状态目录超过4096个记录文件仍可正常保存与读取（轻量元数据，不逐个走_save）。"""
    for index in range(4100):
        fd = os.open(
            f"bulk-{index:05d}.json",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=storage.fd,
        )
        try:
            os.write(fd, b'{"n":0}')
        finally:
            os.close(fd)
    storage._save("marker.json", {"ok": True})
    assert storage._read("marker.json") == {"ok": True}
    assert storage._read("bulk-00000.json") == {"n": 0}


def test_checkpoint_partition_under_unlimited_registers_zone_without_kernel_limits(storage):
    env = str(uuid.uuid4())
    number = prepare_unlimited(storage, env)
    checkpoint_number = number + 0x40000000
    partition = storage.quota.checkpoint_partition(env, QuotaLimits(**unlimited_limits()))
    assert partition == {
        "checkpoint_number": checkpoint_number,
        "checkpoint_bytes": 0,
        "data_bytes": 0,
    }
    zone = storage.quota.usage(checkpoint_number)
    assert zone["byte_limit"] == 0 and zone["inode_limit"] == 0
    assert storage.quota.usage(number)["byte_limit"] == 0  # 数据区未被改写
    assert not any(
        command.endswith(f" {number}") and command.startswith("limit -p")
        for command in storage.quota.commands
    )
    with pytest.raises(ServiceError, match="sandbox_limit"):
        storage.quota.checkpoint_partition(
            env, QuotaLimits(**unlimited_limits(checkpoint_bytes=0, checkpoint_count=0))
        )


# ---------- 调额协议：unlimited清零迁移 ----------


def test_unlimited_migration_clears_both_zone_limits_with_readback(storage):
    """已存有限额账号：账号与检查点两个project ID都清零并读回校验。"""
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
    checkpoint_number = number + 0x40000000
    changes = QuotaChanges(storage)
    plan = changes.preview(env, unlimited_limits())

    assert plan["target"]["limits"]["resource_policy"] == "unlimited"
    assert plan["target"]["normalized"] == {
        "environment_bytes": 8 * MIB,
        "environment_inodes": 4000,
        "data_bytes": 0,
        "data_inodes": 0,
        "checkpoint_bytes": 0,
        "checkpoint_inodes": 0,
    }
    assert plan["target"]["zone_actions"] == {"data": "grow", "checkpoint": "grow"}
    assert plan["target"]["checkpoint_enabled"] is True
    assert plan["reclaim"]["required"] is False

    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert result["quota_state"] == "applied"
    data = storage.quota.usage(number)
    zone = storage.quota.usage(checkpoint_number)
    assert data["byte_limit"] == 0 and data["inode_limit"] == 0
    assert zone["byte_limit"] == 0 and zone["inode_limit"] == 0
    assert any(
        command.startswith("limit -p") and command.endswith(f" {number}")
        and "bhard=0k ihard=0" in command
        for command in storage.quota.commands
    )
    assert any(
        command.startswith("limit -p") and command.endswith(f" {checkpoint_number}")
        and "bhard=0k ihard=0" in command
        for command in storage.quota.commands
    )
    record = storage._environment(env)
    assert record["limits"]["resource_policy"] == "unlimited"
    assert record["data_bytes"] == 0 and record["checkpoint_bytes"] == 0
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["resource_policy"] == "unlimited"
    assert result["gate"]["new_writes_allowed"] is True
    assert result["zones"]["data"]["status"] == "applied"
    assert result["zones"]["checkpoint"]["status"] == "applied"


def test_unlimited_migration_waits_for_lease_then_clears(storage):
    """迁移不中断租约：租约活跃时waiting_idle零内核写入，释放后同操作继续。"""
    env = str(uuid.uuid4())
    number = prepare_split(storage, env)
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
    changes = QuotaChanges(storage)
    plan = changes.preview(env, unlimited_limits())
    operation = str(uuid.uuid4())
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    assert result["quota_state"] == "waiting_idle"
    assert result["waiting_on"]["leases"] == [project]
    assert storage.quota.commands == []  # 等待期间零内核写入

    storage.dispatch(
        "release", {"environment_id": env, "project_id": project, "lease_id": lease, "clean": True}
    )
    result = changes.status(env, operation)
    assert result["quota_state"] == "applied"
    assert storage.quota.usage(number)["byte_limit"] == 0


def test_unlimited_target_with_no_changes_applies_without_kernel_writes(storage):
    env = str(uuid.uuid4())
    prepare_unlimited(storage, env)
    changes = QuotaChanges(storage)
    plan = changes.preview(env, unlimited_limits(checkpoint_bytes=0, checkpoint_count=0))
    assert plan["target"]["zone_actions"] == {"data": "none", "checkpoint": "not_provisioned"}
    storage.quota.commands.clear()
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert result["quota_state"] == "applied"
    assert storage.quota.commands == []


def test_unlimited_enable_initializes_checkpoint_zone_without_kernel_limits(storage):
    env = str(uuid.uuid4())
    number = prepare_unlimited(storage, env)
    checkpoint_number = number + 0x40000000
    changes = QuotaChanges(storage)
    plan = changes.preview(env, unlimited_limits())
    assert plan["target"]["zone_actions"] == {"data": "none", "checkpoint": "initialize"}
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert result["quota_state"] == "applied"
    assert storage.quota.usage(checkpoint_number)["byte_limit"] == 0
    assert storage.quota.usage(checkpoint_number)["inode_limit"] == 0
    assert storage.quota.usage(number)["byte_limit"] == 0
    record = storage._environment(env)
    assert record["checkpoint_number"] == checkpoint_number
    assert record["checkpoint_bytes"] == 0 and record["data_bytes"] == 0
    assert any(command.startswith("project -s -p") for command in storage.quota.commands)


def test_unlimited_checkpoint_disable_still_requires_reclaim(storage):
    """unlimited不越过能力禁用：C0目标仍停在pending_reclaim等待显式清理。"""
    env = str(uuid.uuid4())
    prepare_split(storage, env)
    changes = QuotaChanges(storage)
    plan = changes.preview(env, unlimited_limits(checkpoint_bytes=0, checkpoint_count=0))
    assert plan["reclaim"]["required"] is True
    assert "checkpoint_disable_requires_reclaim" in plan["reclaim"]["reasons"]
    operation = str(uuid.uuid4())
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    assert result["quota_state"] == "pending_reclaim"
    assert result["blocked_reason"] == "checkpoint_disable_requires_reclaim"
    assert result["zones"]["checkpoint"]["status"] == "kept_applied"
    assert storage.quota.commands == []  # 未清历史前内核不动
    assert changes.cancel(env, operation)["quota_state"] == "cancelled"


def test_legacy_plan_without_policy_field_keeps_limited_semantics(storage):
    """旧格式plan字典（无resource_policy）确认仍按有限额执行，不被新默认改写。"""
    env = str(uuid.uuid4())
    prepare_split(storage, env)
    legacy = unlimited_limits()
    legacy.pop("resource_policy")
    changes = QuotaChanges(storage)
    plan = changes.preview(env, legacy)
    assert plan["target"]["limits"]["resource_policy"] == "limited"
    assert plan["target"]["normalized"]["data_bytes"] == 6 * MIB
    assert plan["target"]["zone_actions"] == {"data": "none", "checkpoint": "none"}
    storage.quota.commands.clear()
    result = changes.confirm(env, plan["plan_id"], plan["plan_digest"], str(uuid.uuid4()))
    assert result["quota_state"] == "applied"
    assert storage.quota.commands == []  # 数值未变：无内核写入，语义与旧协议一致
    assert storage.quota.usage(storage.quota.project_number(env))["byte_limit"] == 6 * MIB


# ---------- 退役：unlimited目标强制清0旧检查点限额 ----------


def test_retirement_under_unlimited_clears_zone_limits_after_quarantine(xfs_storage):
    """unlimited退役：隔离核实后强制清0旧区限额，data写0/0，读回校验后完成。"""
    storage = xfs_storage
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = xfs_prepare_split(storage, env)
    checkpoint_number = number + 0x40000000
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    service = retirement(storage)
    target = {**LIMITED_RETIRE_TARGET, "resource_policy": "unlimited"}

    plan = service.preview(env, target)
    assert plan["kernel_zero_requested"] is True  # unlimited目标强制清0
    assert plan["applicable_now"] is False  # 有受保护历史
    operation = str(uuid.uuid4())
    view = service.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    assert view["retirement_state"] == "pending_reclaim"
    assert storage.quota.usage(checkpoint_number)["byte_limit"] == 2 * MIB  # 未清历史前保留

    clear_history(storage, env, project, checkpoint_id)
    view = service.status(env, operation)
    assert view["retirement_state"] == "retired"
    data = storage.quota.usage(number)
    zone = storage.quota.usage(checkpoint_number)
    assert data["byte_limit"] == 0 and data["inode_limit"] == 0
    assert zone["byte_limit"] == 0 and zone["inode_limit"] == 0
    record = storage._environment(env)
    assert record["limits"]["resource_policy"] == "unlimited"
    assert record["limits"]["checkpoint_bytes"] == 0
    allocation = storage._read("accounts.json")["allocations"][env]
    assert allocation["limits"]["resource_policy"] == "unlimited"
    # data区由data_grow写0/0（bhard=0k），旧检查点区由release的kernel_zero清0（bhard=0）
    assert any(
        command == f"limit -p bhard=0k ihard=0 {number}" for command in storage.quota.commands
    )
    assert any(
        command == f"limit -p bhard=0 ihard=0 {checkpoint_number}"
        for command in storage.quota.commands
    )


def test_retirement_under_limited_keeps_zone_limits_by_default(xfs_storage):
    """有限额退役协议保持keep语义：adapter测试全覆盖，这里只核对照入口不串策略。"""
    storage = xfs_storage
    env = str(uuid.uuid4())
    project = str(uuid.uuid4())
    number = xfs_prepare_split(storage, env)
    checkpoint_number = number + 0x40000000
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    checkpoint_id = capture_checkpoint(storage, env, project)
    service = retirement(storage)
    plan = service.preview(env, dict(LIMITED_RETIRE_TARGET))
    assert plan["kernel_zero_requested"] is False
    operation = str(uuid.uuid4())
    service.confirm(env, plan["plan_id"], plan["plan_digest"], operation)
    clear_history(storage, env, project, checkpoint_id)
    view = service.status(env, operation)
    assert view["retirement_state"] == "retired"
    assert storage.quota.usage(number)["byte_limit"] == 8 * MIB  # 有限额目标写有限额
    assert storage.quota.usage(checkpoint_number)["byte_limit"] == 2 * MIB  # keep模式不清0
    assert adapter_limits()["resource_policy"] == "limited"  # 旧协议测试显式limited
