"""沙箱配额模型单元测试：默认值、三层合并、严格类型与用量评估；纯模型，无文件与数据库。"""

import pytest
from pydantic import ValidationError

from krim_agent.sandbox.quota import (
    QuotaLimits,
    QuotaOverage,
    QuotaOverrides,
    QuotaPolicy,
    QuotaUsage,
)

MIB = 1024**2
GIB = 1024**3


def test_default_limits_match_plan_baseline():
    """默认额度对应方案基线：环境 5 GiB/10 万 inode，检查点 500 MiB/20 个/7 天，冻结未启用。"""
    limits = QuotaLimits()
    assert limits.environment_bytes == 5 * GIB
    assert limits.environment_inodes == 100_000
    assert limits.checkpoint_bytes == 500 * MIB
    assert limits.checkpoint_count == 20
    assert limits.checkpoint_retention_seconds == 7 * 86400
    assert limits.frozen_bytes is None


def test_resolve_applies_profile_then_account_overrides():
    """合并顺序：全局默认 → 命名规格 → 单账号覆盖；规格未给出的字段保持默认。"""
    policy = QuotaPolicy(
        profiles={
            "small": QuotaOverrides(
                environment_bytes=1 * GIB,
                environment_inodes=10_000,
                checkpoint_bytes=64 * MIB,
            )
        }
    )
    assert policy.resolve() == policy.defaults

    profiled = policy.resolve("small")
    assert profiled.environment_bytes == 1 * GIB
    assert profiled.environment_inodes == 10_000
    assert profiled.checkpoint_bytes == 64 * MIB
    assert profiled.checkpoint_count == 20
    assert profiled.frozen_bytes is None

    per_account = policy.resolve(
        "small", QuotaOverrides(environment_inodes=20_000, frozen_bytes=2 * GIB)
    )
    assert per_account.environment_bytes == 1 * GIB
    assert per_account.environment_inodes == 20_000
    assert per_account.frozen_bytes == 2 * GIB


def test_resolve_accepts_plain_dict_overrides():
    """单账号覆盖可用普通字典；未知字段拒绝，显式 frozen_bytes=None 同样生效。"""
    resolved = QuotaPolicy().resolve(overrides={"checkpoint_count": 5, "frozen_bytes": None})
    assert resolved.checkpoint_count == 5
    assert resolved.frozen_bytes is None
    with pytest.raises(ValidationError):
        QuotaPolicy().resolve(overrides={"unknown_field": 1})


def test_explicit_frozen_none_keeps_disabled_semantics():
    """exclude_unset 保留显式 frozen_bytes=None；模板启用冻结后，账号显式 None 恢复禁用。"""
    assert QuotaOverrides().model_dump(exclude_unset=True) == {}
    assert QuotaOverrides(frozen_bytes=None).model_dump(exclude_unset=True) == {
        "frozen_bytes": None
    }
    assert QuotaOverrides(checkpoint_count=3).model_dump(exclude_unset=True) == {
        "checkpoint_count": 3
    }

    policy = QuotaPolicy(
        defaults=QuotaLimits(frozen_bytes=1 * GIB),
        profiles={"tiny": QuotaOverrides(frozen_bytes=None)},
    )
    assert policy.resolve("tiny").frozen_bytes is None
    assert policy.resolve().frozen_bytes == 1 * GIB


def test_resolve_keeps_inputs_unchanged_and_models_frozen():
    """合并不修改任何输入对象；额度模型冻结，不可就地改写。"""
    policy = QuotaPolicy(
        defaults=QuotaLimits(environment_bytes=2 * GIB),
        profiles={"p": QuotaOverrides(environment_bytes=1 * GIB)},
    )
    overrides = QuotaOverrides(checkpoint_count=7)
    raw = {"checkpoint_count": 3}
    before = policy.model_dump()

    resolved = policy.resolve("p", overrides)
    policy.resolve(overrides=raw)

    assert policy.model_dump() == before
    assert overrides.checkpoint_count == 7
    assert raw == {"checkpoint_count": 3}
    assert resolved.environment_bytes == 1 * GIB
    assert resolved.checkpoint_count == 7
    with pytest.raises(ValidationError):
        resolved.environment_bytes = 1


@pytest.mark.parametrize("value", [True, 5.0, "5"])
def test_limits_reject_non_int_values(value):
    """严格整数：bool、浮点、字符串一律拒绝，不做隐式转换。"""
    with pytest.raises(ValidationError):
        QuotaLimits(environment_bytes=value)
    with pytest.raises(ValidationError):
        QuotaUsage(environment_bytes=value)


@pytest.mark.parametrize("field", ["environment_bytes", "environment_inodes"])
def test_environment_capacity_rejects_zero(field):
    """环境容量与 inode 必须为正；环境存在就要有可用空间。"""
    with pytest.raises(ValidationError):
        QuotaLimits(**{field: 0})


@pytest.mark.parametrize(
    "field, value",
    [
        ("environment_bytes", -1),
        ("environment_inodes", -1),
        ("checkpoint_bytes", -1),
        ("checkpoint_count", -1),
        ("checkpoint_retention_seconds", -1),
        ("frozen_bytes", -1),
    ],
)
def test_limits_reject_negative(field, value):
    with pytest.raises(ValidationError):
        QuotaLimits(**{field: value})


def test_zero_semantics_differ_by_field():
    """0 的含义按字段区分：检查点预算/数量/保留期允许 0（等同关闭）；冻结预算 0 拒绝。"""
    limits = QuotaLimits(checkpoint_bytes=0, checkpoint_count=0, checkpoint_retention_seconds=0)
    assert limits.checkpoint_bytes == 0
    assert limits.checkpoint_count == 0
    assert limits.checkpoint_retention_seconds == 0
    with pytest.raises(ValidationError):
        QuotaLimits(frozen_bytes=0)


@pytest.mark.parametrize(
    "field, value",
    [
        ("environment_bytes", 2 * 1024**4),
        ("environment_inodes", 10_000_001),
        ("checkpoint_bytes", 512 * GIB + 1),
        ("checkpoint_count", 1001),
        ("checkpoint_retention_seconds", 367 * 86400),
        ("frozen_bytes", 2 * 1024**4),
    ],
)
def test_limits_reject_oversize(field, value):
    """超过硬上限的配置拒绝；字段级约束对所有层级一致。"""
    with pytest.raises(ValidationError):
        QuotaLimits(**{field: value})


def test_unknown_fields_rejected():
    """未知字段（含拼写错误）一律拒绝，不被静默忽略。"""
    with pytest.raises(ValidationError):
        QuotaLimits(total_bytes=1)
    with pytest.raises(ValidationError):
        QuotaOverrides(noop=1)
    with pytest.raises(ValidationError):
        QuotaPolicy(default=QuotaLimits())
    with pytest.raises(ValidationError):
        QuotaUsage(storage_bytes=1)


def test_checkpoint_budget_must_leave_room_in_environment():
    """检查点预算计入环境总量：超过一半拒绝，正好一半是允许的边界。"""
    with pytest.raises(ValidationError):
        QuotaLimits(environment_bytes=1 * GIB, checkpoint_bytes=512 * MIB + 1)
    edge = QuotaLimits(environment_bytes=1 * GIB, checkpoint_bytes=512 * MIB)
    assert edge.checkpoint_bytes == 512 * MIB


def test_merged_result_is_validated_as_a_whole():
    """模板与账号覆盖合并后整体校验：单项合法的组合仍可能非法。"""
    policy = QuotaPolicy(
        defaults=QuotaLimits(environment_bytes=1 * GIB),
        profiles={"big": QuotaOverrides(checkpoint_bytes=600 * MIB)},
    )
    with pytest.raises(ValidationError):
        policy.resolve("big")
    with pytest.raises(ValidationError):
        QuotaPolicy().resolve(overrides={"checkpoint_bytes": 3 * GIB})


def test_frozen_budget_is_independent_of_environment():
    """冻结预算独立于环境总量：大于环境总量也合法；检查点预算仍受环境约束。"""
    limits = QuotaLimits(environment_bytes=1 * GIB, checkpoint_bytes=0, frozen_bytes=100 * GIB)
    assert limits.frozen_bytes == 100 * GIB
    with pytest.raises(ValidationError):
        QuotaLimits(environment_bytes=1 * GIB, checkpoint_bytes=600 * MIB)


def test_unknown_profile_rejected():
    policy = QuotaPolicy(profiles={"small": QuotaOverrides()})
    with pytest.raises(ValueError) as info:
        policy.resolve("missing")
    assert "missing" in str(info.value)


@pytest.mark.parametrize(
    "field",
    [
        "environment_bytes",
        "environment_inodes",
        "checkpoint_bytes",
        "checkpoint_count",
        "checkpoint_retention_seconds",
    ],
)
def test_override_rejects_explicit_none_except_frozen(field):
    """除 frozen_bytes 外，覆盖字段显式传 None 视为配置错误。"""
    with pytest.raises(ValidationError):
        QuotaOverrides(**{field: None})
    QuotaOverrides(frozen_bytes=None)


@pytest.mark.parametrize(
    "name",
    ["", "a/b", "..", ".hidden", "a b", "a\nb", "a\x00b", "a" * 65, "-lead", "_lead"],
)
def test_policy_rejects_bad_profile_names(name):
    """规格名白名单：排除空名、路径分隔、点前缀、空白、控制字符、超长与非法首字符。"""
    with pytest.raises(ValidationError):
        QuotaPolicy(profiles={name: QuotaOverrides()})


def test_policy_accepts_reasonable_profile_names_and_caps_count():
    QuotaPolicy(
        profiles={"small": QuotaOverrides(), "dev_2": QuotaOverrides(), "a" * 64: QuotaOverrides()}
    )
    with pytest.raises(ValidationError):
        QuotaPolicy(profiles={f"p{i}": QuotaOverrides() for i in range(65)})


@pytest.mark.parametrize("value", [-1, 1.5, True, "1"])
def test_usage_rejects_non_counts(value):
    """用量是严格非负整数；负数与一切隐式转换拒绝。"""
    with pytest.raises(ValidationError):
        QuotaUsage(environment_bytes=value)


def test_evaluate_reports_only_exceeded_fields_at_exact_boundary():
    """正好用满不算超额；超 1 即超额，报告项按固定顺序给出 used 与 limit。"""
    limits = QuotaLimits(
        environment_bytes=1 * GIB,
        environment_inodes=1000,
        checkpoint_bytes=256 * MIB,
        checkpoint_count=10,
    )
    exact = QuotaUsage(
        environment_bytes=1 * GIB,
        environment_inodes=1000,
        checkpoint_bytes=256 * MIB,
        checkpoint_count=10,
    )
    assert limits.evaluate(exact) == ()

    over = QuotaUsage(environment_bytes=1 * GIB + 1, checkpoint_bytes=256 * MIB + 1)
    assert limits.evaluate(over) == (
        QuotaOverage(field="environment_bytes", used=1 * GIB + 1, limit=1 * GIB),
        QuotaOverage(field="checkpoint_bytes", used=256 * MIB + 1, limit=256 * MIB),
    )


def test_evaluate_keeps_frozen_separate_from_environment():
    """冻结用量不并入环境用量；冻结启用时只按自身预算判断。"""
    enabled = QuotaLimits(environment_bytes=1 * GIB, frozen_bytes=1 * GIB)
    usage = QuotaUsage(environment_bytes=1 * GIB, checkpoint_bytes=256 * MIB, frozen_bytes=1 * GIB)
    # 环境与检查点正好用满环境预算，冻结也正好用满：全部未超额
    assert enabled.evaluate(usage) == ()
    assert enabled.evaluate(QuotaUsage(frozen_bytes=1 * GIB + 1)) == (
        QuotaOverage(field="frozen_bytes", used=1 * GIB + 1, limit=1 * GIB),
    )


def test_evaluate_reports_any_frozen_usage_when_disabled():
    """frozen_bytes=None 表示冻结能力未配置：任何冻结用量都按超额报告，limit 视为 0。"""
    usage = QuotaUsage(environment_bytes=100, frozen_bytes=5)
    assert QuotaLimits().evaluate(usage) == (QuotaOverage(field="frozen_bytes", used=5, limit=0),)


def test_usage_above_lowered_quota_is_representable():
    """额度调低到当前用量以下时，用量对象仍然合法；评估只报告超额，不触发删除。"""
    limits = QuotaLimits(environment_bytes=1 * GIB, checkpoint_count=2)
    usage = QuotaUsage(environment_bytes=4 * GIB, checkpoint_count=9)
    assert limits.evaluate(usage) == (
        QuotaOverage(field="environment_bytes", used=4 * GIB, limit=1 * GIB),
        QuotaOverage(field="checkpoint_count", used=9, limit=2),
    )
