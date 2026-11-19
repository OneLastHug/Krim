"""账号沙箱配额：额度模型、命名规格与用量评估；纯数据定义，不读文件、不连数据库。

额度语义来自 docs/proposals/account-sandbox-plan.md，并按 2026-11 无配额产品策略调整：
- resource_policy 是显式资源策略，默认 unlimited：不再对存储字节/inode 设置应用或
  内核限额（XFS limit 0=unlimited），也不保留任何容量天花板；数值字段保留旧语义
  作账面值与能力开关输入，limited 保留旧的有限额协议供显式选择与旧协议测试。
- unlimited 的评估只保留能力禁用规则：checkpoint bytes/count 为 0 或 frozen 未配置
  时任何用量按超额报告；正数预算表示启用但无容量上限，不做资源额度比较。
- limited 下保留旧校验：检查点预算计入环境总量（至多一半）、五项逐项比较；它约束
  的是有限额数值向量本身，与 unlimited 无关。
- 冻结任务预算独立于环境总量；frozen_bytes=None 表示未配置，即不启用冻结能力。
- 额度调低到当前用量以下时不删除数据：evaluate 只报告超额项，由调用方放行读取
  与清理，仅拒绝需要新增额度的写入。unlimited 跳过存储资源额度检查，但保留
  检查点与冻结的能力禁用规则。
- 旧记录面（env/操作记录里没有 resource_policy 字段的 limits 字典）按 limited
  解读：落盘时内核确实应用了有限额，缺字段不重解释为 unlimited。
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from krim_agent.contracts import StrictModel

# 默认值可被规格与账号覆盖；下列硬边界对所有层级一致生效，配置无法突破。
_ENVIRONMENT_BYTES_MIN = 1024**2
_ENVIRONMENT_BYTES_MAX = 1024**4
_ENVIRONMENT_INODES_MIN = 1000
_ENVIRONMENT_INODES_MAX = 10_000_000
_CHECKPOINT_BYTES_MAX = 512 * 1024**3
_CHECKPOINT_COUNT_MAX = 1000
_CHECKPOINT_RETENTION_MAX = 366 * 86400
_FROZEN_BYTES_MAX = 1024**4
# 用量是实测值，允许超过任何额度；仅按 JSON 安全整数封顶，与 contracts.check_json 对齐。
_USAGE_MAX = 9007199254740991

EnvironmentBytes = Annotated[int, Field(ge=_ENVIRONMENT_BYTES_MIN, le=_ENVIRONMENT_BYTES_MAX)]
EnvironmentInodes = Annotated[int, Field(ge=_ENVIRONMENT_INODES_MIN, le=_ENVIRONMENT_INODES_MAX)]
CheckpointBytes = Annotated[int, Field(ge=0, le=_CHECKPOINT_BYTES_MAX)]
CheckpointCount = Annotated[int, Field(ge=0, le=_CHECKPOINT_COUNT_MAX)]
CheckpointRetention = Annotated[int, Field(ge=0, le=_CHECKPOINT_RETENTION_MAX)]
FrozenBudget = Annotated[int, Field(ge=1, le=_FROZEN_BYTES_MAX)]
UsageCount = Annotated[int, Field(ge=0, le=_USAGE_MAX)]
# 规格名用白名单：字母数字开头，只含字母数字/下划线/连字符，天然排除路径分隔与控制字符。
ProfileName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")]

OverageField = Literal[
    "environment_bytes",
    "environment_inodes",
    "checkpoint_bytes",
    "checkpoint_count",
    "frozen_bytes",
]

ResourcePolicy = Literal["unlimited", "limited"]


class QuotaOverage(StrictModel):
    """单项超额事实：used > limit。只用于报告，配额调低不会触发删除。"""

    field: OverageField
    used: UsageCount
    limit: UsageCount


class QuotaUsage(StrictModel):
    """当前实测用量；环境用量包含检查点占用，冻结用量独立计量，允许超过额度。"""

    environment_bytes: UsageCount = 0
    environment_inodes: UsageCount = 0
    checkpoint_bytes: UsageCount = 0
    checkpoint_count: UsageCount = 0
    frozen_bytes: UsageCount = 0


class QuotaLimits(StrictModel):
    """合并规格与账号覆盖后的最终配置额度；内核是否已应用由执行后端报告。

    resource_policy 默认 unlimited：存储字节/inode 不设内核限额（XFS 0=unlimited），
    数值字段保留旧取值范围与默认值，仅作账面记录与检查点能力预算的输入；
    limited 走旧的有限额协议。旧序列化记录没有该字段，经 limits_from_record
    按 limited 解读，与当时内核已应用的限额一致。
    """

    resource_policy: ResourcePolicy = "unlimited"
    environment_bytes: EnvironmentBytes = 5 * 1024**3
    environment_inodes: EnvironmentInodes = 100_000
    checkpoint_bytes: CheckpointBytes = 500 * 1024**2
    checkpoint_count: CheckpointCount = 20
    checkpoint_retention_seconds: CheckpointRetention = 7 * 86400
    frozen_bytes: FrozenBudget | None = None

    @model_validator(mode="after")
    def reserve_for_data_and_maintenance(self) -> Self:
        """limited 下检查点预算计入环境总量，至多占一半，其余留给数据与维护。

        该比例是有限额协议的内部约束；unlimited 没有容量天花板，正数检查点预算
        表示启用但不再受环境总量比例约束，校验跳过。
        """
        if (
            self.resource_policy == "limited"
            and self.checkpoint_bytes * 2 > self.environment_bytes
        ):
            raise ValueError("检查点预算超过环境总量的一半，未给数据与维护预留空间")
        return self

    def merge(self, overrides: QuotaOverrides) -> QuotaLimits:
        """用覆盖中显式给出的字段替换当前值；显式 frozen_bytes=None 表示切换为禁用冻结。"""
        merged = self.model_dump()
        merged.update(overrides.model_dump(exclude_unset=True))
        return QuotaLimits.model_validate(merged)

    def evaluate(self, usage: QuotaUsage) -> tuple[QuotaOverage, ...]:
        """逐项比较用量与额度并返回全部超额项；正好用满不算超额，不做任何删除。

        unlimited 只保留能力禁用规则：checkpoint bytes/count 为 0 或 frozen 未配置
        时任何用量都按超额报告（limit 0），由调用方拒绝对应写入；正数预算表示启用
        但无容量上限，不产生超额项。limited 保持旧的五项有限额协议。
        """
        if self.resource_policy == "unlimited":
            rows: tuple[tuple[OverageField, int, int | None], ...] = ()
            if self.checkpoint_bytes == 0:
                rows += (("checkpoint_bytes", usage.checkpoint_bytes, 0),)
            if self.checkpoint_count == 0:
                rows += (("checkpoint_count", usage.checkpoint_count, 0),)
            if self.frozen_bytes is None:
                # 冻结预算独立检查，不并入环境用量；未配置时任何冻结用量都按超额报告。
                rows += (("frozen_bytes", usage.frozen_bytes, 0),)
        else:
            rows = (
                ("environment_bytes", usage.environment_bytes, self.environment_bytes),
                ("environment_inodes", usage.environment_inodes, self.environment_inodes),
                ("checkpoint_bytes", usage.checkpoint_bytes, self.checkpoint_bytes),
                ("checkpoint_count", usage.checkpoint_count, self.checkpoint_count),
                # 冻结预算独立检查，不并入环境用量；未配置预算时任何冻结用量都按超额报告。
                ("frozen_bytes", usage.frozen_bytes, self.frozen_bytes),
            )
        overages: list[QuotaOverage] = []
        for name, used, limit in rows:
            ceiling = 0 if limit is None else limit
            if used > ceiling:
                overages.append(QuotaOverage(field=name, used=used, limit=ceiling))
        return tuple(overages)


def limits_from_record(values: dict[str, Any]) -> QuotaLimits:
    """记录面（env/操作记录里的limits字典）解读：缺 resource_policy 的旧记录按 limited。

    旧记录落盘时内核确实应用了有限额，缺字段重解释为 unlimited 会造成配置面与
    内核实际不一致；迁移到 unlimited 必须经显式的授权调额操作完成。新记录由
    resolve/model_dump 生成，总是显式携带策略字段。
    """
    if not isinstance(values, dict) or "resource_policy" not in values:
        return QuotaLimits.model_validate({**values, "resource_policy": "limited"})
    return QuotaLimits.model_validate(values)


class QuotaOverrides(StrictModel):
    """同字段的可选覆盖；只有显式给出的字段参与合并，未知字段一律拒绝。

    frozen_bytes 是唯一允许显式 None 的字段，None 表达明确关闭冻结能力；
    未给出则保持当前层级取值。其余字段显式传 None 视为配置错误，拒绝而非静默忽略。
    """

    resource_policy: ResourcePolicy | None = None
    environment_bytes: EnvironmentBytes | None = None
    environment_inodes: EnvironmentInodes | None = None
    checkpoint_bytes: CheckpointBytes | None = None
    checkpoint_count: CheckpointCount | None = None
    checkpoint_retention_seconds: CheckpointRetention | None = None
    frozen_bytes: FrozenBudget | None = None

    @model_validator(mode="after")
    def none_only_for_frozen(self) -> Self:
        for name in type(self).model_fields:
            if name == "frozen_bytes":
                continue
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} 不接受显式 None；只有 frozen_bytes 可用 None 关闭冻结")
        return self


class QuotaPolicy(StrictModel):
    """全局默认额度与命名规格模板；resolve 按 默认→规格→单账号覆盖 逐层合并。"""

    defaults: QuotaLimits = Field(default_factory=QuotaLimits)
    profiles: dict[ProfileName, QuotaOverrides] = Field(default_factory=dict, max_length=64)

    def resolve(
        self,
        profile_name: str | None = None,
        overrides: QuotaOverrides | dict[str, int | None] | None = None,
    ) -> QuotaLimits:
        """解析最终额度；未知规格拒绝，合并结果整体校验，输入对象保持不变。"""
        if profile_name is None:
            resolved = self.defaults
        else:
            profile = self.profiles.get(profile_name)
            if profile is None:
                raise ValueError(f"未知配额规格：{profile_name!r}")
            resolved = self.defaults.merge(profile)
        if overrides is not None:
            if not isinstance(overrides, QuotaOverrides):
                overrides = QuotaOverrides.model_validate(overrides)
            resolved = resolved.merge(overrides)
        return resolved
