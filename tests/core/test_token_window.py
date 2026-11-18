"""1M窗口/900k触发边界的合成counter覆盖（计划9.4：边界先用合成counter/适配器）。"""

from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import ValidationError

from krim_agent.context import builder
from krim_agent.context.compaction import Compactor
from krim_agent.context.counter import (
    COMPACTION_TRIGGER_TOKENS,
    CONTEXT_WINDOW_TOKENS,
    TOKEN_WINDOW_VERSION,
    HeuristicTokenCounter,
    UsageCalibratedCounter,
)
from krim_agent.context.serialization import encoded, fingerprint
from krim_agent.context.types import CompactionPolicy, ContextPolicy, upgrade_to_token_window
from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.types import ModelText
from krim_agent.runtime.model_step import ModelStep

# 夹具显式声明的宿主传输容量；非产品默认，仅用于测试字节路径。
DECLARED_REQUEST_LIMIT = 8_388_608


class FixedCounter:
    """合成计数器：count_request返回固定值；版本可区分。"""

    version = "synthetic-fixed/1"
    exact = True

    def __init__(self, value: int) -> None:
        self.value = value
        self.calls = 0

    def count_text(self, text: str) -> int:
        return self.value

    def count_request(self, messages: Any, tools: Any) -> int:
        self.calls += 1
        return self.value


class DummyProvider:
    async def stream(self, messages, tools) -> AsyncIterator[ModelText]:
        yield ModelText(text="unused")


class ReadyStore:
    async def work_read(self, read):
        return await read(None)


def _compactor(policy: ContextPolicy, counter: Any, *, with_snapshot_state: bool) -> Compactor:
    """装配measure/snapshot/prepare所需状态；不做存储、不执行摘要。"""
    compactor = Compactor.__new__(Compactor)
    compactor.policy = policy
    compactor.compaction = CompactionPolicy()
    compactor.max_group = 4
    compactor.remaining_tools = None
    compactor.request_limit = DECLARED_REQUEST_LIMIT
    compactor.step = ModelStep(
        DummyProvider(),
        lambda kind, data: None,
        lambda: None,
        max_steps=8,
        max_output=None,
        deadline=None,
        workspace=True,
    )
    compactor.max_group = 4
    compactor.remaining_tools = None
    compactor._counter = counter
    compactor.counts_tokens = policy.counts_tokens()
    if with_snapshot_state:
        compactor.system = "system"
        compactor.history = []
        compactor.anchor_refs = ()
        compactor.source_anchor = fingerprint([])
        compactor.summary, compactor.covered, compactor.passes = "", 0, 0
        compactor.compaction_changes = []
        compactor.projections = []
        compactor.tools = []
        compactor.extension_snapshot = {"version": "extensions/2", "revision": 0, "items": []}
        compactor.live_memory = False
        compactor.store = ReadyStore()
        compactor.conversation = sample_messages()[1:]
        compactor.guard = lambda: None

        async def verify(_conn):
            return None

        compactor.verify = verify
    return compactor


def sample_messages() -> list[Message]:
    return [Message(role="system", content="system"), Message(role="user", content="current")]


def test_fixed_window_is_one_million_and_trigger_is_nine_hundred_thousand():
    policy = ContextPolicy()
    assert policy.max_tokens == 1_000_000
    assert policy.compaction_trigger_tokens == 900_000
    assert policy.counter_version == TOKEN_WINDOW_VERSION
    # 默认不设应用层字节封顶：不引入任何新的默认cap，字节预算是显式可选项。
    assert policy.max_bytes is None


def test_admission_rejects_input_beyond_window_minus_reserved_output():
    policy = ContextPolicy(reserved_output_tokens=100_000)
    counter = FixedCounter(policy.max_tokens - policy.reserved_output_tokens)
    builder.check_budget(sample_messages(), [], policy, DECLARED_REQUEST_LIMIT, counter)
    counter = FixedCounter(policy.max_tokens - policy.reserved_output_tokens + 1)
    with pytest.raises(ServiceError, match="context_budget"):
        builder.check_budget(sample_messages(), [], policy, DECLARED_REQUEST_LIMIT, counter)


def test_declared_byte_limits_still_bind_when_configured():
    # 显式字节预算（策略或传输声明）仍然执行；默认None不比较。
    policy = ContextPolicy(max_bytes=4096, reserved_output_tokens=32)
    messages = [
        Message(role="system", content="s"),
        Message(role="user", content="x" * 4000),
    ]
    with pytest.raises(ServiceError, match="context_budget"):
        builder.check_budget(messages, [], policy, DECLARED_REQUEST_LIMIT, FixedCounter(10))
    undeclared = ContextPolicy()  # max_bytes默认None：同样负载不被字节预算拒绝
    builder.check_budget(messages, [], undeclared, DECLARED_REQUEST_LIMIT, FixedCounter(10))


def test_legacy_frozen_policy_keeps_byte_semantics_and_ignores_token_window():
    policy = ContextPolicy(
        max_bytes=4096,
        reserved_output_tokens=32,
        counter_version="approx-utf8/1",
    )
    # 旧代不受token窗口判定影响：巨大合成计数不改变旧策略的字节预算结论。
    huge = FixedCounter(CONTEXT_WINDOW_TOKENS * 4)
    builder.check_budget(sample_messages(), [], policy, DECLARED_REQUEST_LIMIT, huge)
    with pytest.raises(ServiceError, match="context_budget"):
        builder.check_budget(sample_messages(), [], policy, 512, huge)


def test_trigger_fires_exactly_at_900_000_synthetic_tokens():
    policy = ContextPolicy(reserved_output_tokens=1_000)
    under = _compactor(policy, FixedCounter(COMPACTION_TRIGGER_TOKENS - 1),
                       with_snapshot_state=False)
    assert under.measure(sample_messages(), []).triggered is False
    at = _compactor(policy, FixedCounter(COMPACTION_TRIGGER_TOKENS), with_snapshot_state=False)
    assert at.measure(sample_messages(), []).triggered is True
    over = _compactor(policy, FixedCounter(COMPACTION_TRIGGER_TOKENS + 50_000),
                      with_snapshot_state=False)
    assert over.measure(sample_messages(), []).triggered is True


def test_legacy_generation_still_triggers_by_byte_ratio_not_synthetic_tokens():
    policy = ContextPolicy(
        max_bytes=8192,
        reserved_output_tokens=32,
        counter_version="approx-utf8/1",
    )
    compactor = _compactor(policy, FixedCounter(COMPACTION_TRIGGER_TOKENS),
                           with_snapshot_state=False)
    measurement = compactor.measure(sample_messages(), [])
    assert measurement.triggered is False  # 字节远小于 8192*0.8；合成token不参与判定


def test_input_over_window_minus_reserved_cannot_be_admitted_even_by_snapshot():
    policy = ContextPolicy(max_tokens=50_000, compaction_trigger_tokens=48_000,
                           reserved_output_tokens=2_000)
    counter = FixedCounter(policy.max_tokens)  # 输入占满整个窗口
    compactor = _compactor(policy, counter, with_snapshot_state=True)
    with pytest.raises(ServiceError, match="context_budget"):
        compactor.snapshot("", 0)


def test_trigger_plus_reserved_must_stay_inside_window():
    with pytest.raises(ValidationError):
        ContextPolicy(
            max_tokens=100_000,
            compaction_trigger_tokens=100_000,
            reserved_output_tokens=1,
        )


def test_snapshot_records_real_counter_tokens_and_generation():
    policy = ContextPolicy(reserved_output_tokens=64)
    counter = FixedCounter(12_345)
    compactor = _compactor(policy, counter, with_snapshot_state=True)
    compactor.conversation = sample_messages()[1:]
    prepared = compactor.snapshot("", 0)
    assert prepared.estimated_tokens == 12_345
    assert prepared.counter_version == TOKEN_WINDOW_VERSION
    assert counter.calls >= 1


def test_prepared_context_of_legacy_generation_keeps_byte_share_tokens():
    policy = ContextPolicy(
        max_bytes=8192, reserved_output_tokens=32, counter_version="approx-utf8/1"
    )
    counter = FixedCounter(999_999)
    compactor = _compactor(policy, counter, with_snapshot_state=True)
    compactor.conversation = sample_messages()[1:]
    prepared = compactor.snapshot("", 0)
    assert prepared.counter_version == "approx-utf8/1"
    assert prepared.estimated_tokens == (prepared.input_bytes + 3) // 4


async def test_prepare_admission_uses_counter_before_sending_request():
    policy = ContextPolicy(max_tokens=50_000, compaction_trigger_tokens=48_000,
                           reserved_output_tokens=2_000)
    counter = FixedCounter(policy.max_tokens)
    compactor = _compactor(policy, counter, with_snapshot_state=True)
    with pytest.raises(ServiceError, match="context_budget"):
        await compactor.prepare(
            "run", [Message(role="user", content="current")], [],
            max_group=2, remaining_tools=None,
        )


def test_heuristic_counter_declares_precision_and_counts_by_segments():
    counter = HeuristicTokenCounter()
    assert counter.exact is False
    assert counter.count_text("") == 0
    # 40个汉字 → 约40 token（每字符约1）；120个ASCII字符 → 约38 token（约3.2字符/token）。
    assert counter.count_text("汉字测试" * 10) == 40
    assert 30 <= counter.count_text("hello world " * 10) <= 45
    # emoji按星面字符从宽计入。
    assert counter.count_text("🤖" * 5) >= 5


def test_usage_calibration_converges_toward_reported_prompt_tokens():
    base = HeuristicTokenCounter()
    calibrated = UsageCalibratedCounter(base)
    text = "hello world " * 10
    assert calibrated.count_text(text) == base.count_text(text)  # 未校正前透传
    for _ in range(40):
        calibrated.observe(base.count_text(text), 20)
    assert abs(calibrated.count_text(text) - 20) <= 2
    assert calibrated.version.startswith("calibrated/")
    # 持续异常usage把比率推向夹取上界4.0：38*4=152（基础估计38）。
    for _ in range(40):
        calibrated.observe(38, 10_000)
    assert calibrated.count_text(text) == 152
    calibrated.observe(0, 0)  # 非正样本忽略
    assert calibrated.count_text(text) == 152


def test_upgrade_to_token_window_moves_old_policies_to_fixed_window():
    legacy = ContextPolicy.model_validate_json(
        encoded(
            {
                "max_bytes": 131072,
                "max_messages": 32,
                "reserved_output_tokens": 512,
                "counter_version": "approx-utf8/1",
            }
        )
    )
    upgraded = upgrade_to_token_window(legacy)
    assert upgraded.counter_version == TOKEN_WINDOW_VERSION
    assert upgraded.max_tokens == 1_000_000
    assert upgraded.compaction_trigger_tokens == 900_000
    # 旧演示限制随升级解除：条数上限与字节封顶清空，输出预留回到当前默认。
    assert upgraded.max_messages is None
    assert upgraded.max_bytes is None
    assert upgraded.reserved_output_tokens == 2048
    assert upgrade_to_token_window(ContextPolicy()) == ContextPolicy()
