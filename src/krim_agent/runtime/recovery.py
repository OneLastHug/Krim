"""停靠恢复点（ADR-0015 §3）：park_checkpoints 表读回 + 事件流核验。

通俗说明：运行停靠等待人工审批时，进程内不挂任何等待协程——
"接下来从哪儿继续"所需的全部状态（对话快照、下一步步号、已耗
预算、待批调用清单）都写在停靠恢复点里。本模块把恢复点读回为
RecoveryState 供引擎续跑；内容与指纹不符时如实失败，不猜着续跑。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.contracts import Message, ServiceError, fingerprint
from krim_agent.tools.core import Invocation

CHECKPOINT_SCHEMA = 1


@dataclass(frozen=True)
class PendingAsk:
    """停靠批里一个已答复的待批调用：调用快照 + 人的批复。"""

    invocation: Invocation
    allow: bool


@dataclass(frozen=True)
class RecoveryState:
    """一次停靠运行的恢复点（由 park_checkpoints 读回）。"""

    conversation: tuple[Message, ...]  # 停靠时刻的完整对话（含本步 assistant 与已执行工具消息）
    step: int  # 下一个要执行的步号
    output_bytes: int  # 已产生的输出字节数（输出预算消耗不可恢复）
    tool_count: int  # 已声明的调用总数
    seen: tuple[str, ...]  # 已出现过的模型侧调用 ID（防重复）
    budget_remaining: float | None  # 执行预算剩余（秒；None 表示无预算约束）
    pending: tuple[PendingAsk, ...]  # 已答复的待批调用（声明序）


def build_checkpoint(
    *,
    conversation: list[Message],
    step: int,
    output_bytes: int,
    tool_count: int,
    seen: set[str],
    budget_remaining: float | None,
    pending: list[Invocation],
) -> dict[str, JsonValue]:
    """引擎在停靠时构造恢复点内容；全部为可规范编码的 JSON 值。"""

    def invocation_fields(invocation: Invocation) -> dict[str, JsonValue]:
        return {
            "call_id": invocation.call_id,
            "span_id": invocation.span_id,
            "step_index": invocation.step_index,
            "name": invocation.name,
            "arguments_json": invocation.arguments_json,
            "tool_hash": invocation.tool_hash,
            "provider_call_id": invocation.provider_call_id,
        }

    return {
        "schema_version": CHECKPOINT_SCHEMA,
        "conversation": [message.model_dump(mode="json") for message in conversation],
        "step": step,
        "output_bytes": output_bytes,
        "tool_count": tool_count,
        "seen": cast("list[JsonValue]", sorted(seen)),
        "budget_remaining": budget_remaining,
        "pending": [invocation_fields(invocation) for invocation in pending],
    }


def load_checkpoint(
    run_id: str,
    checkpoint: dict[str, Any],
    stored_fingerprint: str,
    replies: dict[str, bool],
) -> RecoveryState:
    """读回恢复点并核验完整性；待批复从事件流答复映射注入。

    replies：引擎侧调用 ID（call_id）→ 是否批准。恢复点自身不含批复
    （批复是事件流事实），批复缺失视为事实损坏，不猜着续跑。
    """
    if fingerprint(checkpoint) != stored_fingerprint:
        raise ServiceError("facts_corrupt")  # 恢复点被改动：拒绝续跑
    if checkpoint.get("schema_version") != CHECKPOINT_SCHEMA:
        raise ServiceError("facts_corrupt")
    try:
        conversation = tuple(
            Message.model_validate(item) for item in checkpoint["conversation"]
        )
        step = int(checkpoint["step"])
        output_bytes = int(checkpoint["output_bytes"])
        tool_count = int(checkpoint["tool_count"])
        seen = tuple(str(item) for item in checkpoint["seen"])
        budget_raw = checkpoint["budget_remaining"]
        pending_items = list(checkpoint["pending"])
    except (KeyError, TypeError, ValueError):
        raise ServiceError("facts_corrupt") from None
    pending: list[PendingAsk] = []
    for item in pending_items:
        if not isinstance(item, dict):
            raise ServiceError("facts_corrupt")
        call_id = str(item.get("call_id"))
        if call_id not in replies:
            raise ServiceError("facts_corrupt")  # 无批复的待批项不得恢复执行
        provider_call_id = item.get("provider_call_id")
        invocation = Invocation(
            run_id,
            call_id,
            str(item["span_id"]),
            int(item["step_index"]),
            str(item["name"]),
            str(item["arguments_json"]),
            str(item["tool_hash"]),
            provider_call_id=str(provider_call_id)
            if provider_call_id is not None
            else None,
        )
        pending.append(PendingAsk(invocation, replies[call_id]))
    if not pending:
        raise ServiceError("facts_corrupt")  # 没有待批项却请求恢复
    budget = None if budget_raw is None else float(budget_raw)
    return RecoveryState(
        conversation, step, output_bytes, tool_count, seen, budget, tuple(pending)
    )
