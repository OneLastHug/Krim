"""SDK recovery envelopes; old dispatch checkpoints are never translated.

RunService remains the sole run owner. It claims the run, restores the saved
execution budget and validates target/approval facts before SDK replay. Legacy
schema_version=1 checkpoints and uncertain operations remain stored, but cannot
be automatically migrated into a new dispatcher.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from krim_agent.contracts import ServiceError, fingerprint


@dataclass(frozen=True)
class SDKResume:
    checkpoint: dict[str, Any]
    fingerprint: str
    replies: Mapping[str, bool]


def sdk_resume(checkpoint: dict[str, Any], stored: str, replies: Mapping[str, bool]) -> SDKResume:
    """Accept only complete SDK checkpoints, never convert legacy operations."""
    if (checkpoint.get("format") not in {"sdk-probe-v1", "sdk-host-v2"}
            or fingerprint(checkpoint) != stored):
        raise ServiceError("facts_corrupt")
    return SDKResume(checkpoint, stored, replies)


BOOTSTRAP_CHECKPOINT_FORMAT = "isolated-bootstrap/1"


@dataclass(frozen=True)
class BootstrapResume:
    """隔离引导SDK的可恢复审批信封；不携带也不伪造SDKHostState。

    checkpoint由bootstrap helper在停车前写入（format=isolated-bootstrap/1，
    含run_id/ownership/identity/budget/pending审批）；RunService按既有
    approval_reply匹配、policy重评与取消语义推进；引导完成后bootstrap
    sdk_loop以resume=None重新进入execute_sdk。仅用于child development
    isolated profile；旧SDKResume路径不变。缺失/格式不符一律facts_corrupt，
    不存在无checkpoint的任意审批恢复。
    """

    checkpoint: dict[str, Any]
    fingerprint: str
    replies: Mapping[str, bool]
    ownership: dict[str, Any]
    identity: dict[str, Any]
    budget_remaining: float | None
    pending: tuple[dict[str, Any], ...]


def bootstrap_resume(
    checkpoint: dict[str, Any], stored: str, replies: Mapping[str, bool]
) -> BootstrapResume:
    """校验并解析isolated-bootstrap/1停靠点；未知形状按facts_corrupt拒绝。"""
    if checkpoint.get("format") != BOOTSTRAP_CHECKPOINT_FORMAT or fingerprint(checkpoint) != stored:
        raise ServiceError("facts_corrupt")
    run_id = checkpoint.get("run_id")
    ownership = checkpoint.get("ownership")
    identity = checkpoint.get("identity")
    pending_raw = checkpoint.get("pending_invocations")
    if not isinstance(run_id, str) or not run_id:
        raise ServiceError("facts_corrupt")
    if not isinstance(ownership, dict) or not isinstance(identity, dict):
        raise ServiceError("facts_corrupt")
    if not isinstance(pending_raw, list) or any(
        not isinstance(item, dict) for item in pending_raw
    ):
        raise ServiceError("facts_corrupt")
    budget = checkpoint.get("budget")
    budget_remaining = budget.get("remaining") if isinstance(budget, dict) else None
    if budget_remaining is not None and (
        isinstance(budget_remaining, bool)
        or not isinstance(budget_remaining, (int, float))
        or budget_remaining < 0
    ):
        raise ServiceError("facts_corrupt")
    return BootstrapResume(
        checkpoint=checkpoint,
        fingerprint=stored,
        replies=dict(replies),
        ownership=ownership,
        identity=identity,
        budget_remaining=(
            float(budget_remaining) if budget_remaining is not None else None
        ),
        pending=tuple(pending_raw),
    )


def prepare_resume(
    checkpoint: dict[str, Any], stored: str, replies: Mapping[str, bool]
) -> SDKResume | BootstrapResume:
    """按checkpoint format分发恢复信封；未知格式一律facts_corrupt。"""
    fmt = checkpoint.get("format")
    if fmt in {"sdk-probe-v1", "sdk-host-v2"}:
        return sdk_resume(checkpoint, stored, replies)
    if fmt == BOOTSTRAP_CHECKPOINT_FORMAT:
        return bootstrap_resume(checkpoint, stored, replies)
    raise ServiceError("facts_corrupt")
