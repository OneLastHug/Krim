"""Provider 报告用量与持久请求覆盖；参考 SRC-OSS-016，差异见 ADR-0041。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Literal

from pydantic import ConfigDict, Field, model_validator

from .protocol.domain import ServiceError
from .protocol.models import StreamFrame, StrictModel

SafeCount = Annotated[int, Field(ge=0, le=9_007_199_254_740_991)]
UsageField = Literal[
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_input_tokens",
    "reasoning_output_tokens",
]
USAGE_FIELDS: tuple[UsageField, ...] = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_input_tokens",
    "reasoning_output_tokens",
)


class TokenUsage(StrictModel):
    """一次请求的供应商报告；None 表示该字段未报告，不推算缺失值。"""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    source: Literal["provider"] = "provider"
    input_tokens: SafeCount | None = None
    output_tokens: SafeCount | None = None
    total_tokens: SafeCount | None = None
    cached_input_tokens: SafeCount | None = None
    reasoning_output_tokens: SafeCount | None = None

    @model_validator(mode="after")
    def validate_counts(self) -> TokenUsage:
        if all(getattr(self, name) is None for name in USAGE_FIELDS):
            raise ValueError("用量必须至少报告一个计数")
        if self.total_tokens is not None:
            known = [
                value for value in (self.input_tokens, self.output_tokens) if value is not None
            ]
            if self.total_tokens < sum(known):
                raise ValueError("总量小于已报告输入/输出")
            if len(known) == 2 and self.total_tokens != sum(known):
                raise ValueError("总量与输入/输出不一致")
            if any(
                value is not None and value > self.total_tokens
                for value in (self.cached_input_tokens, self.reasoning_output_tokens)
            ):
                raise ValueError("细分计数超过总量")
        if self.cached_input_tokens is not None and self.input_tokens is not None:
            if self.cached_input_tokens > self.input_tokens:
                raise ValueError("缓存输入超过输入总量")
        if self.reasoning_output_tokens is not None and self.output_tokens is not None:
            if self.reasoning_output_tokens > self.output_tokens:
                raise ValueError("推理输出超过输出总量")
        return self

    def follows(self, previous: TokenUsage) -> bool:
        """请求内累计快照可补字段或增长，不能降低或丢失已有计数。"""
        for name in USAGE_FIELDS:
            old, new = getattr(previous, name), getattr(self, name)
            if old is not None and (new is None or new < old):
                return False
        return True


class ModelStepUsage(StrictModel):
    step: int = Field(ge=0, le=31)
    provider: str = Field(min_length=1, max_length=256)
    completed: bool = False
    usage: TokenUsage | None = None
    final: bool = False


class UsageTotals(StrictModel):
    """完整字段总量；已报告部分的小计独立登记，避免混作完整账单。"""

    source: Literal["provider"] = "provider"
    input_tokens: SafeCount | None = None
    output_tokens: SafeCount | None = None
    total_tokens: SafeCount | None = None
    cached_input_tokens: SafeCount | None = None
    reasoning_output_tokens: SafeCount | None = None
    reported: dict[UsageField, SafeCount] = Field(default_factory=dict)
    attempted_steps: int = Field(ge=0, le=32)
    reported_steps: int = Field(ge=0, le=32)
    confirmed_steps: int = Field(ge=0, le=32)
    usage_complete: bool = False


class RunUsageView(StrictModel):
    run_id: str
    trace_available: bool = False
    steps: list[ModelStepUsage] = Field(default_factory=list, max_length=32)
    token_usage: UsageTotals | None = None
    usage_complete: bool = False


def aggregate(steps: Sequence[ModelStepUsage]) -> UsageTotals | None:
    reported_steps = [item for item in steps if item.usage is not None]
    if not reported_steps:
        return None
    confirmed = [item for item in reported_steps if item.completed and item.final]
    values: dict[str, object] = {}
    subtotals: dict[UsageField, int] = {}
    for name in USAGE_FIELDS:
        found = [getattr(item.usage, name) for item in reported_steps]
        known = [value for value in found if value is not None]
        if known:
            subtotals[name] = sum(known)
        values[name] = (
            sum(known) if len(confirmed) == len(steps) and len(known) == len(steps) else None
        )
    complete = len(confirmed) == len(steps) and all(
        values[name] is not None for name in ("input_tokens", "output_tokens", "total_tokens")
    )
    return UsageTotals.model_validate(
        {
            **values,
            "reported": subtotals,
            "attempted_steps": len(steps),
            "reported_steps": len(reported_steps),
            "confirmed_steps": len(confirmed),
            "usage_complete": complete,
        }
    )


def usage_from_events(run_id: str, events: Sequence[StreamFrame]) -> RunUsageView:
    steps: dict[int, ModelStepUsage] = {}
    ended: set[int] = set()
    trace_available = False
    try:
        for event in events:
            if event.stream_id != run_id:
                raise ValueError("run")
            data = event.data if isinstance(event.data, dict) else {}
            if (
                event.kind == "start"
                and type(data.get("model_usage_trace")) is int
                and data.get("model_usage_trace") == 1
            ):
                trace_available = True
            if event.kind != "chunk":
                continue
            kind = data.get("type")
            if kind not in {"model_step", "token_usage"}:
                continue
            step = data.get("step")
            if type(step) is not int or not 0 <= step <= 31:
                raise ValueError("step")
            if kind == "model_step" and data.get("phase") == "start":
                if step in steps or step != len(steps) or (step > 0 and step - 1 not in ended):
                    raise ValueError("start")
                provider = data.get("provider")
                if not isinstance(provider, str):
                    raise ValueError("provider")
                steps[step] = ModelStepUsage(step=step, provider=provider)
                trace_available = True
                continue
            if step not in steps or step in ended:
                raise ValueError("missing start or already ended")
            current = steps[step]
            if kind == "model_step":
                if data.get("phase") != "end" or type(data.get("completed")) is not bool:
                    raise ValueError("end")
                if data.get("provider") is not None and data.get("provider") != current.provider:
                    raise ValueError("provider mismatch")
                current.completed = bool(data["completed"])
                ended.add(step)
            else:
                usage = TokenUsage.model_validate(data.get("usage"))
                final = data.get("final")
                if type(final) is not bool or (
                    current.usage is not None and not usage.follows(current.usage)
                ):
                    raise ValueError("report")
                if current.final and (usage != current.usage or not final):
                    raise ValueError("final")
                current.usage, current.final = usage, final
        ordered = list(steps.values())
        totals = aggregate(ordered)
        return RunUsageView(
            run_id=run_id,
            trace_available=trace_available,
            steps=ordered,
            token_usage=totals,
            usage_complete=bool(totals and totals.usage_complete),
        )
    except (TypeError, ValueError, OverflowError) as error:
        raise ServiceError("usage_invalid_event", "持久用量事件无效", 409) from error
