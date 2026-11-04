"""同Run共用模型步：协议、预算、流finally；不拥有Provider或producer。"""
from __future__ import annotations

import asyncio
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pydantic import JsonValue, ValidationError

from krim_agent.contracts import Message, RunEvent, ServiceError
from krim_agent.providers.types import (
    ModelProvider,
    ModelRequestBudget,
    ModelText,
    ModelToolCall,
    ModelUsage,
)
from krim_agent.runtime.contracts import ExecutionBudget, settled
from krim_agent.source_fingerprints import source_digest
from krim_agent.usage import TokenUsage

_BUDGET_PROMPT = (
    "Krim request budget/1: Remaining model steps including this request: {steps}; "
    "remaining tool calls for this run: {tools}; tool calls allowed in this request: {step_tools}. "
    "Stay within these limits. If no tools are allowed, answer without requesting tools."
)


@dataclass(frozen=True)
class StepResult:
    text: str
    calls: tuple[ModelToolCall, ...]
    index: int


class ModelStep:
    def __init__(self, provider: ModelProvider,
                 emit: Callable[[str, dict[str, JsonValue]], Awaitable[RunEvent]],
                 check: Callable[[], None], *, max_steps: int, max_output: int,
                 deadline: float | None, workspace: bool,
                 execution_budget: ExecutionBudget | None = None) -> None:
        self.provider, self.emit, self.check = provider, emit, check
        self.max_steps, self.max_output = max_steps, max_output
        self.deadline, self.workspace = deadline, workspace
        self.execution_budget = execution_budget
        self.index = self.output_bytes = 0
        self.seen: set[str] = set()

    def request_budget(
        self,
        max_group: int,
        remaining_tools: int,
        *,
        purpose: str = "task",
    ) -> ModelRequestBudget:
        remaining_steps = self.max_steps - self.index
        if remaining_steps <= 0:
            raise ServiceError("step_limit")
        provider_cap = getattr(getattr(self.provider, "config", None), "max_tool_calls", None)
        limits = [max_group, remaining_tools]
        if type(provider_cap) is int:
            limits.append(provider_cap)
        max_this_step = 0 if purpose == "summary" else min(limits)
        return ModelRequestBudget(
            remaining_steps=remaining_steps,
            remaining_tool_calls=remaining_tools,
            max_tool_calls_this_step=max_this_step,
        )

    @staticmethod
    def with_budget_prompt(
        messages: list[Message], budget: ModelRequestBudget
    ) -> list[Message]:
        result = list(messages)
        prompt = _BUDGET_PROMPT.format(
            steps=budget.remaining_steps,
            tools=budget.remaining_tool_calls,
            step_tools=budget.max_tool_calls_this_step,
        )
        if result and result[0].role == "system":
            system = result[0]
            result[0] = system.model_copy(
                update={"content": system.content + "\n\n" + prompt}
            )
        else:
            result.insert(0, Message(role="system", content=prompt))
        return result

    async def call(self, messages: list[Message], tools: list[dict[str, JsonValue]], *,
                   purpose: str = "task", max_group: int = 1,
                   remaining_tools: int = 1, summary_bytes: int = 2048,
                   request_budget: ModelRequestBudget | None = None,
                   request_prepared: bool = False) -> StepResult:
        self.check()
        budget = request_budget or self.request_budget(
            max_group, remaining_tools, purpose=purpose
        )
        if self.index >= self.max_steps:
            raise ServiceError("step_limit")
        index, self.index = self.index, self.index+1
        clock = asyncio.get_running_loop().time
        run_deadline = (self.execution_budget.deadline if self.execution_budget is not None
                        else self.deadline)
        end = min(run_deadline, clock()+20) if run_deadline is not None else None
        span = secrets.token_hex(8)
        extra: dict[str, JsonValue] = {"purpose": purpose} if self.workspace else {}
        text, calls = "", []
        usage: TokenUsage | None = None
        final_usage = False
        request_messages = (
            messages if request_prepared else self.with_budget_prompt(messages, budget)
        )
        request_tools = tools if budget.max_tool_calls_this_step > 0 else []
        async with asyncio.timeout_at(end):
            await self.emit("model_start", {"span_id": span, "step_index": index, **extra})
            self.check()
            stream_with_budget = getattr(self.provider, "stream_with_budget", None)
            if callable(stream_with_budget):
                stream = stream_with_budget(request_messages, request_tools, budget)
            else:
                stream = self.provider.stream(request_messages, request_tools)
            try:
                async for delta in stream:
                    self.check()
                    if final_usage and not isinstance(delta, ModelUsage):
                        raise ServiceError("provider_event")
                    try:
                        if type(delta) is ModelText:
                            item = ModelText.model_validate(delta.model_dump(mode="json"))
                            self.output_bytes += len(item.text.encode())
                            if self.output_bytes > self.max_output:
                                raise ServiceError("output_limit")
                            text += item.text
                            if purpose == "summary" and len(text.encode()) > summary_bytes:
                                raise ServiceError("summary_failed")
                            await self.emit("summary_text" if purpose == "summary" else "text",
                                {"text": item.text, "step_index": index, **extra})
                        elif type(delta) is ModelToolCall:
                            call = ModelToolCall.model_validate(delta.model_dump(mode="json"))
                            if purpose == "summary":
                                raise ServiceError("summary_protocol")
                            if call.id in self.seen:
                                raise ServiceError("duplicate_tool_call")
                            self.seen.add(call.id)
                            calls.append(call)
                            if len(calls) > budget.max_tool_calls_this_step:
                                raise ServiceError("tool_limit")
                        elif type(delta) is ModelUsage:
                            measured = ModelUsage.model_validate(delta.model_dump(mode="json"))
                            if (usage is not None and not measured.usage.follows(usage)
                                    or final_usage and (not measured.final
                        or measured.usage != usage)):
                                raise ServiceError("provider_event")
                            usage, final_usage = measured.usage, measured.final
                            await self.emit("usage", {"step_index": index,
                                "usage": usage.model_dump(mode="json"), "final": final_usage,
                                **extra})
                        else:
                            raise ServiceError("provider_event")
                    except (ValidationError, ValueError):
                        raise ServiceError("provider_event") from None
                    self.check()
            finally:
                close = getattr(stream, "aclose", None)
                if close is not None:
                    await settled(close())
            self.check()
            if purpose == "summary" and not text.strip():
                raise ServiceError("summary_failed")
            await self.emit("model_end", {"span_id": span, "step_index": index,
                "tool_calls": [call.model_dump(mode="json") for call in calls], **extra})
            self.check()
        return StepResult(text, tuple(calls), index)


def binding_token() -> tuple[object, ...]:
    return (
        id(ModelStep),
        id(ModelStep.call),
        id(ModelStep.__init__),
        id(ModelStep.request_budget),
        id(ModelStep.with_budget_prompt),
        id(ModelRequestBudget),
        id(ModelRequestBudget.__post_init__),
        source_digest(__file__),
    )
