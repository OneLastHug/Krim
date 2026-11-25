"""同Run共用模型步：协议、预算、流finally；不拥有Provider或producer。"""
from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path

from pydantic import JsonValue, ValidationError

from krim_agent.context.counter import UsageCalibratedCounter, counter_for_model
from krim_agent.context.manifest import ItemMetadata, build_context_manifest
from krim_agent.contracts import Message, RunEvent, ServiceError
from krim_agent.providers.types import (
    ModelProgress,
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


@asynccontextmanager
async def _attributed_model_timeout(
    deadline: float | None,
    *,
    model_deadline: float | None,
    run_deadline: float | None,
) -> AsyncIterator[None]:
    timeout = asyncio.timeout_at(deadline)
    try:
        async with timeout:
            yield
    except TimeoutError:
        now = asyncio.get_running_loop().time()
        if (
            timeout.expired()
            and model_deadline is not None
            and (run_deadline is None or model_deadline < run_deadline)
            and (run_deadline is None or now < run_deadline)
        ):
            raise ServiceError("model_timeout") from None
        raise


class ModelStep:
    def __init__(self, provider: ModelProvider,
                 emit: Callable[[str, dict[str, JsonValue]], Awaitable[RunEvent]],
                 check: Callable[[], None], *, max_steps: int | None, max_output: int | None,
                 deadline: float | None, workspace: bool,
                 execution_budget: ExecutionBudget | None = None,
                 model_timeout: float | None = None,
                 request_slot: Callable[[int], AbstractAsyncContextManager[None]] | None = None,
                 stream_check: Callable[[], None] | None = None,
                 ) -> None:
        self.provider, self.emit, self.check = provider, emit, check
        self.stream_check = stream_check if stream_check is not None else check
        self.model_timeout = model_timeout
        self.request_slot = request_slot
        self.max_steps, self.max_output = max_steps, max_output
        self.deadline, self.workspace = deadline, workspace
        self.execution_budget = execution_budget
        self.index = self.output_bytes = 0
        self.seen: set[str] = set()
        model = getattr(getattr(provider, "config", None), "model", None)
        self._raw_counter = counter_for_model(model if isinstance(model, str) else None)
        self.token_counter = UsageCalibratedCounter(self._raw_counter)

    def request_budget(
        self,
        max_group: int | None,
        remaining_tools: int | None,
        *,
        purpose: str = "task",
    ) -> ModelRequestBudget:
        remaining_steps = None if self.max_steps is None else self.max_steps - self.index
        if remaining_steps is not None and remaining_steps <= 0:
            raise ServiceError("step_limit")
        provider_cap = getattr(getattr(self.provider, "config", None), "max_tool_calls", None)
        limits = [value for value in (max_group, remaining_tools) if value is not None]
        if type(provider_cap) is int:
            limits.append(provider_cap)
        # None means this request admits the full returned batch; only configured
        # limits narrow it, and none are folded into a default ceiling.
        max_this_step = 0 if purpose == "summary" else min(limits) if limits else None
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
            steps=(budget.remaining_steps if budget.remaining_steps is not None else "unlimited"),
            tools=(budget.remaining_tool_calls
                   if budget.remaining_tool_calls is not None else "unlimited"),
            step_tools=(budget.max_tool_calls_this_step
                        if budget.max_tool_calls_this_step is not None else "unlimited"),
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
                   purpose: str = "task", max_group: int | None = 1,
                   remaining_tools: int | None = 1, summary_bytes: int | None = None,
                   request_budget: ModelRequestBudget | None = None,
                   request_prepared: bool = False,
                   request_metadata: Sequence[ItemMetadata | None] = (),
                   model_context_window: int | None = None,
                   reserved_output_tokens: int | None = None,
                   input_budget_bytes: int | None = None,
                   compaction_changes: Sequence[dict[str, JsonValue]] = ()) -> StepResult:
        self.check()
        budget = request_budget or self.request_budget(
            max_group, remaining_tools, purpose=purpose
        )
        if self.max_steps is not None and self.index >= self.max_steps:
            raise ServiceError("step_limit")
        index, self.index = self.index, self.index+1
        clock = asyncio.get_running_loop().time
        run_deadline = (self.execution_budget.deadline if self.execution_budget is not None
                        else self.deadline)
        model_deadline = None if self.model_timeout is None else clock() + self.model_timeout
        deadlines = [value for value in (run_deadline, model_deadline) if value is not None]
        end = min(deadlines) if deadlines else None
        span = secrets.token_hex(8)
        extra: dict[str, JsonValue] = {"purpose": purpose}
        text, calls = "", []
        usage: TokenUsage | None = None
        final_usage = False
        request_messages = (
            messages if request_prepared else self.with_budget_prompt(messages, budget)
        )
        request_tools = (
            tools if budget.max_tool_calls_this_step is None
            or budget.max_tool_calls_this_step > 0 else []
        )
        if not request_prepared and (not messages or messages[0].role != "system"):
            request_metadata = (None, *request_metadata)
        raw_input_tokens = self._raw_counter.count_request(request_messages, request_tools)
        context_manifest = build_context_manifest(
            request_messages,
            request_tools,
            message_metadata=request_metadata,
            model_context_window=model_context_window,
            reserved_output_tokens=reserved_output_tokens,
            input_budget_bytes=input_budget_bytes,
            counter=self.token_counter,
        )
        if compaction_changes:
            context_manifest["compaction_changes"] = list(compaction_changes)
        started_at = clock()
        first_content_at: float | None = None
        first_text_at: float | None = None
        last_activity_at: float | None = None
        last_activity_phase: str | None = None
        last_activity_emit_at = started_at
        activity_sequence = 0
        argument_bytes = 0
        async with _attributed_model_timeout(
            end, model_deadline=model_deadline, run_deadline=run_deadline,
        ), (self.request_slot(index) if self.request_slot is not None else nullcontext()):
            await self.emit("model_start", {"span_id": span, "step_index": index,
                "context_manifest": context_manifest, **extra})
            self.check()
            stream_with_diagnostics = getattr(self.provider, "stream_with_diagnostics", None)
            stream_with_budget = getattr(self.provider, "stream_with_budget", None)
            if callable(stream_with_diagnostics):
                stream = stream_with_diagnostics(request_messages, request_tools, budget)
            elif callable(stream_with_budget):
                stream = stream_with_budget(request_messages, request_tools, budget)
            else:
                stream = self.provider.stream(request_messages, request_tools)
            try:
                async for delta in stream:
                    # Pure text/progress cannot dispatch tools. Keep their
                    # identity fences cheap; usage/calls and step boundaries
                    # still run the complete live-definition check.
                    check_delta = (self.stream_check
                                   if type(delta) in (ModelText, ModelProgress) else self.check)
                    check_delta()
                    if final_usage and not isinstance(delta, ModelUsage):
                        raise ServiceError("provider_event")
                    try:
                        if type(delta) is ModelText:
                            item = ModelText.model_validate(delta.model_dump(mode="json"))
                            received_at = clock()
                            last_activity_at = received_at
                            if item.text:
                                if first_content_at is None:
                                    first_content_at = received_at
                                if first_text_at is None:
                                    first_text_at = received_at
                            self.output_bytes += len(item.text.encode())
                            if self.max_output is not None and self.output_bytes > self.max_output:
                                raise ServiceError("output_limit")
                            text += item.text
                            if (purpose == "summary" and summary_bytes is not None
                                    and len(text.encode()) > summary_bytes):
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
                            if (budget.max_tool_calls_this_step is not None
                                    and len(calls) > budget.max_tool_calls_this_step):
                                raise ServiceError("tool_limit")
                            last_activity_at = clock()
                            if first_content_at is None:
                                first_content_at = last_activity_at
                        elif type(delta) is ModelUsage:
                            measured = ModelUsage.model_validate(delta.model_dump(mode="json"))
                            if (usage is not None and not measured.usage.follows(usage)
                                    or final_usage and (not measured.final
                        or measured.usage != usage)):
                                raise ServiceError("provider_event")
                            usage, final_usage = measured.usage, measured.final
                            last_activity_at = clock()
                            await self.emit("usage", {"step_index": index,
                                "usage": usage.model_dump(mode="json"), "final": final_usage,
                                **extra})
                        elif type(delta) is ModelProgress:
                            item = ModelProgress.model_validate(delta.model_dump(mode="json"))
                            if item.argument_bytes < argument_bytes:
                                raise ServiceError("provider_event")
                            argument_bytes = item.argument_bytes
                            received_at = clock()
                            last_activity_at = received_at
                            activity_sequence += 1
                            if item.phase != "response" and first_content_at is None:
                                first_content_at = received_at
                            if (item.phase != last_activity_phase
                                    or received_at - last_activity_emit_at >= 1.0):
                                await self.emit("model_activity", {
                                    "span_id": span,
                                    "step_index": index,
                                    "phase": item.phase,
                                    "activity_index": activity_sequence,
                                    "elapsed_ms": max(0.0, (received_at - started_at) * 1000),
                                    "argument_bytes": argument_bytes,
                                })
                                last_activity_phase = item.phase
                                last_activity_emit_at = received_at
                        else:
                            raise ServiceError("provider_event")
                    except (ValidationError, ValueError):
                        raise ServiceError("provider_event") from None
                    check_delta()
            finally:
                close = getattr(stream, "aclose", None)
                if close is not None:
                    await settled(close())
            self.check()
            if purpose == "summary" and not text.strip():
                raise ServiceError("summary_failed")
            if final_usage and usage is not None and usage.input_tokens is not None:
                self.token_counter.observe(raw_input_tokens, usage.input_tokens)
            await self.emit("model_end", {"span_id": span, "step_index": index,
                "tool_calls": [call.model_dump(mode="json") for call in calls],
                "timing_origin": "provider_event",
                "elapsed_ms": max(0.0, (clock() - started_at) * 1000),
                "last_activity_ms": (
                    max(0.0, (last_activity_at - started_at) * 1000)
                    if last_activity_at is not None else None
                ),
                "tool_argument_bytes": argument_bytes,
                "activity_count": activity_sequence,
                "first_content_ms": (max(0.0, (first_content_at - started_at) * 1000)
                                      if first_content_at is not None else None),
                "first_text_ms": (max(0.0, (first_text_at - started_at) * 1000)
                                  if first_text_at is not None else None),
                **extra})
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
        id(build_context_manifest),
        source_digest(Path(__file__).parents[1].joinpath("context/manifest.py")),
        source_digest(__file__),
    )
