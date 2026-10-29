"""同Run共用模型步：协议、预算、流finally；不拥有Provider或producer。"""
from __future__ import annotations

import asyncio
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pydantic import JsonValue, ValidationError

from krim_agent.contracts import Message, RunEvent, ServiceError
from krim_agent.providers.types import ModelProvider, ModelText, ModelToolCall, ModelUsage
from krim_agent.runtime.contracts import settled
from krim_agent.source_fingerprints import source_digest
from krim_agent.usage import TokenUsage


@dataclass(frozen=True)
class StepResult:
    text: str
    calls: tuple[ModelToolCall, ...]
    index: int


class ModelStep:
    def __init__(self, provider: ModelProvider,
                 emit: Callable[[str, dict[str, JsonValue]], Awaitable[RunEvent]],
                 check: Callable[[], None], *, max_steps: int, max_output: int,
                 deadline: float | None, workspace: bool) -> None:
        self.provider, self.emit, self.check = provider, emit, check
        self.max_steps, self.max_output = max_steps, max_output
        self.deadline, self.workspace = deadline, workspace
        self.index = self.output_bytes = 0
        self.seen: set[str] = set()

    async def call(self, messages: list[Message], tools: list[dict[str, JsonValue]], *,
                   purpose: str = "task", max_group: int = 1,
                   remaining_tools: int = 1, summary_bytes: int = 2048) -> StepResult:
        self.check()
        if self.index >= self.max_steps:
            raise ServiceError("step_limit")
        index, self.index = self.index, self.index+1
        clock = asyncio.get_running_loop().time
        end = min(self.deadline, clock()+20) if self.deadline is not None else None
        span = secrets.token_hex(8)
        extra: dict[str, JsonValue] = {"purpose": purpose} if self.workspace else {}
        text, calls = "", []
        usage: TokenUsage | None = None
        final_usage = False
        async with asyncio.timeout_at(end):
            await self.emit("model_start", {"span_id": span, "step_index": index, **extra})
            self.check()
            stream = self.provider.stream(messages, tools)
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
                            if len(calls) > max_group or len(calls) > remaining_tools:
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
    return (id(ModelStep), id(ModelStep.call), id(ModelStep.__init__),
            source_digest(__file__))
