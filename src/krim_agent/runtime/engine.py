"""有界模型→工具→模型循环；不装配服务、数据库或旧管理体系。"""

from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable
from functools import partial
from typing import cast

from pydantic import JsonValue, ValidationError

from krim_agent.config import Settings
from krim_agent.context import builder
from krim_agent.contracts import Message, RunEvent, ServiceError, encoded
from krim_agent.providers.types import ModelProvider, ModelText, ModelToolCall, ModelUsage
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import Approve, ToolContext, ToolExecutor
from krim_agent.usage import TokenUsage

from .contracts import RunStore


class AgentLoop:
    def __init__(
        self, store: RunStore, provider: ModelProvider, executor: ToolExecutor, settings: Settings
    ) -> None:
        self.store, self.provider, self.executor, self.settings = (
            store,
            provider,
            executor,
            settings,
        )

    async def run(
        self,
        run_id: str,
        approve: Approve,
        assert_target_current: Callable[[], None] = lambda: None,
    ) -> None:
        snapshot = await self.store.snapshot(run_id)
        messages = [
            Message(role="system", content=self.settings.system_prompt),
            Message(role="user", content=snapshot.request.text),
        ]
        if snapshot.session_context is not None:
            messages = list(snapshot.session_context.context.messages)
        seen: set[str] = set()
        tool_count = 0
        output_bytes = 0
        emit = partial(self.store.append, run_id)
        for step in range(self.settings.max_steps):
            assert_target_current()
            self.executor.registry.assert_current()
            schemas = self.executor.registry.schemas()
            if (
                len(
                    encoded(
                        {
                            "messages": [m.model_dump(mode="json") for m in messages],
                            "tools": schemas,
                        }
                    ).encode()
                )
                > self.settings.max_request_bytes
            ):
                raise ServiceError("input_limit")
            if snapshot.session_context is not None:
                builder.check_budget(
                    messages,
                    schemas,
                    snapshot.session_context.context.policy,
                    self.settings.max_request_bytes,
                )
            span_id = secrets.token_hex(8)
            await emit("model_start", {"span_id": span_id, "step_index": step})
            self.executor.registry.assert_current()
            calls: list[ModelToolCall] = []
            text = ""
            prior_usage: TokenUsage | None = None
            final_usage = False
            assert_target_current()
            stream = self.provider.stream(messages, schemas)
            try:
                async for delta in stream:
                    if final_usage and not isinstance(delta, ModelUsage):
                        raise ServiceError("provider_event")
                    try:
                        if type(delta) is ModelText:
                            delta = ModelText.model_validate(delta.model_dump(mode="json"))
                            output_bytes += len(delta.text.encode())
                            if output_bytes > self.settings.max_output_bytes:
                                raise ServiceError("output_limit")
                            text += delta.text
                            await emit("text", {"text": delta.text, "step_index": step})
                        elif type(delta) is ModelToolCall:
                            delta = ModelToolCall.model_validate(delta.model_dump(mode="json"))
                            if delta.id in seen:
                                raise ServiceError("duplicate_tool_call")
                            seen.add(delta.id)
                            calls.append(delta)
                            if (
                                len(calls) > self.settings.max_tools_per_step
                                or tool_count + len(calls) > self.settings.max_tool_calls
                            ):
                                raise ServiceError("tool_limit")
                        elif type(delta) is ModelUsage:
                            delta = ModelUsage.model_validate(delta.model_dump(mode="json"))
                            if (
                                prior_usage is not None
                                and not delta.usage.follows(prior_usage)
                                or final_usage
                                and (not delta.final or delta.usage != prior_usage)
                            ):
                                raise ServiceError("provider_event")
                            prior_usage, final_usage = delta.usage, delta.final
                            await emit(
                                "usage",
                                {
                                    "step_index": step,
                                    "usage": delta.usage.model_dump(mode="json"),
                                    "final": delta.final,
                                },
                            )
                        else:
                            raise ServiceError("provider_event")
                    except (ValidationError, ValueError):
                        raise ServiceError("provider_event") from None
            finally:
                close = getattr(stream, "aclose", None)
                if close is not None:
                    await close()
            await emit(
                "model_end",
                {
                    "span_id": span_id,
                    "step_index": step,
                    "tool_calls": [call.model_dump(mode="json") for call in calls],
                },
            )
            if not calls:
                if not text:
                    raise ServiceError("no_output")
                return
            messages.append(
                Message(
                    role="assistant",
                    content=text,
                    tool_calls=[call.model_dump(mode="json") for call in calls],
                )
            )
            for call in calls:
                assert_target_current()
                tool_count += 1
                try:
                    spec = self.executor.registry.get(call.name)
                    digest = spec.fingerprint
                except ServiceError as error:
                    if error.code != "unknown_tool":
                        raise
                    digest = ""
                invocation = Invocation(
                    run_id,
                    secrets.token_hex(16),
                    secrets.token_hex(8),
                    step,
                    call.name,
                    encoded(call.arguments),
                    digest,
                    provider_call_id=call.id,
                )
                context = ToolContext(
                    cast(Callable[[str, dict[str, JsonValue]], Awaitable["RunEvent"]], emit),
                    approve,
                    self.settings.tool_timeout,
                    assert_target_current,
                )
                result = await self.executor.execute(invocation, context)
                if result.status != "ok":
                    raise ServiceError(result.code or "tool_error")
                messages.append(
                    Message(role="tool", content=encoded(result.value), call_id=call.id)
                )
        raise ServiceError("step_limit")
