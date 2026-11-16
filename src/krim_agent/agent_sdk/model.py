"""SDK provider facade over Krim's existing validated model-step boundary.

The SDK owns continuation and tool execution. ModelStep retains the established
wire validation, usage, budgets and persistent model events. This facade performs
only message/event conversion; it never runs an agent loop or a tool.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from typing import Any, Literal, cast

import ai
from ai import events
from ai import messages as sdk
from ai.types.tools import Tool
from ai.types.tools import ToolSpec as SDKToolSpec
from pydantic import JsonValue, PrivateAttr

from krim_agent.context.builder import check_budget
from krim_agent.context.manifest import ItemMetadata, session_message_metadata
from krim_agent.context.types import ContextPolicy, HistorySource
from krim_agent.contracts import Message, ServiceError, encoded, parse_json
from krim_agent.runtime.model_step import ModelStep
from krim_agent.tools.registry import ToolRegistry

from .context import CompactorHistory
from .host_media import HostMediaCallback


def host_messages(history: Sequence[sdk.Message]) -> list[Message]:
    result: list[Message] = []
    call_order: dict[str, int] = {}
    next_call_order = 0
    for message in history:
        text: list[str] = []
        calls: list[dict[str, JsonValue]] = []
        outputs: list[tuple[int, Message]] = []
        for part in message.parts:
            if isinstance(part, sdk.TextPart):
                text.append(part.text)
            elif isinstance(part, sdk.ToolCallPart):
                arguments = parse_json(part.tool_args)
                if not isinstance(arguments, dict):
                    raise ServiceError("provider_tool_arguments")
                calls.append({"kind": "tool_call", "id": part.tool_call_id,
                              "name": part.tool_name, "arguments": arguments})
                call_order[part.tool_call_id] = next_call_order
                next_call_order += 1
            elif isinstance(part, sdk.ToolResultPart):
                if part.is_hook_deferred:
                    raise ServiceError("approval_unavailable")
                value = part.get_model_input()
                # Existing wire history contains tool values, not internal
                # observations/host invocation ids. Denials remain explicit.
                if isinstance(value, dict) and value.get("status") == "ok" and "value" in value:
                    value = value["value"]
                elif (isinstance(value, dict) and value.get("status") == "denied"
                      and isinstance(value.get("call_id"), str)
                      and isinstance(value.get("name"), str)):
                    value = {
                        "call_id": value["call_id"], "name": value["name"],
                        "status": "denied",
                        "code": value.get("code") if isinstance(value.get("code"), str)
                        else "permission_denied",
                    }
                outputs.append((
                    call_order.get(part.tool_call_id, next_call_order + len(outputs)),
                    Message(role="tool", call_id=part.tool_call_id, content=encoded(value)),
                ))
            elif isinstance(part, sdk.HookPart):
                continue  # Host approval metadata is not a new model message.
            else:
                raise ServiceError("invalid_input")
        if text or calls or not message.parts:
            if message.role not in {"system", "user", "assistant"}:
                raise ServiceError("provider_event")
            result.append(Message(role=cast(Literal["system", "user", "assistant"], message.role),
                                  content="".join(text), tool_calls=calls))
        result.extend(output for _, output in sorted(outputs, key=lambda item: item[0]))
    return result


def sdk_messages(history: Sequence[Message]) -> list[sdk.Message]:
    result: list[sdk.Message] = []
    names: dict[str, str] = {}
    for message in history:
        parts: list[Any] = []
        if message.role == "tool":
            if message.call_id is None or message.call_id not in names:
                raise ServiceError("provider_event")
            try:
                value: Any = parse_json(message.content)
            except ValueError:
                value = message.content
            parts.append(sdk.ToolResultPart(
                tool_call_id=message.call_id, tool_name=names[message.call_id],
                result=value, model_input=value,
            ))
        else:
            if message.content:
                parts.append(sdk.TextPart(text=message.content))
            for call in message.tool_calls:
                call_id, name = call.get("id"), call.get("name")
                if not isinstance(call_id, str) or not isinstance(name, str):
                    raise ServiceError("provider_event")
                names[call_id] = name
                parts.append(sdk.ToolCallPart(tool_call_id=call_id, tool_name=name,
                                              tool_args=encoded(call.get("arguments"))))
        result.append(sdk.Message(role=message.role, parts=parts))
    return result


def agent_tools(registry: ToolRegistry) -> list[ai.AgentTool]:
    """Declare the already frozen host schema; SDKAdapter supplies execution."""
    result = []
    for descriptor in registry.schemas():
        name = cast(str, descriptor["name"])
        spec = registry.get(name)

        async def adapter_required(**kwargs: Any) -> JsonValue:
            raise RuntimeError("tool must be attached to the host SDK adapter")

        result.append(ai.AgentTool(
            tool=Tool(kind="function", name=name, spec=SDKToolSpec(
                description=spec.description or None, params=spec.parameters.model_json_schema(),
            )), fn=adapter_required, validator=spec.parameters,
        ))
    return result


class HostModelProvider(ai.Provider):
    provider_class_id: str = "krim-validated-host-model"
    name: str = "krim-validated-host-model"
    default_base_url: str = "http://unused.invalid"
    _stepper: ModelStep = PrivateAttr()
    _registry: ToolRegistry = PrivateAttr()
    _max_group: int = PrivateAttr()
    _max_calls: int = PrivateAttr()
    _request_bytes: int = PrivateAttr()
    _context_policy: ContextPolicy | None = PrivateAttr(default=None)
    _session_sources: tuple[HistorySource, ...] | None = PrivateAttr(default=None)
    _tool_count: int = PrivateAttr(default=0)
    _history: CompactorHistory | None = PrivateAttr(default=None)
    _host_media: HostMediaCallback | None = PrivateAttr(default=None)

    def __init__(self, stepper: ModelStep, registry: ToolRegistry, *, max_group: int,
                 max_calls: int, request_bytes: int, context_policy: ContextPolicy | None = None,
                 session_sources: Sequence[HistorySource] | None = None,
                 history: CompactorHistory | None = None,
                 host_media: HostMediaCallback | None = None):
        super().__init__()
        if history is not None:
            if history.compactor.step is not stepper:
                raise ValueError("compaction must share the host ModelStep")
            if history.compactor.request_limit != request_bytes:
                raise ValueError("compaction request limit differs from the host")
            if context_policy is not None and context_policy != history.compactor.policy:
                raise ValueError("compaction context policy differs from the host")
            context_policy = history.compactor.policy
        self._stepper, self._registry = stepper, registry
        self._max_group, self._max_calls = max_group, max_calls
        self._request_bytes, self._context_policy = request_bytes, context_policy
        self._session_sources = tuple(session_sources) if session_sources is not None else None
        self._history = history
        self._host_media = host_media

    def checkpoint(self) -> dict[str, Any]:
        state = {"step": self._stepper.index, "output_bytes": self._stepper.output_bytes,
                 "seen": sorted(self._stepper.seen), "tool_count": self._tool_count}
        if self._history is not None:
            state["context"] = self._history.checkpoint()
        return state

    def restore(self, value: dict[str, Any]) -> None:
        fields = [value.get(name) for name in ("step", "output_bytes", "tool_count")]
        if any(type(item) is not int or item < 0 for item in fields):
            raise ServiceError("facts_corrupt")
        step, output, count = (cast(int, item) for item in fields)
        seen = value.get("seen")
        if (step > self._stepper.max_steps or output > self._stepper.max_output
                or count > self._max_calls or not isinstance(seen, list)
                or any(not isinstance(item, str) for item in seen) or len(set(seen)) != len(seen)):
            raise ServiceError("facts_corrupt")
        context = value.get("context")
        if self._history is not None:
            if not isinstance(context, dict):
                raise ServiceError("facts_corrupt")
            self._history.restore(context)
        elif context is not None:
            raise ServiceError("facts_corrupt")
        self._stepper.index, self._stepper.output_bytes = step, output
        self._stepper.seen, self._tool_count = set(seen), count

    def step_index(self) -> int:
        return self._stepper.index - 1

    async def stream(self, model: ai.Model, messages: list[sdk.Message],
                     **kwargs: Any) -> AsyncGenerator[events.Event]:
        self._registry.assert_current()
        history = host_messages(messages)
        schemas = self._registry.schemas()
        remaining = self._max_calls - self._tool_count
        metadata: tuple[ItemMetadata | None, ...] = ()
        changes: tuple[dict[str, JsonValue], ...] = ()
        if self._history is not None:
            history = await self._history.prepare(
                history, schemas, max_group=self._max_group, remaining_tools=remaining,
            )
            metadata = tuple(self._history.compactor.manifest_metadata(history))
            changes = tuple(self._history.compactor.compaction_changes)
        # The host callback resolves only canonical, currently authorized media.
        # It runs for every request so resumed runs must authorize references anew.
        if self._host_media is not None:
            history = await self._host_media(history)
            if not isinstance(history, list) or any(
                not isinstance(item, Message) for item in history
            ):
                raise ServiceError("invalid_input")
        # Summary calls consumed this same ModelStep. Recompute, never reuse the
        # pre-compaction model-step allowance or restore counters from history size.
        budget = self._stepper.request_budget(self._max_group, remaining)
        prepared = self._stepper.with_budget_prompt(history, budget)
        if self._history is None and self._session_sources is not None:
            metadata = tuple(session_message_metadata(prepared, self._session_sources))
        tools = schemas if budget.max_tool_calls_this_step else []
        if len(encoded({"messages": [item.model_dump(mode="json") for item in prepared],
                        "tools": tools}).encode()) > self._request_bytes:
            raise ServiceError("input_limit")
        input_bytes = self._request_bytes
        if self._context_policy is not None:
            check_budget(prepared, tools, self._context_policy, self._request_bytes)
            input_bytes = max(0, min(
                self._request_bytes - 1024,
                self._context_policy.max_bytes - 1024
                - self._context_policy.reserved_output_tokens * 4,
            ))
        response = await self._stepper.call(
            prepared, tools, max_group=self._max_group, remaining_tools=remaining,
            request_budget=budget, request_prepared=True, input_budget_bytes=input_bytes,
            request_metadata=metadata, compaction_changes=changes,
            reserved_output_tokens=(
                self._context_policy.reserved_output_tokens if self._context_policy else None
            ),
        )
        self._tool_count += len(response.calls)
        if not response.calls and not response.text:
            raise ServiceError("no_output")
        yield events.StreamStart()
        if response.text:
            yield events.TextStart(block_id="text")
            yield events.TextDelta(block_id="text", chunk=response.text)
            yield events.TextEnd(block_id="text")
        for call in response.calls:
            args = encoded(call.arguments)
            yield events.ToolStart(tool_call_id=call.id, tool_name=call.name)
            yield events.ToolDelta(tool_call_id=call.id, chunk=args)
            yield events.ToolEnd(tool_call_id=call.id, tool_call=sdk.ToolCallPart(
                tool_call_id=call.id, tool_name=call.name, tool_args=args,
            ))
        yield events.StreamEnd(finish_reason="tool_call" if response.calls else "stop")
