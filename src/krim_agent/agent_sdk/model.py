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
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import Message, ServiceError, encoded, parse_json
from krim_agent.runtime.model_step import ModelStep
from krim_agent.tools.registry import ToolRegistry


def host_messages(history: Sequence[sdk.Message]) -> list[Message]:
    result: list[Message] = []
    for message in history:
        text: list[str] = []
        calls: list[dict[str, JsonValue]] = []
        outputs: list[Message] = []
        for part in message.parts:
            if isinstance(part, sdk.TextPart):
                text.append(part.text)
            elif isinstance(part, sdk.ToolCallPart):
                arguments = parse_json(part.tool_args)
                if not isinstance(arguments, dict):
                    raise ServiceError("provider_tool_arguments")
                calls.append({"kind": "tool_call", "id": part.tool_call_id,
                              "name": part.tool_name, "arguments": arguments})
            elif isinstance(part, sdk.ToolResultPart):
                if part.is_hook_deferred:
                    raise ServiceError("approval_unavailable")
                value = part.get_model_input()
                # Existing wire history contains tool values, not internal
                # observations/host invocation ids. Denials remain explicit.
                if isinstance(value, dict) and value.get("status") == "ok" and "value" in value:
                    value = value["value"]
                outputs.append(Message(role="tool", call_id=part.tool_call_id,
                                       content=encoded(value)))
            elif isinstance(part, sdk.HookPart):
                continue  # Host approval metadata is not a new model message.
            else:
                raise ServiceError("invalid_input")
        if text or calls or not message.parts:
            if message.role not in {"system", "user", "assistant"}:
                raise ServiceError("provider_event")
            result.append(Message(role=cast(Literal["system", "user", "assistant"], message.role),
                                  content="".join(text), tool_calls=calls))
        result.extend(outputs)
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
    _tool_count: int = PrivateAttr(default=0)

    def __init__(self, stepper: ModelStep, registry: ToolRegistry, *, max_group: int,
                 max_calls: int, request_bytes: int, context_policy: ContextPolicy | None = None):
        super().__init__()
        self._stepper, self._registry = stepper, registry
        self._max_group, self._max_calls = max_group, max_calls
        self._request_bytes, self._context_policy = request_bytes, context_policy

    def checkpoint(self) -> dict[str, Any]:
        return {"step": self._stepper.index, "output_bytes": self._stepper.output_bytes,
                "seen": sorted(self._stepper.seen), "tool_count": self._tool_count}

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
        budget = self._stepper.request_budget(self._max_group, remaining)
        prepared = self._stepper.with_budget_prompt(history, budget)
        tools = schemas if budget.max_tool_calls_this_step else []
        if len(encoded({"messages": [item.model_dump(mode="json") for item in prepared],
                        "tools": tools}).encode()) > self._request_bytes:
            raise ServiceError("input_limit")
        if self._context_policy is not None:
            check_budget(prepared, tools, self._context_policy, self._request_bytes)
        response = await self._stepper.call(
            prepared, tools, max_group=self._max_group, remaining_tools=remaining,
            request_budget=budget, request_prepared=True, input_budget_bytes=self._request_bytes,
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
