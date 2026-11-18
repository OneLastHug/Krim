"""SDK provider facade over Krim's existing validated model-step boundary.

The SDK owns continuation and tool execution. ModelStep retains the established
wire validation, usage, budgets and persistent model events. This facade performs
only message/event conversion; it never runs an agent loop or a tool.
"""

from __future__ import annotations

import copy
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from typing import Any, Protocol, cast

import ai
from ai import events
from ai import messages as sdk
from ai.types.tools import Tool
from ai.types.tools import ToolSpec as SDKToolSpec
from pydantic import JsonValue, PrivateAttr

from krim_agent.context.builder import check_budget
from krim_agent.context.manifest import ItemMetadata, session_message_metadata
from krim_agent.context.types import ContextPolicy, HistorySource
from krim_agent.contracts import (
    Message,
    ServiceError,
    check_json,
    encoded,
    fingerprint,
    parse_json,
)
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
            result.append(Message(role=message.role, content="".join(text), tool_calls=calls))
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
    descriptors = registry.schemas()
    specs = {spec.name: spec for spec, _ in registry._frozen}
    for descriptor in descriptors:
        name = cast(str, descriptor["name"])
        spec = specs[name]

        async def adapter_required(**kwargs: Any) -> JsonValue:
            raise RuntimeError("tool must be attached to the host SDK adapter")

        result.append(ai.AgentTool(
            tool=Tool(kind="function", name=name, spec=SDKToolSpec(
                description=spec.description or None,
                params=cast(dict[str, Any], descriptor["input_schema"]),
            )), fn=adapter_required, validator=spec.parameters,
        ))
    registry.assert_current()
    return result


class RunToolsPort(Protocol):
    """One run's authorized model-visible tool selection over a frozen registry.

    The production implementation is ``krim_agent.runtime.run_tools.RunTools``.
    The port stays local so this SDK bridge never imports runtime composition.
    Every method is synchronous; execution and approval authority remain with
    the executor, so a returned schema can only ever narrow model visibility.
    """

    def schemas(self) -> list[dict[str, JsonValue]]: ...

    def checkpoint(self) -> dict[str, Any]: ...

    def restore(self, state: dict[str, Any]) -> None: ...


class HostMediaRun(Protocol):
    """Media projection run object (``runtime.session_media.SessionMediaRun``).

    The plain host-media callback stays accepted; an object that additionally
    exposes a synchronous refs-only ``checkpoint`` participates in host
    checkpoint/restore, and its authorization is acknowledged only after the
    model step that consumed the projection returned successfully.
    """

    async def __call__(self, history: list[Message]) -> list[Message]: ...

    def checkpoint(self) -> dict[str, Any] | None: ...


class HostModelProvider(ai.Provider):
    provider_class_id: str = "krim-validated-host-model"
    name: str = "krim-validated-host-model"
    default_base_url: str = "http://unused.invalid"
    _stepper: ModelStep = PrivateAttr()
    _registry: ToolRegistry = PrivateAttr()
    _max_group: int | None = PrivateAttr()
    _max_calls: int | None = PrivateAttr()
    _request_bytes: int | None = PrivateAttr()
    _context_policy: ContextPolicy | None = PrivateAttr(default=None)
    _session_sources: tuple[HistorySource, ...] | None = PrivateAttr(default=None)
    _tool_count: int = PrivateAttr(default=0)
    _history: CompactorHistory | None = PrivateAttr(default=None)
    _host_media: HostMediaCallback | None = PrivateAttr(default=None)
    _run_tools: RunToolsPort | None = PrivateAttr(default=None)
    _media_state: bool = PrivateAttr(default=False)
    _media_checkpoint: Callable[[], object] | None = PrivateAttr(default=None)
    _media_restore_checkpoint: Callable[[dict[str, Any] | None], None] | None = PrivateAttr(
        default=None)
    _media_async_restore: (
        Callable[[dict[str, Any], list[Message]], Awaitable[list[Message]]] | None
    ) = PrivateAttr(default=None)
    _media_ack: Callable[[], Awaitable[None]] | None = PrivateAttr(default=None)
    _media_pending: dict[str, Any] | None = PrivateAttr(default=None)

    def __init__(self, stepper: ModelStep, registry: ToolRegistry, *,
                 max_group: int | None, max_calls: int | None,
                 request_bytes: int | None,
                 context_policy: ContextPolicy | None = None,
                 session_sources: Sequence[HistorySource] | None = None,
                 history: CompactorHistory | None = None,
                 host_media: HostMediaCallback | None = None,
                 run_tools: RunToolsPort | None = None):
        super().__init__()
        if max_group is not None and (type(max_group) is not int or max_group < 1):
            raise ValueError("invalid per-request tool group")
        if request_bytes is not None and (type(request_bytes) is not int or request_bytes < 0):
            raise ValueError("invalid request byte budget")
        if history is not None:
            if history.compactor.step is not stepper:
                raise ValueError("compaction must share the host ModelStep")
            if history.compactor.request_limit != request_bytes:
                raise ValueError("compaction request limit differs from the host")
            if context_policy is not None and context_policy != history.compactor.policy:
                raise ValueError("compaction context policy differs from the host")
            context_policy = history.compactor.policy
        self._stepper, self._registry = stepper, registry
        # None keeps the full returned batch: ModelStep.request_budget then only
        # applies remaining_tools/provider caps that are actually configured, and
        # never invents a product-level call ceiling in their place.
        self._max_group = max_group
        self._max_calls, self._request_bytes = max_calls, request_bytes
        self._context_policy = context_policy
        self._session_sources = tuple(session_sources) if session_sources is not None else None
        self._history = history
        self._host_media = host_media
        self._run_tools = run_tools
        # A plain callback keeps the legacy behavior. A media run object adds a
        # synchronous refs-only checkpoint, an authorization restore (the run's
        # own binding save, or the async projection reconstruction stashed for
        # the next stream), and an acknowledgment gated on step success.
        checkpoint = getattr(host_media, "checkpoint", None)
        if callable(checkpoint):
            self._media_state = True
            self._media_checkpoint = checkpoint
            restore_sync = getattr(host_media, "restore_checkpoint", None)
            if callable(restore_sync):
                self._media_restore_checkpoint = restore_sync
            restore_async = getattr(host_media, "restore", None)
            if callable(restore_async):
                self._media_async_restore = restore_async
            acknowledge = getattr(host_media, "accepted", None)
            if not callable(acknowledge):
                acknowledge = getattr(host_media, "acknowledge", None)
            if callable(acknowledge):
                self._media_ack = acknowledge

    def checkpoint(self) -> dict[str, Any]:
        state = {"step": self._stepper.index, "output_bytes": self._stepper.output_bytes,
                 "seen": sorted(self._stepper.seen), "tool_count": self._tool_count}
        if self._history is not None:
            state["context"] = self._history.checkpoint()
        if self._run_tools is not None:
            port_state = self._run_tools.checkpoint()
            if not isinstance(port_state, dict):
                raise ServiceError("facts_corrupt")
            try:
                check_json(port_state)
            except (ValueError, TypeError, RecursionError):
                raise ServiceError("facts_corrupt") from None
            state["run_tools"] = copy.deepcopy(port_state)
        if self._media_checkpoint is not None:
            # References only; inline image bytes never enter durable state.
            media = self._media_checkpoint()
            if media is not None:
                if not isinstance(media, dict):
                    raise ServiceError("facts_corrupt")
                try:
                    check_json(media)
                except (ValueError, TypeError, RecursionError):
                    raise ServiceError("facts_corrupt") from None
                media = copy.deepcopy(media)
            state["media"] = media
        return state

    def restore(self, value: dict[str, Any]) -> None:
        fields = [value.get(name) for name in ("step", "output_bytes", "tool_count")]
        if any(type(item) is not int or item < 0 for item in fields):
            raise ServiceError("facts_corrupt")
        step, output, count = (cast(int, item) for item in fields)
        seen = value.get("seen")
        if (self._stepper.max_steps is not None and step > self._stepper.max_steps
                or self._stepper.max_output is not None and output > self._stepper.max_output
                or self._max_calls is not None and count > self._max_calls
                or not isinstance(seen, list)
                or any(not isinstance(item, str) for item in seen) or len(set(seen)) != len(seen)):
            raise ServiceError("facts_corrupt")
        context = value.get("context")
        run_tools = value.get("run_tools")
        media = value.get("media")
        if self._run_tools is None:
            if "run_tools" in value:
                raise ServiceError("facts_corrupt")
        elif not isinstance(run_tools, dict):
            raise ServiceError("facts_corrupt")
        if self._media_state:
            if "media" not in value:
                raise ServiceError("facts_corrupt")
        elif "media" in value:
            raise ServiceError("facts_corrupt")
        if self._history is not None:
            if not isinstance(context, dict):
                raise ServiceError("facts_corrupt")
            self._history.restore(context)
        elif context is not None:
            raise ServiceError("facts_corrupt")
        # Tool and media state are applied before the counters; a port that
        # rejects its checkpoint (drift keeps the port's own ServiceError) must
        # leave the restored host counters untouched.
        if self._run_tools is not None:
            self._run_tools.restore(cast(dict[str, Any], run_tools))
        if self._media_state:
            if self._media_restore_checkpoint is not None:
                self._media_restore_checkpoint(cast(dict[str, Any] | None, media))
            elif media is not None:
                if not isinstance(media, dict) or self._media_async_restore is None:
                    raise ServiceError("facts_corrupt")
                # The async projection restore needs live history; stash the
                # selection and reconstruct authorization on the next stream.
                self._media_pending = copy.deepcopy(media)
        self._stepper.index, self._stepper.output_bytes = step, output
        self._stepper.seen, self._tool_count = set(seen), count

    def step_index(self) -> int:
        return self._stepper.index - 1

    def _active_schemas(self) -> list[dict[str, JsonValue]]:
        """Model-visible schemas for this request; discovery cannot widen authority.

        The port returns a selection, never new tools: every returned descriptor
        must equal, byte for byte, the same-name frozen descriptor of the full
        registry, and unknown or duplicate names fail as binding changes.
        """
        if self._run_tools is None:
            return self._registry.schemas()
        selected = self._run_tools.schemas()
        if not isinstance(selected, list):
            raise ServiceError("tool_binding_changed")
        frozen = {cast(str, item["name"]): (item, fingerprint(item))
                  for item in self._registry.schemas()}
        active: list[dict[str, JsonValue]] = []
        seen: set[str] = set()
        for descriptor in selected:
            name = descriptor.get("name") if isinstance(descriptor, dict) else None
            if not isinstance(name, str) or name in seen or name not in frozen:
                raise ServiceError("tool_binding_changed")
            try:
                drifted = fingerprint(descriptor) != frozen[name][1]
            except (ValueError, TypeError, RecursionError):
                raise ServiceError("tool_binding_changed") from None
            if drifted:
                raise ServiceError("tool_binding_changed")
            seen.add(name)
            active.append(frozen[name][0])
        return active

    async def stream(self, model: ai.Model, messages: list[sdk.Message],
                     **kwargs: Any) -> AsyncGenerator[events.Event]:
        self._registry.assert_current()
        history = host_messages(messages)
        schemas = self._active_schemas()
        remaining = None if self._max_calls is None else self._max_calls - self._tool_count
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
            pending = self._media_pending
            if pending is not None and self._media_async_restore is not None:
                history = await self._media_async_restore(pending, history)
            else:
                history = await self._host_media(history)
            if not isinstance(history, list) or any(
                not isinstance(item, Message) for item in history
            ):
                raise ServiceError("invalid_input")
            # A failed or cancelled authorization restore must remain retryable.
            self._media_pending = None
        # Summary calls consumed this same ModelStep. Recompute, never reuse the
        # pre-compaction model-step allowance or restore counters from history size.
        budget = self._stepper.request_budget(self._max_group, remaining)
        prepared = self._stepper.with_budget_prompt(history, budget)
        if self._history is None and self._session_sources is not None:
            metadata = tuple(session_message_metadata(prepared, self._session_sources))
        tools = (
            schemas if budget.max_tool_calls_this_step is None
            or budget.max_tool_calls_this_step > 0 else []
        )
        if self._request_bytes is not None and len(encoded(
                {"messages": [item.model_dump(mode="json") for item in prepared],
                 "tools": tools}).encode()) > self._request_bytes:
            raise ServiceError("input_limit")
        input_bytes = self._request_bytes
        if self._context_policy is not None:
            policy = self._context_policy
            # check_budget keeps the policy's own window and reserved output; a
            # host without a transport byte limit still checks the policy window.
            check_budget(
                prepared, tools, policy, self._request_bytes,
                counter=getattr(self._stepper, "token_counter", None),
            )
            if policy.counts_tokens():
                # Transport bytes and model tokens are different units. Never
                # reserve output by multiplying token counts by four here.
                from krim_agent.config import minimum_limit

                input_bytes = minimum_limit(policy.max_bytes, self._request_bytes)
            elif policy.max_bytes is not None:
                input_bytes = max(0, policy.max_bytes - 1024 - policy.reserved_output_tokens * 4)
                if self._request_bytes is not None:
                    input_bytes = min(input_bytes, self._request_bytes - 1024)
        response = await self._stepper.call(
            prepared, tools, max_group=self._max_group, remaining_tools=remaining,
            request_budget=budget, request_prepared=True, input_budget_bytes=input_bytes,
            request_metadata=metadata, compaction_changes=changes,
            reserved_output_tokens=(
                self._context_policy.reserved_output_tokens if self._context_policy else None
            ),
            model_context_window=(
                self._context_policy.max_tokens
                if self._context_policy is not None and self._context_policy.counts_tokens()
                else None
            ),
        )
        self._tool_count += len(response.calls)
        if not response.calls and not response.text:
            raise ServiceError("no_output")
        # Acknowledge the consumed media projection only after the model step
        # returned a usable response; failure or cancellation never acknowledges.
        if self._media_ack is not None:
            await self._media_ack()
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
