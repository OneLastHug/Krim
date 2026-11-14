"""Narrow, in-process SDK probe using the existing executor and RunStore ports.

Agent.loop, ToolRunner, schema validation, tool results, hook events and replay
are owned by ai==0.9.0. This module only adds response admission, ordered host
execution, durable operation lookup and the host's park/stop fences.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from typing import Any, cast
from uuid import uuid4

import ai
from ai import events, messages
from ai.agents import HookDeferredException, HookRegistry, deferred_tool_result
from ai.types.tools import ToolApproval
from ai.types.tools import ToolSpec as SDKToolSpec
from pydantic import JsonValue

from krim_agent.contracts import TERMINAL, ToolObservation, encoded, fingerprint, parse_json
from krim_agent.development.contracts import (
    EnvironmentHandle,
    OperationIdentity,
    OperationReceipt,
    OperationStore,
    RunOwnership,
)
from krim_agent.runtime.contracts import RunStore
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import Admission, ToolContext, ToolExecutor

from .host import CURRENT_OPERATION, HostExecution
from .response import CompleteResponseProvider


class ProbeStopped(RuntimeError):
    """Host stop won over SDK continuation."""


@dataclass(frozen=True)
class ProbeResult:
    status: str
    messages: list[messages.Message]
    events: list[events.AgentEvent]


@dataclass
class _Entry:
    invocation: Invocation
    admission: Admission
    identity: OperationIdentity | None
    gate: asyncio.Event
    predecessor: asyncio.Event | None = None


class _OrderedAgent(ai.Agent):
    """Keep SDK's entire loop; wrap only the public resolve dispatch seam."""

    def __init__(self, owner: SDKAdapter, tools: Sequence[ai.AgentTool]) -> None:
        super().__init__(tools=tools)
        self.owner = owner

    def resolve(self, part: Any) -> Any:
        if isinstance(part, Sequence) and not isinstance(part, messages.ToolCallPart):
            return [self.resolve(item) for item in part]
        native = super().resolve(part)
        owner = self.owner
        entry = owner.entries[part.tool_call_id]

        async def ordered() -> events.ToolCallResult:
            if entry.predecessor is not None:
                await entry.predecessor.wait()
            try:
                await owner.check_stop()
                if owner.deferred:
                    # Do not start a later call while an earlier one is parked.
                    # It remains in the checkpoint's assistant turn for replay.
                    return deferred_tool_result(
                        owner.deferred,
                        tool_call_id=part.tool_call_id,
                        tool_name=part.tool_name,
                    )
                # BoundToolCall catches ordinary tool exceptions. The wrapper is
                # outside it so HookDeferredException can become SDK's own
                # deferred result rather than a generic error result.
                if entry.admission.decision == "ask" and part.cached_result is None:
                    try:
                        approval = await ai.hook(
                            f"approve_{entry.invocation.call_id}",
                            payload=ToolApproval,
                            metadata={"tool": part.tool_name, "call_id": entry.invocation.call_id},
                            tool_call_id=part.tool_call_id,
                        )
                    except HookDeferredException as error:
                        owner.deferred = error.hook
                        return deferred_tool_result(
                            error.hook,
                            tool_call_id=part.tool_call_id,
                            tool_name=part.tool_name,
                        )
                    await owner.check_stop()
                    entry.admission = replace(
                        entry.admission,
                        approved=approval.granted,
                        decision="ask" if approval.granted else "deny",
                        code=None if approval.granted else "approval_denied",
                    )
                return cast(events.ToolCallResult, await native())
            finally:
                entry.gate.set()

        # The SDK runner deduplicates by this id *within this run only*.
        # Operation identity below is independently allocated and persisted.
        ordered.id = part.tool_call_id  # type: ignore[attr-defined]
        return ordered


class SDKAdapter:
    """Explicit probe instance for one host run; no application registration.

    ``read_result`` reads a durable result_ref; it never re-executes an operation.
    Approval replies supplied to run() must already be authorized by the host.
    The probe uses serial execution for all tools, retaining write isolation.
    """

    CHECKPOINT = "sdk-probe-v1"

    def __init__(
        self,
        *,
        store: RunStore,
        operations: OperationStore,
        executor: ToolExecutor,
        tools: Sequence[ai.AgentTool],
        environment: EnvironmentHandle,
        ownership: RunOwnership,
        read_result: Callable[[str], Awaitable[JsonValue]],
        approval_request: Callable[[Invocation, float | None], Awaitable[dict[str, JsonValue]]],
        host: HostExecution | None = None,
    ) -> None:
        if environment.owner_id != ownership.owner_id:
            raise ValueError("environment ownership mismatch")
        self.store, self.operations, self.executor = store, operations, executor
        self.environment, self.ownership = environment, ownership
        self.read_result = read_result
        self.approval_request = approval_request
        self.host = host
        self.entries: dict[str, _Entry] = {}
        self.deferred: messages.HookPart[Any] | None = None
        self.step = 0
        self.failure: Exception | None = None
        self.tool_fingerprint = fingerprint(executor.registry.schemas())
        bridged: list[ai.AgentTool] = []
        for tool in tools:
            spec = executor.registry.get(tool.name)

            async def invoke(**kwargs: Any) -> JsonValue:
                # The SDK has validated kwargs; the host's strict admission was
                # already applied to the entire response before dispatch.
                call = ai.current_tool_call()
                try:
                    return await self.execute(self.entries[call.id])
                except Exception as error:
                    self.failure = error
                    raise

            # Reuse the existing schema rather than nesting parameters or
            # generating another model-visible command/tool vocabulary.
            declaration = tool.tool.model_copy(
                update={
                    "spec": SDKToolSpec(
                        description=spec.description or None,
                        params=spec.parameters.model_json_schema(),
                    ),
                    "require_approval": False,
                }
            )
            bridged.append(replace(tool, fn=invoke, tool=declaration, validator=spec.parameters))
        if len({tool.name for tool in bridged}) != len(bridged):
            raise ValueError("duplicate tool registration")
        self.tool_names = frozenset(tool.name for tool in bridged)
        self.agent = _OrderedAgent(self, bridged)

    @property
    def run_id(self) -> str:
        return self.ownership.run_id

    async def check_stop(self) -> None:
        if self.host is not None:
            self.host.context.assert_target_current()
            if self.host.context.scope is not None:
                self.host.context.scope.check()
        if self.failure is not None:
            raise self.failure
        snapshot = await self.store.snapshot(self.run_id)
        if snapshot.run.stop_requested or snapshot.run.status in TERMINAL:
            raise ProbeStopped("run stopped")

    def context(self) -> ToolContext:
        async def emit(kind: str, data: dict[str, JsonValue]) -> Any:
            if kind == "tool_intent":
                await self.check_stop()
            event = (await self.host.context.emit(kind, data) if self.host is not None
                     else await self.store.append(self.run_id, kind, data))
            if kind == "tool_intent":
                await self.check_stop()
            return event

        async def unexpected_approval(invocation: Invocation, timeout_seconds: float) -> bool:
            raise RuntimeError("approval must pass through SDK hook before execution")

        if self.host is not None:
            return replace(self.host.context, emit=emit, approve=unexpected_approval)
        return ToolContext(emit=emit, approve=unexpected_approval, timeout=5.0)

    async def prepare(self, calls: Sequence[messages.ToolCallPart]) -> None:
        await self.check_stop()
        self.step = self.host.model_step() if self.host is not None else self.step + 1
        prepared: dict[str, _Entry] = {}
        predecessor: asyncio.Event | None = None
        for call in calls:
            if call.tool_call_id in self.entries:
                raise ValueError("provider reused a call id")
            if call.tool_name not in self.tool_names:
                raise ValueError("tool is not exposed to this agent")
            # Resolve every name and strict argument set before releasing even
            # the first ToolEnd to Agent's eager dispatcher.
            spec = self.executor.registry.get(call.tool_name)
            args = parse_json(call.tool_args)
            if not isinstance(args, dict):
                raise ValueError("tool arguments must be an object")
            invocation = Invocation(
                run_id=self.run_id,
                call_id=f"call-{uuid4().hex}",
                span_id=f"span-{uuid4().hex}",
                step_index=self.step,
                name=call.tool_name,
                arguments_json=encoded(args),
                tool_hash=spec.fingerprint,
                provider_call_id=call.tool_call_id,
            )
            context = self.context()
            admission = await self.executor.admit(
                invocation, context, timeout_seconds=context.timeout,
            )
            identity = None
            if spec.risk != "read":
                identity = OperationIdentity(
                    operation_id=f"op-{uuid4().hex}",
                    call_id=invocation.call_id,
                    environment=self.environment,
                    ownership=self.ownership,
                    request_digest=fingerprint(
                        {"name": invocation.name, "args": args, "tool_hash": spec.fingerprint}
                    ),
                )
            entry = _Entry(
                invocation,
                replace(admission, start_emitted=True),
                identity,
                asyncio.Event(),
                predecessor,
            )
            prepared[call.tool_call_id] = entry
            predecessor = entry.gate
        # Only after every admission succeeds do tool_start events become visible.
        for entry in prepared.values():
            await self.store.append(
                self.run_id,
                "tool_start",
                ToolObservation(
                    call_id=entry.invocation.call_id,
                    provider_call_id=entry.invocation.provider_call_id,
                    name=entry.invocation.name, step_index=entry.invocation.step_index,
                    span_id=entry.invocation.span_id,
                    arguments=cast(dict[str, JsonValue],
                                   parse_json(entry.invocation.arguments_json)),
                    status="unknown", decision="deny", executor_started=False,
                    result_available=False,
                ).model_dump(mode="json"),
            )
        self.entries.update(prepared)
        await self.check_stop()

    async def execute(self, entry: _Entry) -> JsonValue:
        await self.check_stop()
        identity = entry.identity
        if identity is not None and entry.admission.decision != "deny":
            receipt = await self.operations.lookup(identity)
            if receipt is None:
                await self.check_stop()
                admission = await self.operations.admit(identity)
                receipt = admission.receipt
                execute = admission.execute
            else:
                execute = False
            if receipt.identity != identity:
                raise ValueError("operation receipt binding changed")
            if not execute:
                if receipt.result_ref is not None and receipt.state == "completed":
                    if self.host is not None:
                        return await self.host.read_receipt(receipt)
                    return await self.read_result(receipt.result_ref)
                return {
                    "status": "unknown",
                    "operation_id": identity.operation_id,
                    "state": receipt.state,
                    "code": "operation_not_replayed",
                }
            await self.check_stop()
        token = CURRENT_OPERATION.set(identity)
        try:
            observation = await self.executor.execute(
                entry.invocation, self.context(), entry.admission,
            )
        finally:
            CURRENT_OPERATION.reset(token)
        # Existing RunStore tool_end is the result record; the C0 receipt stores
        # only its reference. A crash before this receipt leaves accepted/unknown,
        # which lookup will not dispatch again.
        if identity is not None and entry.admission.decision != "deny":
            await self.operations.save_receipt(
                OperationReceipt(
                    identity=identity,
                    state="completed" if observation.status == "ok" else "unknown",
                    result_ref=entry.invocation.call_id if observation.status == "ok" else None,
                    error_code=observation.code,
                )
            )
        await self.check_stop()
        return observation.model_dump(mode="json")

    async def commit_response(
        self,
        history: list[messages.Message],
        complete: messages.Message,
    ) -> None:
        if complete.tool_calls:
            checkpoint = self.checkpoint([*history, complete])
            await self.store.write_checkpoint(
                self.run_id, checkpoint,
                fingerprint(checkpoint) if self.host is not None else self.tool_fingerprint,
            )
        await self.check_stop()

    def checkpoint(self, history: Sequence[messages.Message]) -> dict[str, Any]:
        return {
            "format": self.CHECKPOINT,
            "response_complete": True,
            "tool_fingerprint": self.tool_fingerprint,
            "environment": self.environment.model_dump(mode="json"),
            "ownership": self.ownership.model_dump(mode="json"),
            "step": self.step,
            "budget_remaining": self.host.budget.remaining if self.host is not None else None,
            "host_state": self.host.checkpoint_state() if self.host is not None else None,
            "messages": [message.model_dump(mode="json") for message in history],
            "entries": {
                call_id: {
                    "invocation": asdict(entry.invocation),
                    "identity": entry.identity.model_dump(mode="json") if entry.identity else None,
                    "decision": entry.admission.decision,
                    "code": entry.admission.code,
                }
                for call_id, entry in self.entries.items()
            },
        }

    def restore(
        self, checkpoint: dict[str, Any], stored_fingerprint: str
    ) -> list[messages.Message]:
        if (
            checkpoint.get("format") != self.CHECKPOINT
            or checkpoint.get("response_complete") is not True
            or checkpoint.get("tool_fingerprint", self.tool_fingerprint) != self.tool_fingerprint
            or stored_fingerprint != (
                fingerprint(checkpoint) if self.host is not None else self.tool_fingerprint
            )
            or checkpoint.get("environment") != self.environment.model_dump(mode="json")
            or checkpoint.get("ownership") != self.ownership.model_dump(mode="json")
        ):
            raise ValueError("checkpoint binding changed")
        self.step = checkpoint["step"]
        if self.host is not None:
            state = checkpoint.get("host_state")
            if not isinstance(state, dict):
                raise ValueError("checkpoint lost host counters")
            self.host.restore_state(state)
        self.entries.clear()
        predecessor: asyncio.Event | None = None
        for call_id, item in checkpoint["entries"].items():
            invocation = Invocation(**item["invocation"])
            spec = self.executor.registry.get(invocation.name)
            if spec.fingerprint != invocation.tool_hash or invocation.run_id != self.run_id:
                raise ValueError("tool binding changed")
            parsed = spec.parameters.model_validate(
                parse_json(invocation.arguments_json), strict=True
            )
            identity = (
                OperationIdentity.model_validate(item["identity"]) if item["identity"] else None
            )
            if (spec.risk != "read") != (identity is not None):
                raise ValueError("checkpoint lost operation identity")
            if identity is not None and (
                identity.environment != self.environment
                or identity.ownership != self.ownership
                or identity.call_id != invocation.call_id
                or identity.request_digest
                != fingerprint(
                    {
                        "name": invocation.name,
                        "args": parse_json(invocation.arguments_json),
                        "tool_hash": spec.fingerprint,
                    }
                )
            ):
                raise ValueError("checkpoint operation binding changed")
            admission = Admission(spec, parsed, item["decision"], item["code"], start_emitted=True)
            self.entries[call_id] = _Entry(
                invocation, admission, identity, asyncio.Event(), predecessor
            )
            predecessor = self.entries[call_id].gate
        history = [messages.Message.model_validate(item) for item in checkpoint["messages"]]
        # Only the final incomplete assistant batch will be replayed by the SDK.
        pending: set[str] = set()
        for message in history[-2:]:
            if message.role != "assistant":
                continue
            for part in message.tool_calls:
                entry = self.entries.get(part.tool_call_id)
                if (
                    entry is None
                    or entry.invocation.name != part.tool_name
                    or parse_json(entry.invocation.arguments_json) != parse_json(part.tool_args)
                ):
                    raise ValueError("checkpoint message binding changed")
                pending.add(part.tool_call_id)
        previous: asyncio.Event | None = None
        for call_id, entry in self.entries.items():
            if call_id in pending:
                entry.predecessor = previous
                previous = entry.gate
            else:
                entry.gate.set()
        return history

    async def append_approval_request(self, hook: messages.HookPart[Any]) -> None:
        """Use the host's existing approval event and verify its invocation binding."""
        entry = self.entries.get(hook.tool_call_id or "")
        if entry is None:
            raise ValueError("approval hook binding changed")
        invocation = entry.invocation
        if (
            hook.metadata.get("call_id") != invocation.call_id
            or hook.metadata.get("tool") != invocation.name
        ):
            raise ValueError("approval hook binding changed")
        await self.check_stop()
        remaining = self.host.budget.remaining if self.host is not None else None
        data = await self.approval_request(invocation, remaining)
        expected = {
            "run_id": invocation.run_id,
            "call_id": invocation.call_id,
            "step_index": invocation.step_index,
            "name": invocation.name,
            "tool_hash": invocation.tool_hash,
            "arguments_hash": fingerprint(parse_json(invocation.arguments_json)),
        }
        if not isinstance(data, dict) or any(
            data.get(key) != value for key, value in expected.items()
        ):
            raise ValueError("approval request binding changed")
        if fingerprint(data.get("arguments")) != expected["arguments_hash"]:
            raise ValueError("approval request arguments changed")
        expires = data.get("expires_at")
        if (
            not isinstance(data.get("request_id"), str)
            or not data["request_id"]
            or isinstance(expires, bool)
            or not isinstance(expires, (int, float))
            or not math.isfinite(expires)
            or "budget_remaining" not in data
        ):
            raise ValueError("incomplete host approval request")
        await self.check_stop()
        await self.store.append(
            self.run_id,
            "approval_request",
            {
                **data,
                "hook_id": hook.hook_id,
            },
        )
        await self.check_stop()

    async def run(
        self,
        model: ai.Model,
        history: list[messages.Message] | None = None,
        *,
        resume: bool = False,
        replies: Mapping[str, bool] | None = None,
    ) -> ProbeResult:
        """Run the SDK, deferring approval to a durable checkpoint when necessary.

        replies map *host call IDs* to pre-authorized decisions. The application
        must use its existing reply_approval binding/expiry checks before here.
        """
        await self.check_stop()
        if resume:
            saved = await self.store.read_checkpoint(self.run_id)
            if saved is None:
                raise ValueError("missing checkpoint")
            history = self.restore(*saved)
            if self.host is None:
                await self.store.set_running(self.run_id)
            elif (await self.store.snapshot(self.run_id)).run.status != "running":
                raise ProbeStopped("host has not claimed the run")
        if history is None:
            raise ValueError("missing history")
        self.deferred = None
        registry = HookRegistry()
        for call_id, granted in (replies or {}).items():
            ai.resolve_hook(f"approve_{call_id}", {"granted": granted}, registry=registry)
        model = ai.Model(
            id=model.id,
            provider=CompleteResponseProvider(
                model,
                self.prepare,
                commit=self.commit_response,
                guard=self.check_stop,
            ),
        )
        collected: list[events.AgentEvent] = []
        async with self.agent.run(model, history, hook_registry=registry) as stream:
            async for event in stream:
                collected.append(event)
                if isinstance(event, events.HookEvent) and event.hook.status == "pending":
                    await self.append_approval_request(event.hook)
                    ai.defer_hook(event.hook, registry=registry)
            current = list(stream.messages)
        await self.check_stop()
        if self.deferred is not None:
            checkpoint = self.checkpoint(current)
            await self.store.write_checkpoint(
                self.run_id, checkpoint,
                fingerprint(checkpoint) if self.host is not None else self.tool_fingerprint,
            )
            if self.host is None and not await self.store.set_awaiting(self.run_id):
                raise ProbeStopped("stop won awaiting transition")
            await self.check_stop()
            return ProbeResult("awaiting", current, collected)
        return ProbeResult("completed", current, collected)
