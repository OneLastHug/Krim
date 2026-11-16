"""Narrow, in-process SDK probe using the existing executor and RunStore ports.

Agent.loop, ToolRunner, schema validation, tool results, hook events and replay
are owned by ai==0.9.0. This module only adds response admission, ordered host
execution, durable operation lookup and the host's park/stop fences.
"""

from __future__ import annotations

import asyncio
import math
import time
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

from krim_agent.contracts import (
    TERMINAL,
    ServiceError,
    ToolObservation,
    encoded,
    fingerprint,
    parse_json,
)
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

from .host import CURRENT_OPERATION, HostExecution, sdk_lifetime
from .media import DEFAULT_MEDIA_LIMITS, MediaLimits, authorize_history
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
    predecessors: tuple[asyncio.Event, ...] = ()


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
            for predecessor in entry.predecessors:
                await predecessor.wait()
            try:
                await owner.check_stop()
                if owner.deferred and not owner.batch_approvals:
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
    """One host run bridged to the pinned SDK's native loop and ToolRunner.

    Development writes use the existing OperationStore. Other targets use the
    host's durable intent/result events. Neither path re-executes uncertain work.
    Approval replies supplied to run() must already be authorized by the host.
    """

    CHECKPOINT = "sdk-probe-v1"

    def __init__(
        self,
        *,
        store: RunStore,
        operations: OperationStore | None = None,
        executor: ToolExecutor,
        tools: Sequence[ai.AgentTool],
        environment: EnvironmentHandle | None = None,
        ownership: RunOwnership | None = None,
        run_id: str | None = None,
        read_parallelism: int = 1,
        batch_approvals: bool = False,
        fail_on_tool_error: bool = False,
        read_result: Callable[[str], Awaitable[JsonValue]],
        approval_request: Callable[[Invocation, float | None], Awaitable[dict[str, JsonValue]]],
        host: HostExecution | None = None,
        authorize_media: Callable[[messages.FilePart], Awaitable[None]] | None = None,
        media_limits: MediaLimits = DEFAULT_MEDIA_LIMITS,
    ) -> None:
        if operations is not None:
            if (environment is None or ownership is None
                    or environment.owner_id != ownership.owner_id):
                raise ValueError("environment ownership mismatch")
            if run_id is not None and run_id != ownership.run_id:
                raise ValueError("run ownership mismatch")
            run_id = ownership.run_id
        elif environment is not None or ownership is not None or host is None:
            raise ValueError("generic SDK runs require a host and no development identity")
        if not run_id or read_parallelism < 1:
            raise ValueError("missing run or invalid read parallelism")
        self._run_id = run_id
        self.read_parallelism = read_parallelism
        self.batch_approvals = batch_approvals
        self.fail_on_tool_error = fail_on_tool_error
        self.batch_expires: float | None = None
        self.store, self.operations, self.executor = store, operations, executor
        self.environment, self.ownership = environment, ownership
        self.read_result = read_result
        self.approval_request = approval_request
        self.host = host
        self.authorize_media, self.media_limits = authorize_media, media_limits
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
        return self._run_id

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

    def order(self, entries: Sequence[_Entry]) -> None:
        """Fence native SDK tasks in bounded adjacent read groups.

        SDK ToolRunner still creates, joins and cancels every task. These gates
        express host ordering only; they do not execute or schedule a tool.
        """
        plan = list(entries)
        if self.batch_approvals:
            # Legacy host batches execute all allowed calls before opening the
            # common approval window. A first ask must not consume that window
            # while later allowed work is still running.
            plan = [entry for entry in entries if entry.admission.decision != "ask"]
            plan += [entry for entry in entries if entry.admission.decision == "ask"]
        previous: tuple[asyncio.Event, ...] = ()
        index = 0
        while index < len(plan):
            group = [plan[index]]
            while (len(group) < self.read_parallelism
                   and index + len(group) < len(plan)
                   and group[-1].admission.spec.risk == "read"
                   and plan[index + len(group)].admission.spec.risk == "read"
                   and group[-1].admission.decision == "allow"
                   and plan[index + len(group)].admission.decision == "allow"):
                group.append(plan[index + len(group)])
            for entry in group:
                entry.predecessors = previous
            previous = tuple(entry.gate for entry in group)
            index += len(group)

    async def prepare(self, calls: Sequence[messages.ToolCallPart]) -> None:
        await self.check_stop()
        self.step = self.host.model_step() if self.host is not None else self.step + 1
        prepared: dict[str, _Entry] = {}
        self.batch_expires = None
        for call in calls:
            if call.tool_call_id in self.entries:
                raise ValueError("provider reused a call id")
            if call.tool_name not in self.tool_names:
                if self.operations is None:
                    raise ServiceError("unknown_tool")
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
            if spec.risk != "read" and self.operations is not None:
                assert self.environment is not None and self.ownership is not None
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
            )
            prepared[call.tool_call_id] = entry
        self.order(list(prepared.values()))
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
            assert self.operations is not None
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
        if self.operations is None:
            # The existing event ledger is the operation fence for non-development
            # targets. An intent or any terminal observation forbids dispatch on
            # resume, including an executor that crashed before recording output.
            snapshot = await self.store.snapshot(self.run_id)
            prior = [event for event in snapshot.events
                     if event.data.get("call_id") == entry.invocation.call_id
                     and event.kind in {"tool_intent", "tool_end"}]
            if prior:
                last = prior[-1]
                if last.kind == "tool_end":
                    observed = ToolObservation.model_validate(last.data)
                    if (observed.name != entry.invocation.name
                            or observed.provider_call_id != entry.invocation.provider_call_id
                            or observed.step_index != entry.invocation.step_index):
                        raise ServiceError("facts_corrupt")
                    if observed.status == "ok" and observed.result_available is True:
                        return observed.model_dump(mode="json")
                raise ServiceError("facts_corrupt")
        if self.operations is None and entry.admission.decision == "deny":
            # A denied legacy batch item never entered execute/wrap. Preserve
            # that boundary, including an explicit rejected approval observation.
            invocation = entry.invocation
            observation = ToolObservation(
                call_id=invocation.call_id, provider_call_id=invocation.provider_call_id,
                span_id=invocation.span_id, step_index=invocation.step_index,
                name=invocation.name,
                arguments=cast(dict[str, JsonValue], parse_json(invocation.arguments_json)),
                status="denied", executor_started=False, result_available=False,
                decision="ask" if entry.admission.code == "approval_denied" else "deny",
                code=entry.admission.code or "permission_denied",
            )
            await self.context().emit("tool_end", observation.model_dump(mode="json"))
            await self.check_stop()
            return observation.model_dump(mode="json")
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
            assert self.operations is not None
            await self.operations.save_receipt(
                OperationReceipt(
                    identity=identity,
                    state="completed" if observation.status == "ok" else "unknown",
                    result_ref=entry.invocation.call_id if observation.status == "ok" else None,
                    error_code=observation.code,
                )
            )
        await self.check_stop()
        if (self.fail_on_tool_error and observation.status != "ok"
                and entry.admission.decision != "deny"):
            raise ServiceError(observation.code or "tool_error")
        return observation.model_dump(mode="json")

    async def prepare_input(self, history: Sequence[messages.Message]) -> list[messages.Message]:
        await self.check_stop()
        prepared = await authorize_history(history, self.authorize_media, self.media_limits)
        await self.check_stop()
        return prepared

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
            "format": self.CHECKPOINT if self.operations is not None else "sdk-host-v2",
            "run_id": self.run_id,
            "response_complete": True,
            "tool_fingerprint": self.tool_fingerprint,
            "environment": self.environment.model_dump(mode="json") if self.environment else None,
            "ownership": self.ownership.model_dump(mode="json") if self.ownership else None,
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
            checkpoint.get("format") != (
                self.CHECKPOINT if self.operations is not None else "sdk-host-v2"
            )
            or checkpoint.get("run_id", self.run_id) != self.run_id
            or checkpoint.get("response_complete") is not True
            or checkpoint.get("tool_fingerprint", self.tool_fingerprint) != self.tool_fingerprint
            or stored_fingerprint != (
                fingerprint(checkpoint) if self.host is not None else self.tool_fingerprint
            )
            or checkpoint.get("environment") != (
                self.environment.model_dump(mode="json") if self.environment else None
            )
            or checkpoint.get("ownership") != (
                self.ownership.model_dump(mode="json") if self.ownership else None
            )
        ):
            raise ValueError("checkpoint binding changed")
        self.step = checkpoint["step"]
        if self.host is not None:
            state = checkpoint.get("host_state")
            if not isinstance(state, dict):
                raise ValueError("checkpoint lost host counters")
            self.host.restore_state(state)
        self.entries.clear()
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
            if (spec.risk != "read" and self.operations is not None) != (identity is not None):
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
                invocation, admission, identity, asyncio.Event()
            )
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
        self.order([entry for call_id, entry in self.entries.items() if call_id in pending])
        for call_id, entry in self.entries.items():
            if call_id not in pending:
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
        expires_at = data.get("expires_at") if isinstance(data, dict) else None
        if self.batch_approvals and isinstance(expires_at, (float, int)):
            if self.batch_expires is None:
                self.batch_expires = min(
                    expires_at, time.time() + self.context().approval_timeout
                )
            data["expires_at"] = min(expires_at, self.batch_expires)
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
        # SDK replay bypasses the provider; authorize before entering Agent.run
        # as well as at each real model request. No checkpoint implies permission.
        history = await self.prepare_input(history)
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
                prepare_input=self.prepare_input,
            ),
        )
        collected: list[events.AgentEvent] = []
        async with sdk_lifetime(), self.agent.run(model, history, hook_registry=registry) as stream:
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
