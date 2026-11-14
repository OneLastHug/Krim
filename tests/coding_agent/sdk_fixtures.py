"""Real ai 0.9.0 with a local scripted provider; no model network requests."""

from __future__ import annotations

import asyncio
import copy
import json
import time
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import ai
from ai import events, messages
from pydantic import PrivateAttr

from krim_agent.agent_sdk import SDKAdapter
from krim_agent.contracts import RunEvent, ServiceError, StrictModel, fingerprint, parse_json
from krim_agent.development.contracts import (
    EnvironmentHandle,
    OperationAdmission,
    OperationReceipt,
    RunOwnership,
)
from krim_agent.tools.core import ToolSource, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry


class Scripted(ai.Provider):
    provider_class_id: str = "krim-sdk-adapter-test-script"
    name: str = "scripted"
    default_base_url: str = "http://unused.invalid"
    _turns: list[Any] = PrivateAttr()
    _seen: list[Any] = PrivateAttr(default_factory=list)
    _schemas: list[Any] = PrivateAttr(default_factory=list)

    def __init__(self, turns: list[Any]) -> None:
        super().__init__()
        self._turns = turns

    async def stream(self, model: Any, history: Any, **kwargs: Any) -> AsyncGenerator[Any]:
        self._seen.append(copy.deepcopy(history))
        self._schemas.append(kwargs.get("tools"))
        index = len(self._seen) - 1
        assert index < len(self._turns), "unexpected extra model call"
        turn = self._turns[index]
        if callable(turn):
            async for event in turn():
                yield event
        else:
            for event in turn:
                if isinstance(event, Exception):
                    raise event
                yield event


def model(provider: Scripted) -> ai.Model:
    return ai.Model(id="local-script", provider=provider)


def tool_events(name: str, call_id: str, value: Any = 1) -> list[Any]:
    raw = json.dumps({"value": value})
    part = messages.ToolCallPart(tool_call_id=call_id, tool_name=name, tool_args=raw)
    return [
        events.ToolStart(tool_call_id=call_id, tool_name=name),
        events.ToolDelta(tool_call_id=call_id, chunk=raw),
        events.ToolEnd(tool_call_id=call_id, tool_call=part),
    ]


def turn(*calls: tuple[str, str, Any], finish: str = "tool_call") -> list[Any]:
    result: list[Any] = [events.StreamStart()]
    for args in calls:
        result.extend(tool_events(*args))
    result.append(events.StreamEnd(finish_reason=finish))
    return result


def done() -> list[Any]:
    return [
        events.StreamStart(),
        events.TextStart(block_id="text"),
        events.TextDelta(block_id="text", chunk="done"),
        events.TextEnd(block_id="text"),
        events.StreamEnd(finish_reason="stop"),
    ]


class MemoryRunStore:
    """Test double for existing RunStore; not a replacement storage backend."""

    def __init__(self) -> None:
        self.stop_requested = False
        self.status = "running"
        self.events: list[RunEvent] = []
        self.saved: Any = None
        self.stop_at_awaiting = False
        self.stop_at_running = False
        self.stop_at_intent = False
        self.trace: list[str] = []

    async def snapshot(self, run_id: str) -> Any:
        return SimpleNamespace(
            run=SimpleNamespace(stop_requested=self.stop_requested, status=self.status)
        )

    async def append(self, run_id: str, kind: str, data: Any, **kwargs: Any) -> RunEvent:
        if kind == "tool_intent" and self.stop_at_intent:
            self.stop_requested = True
        event = RunEvent(
            run_id=run_id,
            seq=len(self.events),
            event_id=f"event-{len(self.events)}",
            kind=kind,
            data=data,
        )
        self.events.append(event)
        self.trace.append(kind)
        return event

    async def write_checkpoint(self, run_id: str, checkpoint: Any, fingerprint: str) -> None:
        self.saved = (copy.deepcopy(checkpoint), fingerprint)

    async def read_checkpoint(self, run_id: str) -> Any:
        return copy.deepcopy(self.saved)

    async def set_awaiting(self, run_id: str) -> bool:
        if self.stop_at_awaiting:
            self.stop_requested = True
        if self.stop_requested:
            return False
        self.status = "awaiting"
        return True

    async def set_running(self, run_id: str) -> None:
        if self.stop_at_running:
            self.stop_requested = True
        if self.stop_requested:
            raise ServiceError("cancelled")
        if self.status not in {"queued", "awaiting"}:
            raise ServiceError("run_closed")
        self.status = "running"

    async def request_stop(self, run_id: str) -> None:
        self.stop_requested = True

    async def read_result(self, result_ref: str) -> Any:
        return next(
            copy.deepcopy(event.data)
            for event in reversed(self.events)
            if event.kind == "tool_end" and event.data["call_id"] == result_ref
        )


class MemoryOperations:
    """Atomic test contract implementation; shared production storage remains I's."""

    def __init__(self) -> None:
        self.receipts: dict[str, OperationReceipt] = {}
        self.trace: list[str] = []
        self.on_lookup: Any = None

    async def lookup(self, identity: Any) -> Any:
        self.trace.append("lookup")
        if self.on_lookup is not None:
            await self.on_lookup()
        receipt = self.receipts.get(identity.operation_id)
        if receipt is not None and receipt.identity != identity:
            raise ValueError("binding changed")
        return receipt

    async def admit(self, identity: Any) -> Any:
        self.trace.append("admit")
        if identity.operation_id in self.receipts:
            return OperationAdmission(receipt=self.receipts[identity.operation_id], execute=False)
        receipt = OperationReceipt(identity=identity, state="accepted")
        self.receipts[identity.operation_id] = receipt
        return OperationAdmission(receipt=receipt, execute=True)

    async def save_receipt(self, receipt: Any) -> None:
        self.trace.append("save_receipt")
        self.receipts[receipt.identity.operation_id] = receipt


class Args(StrictModel):
    value: int


@ai.tool
async def read(value: int) -> Any:
    return value


@ai.tool
async def write(value: int) -> Any:
    return value


def setup(*, approval: bool = True) -> Any:
    store, operations = MemoryRunStore(), MemoryOperations()
    trace: list[str] = []
    counts = {"read": 0, "write": 0}

    async def execute_read(args: Args) -> Any:
        trace.append("read")
        counts["read"] += 1
        await asyncio.sleep(0)
        return {"value": args.value}

    async def unavailable(args: Args) -> Any:
        raise AssertionError("external executor must be used")

    async def execute_write(invocation: Any, args: Args) -> Any:
        trace.append("write")
        counts["write"] += 1
        await asyncio.sleep(0)
        return {"value": args.value}

    async def policy(invocation: Any) -> str:
        trace.append(f"admit:{invocation.name}")
        return "ask" if invocation.name == "write" and approval else "allow"

    schema = {
        "type": "object",
        "properties": {"value": {"type": "integer"}},
        "required": ["value"],
        "additionalProperties": False,
    }
    specs = (
        ToolSpec("read", Args, execute_read, schema, "1", "a" * 64),
        ToolSpec(
            "write",
            Args,
            unavailable,
            schema,
            "1",
            "b" * 64,
            risk="write",
            external_executor=execute_write,
            source=ToolSource("mcp", "fixture", "write", "c" * 64),
        ),
    )
    executor = ToolExecutor(ToolRegistry(specs), policy)
    environment = EnvironmentHandle(
        owner_id="owner", project_id="project", environment_id="env", generation=1, state="ready"
    )
    ownership = RunOwnership(owner_id="owner", session_id="session", run_id="run")
    approval_calls: list[Any] = []

    async def approval_request(invocation: Any, budget_remaining: float | None) -> Any:
        data = {
            "request_id": f"approval-{invocation.call_id}",
            "run_id": invocation.run_id,
            "call_id": invocation.call_id,
            "step_index": invocation.step_index,
            "arguments_hash": fingerprint(parse_json(invocation.arguments_json)),
            "tool_hash": invocation.tool_hash,
            "arguments": parse_json(invocation.arguments_json),
            "name": invocation.name,
            "expires_at": time.time() + 300,
            "budget_remaining": budget_remaining,
            "risk": "write",
            "source": {"kind": "mcp", "server": "fixture"},
        }
        approval_calls.append(copy.deepcopy(data))
        return data

    def adapter() -> SDKAdapter:
        return SDKAdapter(
            store=store,
            operations=operations,
            executor=executor,
            tools=[read, write],
            environment=environment,
            ownership=ownership,
            read_result=store.read_result,
            approval_request=approval_request,
        )

    return SimpleNamespace(
        store=store,
        operations=operations,
        trace=trace,
        counts=counts,
        adapter=adapter,
        environment=environment,
        ownership=ownership,
        approval_calls=approval_calls,
    )
