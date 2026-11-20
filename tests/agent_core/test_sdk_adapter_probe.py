"""Real ai 0.9.0 with a local scripted provider; no model network requests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import replace
from typing import Any

import ai
import pytest
from ai import events, messages

from krim_agent.agent_sdk import ProbeStopped
from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import (
    OperationReceipt,
)
from tests.coding_agent.sdk_fixtures import (
    Args,
    Scripted,
    done,
    model,
    setup,
    turn,
)


@pytest.mark.asyncio
async def test_bare_sdk_dispatches_before_response_finishes() -> None:
    started, release = asyncio.Event(), asyncio.Event()

    @ai.tool
    async def effect(value: int) -> int:
        started.set()
        return value

    async def broken() -> AsyncGenerator[Any]:
        for event in turn(("effect", "native-call", 1))[:-1]:
            yield event
        await release.wait()
        raise RuntimeError("stream failed after ToolEnd")

    provider = Scripted([broken])

    async def consume() -> None:
        async with ai.Agent(tools=[effect]).run(model(provider), [ai.user_message("go")]) as stream:
            async for _ in stream:
                pass

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        assert not task.done()  # tool ran while the provider was still unfinished
    finally:
        release.set()
        with pytest.raises(ExceptionGroup):
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    ["missing_end", "after_tool", "after_end", "length", "content_filter", "error", "other"],
)
async def test_response_failure_never_executes_write(failure: str) -> None:
    harness = setup(approval=False)
    sequence = turn(("write", "write-1", 1))
    if failure == "missing_end":
        sequence.pop()
    elif failure == "after_tool":
        sequence[-1] = RuntimeError("broken stream")
    elif failure == "after_end":
        sequence.append(RuntimeError("failure after finish marker"))
    else:
        sequence[-1] = events.StreamEnd(finish_reason=failure)
    provider = Scripted([sequence])
    with pytest.raises(ExceptionGroup):
        await harness.adapter().run(model(provider), [ai.user_message("go")])
    assert harness.counts == {"read": 0, "write": 0}
    assert harness.operations.trace == []


@pytest.mark.asyncio
async def test_response_gate_waits_until_eof() -> None:
    harness = setup(approval=False)
    yielded_end, release = asyncio.Event(), asyncio.Event()

    async def delayed() -> AsyncGenerator[Any]:
        for event in turn(("write", "write-1", 1)):
            yield event
        yielded_end.set()
        await release.wait()

    provider = Scripted([delayed, done()])
    task = asyncio.create_task(harness.adapter().run(model(provider), [ai.user_message("go")]))
    await asyncio.wait_for(yielded_end.wait(), timeout=3)
    await asyncio.sleep(0)
    assert harness.counts["write"] == 0
    release.set()
    result = await asyncio.wait_for(task, timeout=3)
    assert result.status == "completed"
    assert harness.counts["write"] == 1


@pytest.mark.asyncio
async def test_real_sdk_read_approval_resume_and_results() -> None:
    harness = setup()
    provider = Scripted([turn(("read", "read-1", 1)), turn(("write", "write-1", 2)), done()])
    first = await asyncio.wait_for(
        harness.adapter().run(model(provider), [ai.user_message("go")]), 5
    )
    assert first.status == "awaiting"
    assert harness.counts == {"read": 1, "write": 0}
    assert len(provider._seen) == 2
    assert any(isinstance(event, events.HookEvent) for event in first.events)
    assert any(isinstance(event, events.ToolCallResult) for event in first.events)
    approval = next(event for event in harness.store.events if event.kind == "approval_request")
    second = await asyncio.wait_for(
        harness.adapter().run(
            model(provider), resume=True, replies={approval.data["call_id"]: True}
        ),
        5,
    )
    assert second.status == "completed"
    assert len(provider._seen) == 3  # resume did not call the model for pending write
    assert harness.counts == {"read": 1, "write": 1}
    assert harness.operations.trace == ["lookup", "admit", "save_receipt"]
    assert provider._schemas[0][0].spec.params == Args.model_json_schema()
    results = [part for message in provider._seen[-1] for part in message.tool_results]
    assert len(results) == 2 and all(result.result["status"] == "ok" for result in results)
    assert len(harness.approval_calls) == 1  # approved replay neither regenerates nor renews it
    expected = harness.approval_calls[0]
    assert approval.data == {**expected, "hook_id": f"approve_{expected['call_id']}"}
    assert expected["budget_remaining"] is None
    assert expected["request_id"] and expected["expires_at"] > approval.at


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "another-run"),
        ("call_id", "another-call"),
        ("tool_hash", "0" * 64),
        ("name", "read"),
        ("arguments_hash", "0" * 64),
        ("arguments", {"value": 99}),
        ("request_id", None),
        ("expires_at", "invalid"),
        ("budget_remaining", "missing"),
    ],
)
async def test_invalid_host_approval_is_rejected_before_append(field: str, value: Any) -> None:
    harness = setup()
    adapter = harness.adapter()
    host_request = adapter.approval_request

    async def invalid_request(invocation: Any, budget_remaining: float | None) -> Any:
        data = await host_request(invocation, budget_remaining)
        if field == "budget_remaining":
            data.pop(field)
        else:
            data[field] = value
        return data

    adapter.approval_request = invalid_request
    provider = Scripted([turn(("write", "write-1", 1))])
    with pytest.raises(ValueError, match="approval request"):
        await asyncio.wait_for(adapter.run(model(provider), [ai.user_message("go")]), 5)
    assert not any(event.kind == "approval_request" for event in harness.store.events)
    assert harness.store.status == "running"
    assert harness.counts["write"] == 0 and harness.operations.trace == []


@pytest.mark.asyncio
async def test_stop_while_host_builds_approval_prevents_append() -> None:
    harness = setup()
    adapter = harness.adapter()
    host_request = adapter.approval_request

    async def stopped_request(invocation: Any, budget_remaining: float | None) -> Any:
        data = await host_request(invocation, budget_remaining)
        await harness.store.request_stop("run")
        return data

    adapter.approval_request = stopped_request
    provider = Scripted([turn(("write", "write-1", 1))])
    with pytest.raises(ProbeStopped):
        await asyncio.wait_for(adapter.run(model(provider), [ai.user_message("go")]), 5)
    assert not any(event.kind == "approval_request" for event in harness.store.events)
    assert harness.counts["write"] == 0


@pytest.mark.asyncio
async def test_denial_returns_result_without_executing_write() -> None:
    harness = setup()
    provider = Scripted([turn(("write", "write-1", 1)), done()])
    await harness.adapter().run(model(provider), [ai.user_message("go")])
    approval = harness.store.events[-1]
    result = await harness.adapter().run(
        model(provider), resume=True, replies={approval.data["call_id"]: False}
    )
    assert result.status == "completed"
    assert harness.counts["write"] == 0 and harness.operations.trace == []
    observed = [event for event in harness.store.events if event.kind == "tool_end"]
    assert observed[-1].data["code"] == "approval_denied"


@pytest.mark.asyncio
@pytest.mark.parametrize("moment", ["before", "awaiting", "resume", "after_approval"])
async def test_stop_wins_approval_and_awaiting(moment: str) -> None:
    harness = setup()
    provider = Scripted([turn(("write", "write-1", 1)), done()])
    if moment == "before":
        await harness.store.request_stop("run")
    elif moment == "awaiting":
        harness.store.stop_at_awaiting = True
    if moment in {"before", "awaiting"}:
        with pytest.raises(ProbeStopped):
            await harness.adapter().run(model(provider), [ai.user_message("go")])
    else:
        await harness.adapter().run(model(provider), [ai.user_message("go")])
        approval = harness.store.events[-1]
        if moment == "resume":
            harness.store.stop_at_running = True
        else:
            harness.operations.on_lookup = lambda: harness.store.request_stop("run")
        with pytest.raises(ServiceError if moment == "resume" else ExceptionGroup):
            await harness.adapter().run(
                model(provider), resume=True, replies={approval.data["call_id"]: True}
            )
    assert harness.counts["write"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["accepted", "running", "unknown", "completed"])
async def test_recovery_queries_operation_instead_of_reexecuting(state: str) -> None:
    harness = setup(approval=False)
    adapter = harness.adapter()
    part = messages.ToolCallPart(
        tool_call_id="first-sdk-id", tool_name="write", tool_args='{"value":1}'
    )
    await adapter.prepare([part])
    entry = adapter.entries["first-sdk-id"]
    assert entry.identity is not None
    if state == "completed":
        await harness.store.append(
            "run",
            "tool_end",
            {"call_id": entry.invocation.call_id, "status": "ok", "value": {"value": 1}},
        )
    harness.operations.receipts[entry.identity.operation_id] = OperationReceipt(
        identity=entry.identity,
        state=state,
        result_ref=entry.invocation.call_id if state == "completed" else None,
    )
    # An SDK id is just the wire correlation. Persisted host invocation and
    # operation identities remain unchanged even when correlation is remapped.
    entry.invocation = replace(entry.invocation, provider_call_id="different-sdk-id")
    result = await adapter.execute(entry)
    assert harness.operations.trace == ["lookup"]
    assert harness.counts["write"] == 0
    assert result["status"] == ("ok" if state == "completed" else "unknown")


@pytest.mark.asyncio
async def test_batch_admits_every_call_before_ordered_execution() -> None:
    harness = setup(approval=False)
    provider = Scripted(
        [turn(("read", "one", 1), ("write", "two", 2), ("read", "three", 3)), done()]
    )
    await harness.adapter().run(model(provider), [ai.user_message("go")])
    trace = harness.trace
    first_dispatch = next(index for index, item in enumerate(trace) if item in {"read", "write"})
    initial_admissions = ["admit:read", "admit:write", "admit:read"]
    assert trace[:3] == initial_admissions
    assert all(item.startswith("admit:") for item in trace[:first_dispatch])
    assert [item for item in trace if item in {"read", "write"}] == ["read", "write", "read"]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["invalid_args", "unknown_tool", "duplicate_id"])
async def test_bad_later_call_prevents_earlier_write(bad: str) -> None:
    harness = setup(approval=False)
    second = (
        ("read", "two", "not-an-int")
        if bad == "invalid_args"
        else ("not_registered", "two", 2)
        if bad == "unknown_tool"
        else ("read", "one", 2)
    )
    provider = Scripted([turn(("write", "one", 1), second)])
    with pytest.raises(ExceptionGroup):
        await harness.adapter().run(model(provider), [ai.user_message("go")])
    assert harness.counts["write"] == 0 and not harness.operations.trace


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "interrupted", "failed", "aborted"])
async def test_terminal_run_never_requests_model_or_executes(status: str) -> None:
    harness = setup(approval=False)
    harness.store.status = status
    provider = Scripted([turn(("write", "write-1", 1)), done()])
    with pytest.raises(ProbeStopped):
        await harness.adapter().run(model(provider), [ai.user_message("go")])
    assert provider._seen == []
    assert harness.counts["write"] == 0


@pytest.mark.asyncio
async def test_stop_committed_during_tool_intent_prevents_executor() -> None:
    harness = setup(approval=False)
    harness.store.stop_at_intent = True
    provider = Scripted([turn(("write", "write-1", 1)), done()])
    with pytest.raises(ExceptionGroup):
        await harness.adapter().run(model(provider), [ai.user_message("go")])
    assert harness.counts["write"] == 0
    assert len(provider._seen) == 1


@pytest.mark.asyncio
async def test_lookup_then_atomic_admit_loser_never_dispatches() -> None:
    harness = setup(approval=False)
    adapter = harness.adapter()
    part = messages.ToolCallPart(tool_call_id="sdk-id", tool_name="write", tool_args='{"value":1}')
    await adapter.prepare([part])
    entry = adapter.entries["sdk-id"]
    original_lookup = harness.operations.lookup

    async def racing_lookup(identity: Any) -> Any:
        assert await original_lookup(identity) is None
        # Another worker wins between lookup and atomic admission.
        harness.operations.receipts[identity.operation_id] = OperationReceipt(
            identity=identity, state="accepted"
        )
        return None

    harness.operations.lookup = racing_lookup
    result = await adapter.execute(entry)
    assert result["status"] == "unknown"
    assert harness.operations.trace == ["lookup", "admit"]
    assert harness.counts["write"] == 0


@pytest.mark.asyncio
async def test_same_batch_park_restores_completed_read_without_reexecution() -> None:
    harness = setup()
    provider = Scripted([turn(("read", "read-1", 1), ("write", "write-1", 2)), done()])
    first = await harness.adapter().run(model(provider), [ai.user_message("go")])
    assert first.status == "awaiting" and harness.counts == {"read": 1, "write": 0}
    approval = next(event for event in harness.store.events if event.kind == "approval_request")
    result = await asyncio.wait_for(
        harness.adapter().run(
            model(provider), resume=True, replies={approval.data["call_id"]: True}
        ),
        5,
    )
    assert result.status == "completed" and harness.counts == {"read": 1, "write": 1}
    assert len(provider._seen) == 2


@pytest.mark.asyncio
async def test_checkpoint_precedes_admission_but_running_resume_is_rejected() -> None:
    harness = setup(approval=False)
    provider = Scripted([turn(("write", "write-1", 1)), done()])

    async def assert_checkpoint() -> None:
        assert harness.store.saved is not None
        checkpoint, _ = harness.store.saved
        assert checkpoint["response_complete"] is True
        assert checkpoint["entries"]["write-1"]["identity"]["operation_id"].startswith("op-")

    harness.operations.on_lookup = assert_checkpoint
    first = await harness.adapter().run(model(provider), [ai.user_message("go")])
    assert first.status == "completed" and harness.counts["write"] == 1
    # A checkpoint alone does not authorize running -> running. Production
    # recovery marks abandoned running runs interrupted, rather than replaying.
    assert harness.store.status == "running"
    resumed_provider = Scripted([done()])
    with pytest.raises(ServiceError) as error:
        await harness.adapter().run(model(resumed_provider), resume=True)
    assert error.value.code == "run_closed"
    assert resumed_provider._seen == [] and harness.counts["write"] == 1
    assert harness.operations.trace == ["lookup", "admit", "save_receipt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["environment", "identity", "arguments"])
async def test_checkpoint_binding_drift_is_rejected(field: str) -> None:
    harness = setup()
    provider = Scripted([turn(("write", "write-1", 1))])
    await harness.adapter().run(model(provider), [ai.user_message("go")])
    checkpoint, digest = harness.store.saved
    if field == "environment":
        checkpoint["environment"]["generation"] += 1
    elif field == "identity":
        checkpoint["entries"]["write-1"]["identity"]["environment"]["generation"] += 1
    else:
        for message in checkpoint["messages"]:
            if message["role"] == "assistant":
                message["parts"][0]["tool_args"] = '{"value":2}'
    harness.store.saved = checkpoint, digest
    with pytest.raises(ValueError, match="binding changed"):
        await harness.adapter().run(model(provider), resume=True)
    assert harness.counts["write"] == 0


@pytest.mark.asyncio
async def test_argument_delta_after_tool_end_is_rejected() -> None:
    harness = setup(approval=False)
    sequence = turn(("write", "one", 1))
    sequence.insert(-1, events.ToolDelta(tool_call_id="one", chunk=" "))
    provider = Scripted([sequence])
    with pytest.raises(ExceptionGroup):
        await harness.adapter().run(model(provider), [ai.user_message("go")])
    assert harness.counts["write"] == 0
