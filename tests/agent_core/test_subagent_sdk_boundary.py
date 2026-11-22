"""Contract tests for the SDK step input boundary, live event outlet and
bounded event retention on SDKAdapter (plan.md 5.3, pinned ai==0.9.0).

Verified with a scripted provider and no model network requests:

- the optional step callback fires only after a completed step's assistant
  and tool-result messages are closed in the context and before the next
  sampling; run()'s initial input, a finished response and a deferred-hook
  park never consume it;
- a resumed run closes its replayed batch before the first boundary of that
  invocation, so no inbox message is injected into an unclosed tool gap;
- on_event delivers every event in stream order while the run is still going;
- max_events bounds only the events retained in ProbeResult (most recent),
  never the delivery;
- without the new arguments the previous single-agent behavior is preserved.
"""

import asyncio
import copy
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import ai
import pytest
from ai import events
from pydantic import PrivateAttr

from krim_agent.agent_sdk.adapter import SDKAdapter, StepBoundary, StepCommit
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import agent_tools
from krim_agent.agent_sdk.response import IncompleteTurn
from krim_agent.contracts import ServiceError
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from tests.coding_agent.sdk_fixtures import (
    Args,
    MemoryRunStore,
    Scripted,
    done,
    turn,
)
from tests.coding_agent.sdk_fixtures import (
    setup as sdk_setup,
)


class Store(MemoryRunStore):
    async def snapshot(self, run_id: str) -> Any:
        value = await super().snapshot(run_id)
        value.events = tuple(self.events)
        return value


SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
    "additionalProperties": False,
}


class Logged(Scripted):
    """Scripted provider that records each sampling's order and input history."""

    _order: list[Any] = PrivateAttr(default_factory=list)

    def __init__(self, turns: list[Any], order: list[Any]) -> None:
        super().__init__(turns)
        self._order = order

    async def stream(self, source: Any, history: Any, **kwargs: Any) -> AsyncGenerator[Any]:
        self._order.append(("sample", len(self._seen), copy.deepcopy(history)))
        async for event in super().stream(source, history, **kwargs):
            yield event


def recording_boundary(order: list[Any], injections: Any = ()) -> Any:
    """Log (kind, step, roles) per boundary and hand out queued injections."""
    queue = list(injections)

    async def step_boundary(boundary: StepBoundary) -> Any:
        order.append(("boundary", boundary.step, tuple(m.role for m in boundary.messages)))
        return queue.pop(0) if queue else None

    return step_boundary


def unwrap(exc: BaseException) -> BaseException:
    """Collapse the SDK TaskGroup's singleton wrapper around loop failures."""
    while isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1:
        exc = exc.exceptions[0]
    return exc


def generic_adapter(
    *,
    read_impl: Any = None,
    step_boundary: Any = None,
    on_step_commit: Any = None,
    on_event: Any = None,
    max_events: int | None = None,
    fail_on_tool_error: bool = False,
) -> Any:
    """Host-mode adapter over a frozen one-tool registry with the new seams."""
    store = Store()
    ran: list[int] = []

    async def default_read(args: Args) -> Any:
        ran.append(args.value)
        await asyncio.sleep(0)
        return {"value": args.value}

    async def allow(_invocation: Any) -> str:
        return "allow"

    registry = ToolRegistry(
        (ToolSpec("read", Args, read_impl or default_read, SCHEMA, "1", "a" * 64),)
    )
    executor = ToolExecutor(registry, allow)

    async def emit(kind: str, data: Any) -> Any:
        return await store.append("run", kind, data)

    async def reject(*_args: Any) -> Any:
        raise AssertionError("approval, receipt and prior result are not expected here")

    counter = {"step": 0}

    def model_step() -> int:
        counter["step"] += 1
        return counter["step"]

    host = HostExecution(
        ToolContext(emit, reject, None),
        ExecutionBudget(None),
        reject,
        model_step,
        lambda: {},
        lambda _state: None,
    )
    adapter = SDKAdapter(
        store=store,
        executor=executor,
        tools=agent_tools(registry),
        run_id="run",
        read_result=reject,
        approval_request=reject,
        host=host,
        step_boundary=step_boundary,
        on_step_commit=on_step_commit,
        on_event=on_event,
        max_events=max_events,
        fail_on_tool_error=fail_on_tool_error,
    )

    def make_adapter(**kwargs: Any) -> SDKAdapter:
        """Fresh adapter over the same store/registry for resume scenarios."""
        return SDKAdapter(
            store=store,
            executor=executor,
            tools=agent_tools(registry),
            run_id="run",
            read_result=reject,
            approval_request=reject,
            host=host,
            step_boundary=step_boundary,
            on_step_commit=on_step_commit,
            on_event=on_event,
            max_events=max_events,
            fail_on_tool_error=fail_on_tool_error,
            **kwargs,
        )

    return SimpleNamespace(store=store, adapter=adapter, ran=ran, make_adapter=make_adapter)


def dev_harness(*, approval: bool = True) -> Any:
    """sdk_fixtures development wiring; bound_adapter accepts the new seams."""
    harness = sdk_setup(approval=approval)
    factory = harness.adapter

    def bound_adapter(**kwargs: Any) -> SDKAdapter:
        adapter = factory()
        for name, value in kwargs.items():
            setattr(adapter, name, value)
        return adapter

    harness.bound_adapter = bound_adapter
    return harness


async def run(harness: Any, provider: Any, **kwargs: Any) -> Any:
    return await harness.adapter.run(
        ai.Model(id="boundary-test", provider=provider), [ai.user_message("go")], **kwargs
    )


async def test_boundary_fires_only_between_closed_steps_and_samples():
    order: list[Any] = []
    harness = generic_adapter(step_boundary=recording_boundary(order))
    provider = Logged([turn(("read", "r1", 1)), turn(("read", "r2", 2)), done()], order)
    result = await run(harness, provider)

    assert result.status == "completed"
    assert harness.ran == [1, 2]
    assert [(kind, step) for kind, step, _ in order] == [
        ("sample", 0),
        ("boundary", 1),
        ("sample", 1),
        ("boundary", 2),
        ("sample", 2),
    ]
    boundary_roles = [roles for kind, _, roles in order if kind == "boundary"]
    assert boundary_roles == [
        ("user", "assistant", "tool"),
        ("user", "assistant", "tool", "assistant", "tool"),
    ]
    samples = [history for kind, _, history in order if kind == "sample"]
    # 首轮采样直接使用 run() 的输入；其之前没有任何边界被消费。
    assert [m.role for m in samples[0]] == ["user"]
    # 下一次采样的输入恰为边界快照：工具结果已闭合，无重复或提前注入。
    assert [m.role for m in samples[1]] == ["user", "assistant", "tool"]
    assert samples[1][-1].tool_results[0].result["status"] == "ok"
    assert [m.role for m in samples[2]] == ["user", "assistant", "tool", "assistant", "tool"]
    assert [m.role for m in result.messages] == [
        "user", "assistant", "tool", "assistant", "tool", "assistant",
    ]
    assert result.messages[-1].text == "done"


async def test_finished_response_and_initial_input_consume_no_boundary():
    order: list[Any] = []
    harness = generic_adapter(step_boundary=recording_boundary(order))
    result = await run(harness, Logged([done()], order))

    assert result.status == "completed"
    assert [(kind, step) for kind, step, _ in order] == [("sample", 0)]


async def test_boundary_injection_reaches_next_sampling_once():
    order: list[Any] = []
    step_boundary = recording_boundary(order, injections=[ai.user_message("inbox:1")])
    harness = generic_adapter(step_boundary=step_boundary)
    provider = Logged([turn(("read", "r1", 1)), done()], order)
    result = await run(harness, provider)

    assert result.status == "completed"
    samples = [history for kind, _, history in order if kind == "sample"]
    assert [m.role for m in samples[1]] == ["user", "assistant", "tool", "user"]
    assert samples[1][-1].text == "inbox:1"
    # 注入只发生一次，并保留在最终上下文与返回消息里。
    assert [m.role for m in result.messages] == [
        "user", "assistant", "tool", "user", "assistant",
    ]
    assert [m.text for m in result.messages if m.role == "user"] == ["go", "inbox:1"]


async def test_boundary_veto_prevents_next_sampling():
    async def veto(_boundary: StepBoundary) -> None:
        raise RuntimeError("budget exhausted")

    harness = generic_adapter(step_boundary=veto)
    provider = Scripted([turn(("read", "r1", 1)), done()])
    with pytest.raises(ExceptionGroup) as raised:
        await run(harness, provider)
    inner = unwrap(raised.value)
    assert isinstance(inner, RuntimeError)
    assert "budget exhausted" in str(inner)
    # 第一步已闭合执行；取消发生在下一次采样之前，第二次采样未发出。
    assert harness.ran == [1]
    assert len(provider._seen) == 1


@pytest.mark.parametrize(
    "payload",
    [
        ["inbox"],
        [ai.user_message("stale").model_copy(update={"replay": True})],
    ],
    ids=["non-message", "replay-flagged"],
)
async def test_boundary_rejects_injection_outside_the_contract(payload):
    async def inject(_boundary: StepBoundary) -> Any:
        return payload

    harness = generic_adapter(step_boundary=inject)
    provider = Scripted([turn(("read", "r1", 1)), done()])
    with pytest.raises(ExceptionGroup) as raised:
        await run(harness, provider)
    inner = unwrap(raised.value)
    assert isinstance(inner, ValueError)
    assert "non-replay SDK messages" in str(inner)
    assert harness.ran == [1]
    assert len(provider._seen) == 1


async def test_on_event_delivers_live_in_stream_order():
    release = asyncio.Event()
    ran: list[int] = []
    seen: list[events.AgentEvent] = []
    first_step_done = asyncio.Event()

    async def on_event(event: events.AgentEvent) -> None:
        seen.append(event)
        if isinstance(event, events.StreamEnd):
            first_step_done.set()

    async def slow_read(args: Args) -> Any:
        ran.append(args.value)
        await release.wait()
        return {"value": args.value}

    harness = generic_adapter(on_event=on_event, read_impl=slow_read)
    provider = Scripted([turn(("read", "r1", 1)), done()])
    task = asyncio.create_task(run(harness, provider))
    try:
        await asyncio.wait_for(first_step_done.wait(), 3)
        # 第一步的完整响应事件已实时出口；工具仍阻塞，run 未结束、无下一采样。
        assert not task.done()
        assert [type(event).__name__ for event in seen] == [
            "StreamStart", "ToolStart", "ToolDelta", "ToolEnd", "StreamEnd",
        ]
        assert len(provider._seen) == 1
        release.set()
        result = await asyncio.wait_for(task, 5)
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert result.status == "completed"
    assert ran == [1]
    assert len(seen) == len(result.events)
    assert all(a is b for a, b in zip(seen, result.events, strict=True))


async def test_max_events_bounds_only_probe_result_retention():
    seen: list[events.AgentEvent] = []

    async def on_event(event: events.AgentEvent) -> None:
        seen.append(event)

    harness = generic_adapter(on_event=on_event, max_events=5)
    result = await run(harness, Scripted([turn(("read", "r1", 1)), done()]))

    assert result.status == "completed"
    assert len(seen) > 5
    assert len(result.events) == 5
    assert all(a is b for a, b in zip(seen[-5:], result.events, strict=True))


async def test_invalid_event_retention_rejected():
    with pytest.raises(ValueError, match="invalid event retention"):
        generic_adapter(max_events=0)


async def test_on_step_commit_reports_each_complete_response_once():
    commits: list[StepCommit] = []

    async def on_step_commit(commit: StepCommit) -> None:
        commits.append(commit)

    harness = generic_adapter(on_step_commit=on_step_commit)
    result = await run(harness, Scripted([turn(("read", "r1", 1)), done()]))

    assert result.status == "completed"
    assert [(commit.step, len(commit.messages), bool(commit.response.tool_calls))
            for commit in commits] == [(1, 1, True), (2, 3, False)]
    assert commits[1].response.text == "done"
    assert {commit.run_id for commit in commits} == {"run"}


def strip_ids(value: Any) -> Any:
    """Recursively drop generated ids/spans so independent runs compare equal."""
    if isinstance(value, dict):
        return {key: strip_ids(item) for key, item in value.items()
                if key not in {"id", "span_id", "call_id"}}
    if isinstance(value, (list, tuple)):
        normalized = [strip_ids(item) for item in value]
        return tuple(normalized) if isinstance(value, tuple) else normalized
    return value


def message_shape(history: list[Any]) -> list[dict[str, Any]]:
    """ID-free structural view so two independent runs compare as equal."""
    return strip_ids([
        {
            "role": message.role,
            "text": message.text,
            "calls": [(call.tool_call_id, call.tool_name, call.tool_args)
                      for call in message.tool_calls],
            "results": [(part.tool_call_id, part.result) for part in message.tool_results],
        }
        for message in history
    ])


def event_shape(history: list[events.AgentEvent]) -> list[dict[str, Any]]:
    """Event payloads without hydrated message snapshots and generated ids."""
    return strip_ids([event.model_dump(exclude={"message"}) for event in history])


async def test_defaults_and_recording_callbacks_preserve_single_agent_behavior():
    baseline = generic_adapter()
    base = await run(baseline, Scripted([turn(("read", "r1", 1)), done()]))

    recorded: list[events.AgentEvent] = []

    async def on_event(event: events.AgentEvent) -> None:
        recorded.append(event)

    async def observe(_boundary: StepBoundary) -> None:
        return None

    instrumented = generic_adapter(step_boundary=observe, on_event=on_event)
    result = await run(instrumented, Scripted([turn(("read", "r1", 1)), done()]))

    assert message_shape(result.messages) == message_shape(base.messages)
    assert event_shape(result.events) == event_shape(base.events)
    assert recorded == result.events


async def test_incomplete_response_still_blocks_before_any_dispatch():
    order: list[Any] = []
    harness = generic_adapter(step_boundary=recording_boundary(order))
    with pytest.raises(ExceptionGroup) as raised:
        await run(harness, Logged([turn(("read", "r1", 1), finish="length")], order))
    inner = unwrap(raised.value)
    assert isinstance(inner, IncompleteTurn)
    assert "finish reason" in str(inner)
    assert harness.ran == []
    assert [kind for kind, _, _ in order] == ["sample"]


async def test_awaiting_park_and_resume_close_before_first_boundary():
    harness = dev_harness(approval=True)
    order: list[Any] = []
    commits: list[StepCommit] = []

    async def on_step_commit(commit: StepCommit) -> None:
        commits.append(commit)

    provider = Logged([turn(("write", "w1", 7)), done()], order)
    harness.adapter = harness.bound_adapter(
        step_boundary=recording_boundary(order), on_step_commit=on_step_commit,
    )
    first = await run(harness, provider)

    assert first.status == "awaiting"
    # hook 挂起即停靠：边界从未被消费，收件箱不会在未闭合批次上注入。
    assert [(kind, step) for kind, step, _ in order] == [("sample", 0)]
    assert len(provider._seen) == 1
    assert len(commits) == 1

    call_id = harness.approval_calls[0]["call_id"]
    resumed_provider = Logged([done()], order)
    harness.adapter = harness.bound_adapter(
        step_boundary=recording_boundary(order), on_step_commit=on_step_commit,
    )
    second = await harness.adapter.run(
        ai.Model(id="boundary-test", provider=resumed_provider),
        resume=True,
        replies={call_id: True},
    )

    assert second.status == "completed"
    # replay 迭代闭合中断批次时不触发采样（provider 未被调用），
    # 首个边界出现在批次闭合之后、最终采样之前。
    assert [(kind, step) for kind, step, _ in order] == [
        ("sample", 0), ("boundary", 1), ("sample", 0),
    ]
    boundary_roles = [roles for kind, _, roles in order if kind == "boundary"]
    assert boundary_roles == [("user", "assistant", "tool")]
    assert len(resumed_provider._seen) == 1
    # replay 不重发 commit；只有最终完整响应新增一次。
    assert len(commits) == 2
    assert harness.counts["write"] == 1
    assert second.messages[2].tool_results[0].result["value"]["value"] == 7


async def test_recover_tool_backfills_lost_result_without_redispatch():
    calls: list[int] = []

    async def failing_read(args: Args) -> Any:
        calls.append(args.value)
        raise ServiceError("boom")

    harness = generic_adapter(read_impl=failing_read, fail_on_tool_error=True)
    with pytest.raises(ExceptionGroup) as raised:
        await run(harness, Scripted([turn(("read", "r1", 1)), done()]))
    inner = unwrap(raised.value)
    assert isinstance(inner, ServiceError)
    assert calls == [1]
    failed = [event for event in harness.store.events
              if event.kind == "tool_end" and event.data.get("status") == "failed"]
    assert len(failed) == 1

    # Case A：默认（recover_tool=None）——重放维持 facts_corrupt，不重派副作用。
    with pytest.raises(ExceptionGroup) as plain:
        await harness.make_adapter().run(
            ai.Model(id="boundary-test", provider=Scripted([done()])), resume=True)
    assert unwrap(plain.value).code == "facts_corrupt"
    assert calls == [1]

    # Case B：宿主查无回执——围栏不变。
    async def no_receipt(_invocation: Any) -> Any:
        return None

    with pytest.raises(ExceptionGroup) as missing:
        await harness.make_adapter(recover_tool=no_receipt).run(
            ai.Model(id="boundary-test", provider=Scripted([done()])), resume=True)
    assert unwrap(missing.value).code == "facts_corrupt"
    assert calls == [1]

    # Case C：宿主持久回执——回填 ok 观测并补 tool_end，工具不重跑。
    async def accepted_receipt(_invocation: Any) -> Any:
        return {"receipt": "spawn-accepted"}

    result = await harness.make_adapter(recover_tool=accepted_receipt).run(
        ai.Model(id="boundary-test", provider=Scripted([done()])), resume=True)
    assert result.status == "completed"
    assert calls == [1]
    ends = [event for event in harness.store.events if event.kind == "tool_end"]
    assert len(ends) == 2
    assert ends[-1].data["status"] == "ok"
    assert ends[-1].data["value"] == {"receipt": "spawn-accepted"}
    assert ends[-1].data["executor_started"] is True
    assert ends[-1].data["result_available"] is True
