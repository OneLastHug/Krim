"""SDK-owned task gate regressions for sliding parallel slots and fairness."""

import asyncio

import ai
import pytest

from krim_agent.agent_sdk.adapter import SDKAdapter
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import agent_tools
from krim_agent.config import Settings
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.runtime.sdk_execution import SDKExecutionPolicy
from krim_agent.tools.core import ToolSource, ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from tests.coding_agent.sdk_fixtures import Args, MemoryRunStore, Scripted, done, turn


def test_shared_execution_policy_defaults_to_width_eight():
    assert SDKExecutionPolicy.from_settings(Settings()).parallelism == 8


class Store(MemoryRunStore):
    async def snapshot(self, run_id):
        value = await super().snapshot(run_id)
        value.events = tuple(self.events)
        return value


async def test_parallel_slots_refill_and_exclusive_calls_form_a_fair_barrier():
    store = Store()
    active = 0
    maximum = 0
    started: list[int] = []
    two_reads = asyncio.Event()
    read_started = {number: asyncio.Event() for number in (1, 2, 3, 4)}
    release_read = {number: asyncio.Event() for number in (1, 2, 3, 4)}
    exclusive_started, release_exclusive = asyncio.Event(), asyncio.Event()

    async def read(args: Args):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        started.append(args.value)
        read_started[args.value].set()
        if active == 2:
            two_reads.set()
        await release_read[args.value].wait()
        active -= 1
        return args.value

    async def unavailable(_args: Args):
        raise AssertionError("use the registered external executor")

    async def exclusive(_invocation, args: Args):
        assert active == 0
        exclusive_started.set()
        await release_exclusive.wait()
        return args.value

    async def allow(_invocation):
        return "allow"

    registry = ToolRegistry((
        ToolSpec("read", Args, read, {"type": "integer"}, "1", "a" * 64,
                 concurrency="parallel"),
        ToolSpec("barrier", Args, unavailable, {"type": "integer"}, "1", "b" * 64,
                 risk="write", external_executor=exclusive,
                 source=ToolSource("mcp", "fixture", "barrier", "c" * 64)),
    ))
    executor = ToolExecutor(registry, allow)

    async def emit(kind, data):
        return await store.append("run", kind, data)

    async def reject(*_args):
        raise AssertionError("no approval or prior receipt expected")

    context = ToolContext(emit, reject, None)
    budget = ExecutionBudget(asyncio.get_running_loop().time() + 20)
    host = HostExecution(context, budget, reject, lambda: 0, lambda: {}, lambda _state: None)
    adapter = SDKAdapter(
        store=store,
        executor=executor,
        tools=agent_tools(registry),
        run_id="run",
        read_parallelism=2,
        batch_approvals=True,
        fail_on_tool_error=True,
        read_result=reject,
        approval_request=reject,
        host=host,
    )
    provider = Scripted([turn(
        ("read", "r1", 1), ("read", "r2", 2), ("read", "r3", 3),
        ("barrier", "x", 9), ("read", "r4", 4),
    ), done()])
    task = asyncio.create_task(adapter.run(
        ai.Model(id="slot-test", provider=provider), [ai.user_message("go")],
    ))
    try:
        await asyncio.wait_for(two_reads.wait(), 3)
        assert set(started) == {1, 2}
        release_read[1].set()
        await asyncio.wait_for(read_started[3].wait(), 3)
        assert not exclusive_started.is_set()
        assert not read_started[4].is_set()
        release_read[3].set()
        await asyncio.sleep(0)
        assert not exclusive_started.is_set()  # read 2 still occupies a slot
        release_read[2].set()
        await asyncio.wait_for(exclusive_started.wait(), 3)
        assert not read_started[4].is_set()  # queued behind the exclusive barrier
        release_exclusive.set()
        await asyncio.wait_for(read_started[4].wait(), 3)
        release_read[4].set()
        result = await asyncio.wait_for(task, 5)
    finally:
        for signal in release_read.values():
            signal.set()
        release_exclusive.set()
        if not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass

    assert result.status == "completed"
    assert maximum == 2


@pytest.mark.asyncio
async def test_default_eight_slots_complete_twenty_and_refill_before_slowest_finishes():
    store = Store()
    active = 0
    maximum = 0
    completed = set()
    started = {number: asyncio.Event() for number in range(20)}
    full = asyncio.Event()
    release_fast = asyncio.Event()
    release_slow = asyncio.Event()
    fast_finished = asyncio.Event()

    async def read(args: Args):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        started[args.value].set()
        if active == 8:
            full.set()
        try:
            if args.value == 0:
                await release_slow.wait()
            elif args.value < 8:
                await release_fast.wait()
            completed.add(args.value)
            if len(completed) == 19 and 0 not in completed:
                fast_finished.set()
            return args.value
        finally:
            active -= 1

    async def allow(_invocation):
        return "allow"

    async def reject(*_args):
        raise AssertionError("no approval or prior receipt expected")

    async def emit(kind, data):
        return await store.append("run", kind, data)

    registry = ToolRegistry((
        ToolSpec("read", Args, read, {"type": "integer"}, "1", "a" * 64,
                 concurrency="parallel"),
    ))
    host = HostExecution(
        ToolContext(emit, reject, None),
        ExecutionBudget(asyncio.get_running_loop().time() + 30),
        reject, lambda: 0, lambda: {}, lambda _state: None,
    )
    adapter = SDKAdapter(
        store=store, executor=ToolExecutor(registry, allow), tools=agent_tools(registry),
        run_id="run", batch_approvals=True, fail_on_tool_error=True,
        read_result=reject, approval_request=reject, host=host,
    )
    provider = Scripted([turn(*[("read", f"r{n}", n) for n in range(20)]), done()])
    task = asyncio.create_task(adapter.run(
        ai.Model(id="default-eight-test", provider=provider), [ai.user_message("go")],
    ))
    try:
        await asyncio.wait_for(full.wait(), 10)
        assert sum(signal.is_set() for signal in started.values()) == 8
        release_fast.set()
        await asyncio.wait_for(started[8].wait(), 10)
        await asyncio.wait_for(started[9].wait(), 10)
        assert not release_slow.is_set() and 0 not in completed
        await asyncio.wait_for(fast_finished.wait(), 10)
        assert len(completed) == 19
        release_slow.set()
        result = await asyncio.wait_for(task, 10)
    finally:
        release_fast.set()
        release_slow.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert result.status == "completed"
    assert maximum == 8
    assert completed == set(range(20))
    assert len([event for event in store.events if event.kind == "tool_end"]) == 20
