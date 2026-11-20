"""Safe progress events and timeout provenance at the shared model boundary."""

import asyncio

import pytest

from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.types import ModelProgress, ModelText, ModelToolCall
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.runtime.model_step import ModelStep


async def test_stream_progress_records_only_phase_and_argument_byte_counts():
    events = []
    release = asyncio.Event()
    partial = "sensitive-tool-argument"

    class Provider:
        async def stream(self, messages, tools):
            yield ModelProgress(
                phase="tool_arguments", argument_bytes=len(partial.encode("utf-8")),
            )
            await release.wait()
            yield ModelToolCall(id="wire-1", name="read", arguments={"value": 1})

        async def close(self):
            pass

    async def emit(kind, data):
        events.append((kind, data))
        return object()

    step = ModelStep(
        Provider(), emit, lambda: None, max_steps=2, max_output=1024,
        deadline=None, workspace=False,
    )
    task = asyncio.create_task(step.call(
        [Message(role="user", content="go")], [], max_group=None, remaining_tools=None,
    ))
    try:
        await asyncio.sleep(0)
        assert not task.done()  # partial argument observations are not dispatchable calls
        activity = [data for kind, data in events if kind == "model_activity"]
        assert activity and activity[0]["phase"] == "tool_arguments"
        assert activity[0]["argument_bytes"] == len(partial.encode("utf-8"))
        assert partial not in repr(activity)
    finally:
        release.set()
    result = await asyncio.wait_for(task, 3)
    assert [call.name for call in result.calls] == ["read"]
    end = next(data for kind, data in events if kind == "model_end")
    assert end["tool_argument_bytes"] == len(partial.encode("utf-8"))


async def test_local_model_timeout_does_not_mislabel_run_deadline():
    class SlowProvider:
        async def stream(self, messages, tools):
            await asyncio.sleep(0.05)
            yield ModelText(text="late")

        async def close(self):
            pass

    async def emit(_kind, _data):
        return object()

    loop = asyncio.get_running_loop()
    model_budget = ExecutionBudget(loop.time() + 1)
    model_limited = ModelStep(
        SlowProvider(), emit, lambda: None, max_steps=None, max_output=None,
        deadline=model_budget.deadline, workspace=False, execution_budget=model_budget,
        model_timeout=0.005,
    )
    with pytest.raises(ServiceError, match="model_timeout"):
        await model_limited.call(
            [Message(role="user", content="wait")], [], max_group=None,
            remaining_tools=None,
        )

    run_budget = ExecutionBudget(loop.time() + 0.005)
    run_limited = ModelStep(
        SlowProvider(), emit, lambda: None, max_steps=None, max_output=None,
        deadline=run_budget.deadline, workspace=False, execution_budget=run_budget,
        model_timeout=1,
    )
    with pytest.raises(TimeoutError) as caught:
        await run_limited.call(
            [Message(role="user", content="wait")], [], max_group=None,
            remaining_tools=None,
        )
    assert not isinstance(caught.value, ServiceError)
