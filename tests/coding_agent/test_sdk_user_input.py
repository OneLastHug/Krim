"""User input parks the actual pinned SDK and resumes the same tool invocation."""

from types import SimpleNamespace

import ai

from krim_agent.agent_sdk.adapter import SDKAdapter
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import HostModelProvider, agent_tools
from krim_agent.contracts import StrictModel
from krim_agent.interaction.contracts import RequestPending
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.runtime.model_step import ModelStep
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from tests.coding_agent.sdk_fixtures import MemoryRunStore


class Args(StrictModel):
    prompt: str


class Store(MemoryRunStore):
    async def snapshot(self, run_id):
        value = await super().snapshot(run_id)
        value.events = tuple(self.events)
        return value


class Script:
    def __init__(self):
        self.calls = 0
        self.histories = []

    async def stream(self, messages, tools):
        self.calls += 1
        self.histories.append(messages)
        if self.calls == 1:
            yield ModelToolCall(id="ask-wire", name="ask_user", arguments={"prompt": "Which?"})
        else:
            yield ModelText(text="Answer received")

    async def close(self):
        pass


async def test_input_parks_without_failure_and_resumes_once():
    store, script = Store(), Script()
    state = SimpleNamespace(answered=False, request=None, calls=[])

    async def unavailable(_args):
        raise AssertionError("wrong tool path")

    async def execute(invocation, args, context):
        state.calls.append(invocation.call_id)
        if state.request is None:
            state.request = {
                "id": "question-1", "run_id": invocation.run_id, "status": "pending",
                "kind": "user_input", "payload": {"prompt": args.prompt},
            }
        if not state.answered:
            raise RequestPending(state.request)
        return {"id": "question-1", "status": "answered", "response": "first"}

    async def allow(_call):
        return "allow"

    spec = ToolSpec("ask_user", Args, unavailable, {"type": "object"}, "interaction/1",
                    "a" * 64, risk="write", scoped_executor=execute, capability="session_state")
    executor = ToolExecutor(ToolRegistry((spec,)), allow)

    def make():
        async def emit(kind, data):
            return await store.append("run", kind, data)

        async def reject(*_args):
            raise AssertionError("input must not use operation approval")

        context = ToolContext(emit, reject, None)
        stepper = ModelStep(script, emit, lambda: None, max_steps=None, max_output=None,
                            deadline=None, workspace=False)
        provider = HostModelProvider(stepper, executor.registry, max_group=1,
                                     max_calls=None, request_bytes=65536)
        host = HostExecution(context, ExecutionBudget(None), reject, provider.step_index,
                             provider.checkpoint, provider.restore)
        adapter = SDKAdapter(store=store, executor=executor, tools=agent_tools(executor.registry),
                             run_id="run", host=host, read_result=reject, approval_request=reject)
        return adapter, ai.Model(id="input-script", provider=provider)

    adapter, model = make()
    first = await adapter.run(model, [ai.user_message("Ask me")])
    assert first.status == "awaiting"
    assert script.calls == 1
    assert not [event for event in store.events if event.kind == "tool_end"]
    assert [event for event in store.events if event.kind == "user_input_request"]
    assert store.saved[0]["budget_remaining"] is None
    state.answered = True
    await store.append("run", "user_input_reply", {"request_id": "question-1"})
    resumed, model = make()
    second = await resumed.run(model, resume=True)
    assert second.status == "completed"
    assert script.calls == 2
    assert len(state.calls) == 2 and len(set(state.calls)) == 1
    results = [event for event in store.events if event.kind == "tool_end"]
    assert len(results) == 1 and results[0].data["status"] == "ok"
