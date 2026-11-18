"""Real SDK loop through existing ModelStep and ToolExecutor host boundaries."""

import asyncio

import ai
import pytest

from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import HostModelProvider, agent_tools, host_messages, sdk_messages
from krim_agent.contracts import Message, ServiceError
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.runtime.model_step import ModelStep
from tests.coding_agent.sdk_fixtures import setup


class HostScript:
    def __init__(self, *, broken: bool = False, repeated: bool = False):
        self.calls = 0
        self.histories = []
        self.closed = 0
        self.broken = broken
        self.repeated = repeated

    async def stream(self, messages, tools):
        self.calls += 1
        self.histories.append(messages)
        try:
            if self.calls == 1 or self.repeated:
                yield ModelToolCall(id=f'wire-{self.calls}', name='write', arguments={'value': 7})
                if self.broken:
                    raise ServiceError('provider_incomplete_stream')
            else:
                yield ModelText(text='done')
        finally:
            self.closed += 1

    async def close(self):
        pass


def connected(harness, script, *, max_steps=3, max_calls=4, max_group=2):
    adapter = harness.adapter()
    from dataclasses import replace

    async def emit(kind, data):
        return await harness.store.append(harness.ownership.run_id, kind, data)

    context = replace(adapter.context(), emit=emit)
    stepper = ModelStep(script, context.emit, lambda: None, max_steps=max_steps,
                        max_output=65536, deadline=None, workspace=False)
    model = HostModelProvider(stepper, adapter.executor.registry, max_group=max_group,
                             max_calls=max_calls, request_bytes=65536)
    budget = ExecutionBudget(asyncio.get_running_loop().time() + 30)

    async def read_receipt(receipt):
        assert receipt.identity.ownership == harness.ownership
        return await harness.store.read_result(receipt.result_ref)

    adapter.host = HostExecution(context, budget, read_receipt, model.step_index,
                                 model.checkpoint, model.restore)
    # Prove tool declarations can be rebuilt directly from the existing registry.
    assert [tool.name for tool in agent_tools(adapter.executor.registry)] == ['read', 'write']
    return adapter, ai.Model(id='host-fixture', provider=model), stepper


async def test_sdk_continues_using_host_model_events_and_tool_results() -> None:
    harness = setup(approval=False)
    script = HostScript()
    adapter, model, stepper = connected(harness, script)
    result = await adapter.run(model, sdk_messages([Message(role='user', content='go')]))
    assert result.status == 'completed'
    assert harness.counts['write'] == 1
    assert script.calls == script.closed == 2
    assert stepper.index == 2
    assert script.histories[-1][-1].role == 'tool'
    assert script.histories[-1][-1].content == '{"value":7}'
    model_events = [e for e in harness.store.events if e.kind == 'model_end']
    starts = [e for e in harness.store.events if e.kind == 'tool_start']
    assert len(model_events) == 2 and len(starts) == 1
    assert starts[0].data['step_index'] == model_events[0].data['step_index'] == 0
    assert starts[0].data['provider_call_id'] == 'wire-1'


async def test_unlimited_group_keeps_the_full_returned_batch_in_one_step() -> None:
    class GroupScript(HostScript):
        async def stream(self, messages, tools):
            self.calls += 1
            self.histories.append(messages)
            try:
                if self.calls == 1:
                    yield ModelToolCall(id='wire-a', name='write', arguments={'value': 1})
                    yield ModelToolCall(id='wire-b', name='write', arguments={'value': 2})
                else:
                    yield ModelText(text='done')
            finally:
                self.closed += 1

    harness = setup(approval=False)
    script = GroupScript()
    adapter, model, stepper = connected(harness, script, max_group=None, max_calls=None)
    assert model.provider._max_group is None  # None is never folded into a one-call group
    result = await adapter.run(model, sdk_messages([Message(role='user', content='go')]))
    assert result.status == 'completed'
    assert harness.counts['write'] == 2
    assert script.calls == 2 and stepper.index == 2
    starts = [e for e in harness.store.events if e.kind == 'tool_start']
    assert [e.data['step_index'] for e in starts] == [0, 0]


async def test_host_claims_resume_and_parking_once_with_preserved_budget() -> None:
    harness = setup()
    script = HostScript()
    adapter, model, _ = connected(harness, script)
    first = await adapter.run(model, [ai.user_message('go')])
    assert first.status == 'awaiting'
    # Adapter returns a parked result; RunService performs the only state write.
    assert harness.store.status == 'running'
    checkpoint, _ = harness.store.saved
    assert checkpoint['budget_remaining'] > 0
    assert checkpoint['host_state']['tool_count'] == 1
    assert harness.approval_calls[-1]['budget_remaining'] > 0
    assert await harness.store.set_awaiting('run')
    await harness.store.set_running('run')
    resumed, model, stepper = connected(harness, script)
    request = next(e for e in harness.store.events if e.kind == 'approval_request')
    result = await resumed.run(model, resume=True, replies={request.data['call_id']: True})
    assert result.status == 'completed'
    assert harness.counts['write'] == 1 and script.calls == 2
    assert stepper.index == 2


@pytest.mark.parametrize('scenario', ['incomplete', 'tool_budget', 'step_budget'])
async def test_host_failures_still_precede_unapproved_side_effects(scenario: str) -> None:
    harness = setup(approval=False)
    script = HostScript(broken=scenario == 'incomplete', repeated=scenario == 'step_budget')
    adapter, model, _ = connected(harness, script, max_steps=1 if scenario == 'step_budget' else 3,
                                   max_calls=0 if scenario == 'tool_budget' else 4)
    with pytest.raises(ExceptionGroup):
        await adapter.run(model, [ai.user_message('go')])
    assert harness.counts['write'] == (1 if scenario == 'step_budget' else 0)


def test_sdk_tool_declarations_validate_the_catalog_at_batch_boundaries(monkeypatch):
    harness = setup()
    adapter = harness.adapter()
    registry = adapter.executor.registry
    original = registry._checked_schemas
    checks = []

    def checked():
        checks.append(True)
        return original()

    monkeypatch.setattr(registry, "_checked_schemas", checked)
    declared = agent_tools(registry)
    assert [tool.name for tool in declared] == ["read", "write"]
    assert len(checks) == 2
    checks.clear()
    rebuilt = harness.adapter()
    assert rebuilt.tool_names == {"read", "write"}
    assert len(checks) == 2
    assert rebuilt.executor is adapter.executor


def test_legacy_messages_roundtrip_retains_tool_pairing() -> None:
    history = [Message(role='user', content='go'), Message(role='assistant', content='checking',
               tool_calls=[{'kind': 'tool_call', 'id': 'wire', 'name': 'read',
                            'arguments': {'value': 1}}]),
               Message(role='tool', call_id='wire', content='{"value":1}')]
    assert host_messages(sdk_messages(history)) == history
