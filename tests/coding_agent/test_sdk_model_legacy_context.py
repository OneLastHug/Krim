"""Regression tests for legacy ModelStep request preparation in the SDK bridge."""

import ai
from ai import messages as sdk

from krim_agent.agent_sdk.model import HostModelProvider, host_messages
from krim_agent.context.types import ContextPolicy, HistorySource
from krim_agent.contracts import Message, encoded, fingerprint
from krim_agent.providers.types import ModelText
from krim_agent.runtime.model_step import ModelStep
from tests.coding_agent.sdk_fixtures import setup


class Script:
    async def stream(self, messages, tools):
        yield ModelText(text="done")


async def test_session_sources_annotate_prepared_request_and_keep_legacy_budgets():
    harness = setup()
    emitted = []

    async def emit(kind, data):
        emitted.append((kind, data))

    stepper = ModelStep(
        Script(), emit, lambda: None, max_steps=3, max_output=4096,
        deadline=None, workspace=False,
    )
    registry = harness.adapter().executor.registry
    policy = ContextPolicy(max_bytes=4096, reserved_output_tokens=128)
    old_messages = [
        Message(role="user", content="prior request"),
        Message(role="assistant", content="prior answer"),
    ]
    source = HistorySource(
        turn_id="turn-1", run_id="run-1", last_seq=8,
        fingerprint=fingerprint([message.model_dump(mode="json") for message in old_messages]),
    )
    provider = HostModelProvider(
        stepper, registry, max_group=2, max_calls=4, request_bytes=65536,
        context_policy=policy, session_sources=(source,),
    )
    model = ai.Model(id="fixture", provider=provider)
    messages = [
        Message(role="system", content="frozen system"),
        *old_messages,
        Message(role="user", content="current request"),
    ]

    async for _ in provider.stream(model, [
        sdk.Message(role=message.role, parts=[sdk.TextPart(text=message.content)])
        for message in messages
    ]):
        pass

    manifest = next(data["context_manifest"] for kind, data in emitted if kind == "model_start")
    assert manifest["items"]
    prior_items = [item for item in manifest["items"] if item.get("message_index") in {1, 2}]
    assert len(prior_items) == 2
    assert all(item["source_refs"] == [{
        "turn_id": source.turn_id,
        "run_id": source.run_id,
        "last_seq": source.last_seq,
        "fingerprint": source.fingerprint,
    }] for item in prior_items)
    assert manifest["reserved_output_tokens"] == policy.reserved_output_tokens
    assert manifest["input_budget_bytes"] == min(
        65536 - 1024,
        policy.max_bytes - 1024 - policy.reserved_output_tokens * 4,
    )


def test_host_messages_sanitizes_denial_to_legacy_tool_result():
    observation = {
        "call_id": "invocation-1", "provider_call_id": "wire-1",
        "arguments": {"secret": "internal"}, "span_id": "span-1", "step_index": 2,
        "name": "write", "status": "denied", "executor_started": False,
        "result_available": False, "code": "approval_denied", "decision": "ask",
    }
    history = [
        sdk.Message(role="assistant", parts=[sdk.ToolCallPart(
            tool_call_id="wire-1", tool_name="write", tool_args="{}",
        )]),
        sdk.Message(role="tool", parts=[sdk.ToolResultPart(
            tool_call_id="wire-1", tool_name="write", result=observation,
            model_input=observation,
        )]),
    ]

    converted = host_messages(history)

    assert converted[-1].content == encoded({
        "call_id": "invocation-1", "name": "write", "status": "denied",
        "code": "approval_denied",
    })


def test_host_messages_orders_tool_results_by_call_declaration():
    history = [
        sdk.Message(role="assistant", parts=[
            sdk.ToolCallPart(tool_call_id="wire-a", tool_name="read", tool_args="{}"),
            sdk.ToolCallPart(tool_call_id="wire-b", tool_name="read", tool_args="{}"),
        ]),
        sdk.Message(role="tool", parts=[
            sdk.ToolResultPart(tool_call_id="wire-b", tool_name="read",
                               result={"value": "second"}, model_input={"value": "second"}),
            sdk.ToolResultPart(tool_call_id="wire-a", tool_name="read",
                               result={"value": "first"}, model_input={"value": "first"}),
        ]),
    ]

    converted = host_messages(history)

    assert [message.call_id for message in converted if message.role == "tool"] == [
        "wire-a", "wire-b",
    ]
