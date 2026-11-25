"""流式绑定栅栏：轻量检查只做身份核对不生成Schema，完整检查守住流边界。"""

import asyncio
from dataclasses import replace
from types import MappingProxyType

import pytest

from krim_agent.contracts import Message, ServiceError, StrictModel
from krim_agent.providers.types import ModelProgress, ModelText, ModelToolCall
from krim_agent.runtime.model_step import ModelStep
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.registry import ToolRegistry
from tests.agent_core.test_approval_park import allow_reply, park_application, park_echo_turn


class Args(StrictModel):
    value: str = ""


class OtherArgs(StrictModel):
    other: str = ""


async def run(arguments):
    return {"value": arguments.value}


async def other_run(arguments):
    return {"other": arguments.other}


def make_spec(name="local_read", description=""):
    return ToolSpec(
        name, Args, run, {"type": "object"}, "1", "a" * 64, description=description,
    )


def swap_items(registry):
    values = {spec.name: spec for spec, _ in registry._frozen}
    name = next(iter(values))
    values[name] = replace(values[name], description="replaced source entry")
    registry._items = MappingProxyType(values)


def test_runtime_current_does_not_generate_schemas(monkeypatch):
    base = ToolRegistry((make_spec(), make_spec("local_list")))
    derived = ToolRegistry((make_spec("remote_read"),), dependencies=(base,))
    calls = 0
    original = ToolSpec.descriptor

    def counting(self):
        nonlocal calls
        calls += 1
        return original(self)

    monkeypatch.setattr(ToolSpec, "descriptor", counting)
    base.assert_runtime_current()
    derived.assert_runtime_current()
    assert calls == 0
    base.assert_current()
    assert calls > 0


def test_runtime_current_ignores_schema_drift_that_full_check_rejects():
    spec = make_spec(description="original")
    registry = ToolRegistry((spec,))
    derived = ToolRegistry((make_spec("remote_read"),), dependencies=(registry,))
    spec.output_schema["tampered"] = "in-place"
    object.__setattr__(spec, "description", "tampered in place")
    registry.assert_runtime_current()
    derived.assert_runtime_current()
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        registry.assert_current()
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        derived.assert_current()


def test_runtime_current_rejects_swapped_source_items_in_derived_registry():
    base = ToolRegistry((make_spec(),))
    derived = ToolRegistry((make_spec("remote_read"),), dependencies=(base,))
    base.assert_runtime_current()
    derived.assert_runtime_current()
    swap_items(base)
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        base.assert_runtime_current()
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        derived.assert_runtime_current()


def test_runtime_current_rejects_executor_and_parameters_replacement():
    spec = make_spec()
    registry = ToolRegistry((spec,))
    registry.assert_runtime_current()
    object.__setattr__(spec, "executor", other_run)
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        registry.assert_runtime_current()

    typed = make_spec("local_typed")
    typed_registry = ToolRegistry((typed,))
    typed_registry.assert_runtime_current()
    object.__setattr__(typed, "parameters", OtherArgs)
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        typed_registry.assert_runtime_current()


class ReplayProvider:
    """假provider：顺序回放事件，可在事件间注入动作、收尾停滞或失败。"""

    def __init__(self, deltas, *, stall=None, failure=None):
        self.deltas = deltas
        self.stall = stall
        self.failure = failure
        self.closed = False
        self.exhausted = False

    async def stream(self, messages, tools):
        try:
            for item in self.deltas:
                if callable(item):
                    item()
                else:
                    yield item
            if self.stall is not None:
                await asyncio.sleep(self.stall)
            if self.failure is not None:
                raise self.failure
            self.exhausted = True
        finally:
            self.closed = True

    async def close(self):
        self.closed = True


def make_step(
    registry, deltas, *, model_timeout=None, stream_check=True, stall=None, failure=None,
):
    events = []
    counts = {"full": 0, "light": 0}

    async def emit(kind, data):
        events.append((kind, data))
        return object()

    def full_check():
        counts["full"] += 1
        registry.assert_current()

    def light_check():
        counts["light"] += 1
        registry.assert_runtime_current()

    options = {"model_timeout": model_timeout}
    if stream_check:
        options["stream_check"] = light_check
    provider = ReplayProvider(deltas, stall=stall, failure=failure)
    step = ModelStep(
        provider, emit, full_check, max_steps=None, max_output=None,
        deadline=None, workspace=False, **options,
    )
    return step, provider, events, counts


async def run_call(step, *, purpose="task"):
    return await step.call(
        [Message(role="user", content="stream")], [], max_group=None,
        remaining_tools=None, purpose=purpose,
    )


@pytest.mark.parametrize("purpose", ["task", "summary"])
async def test_long_stream_keeps_full_checks_constant_and_output_verbatim(purpose):
    baseline = None
    for count in (1000, 10000):
        registry = ToolRegistry((make_spec(),))
        pieces = [f"chunk-{i} " if i % 3 else f"块{i}\n" for i in range(count)]
        deltas = []
        for i, piece in enumerate(pieces):
            deltas.append(ModelText(text=piece))
            if i % 97 == 0:
                deltas.append(ModelProgress(phase="text"))
        step, provider, events, counts = make_step(registry, deltas)
        result = await run_call(step, purpose=purpose)
        expected = "".join(pieces)
        assert result.text == expected
        assert result.calls == () and result.index == 0
        text_kind = "summary_text" if purpose == "summary" else "text"
        emitted = "".join(data["text"] for kind, data in events if kind == text_kind)
        assert emitted == expected
        assert provider.exhausted and provider.closed
        assert counts["full"] <= 8
        assert counts["light"] >= count
        if baseline is None:
            baseline = counts["full"]
        else:
            assert counts["full"] == baseline


async def test_midstream_schema_tamper_rejected_at_stream_end_without_model_end():
    spec = make_spec()
    registry = ToolRegistry((spec,))

    def tamper():
        spec.output_schema["tampered"] = "midstream"
        object.__setattr__(spec, "description", "midstream tamper")

    step, provider, events, counts = make_step(
        registry, [ModelText(text="first"), tamper, ModelText(text="second")],
    )
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await run_call(step)
    kinds = [kind for kind, _ in events]
    assert kinds[0] == "model_start"
    assert "model_end" not in kinds
    assert [data["text"] for kind, data in events if kind == "text"] == [
        "first", "second",
    ]
    assert counts["light"] >= 2
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        registry.assert_current()


async def test_tool_call_delta_still_runs_full_check_with_stream_check():
    registry = ToolRegistry((make_spec(),))
    call = ModelToolCall(id="wire-1", name="local_read", arguments={"value": "x"})
    step, provider, events, counts = make_step(
        registry, [ModelText(text="a"), call, ModelText(text="b")],
    )
    result = await run_call(step)
    assert result.text == "ab"
    assert [item.id for item in result.calls] == ["wire-1"]
    assert counts["full"] >= 5
    assert counts["light"] >= 2
    assert "model_end" in [kind for kind, _ in events]


async def test_full_check_breach_at_tool_call_delta_aborts_without_model_end():
    spec = make_spec()
    registry = ToolRegistry((spec,))

    def tamper():
        spec.output_schema["tampered"] = True

    call = ModelToolCall(id="wire-1", name="local_read", arguments={"value": "x"})
    step, provider, events, counts = make_step(
        registry, [ModelText(text="a"), tamper, call, ModelText(text="never")],
    )
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await run_call(step)
    kinds = [kind for kind, _ in events]
    assert "model_end" not in kinds
    assert [data["text"] for kind, data in events if kind == "text"] == ["a"]
    assert counts["light"] >= 2
    assert not provider.exhausted


async def test_default_step_without_stream_check_keeps_full_check_per_delta():
    registry = ToolRegistry((make_spec(),))
    pieces = [f"p{i} " for i in range(50)]
    deltas = [ModelText(text=piece) for piece in pieces]
    step, provider, events, counts = make_step(registry, deltas, stream_check=False)
    result = await run_call(step)
    assert result.text == "".join(pieces)
    assert counts["full"] >= 2 * len(pieces)
    assert counts["light"] == 0


async def test_cancelled_stream_keeps_full_checks_and_never_emits_model_end():
    registry = ToolRegistry((make_spec(),))
    call = ModelToolCall(id="wire-1", name="local_read", arguments={"value": "x"})
    step, provider, events, counts = make_step(
        registry, [call], failure=asyncio.CancelledError(),
    )
    with pytest.raises(asyncio.CancelledError):
        await run_call(step)
    kinds = [kind for kind, _ in events]
    assert "model_end" not in kinds
    assert counts["full"] >= 4
    assert provider.closed and not provider.exhausted


async def test_approval_resume_rejects_registry_drift_on_current_target(wire, tmp_path):
    async with park_application(wire, tmp_path, "stream-drift.sqlite") as app:
        run_id, request = await park_echo_turn(app)
        app.service.bindings["target-http"].loop.executor.registry._items = {}
        await app.service.reply_approval(run_id, request["request_id"], allow_reply(request))
        result = await app.service.wait(run_id)
        assert (result.status, result.code) == ("failed", "tool_binding_changed")
        assert not wire.executions
        assert len(wire.requests) == 1


async def test_model_timeout_midstream_propagates_without_model_end():
    registry = ToolRegistry((make_spec(),))
    step, provider, events, counts = make_step(
        registry, [ModelText(text="partial")], model_timeout=0.005, stall=0.05,
    )
    with pytest.raises(ServiceError, match="model_timeout"):
        await run_call(step)
    kinds = [kind for kind, _ in events]
    assert "model_end" not in kinds
    assert [data["text"] for kind, data in events if kind == "text"] == ["partial"]
    assert counts["light"] >= 1
    assert provider.closed and not provider.exhausted
