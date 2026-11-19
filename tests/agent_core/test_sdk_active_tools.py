"""活跃工具选择的真实链路：发现可见性、端口检查点与无限预算边界。

真实 ai 0.9.0 Agent 循环 + 真实 runtime/run_tools.RunTools 端口；内部模型用
确定性脚本替身。发现只改模型可见 schema，不改变执行与审批权限。
"""

import json
from dataclasses import replace
from typing import Any
from uuid import uuid4

import ai
import pytest
from ai import messages as sdk
from ai.types.tools import Tool
from ai.types.tools import ToolSpec as SDKToolSpec
from pydantic import JsonValue

from krim_agent.agent_sdk.model import HostModelProvider, RunToolsPort
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import (
    Message,
    RunEvent,
    ServiceError,
    StrictModel,
    encoded,
)
from krim_agent.mcp import MCPServerConfig
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.runtime.model_step import ModelStep
from krim_agent.runtime.run_tools import open_run_tools
from krim_agent.security import Membership, PrincipalContext
from krim_agent.tools.catalog_search import scope_key
from krim_agent.tools.core import Invocation, ToolSource, ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import SessionToolScope, ToolBundle


class Value(StrictModel):
    value: str = ""


async def _native(arguments: Value) -> JsonValue:
    return {"value": arguments.value}


def native_spec() -> ToolSpec:
    return ToolSpec("local_read", Value, _native, {"type": "object"}, "1", "a" * 64)


def external_spec(name: str = "remote__search", calls: list[Any] | None = None,
                  *, digest: str = "b" * 64) -> ToolSpec:
    async def execute(invocation: Invocation, arguments: Value) -> JsonValue:
        if calls is not None:
            calls.append((invocation.name, arguments.value))
        return {"value": arguments.value}

    return ToolSpec(
        name, Value, _native, {"type": "object"}, "1", "a" * 64,
        risk="read",
        source=ToolSource("mcp", "remote", name, digest),
        external_executor=execute,
        description="Search remote documents",
    )


class Connection:
    def __init__(self) -> None:
        self.config = MCPServerConfig(name="remote", transport="stdio", command="fixture")
        self.supports_resources = True


class FakeHost:
    """ToolRuntime 形状替身：open_run_tools 只借用 bundle 与 connections。"""

    def __init__(self, spec: ToolSpec, connection: Connection) -> None:
        self.registry = ToolRegistry((spec,))
        self.connections = (connection,)
        self.web = None

    def bundle(self, resolve: Any) -> ToolBundle:
        spec = self.registry.get(str(self.registry.schemas()[0]["name"]))

        async def invoke(invocation: Invocation, arguments: Any) -> JsonValue:
            key = scope_key(await resolve(invocation))
            assert spec.external_executor is not None
            value = await spec.external_executor(invocation, arguments)
            if scope_key(await resolve(invocation)) != key:
                raise ServiceError("permission_denied", 403)
            return value

        async def policy(invocation: Invocation) -> str:
            return "allow" if invocation.name == spec.name else "deny"

        return ToolBundle((replace(spec, external_executor=invoke),), policy)


def make_scope() -> SessionToolScope:
    return SessionToolScope(
        PrincipalContext("alice", 1, (Membership(workspace_id="local"),)),
        "local", "session", "turn", "run",
    )


async def open_tools(spec: ToolSpec | None = None) -> Any:
    scope = make_scope()

    async def resolve(call: Invocation) -> SessionToolScope:
        return scope

    base = ToolExecutor(ToolRegistry((native_spec(),)), None)
    host = FakeHost(spec or external_spec(), Connection())
    return await open_run_tools(base, scope=scope, resolve=resolve, host=host)


class Ledger:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []
        self.manifests: list[dict[str, JsonValue]] = []

    async def emit(self, kind: str, data: dict[str, JsonValue]) -> RunEvent:
        if kind == "model_start":
            manifest = data.get("context_manifest")
            self.manifests.append(manifest if isinstance(manifest, dict) else {})
        event = RunEvent(run_id="run", seq=len(self.events),
                         event_id=f"e-{len(self.events)}", kind=kind, data=data)
        self.events.append(event)
        return event

    async def approve(self, call: Any, seconds: float | None) -> bool:
        return True


class Script:
    """确定性内部模型：记录每次请求的工具与消息，按回合回放输出。"""

    def __init__(self, *turns: Any) -> None:
        # A ``None`` turn yields nothing: the model step completes without output.
        self.turns = [None if turn is None else list(turn) for turn in turns]
        self.tools: list[list[dict[str, JsonValue]]] = []
        self.messages: list[list[dict[str, JsonValue]]] = []

    async def stream(self, messages: list[Message], tools: list[dict[str, JsonValue]]) -> Any:
        self.tools.append(list(tools))
        self.messages.append([m.model_dump(mode="json") for m in messages])
        turn = self.turns.pop(0) if self.turns else [ModelText(text="done")]
        for item in ([] if turn is None else turn):
            yield item


def names(script: Script, index: int) -> set[str]:
    return {str(item["name"]) for item in script.tools[index]}


def connect(script: Script, registry: ToolRegistry, *, run_tools: RunToolsPort | None = None,
            max_group: int | None = 2, max_calls: int | None = 4,
            request_bytes: int | None = 65536, context_policy: ContextPolicy | None = None,
            host_media: Any = None, max_steps: int = 8) -> Any:
    ledger = Ledger()
    stepper = ModelStep(script, ledger.emit, lambda: None, max_steps=max_steps,
                        max_output=65536, deadline=None, workspace=False)
    provider = HostModelProvider(
        stepper, registry, max_group=max_group, max_calls=max_calls,
        request_bytes=request_bytes, context_policy=context_policy,
        host_media=host_media, run_tools=run_tools,
    )
    return provider, ledger, stepper


def sdk_user(text: str = "go") -> list[sdk.Message]:
    return [sdk.Message(role="user", parts=[sdk.TextPart(text=text)])]


def _plain(value: Any) -> JsonValue:
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return value


def bridge(executor: Any, ledger: Ledger) -> list[ai.AgentTool]:
    """Declare every registry tool to the SDK; execution goes through the executor."""
    tools = []
    for descriptor in executor.registry.schemas():
        name = str(descriptor["name"])
        spec = executor.registry.get(name)

        async def invoke(**kwargs: Any) -> JsonValue:
            call = ai.current_tool_call()
            bound = executor.registry.get(call.name)
            invocation = Invocation(
                run_id="run", call_id="call-" + uuid4().hex, span_id="span-" + uuid4().hex,
                step_index=0, name=call.name, arguments_json=encoded(_plain(kwargs)),
                tool_hash=bound.fingerprint, provider_call_id=call.id,
            )
            context = ToolContext(ledger.emit, ledger.approve, None)
            admission = await executor.admit(invocation, context, timeout_seconds=None)
            observation = await executor.execute(invocation, context, admission)
            if observation.status != "ok":
                raise ServiceError(observation.code or "tool_error")
            return observation.model_dump(mode="json")

        tools.append(ai.AgentTool(
            tool=Tool(kind="function", name=name, spec=SDKToolSpec(
                description=spec.description or None,
                params=spec.parameters.model_json_schema(),
            )),
            fn=invoke, validator=spec.parameters,
        ))
    return tools


async def test_sdk_loop_hides_external_tool_until_tool_search_activates():
    tools = await open_tools()
    script = Script(
        [ModelToolCall(id="wire-1", name="tool_search",
                      arguments={"names": ["remote__search"]})],
        [ModelText(text="done")],
    )
    provider, ledger, _ = connect(script, tools.executor.registry, run_tools=tools)
    agent = ai.Agent(tools=bridge(tools.executor, ledger))
    async with agent.run(ai.Model(id="fixture", provider=provider),
                         sdk_user("find the remote search tool")) as stream:
        events = [event async for event in stream]
    assert not script.turns
    assert "remote__search" not in names(script, 0)
    assert {"tool_search", "local_read"} <= names(script, 0)
    assert "remote__search" in names(script, 1)
    assert any(getattr(event, "chunk", None) == "done"
               for event in events if type(event).__name__ == "TextDelta")
    await tools.close()


async def test_checkpoint_json_and_rebuilt_port_restore_keep_activation():
    tools = await open_tools()
    script = Script([ModelText(text="ready")])
    provider, ledger, stepper = connect(script, tools.executor.registry, run_tools=tools)
    async for _ in provider.stream(ai.Model(id="fixture", provider=provider), sdk_user()):
        pass
    search = tools.executor.registry.get("tool_search")
    context = ToolContext(ledger.emit, ledger.approve, None)
    observation = await tools.executor.execute(
        Invocation("run", "call", "span", stepper.index, search.name,
                   json.dumps({"names": ["remote__search"]}), search.fingerprint),
        context,
    )
    assert observation.status == "ok"
    state = json.loads(json.dumps(provider.checkpoint()))
    assert any(str(handle).startswith("remote__search:")
               for handle in state["run_tools"]["catalog"]["active_handles"])
    assert state["run_tools"]["version"] == "run-tools/1"

    restored_tools = await open_tools()
    restored_script = Script([ModelText(text="done")])
    restored, _, restored_stepper = connect(
        restored_script, restored_tools.executor.registry, run_tools=restored_tools)
    restored.restore(state)
    assert restored_stepper.index == state["step"]
    assert "remote__search" in {item["name"] for item in restored_tools.schemas()}
    async for _ in restored.stream(ai.Model(id="fixture", provider=restored), sdk_user()):
        pass
    assert "remote__search" in names(restored_script, 0)
    await tools.close()
    await restored_tools.close()


async def test_restore_rejects_drift_and_state_mismatch_before_writing_counters():
    tools = await open_tools()
    provider, _, stepper = connect(Script(), tools.executor.registry, run_tools=tools)
    state = provider.checkpoint()

    drifted = await open_tools(external_spec(digest="d" * 64))
    drift_provider, _, drift_stepper = connect(
        Script(), drifted.executor.registry, run_tools=drifted)
    with pytest.raises(ServiceError) as changed:
        drift_provider.restore(state)
    assert changed.value.code == "tool_binding_changed"
    assert drift_stepper.index == 0

    missing = {key: value for key, value in state.items() if key != "run_tools"}
    with pytest.raises(ServiceError) as corrupt:
        provider.restore(missing)
    assert corrupt.value.code == "facts_corrupt"
    assert stepper.index == 0

    legacy_registry = ToolRegistry((native_spec(),))
    legacy_provider, _, legacy_stepper = connect(Script(), legacy_registry)
    with pytest.raises(ServiceError) as extra:
        legacy_provider.restore(state)
    assert extra.value.code == "facts_corrupt"
    assert legacy_stepper.index == 0
    legacy_state = legacy_provider.checkpoint()
    assert "run_tools" not in legacy_state
    legacy_provider.restore(legacy_state)
    await tools.close()
    await drifted.close()


class FakePort:
    """结构化 RunToolsPort 替身：只回放给定 schema，不接触执行器。"""

    def __init__(self, registry: ToolRegistry, schemas: Any) -> None:
        self._registry = registry
        self._schemas = schemas
        self.restored: dict[str, Any] | None = None

    def schemas(self) -> Any:
        return self._schemas

    def checkpoint(self) -> dict[str, Any]:
        return {"version": "fake-port/1", "schemas": len(self._schemas or [])}

    def restore(self, state: dict[str, Any]) -> None:
        self.restored = state


async def test_port_selection_is_strictly_checked_against_frozen_registry():
    registry = ToolRegistry((native_spec(),))
    frozen = registry.schemas()
    script = Script([ModelText(text="ok")])
    provider, _, _ = connect(script, registry, run_tools=FakePort(registry, list(frozen)))
    async for _ in provider.stream(ai.Model(id="fixture", provider=provider), sdk_user()):
        pass
    assert names(script, 0) == {"local_read"}

    model = ai.Model(id="fixture", provider=provider)
    cases = {
        "unknown": [dict(frozen[0], name="hidden")],
        "duplicate": [frozen[0], dict(frozen[0])],
        "drift": [dict(frozen[0], description="changed")],
        "shape": ["local_read"],
        "container": "local_read",
        "missing_name": [{"description": "no name"}],
    }
    for label, schemas in cases.items():
        blocked = Script()
        rejected, _, _ = connect(blocked, registry, run_tools=FakePort(registry, schemas))
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            async for _ in rejected.stream(model, sdk_user()):
                pass
        assert blocked.tools == [], label


async def test_checkpoint_syncs_port_state_and_restore_passes_it_back():
    registry = ToolRegistry((native_spec(),))
    port = FakePort(registry, registry.schemas())
    provider, _, _ = connect(Script(), registry, run_tools=port)
    state = json.loads(json.dumps(provider.checkpoint()))
    assert state["run_tools"] == {"version": "fake-port/1", "schemas": 1}
    provider.restore(state)
    assert port.restored == state["run_tools"]


async def test_context_policy_governs_budget_when_request_bytes_are_unlimited():
    registry = ToolRegistry((native_spec(),))
    policy = ContextPolicy(max_bytes=4096, reserved_output_tokens=64)
    script = Script([ModelText(text="done")])
    provider, ledger, _ = connect(script, registry, request_bytes=None, context_policy=policy)
    async for _ in provider.stream(ai.Model(id="fixture", provider=provider), sdk_user("go")):
        pass
    assert ledger.manifests[0]["input_budget_bytes"] == (
        policy.max_bytes - 1024 - policy.reserved_output_tokens * 4
    )

    oversized = Script()
    rejected, _, _ = connect(oversized, registry, request_bytes=None, context_policy=policy)
    with pytest.raises(ServiceError, match="context_budget"):
        async for _ in rejected.stream(ai.Model(id="fixture", provider=rejected),
                                       sdk_user("go" * 8000)):
            pass


async def test_unlimited_bytes_keep_host_media_and_full_registry_intact():
    registry = ToolRegistry((native_spec(), external_spec()))
    seen_media: list[bool] = []

    async def media(history: list[Message]) -> list[Message]:
        seen_media.append(True)
        return [*history, Message(role="user", content="HOST_MEDIA_MARKER")]

    script = Script([ModelText(text="done")])
    provider, ledger, _ = connect(script, registry, request_bytes=None, host_media=media)
    async for _ in provider.stream(ai.Model(id="fixture", provider=provider),
                                   sdk_user("go" * 200_000)):
        pass
    assert seen_media == [True]
    assert names(script, 0) == {"local_read", "remote__search"}
    assert any("HOST_MEDIA_MARKER" in json.dumps(m) for m in script.messages[0])
    assert "input_budget_bytes" not in ledger.manifests[0]

    state = json.loads(json.dumps(provider.checkpoint()))
    assert "media" not in state  # a plain callback carries no media state port
    provider.restore(state)
    poisoned = dict(state)
    poisoned["media"] = {"refs": []}
    with pytest.raises(ServiceError, match="facts_corrupt"):
        provider.restore(poisoned)


async def test_none_max_group_falls_back_to_sequential_default_without_total_limit():
    registry = ToolRegistry((native_spec(),))
    script = Script([ModelText(text="done")])
    provider, _, _ = connect(script, registry, max_group=None, max_calls=None,
                             request_bytes=None)
    async for _ in provider.stream(ai.Model(id="fixture", provider=provider), sdk_user("go")):
        pass
    prompt = script.messages[0][0]["content"]
    assert "tool calls allowed in this request: 1" in prompt
    assert "remaining tool calls for this run: unlimited" in prompt


class MediaRun:
    """SessionMediaRun 形状替身：async投影 + 同步refs checkpoint/restore + async确认。"""

    def __init__(self, marker: str = "MEDIA_INJECTED") -> None:
        self.marker = marker
        self.calls = 0
        self.acks = 0
        self.restored: dict[str, Any] | None | str = "unset"
        self.selection = {"version": "media-selection/1", "refs": ["ref-1"]}

    async def __call__(self, history: list[Message]) -> list[Message]:
        self.calls += 1
        return [*history, Message(role="user", content=self.marker)]

    def checkpoint(self) -> dict[str, Any]:
        return dict(self.selection)

    def restore_checkpoint(self, state: dict[str, Any] | None) -> None:
        self.restored = state

    async def accepted(self) -> None:
        self.acks += 1


async def test_media_refs_checkpoint_and_acknowledge_are_gated_on_step_success():
    media = MediaRun()
    script = Script([ModelText(text="done")])
    provider, _, _ = connect(script, ToolRegistry((native_spec(),)),
                             request_bytes=None, host_media=media)
    async for _ in provider.stream(ai.Model(id="fixture", provider=provider), sdk_user("go")):
        pass
    assert media.calls == 1 and media.acks == 1
    assert "MEDIA_INJECTED" in json.dumps(script.messages[0])
    state = json.loads(json.dumps(provider.checkpoint()))
    assert state["media"] == media.selection

    restored_media = MediaRun()
    restored_script = Script([ModelText(text="done")])
    restored, _, _ = connect(restored_script, ToolRegistry((native_spec(),)),
                             request_bytes=None, host_media=restored_media)
    restored.restore(state)
    assert restored_media.restored == media.selection
    async for _ in restored.stream(ai.Model(id="fixture", provider=restored), sdk_user("go")):
        pass
    assert restored_media.acks == 1

    missing = {key: value for key, value in state.items() if key != "media"}
    with pytest.raises(ServiceError, match="facts_corrupt"):
        restored.restore(missing)

    silent = Script(None)
    failing, _, _ = connect(silent, ToolRegistry((native_spec(),)),
                            request_bytes=None, host_media=media)
    with pytest.raises(ServiceError, match="no_output"):
        async for _ in failing.stream(ai.Model(id="fixture", provider=failing), sdk_user("go")):
            pass
    assert media.calls == 2 and media.acks == 1  # the failed step never acknowledged


class MediaRunAsyncRestore:
    """F 当前真实形状：无同步 restore_checkpoint；async restore(selection, history)。"""

    def __init__(self) -> None:
        self.live_marker = "MEDIA_LIVE"
        self.restore_marker = "MEDIA_RECONSTRUCTED"
        self.calls = 0
        self.acks = 0
        self.restore_args: list[Any] = []
        self.selection = {"version": "media-selection/1", "refs": ["ref-1"]}

    async def __call__(self, history: list[Message]) -> list[Message]:
        self.calls += 1
        return [*history, Message(role="user", content=self.live_marker)]

    def checkpoint(self) -> dict[str, Any]:
        return dict(self.selection)

    async def acknowledge(self) -> None:
        self.acks += 1

    async def restore(self, selection: dict[str, Any],
                      history: list[Message]) -> list[Message]:
        self.restore_args.append((selection, [m.model_dump(mode="json") for m in history]))
        return [*history, Message(role="user", content=self.restore_marker)]


@pytest.mark.parametrize("cancelled", [False, True])
async def test_pending_media_restore_remains_retryable_after_error_or_cancellation(cancelled):
    import asyncio

    class FailingMedia(MediaRunAsyncRestore):
        attempts = 0

        async def restore(self, selection, history):
            self.attempts += 1
            if self.attempts == 1:
                if cancelled:
                    raise asyncio.CancelledError
                raise ServiceError("storage_unavailable")
            return await super().restore(selection, history)

    media = FailingMedia()
    script = Script([ModelText(text="done")])
    provider, _, _ = connect(script, ToolRegistry((native_spec(),)),
                             request_bytes=None, host_media=media)
    provider.restore(json.loads(json.dumps(provider.checkpoint())))
    error = asyncio.CancelledError if cancelled else ServiceError
    with pytest.raises(error):
        async for _ in provider.stream(ai.Model(id="fixture", provider=provider), sdk_user()):
            pass
    assert media.acks == 0 and not script.messages
    assert provider._media_pending == media.selection
    async for _ in provider.stream(ai.Model(id="fixture", provider=provider), sdk_user()):
        pass
    assert media.attempts == 2 and media.calls == 0 and media.acks == 1
    assert provider._media_pending is None


async def test_media_selection_stash_reconstructs_authorization_next_stream():
    media = MediaRunAsyncRestore()
    script = Script([ModelText(text="done")])
    provider, _, _ = connect(script, ToolRegistry((native_spec(),)),
                             request_bytes=None, host_media=media)
    state = json.loads(json.dumps(provider.checkpoint()))
    provider.restore(state)  # no sync restore: stashed only, nothing applied yet
    assert media.restore_args == [] and media.calls == 0
    async for _ in provider.stream(ai.Model(id="fixture", provider=provider), sdk_user("go")):
        pass
    assert media.calls == 0  # the resumed request goes through the projection restore
    assert len(media.restore_args) == 1
    assert media.restore_args[0][0] == media.selection
    assert media.acks == 1
    assert "MEDIA_RECONSTRUCTED" in json.dumps(script.messages[0])
    # The stash is consumed; a following request uses the ordinary projection.
    second = Script([ModelText(text="done")])
    again, _, _ = connect(second, ToolRegistry((native_spec(),)),
                          request_bytes=None, host_media=media)
    async for _ in again.stream(ai.Model(id="fixture", provider=again), sdk_user("go")):
        pass
    assert media.calls == 1 and "MEDIA_LIVE" in json.dumps(second.messages[0])
    assert "MEDIA_RECONSTRUCTED" not in json.dumps(second.messages[0])
