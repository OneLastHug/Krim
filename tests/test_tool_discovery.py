import asyncio
import json
import socket
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from mcp.types import ListToolsResult, Tool
from pydantic import BaseModel

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.mcp import MCPBridge, MCPServerConfig
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import (
    ModelText,
    ModelToolCall,
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.sdk import PythonAI, ToolDiscoveryPart
from krim_agent.tools import ShortCircuit, ToolBase, ToolContext, ToolRegistry, tool
from krim_agent.tools.discovery import ToolDiscovery


def registry_for_discovery(effects):
    @tool(
        name="private_lookup",
        description="查询中文编号 lookup",
        risk="read",
        should_defer=True,
        output_schema={"type": "integer"},
    )
    async def lookup(value: int = 3) -> int:
        effects.append(value)
        return value

    @tool(name="always_ready", risk="read", should_defer=True, always_load=True)
    async def ready() -> str:
        return "ready"

    registry = ToolRegistry()
    registry.register_many(lookup, ready)
    return registry


def test_class_style_flags_and_nonboolean_configuration():
    class Inputs(StrictModel):
        pass

    class Deferred(ToolBase):
        name = "class_deferred"
        description = "class tool"
        parameters = Inputs
        risk = "read"
        should_defer = True
        always_load = False

        async def execute(self, _arguments: BaseModel, _context: ToolContext):
            return "value"

    definition = Deferred().definition()
    assert definition.schema().should_defer and not definition.schema().always_load
    with pytest.raises(ValueError, match="布尔"):
        replace(definition, should_defer="yes")
    with pytest.raises(ValueError, match="布尔"):
        replace(definition, always_load=1)


class DiscoveryProvider:
    name = "offline-tool-discovery"

    def __init__(self, *, same_batch=False):
        self.same_batch = same_batch
        self.snapshots = []

    async def stream(self, messages, tools):
        self.snapshots.append([definition.name for definition in tools])
        last_user = max(index for index, message in enumerate(messages) if message.role == "user")
        results = [message for message in messages[last_user + 1 :] if message.role == "tool"]
        if not results:
            yield ModelToolCall(id="search", name="tool_search", arguments={"query": "编号"})
            if self.same_batch:
                yield ModelToolCall(id="guess", name="private_lookup", arguments={"value": 99})
        elif (
            results[-1].details["name"] in {"tool_search", "private_lookup"}
            and "private_lookup" in self.snapshots[-1]
            and not any(
                result.details["name"] == "private_lookup" and not result.details["is_error"]
                for result in results
            )
        ):
            yield ModelToolCall(id="actual", name="private_lookup", arguments={"value": 7})
        else:
            yield ModelText(text="发现流程完成")


@pytest.mark.parametrize("same_batch", [False, True])
async def test_first_step_hidden_next_step_loaded_and_same_batch_guess_is_denied(same_batch):
    effects = []
    registry = registry_for_discovery(effects)
    provider = DiscoveryProvider(same_batch=same_batch)
    async with await PythonAI.create(provider=provider, tools=registry) as ai:
        stream = await ai.stream_text("发现工具", idempotency_key="once")
        result = await stream.collect()
        assert result.run.status == "completed" and effects == [7]
        assert provider.snapshots[0] == ["always_ready", "tool_search"]
        assert "private_lookup" in provider.snapshots[1]
        failures = [
            tool.code for step in result.steps for tool in step.tool_results if tool.is_error
        ]
        assert failures == (["tool_not_loaded"] if same_batch else [])
        parts = [part async for part in stream.full_stream()]
        discovered = [part for part in parts if isinstance(part, ToolDiscoveryPart)]
        assert len(discovered) == 1 and discovered[0].tools == ["private_lookup"]
        assert discovered[0].call_id == "search" and discovered[0].step == 0
        count = len(provider.snapshots)
        again = await ai.stream_text(
            "发现工具", topic_id=stream.run.topic_id, idempotency_key="once"
        )
        assert again.run.id == stream.run.id
        assert [part async for part in again.full_stream()] == parts
        assert len(provider.snapshots) == count and effects == [7]


async def test_new_run_in_same_topic_starts_with_undiscovered_catalog():
    effects = []
    provider = DiscoveryProvider()
    async with await PythonAI.create(
        provider=provider, tools=registry_for_discovery(effects)
    ) as ai:
        first = await ai.generate_text("first")
        count = len(provider.snapshots)
        second = await ai.generate_text("second", topic_id=first.run.topic_id)
        assert second.run.status == "completed"
        assert provider.snapshots[count] == ["always_ready", "tool_search"]
        assert effects == [7, 7]


async def test_concurrent_runs_share_definitions_but_not_discovery_state():
    entered, release = asyncio.Event(), asyncio.Event()
    effects = []

    class ConcurrentProvider(DiscoveryProvider):
        async def stream(self, messages, tools):
            user = next(message.content for message in reversed(messages) if message.role == "user")
            names = [definition.name for definition in tools]
            if user == "second":
                assert "private_lookup" not in names
                if messages[-1].role != "tool":
                    yield ModelToolCall(id="guess", name="private_lookup", arguments={})
                else:
                    assert messages[-1].details["code"] == "tool_not_loaded"
                    yield ModelText(text="未发现工具被拒绝")
                return
            if "private_lookup" in names:
                entered.set()
                await release.wait()
            async for part in super().stream(messages, tools):
                yield part

    async with await PythonAI.create(
        provider=ConcurrentProvider(), tools=registry_for_discovery(effects)
    ) as ai:
        first = await ai.stream_text("first")
        await asyncio.wait_for(entered.wait(), 5)
        second = await ai.generate_text("second")
        assert second.steps[0].tool_results[0].code == "tool_not_loaded" and not effects
        release.set()
        assert (await first.collect()).run.status == "completed" and effects == [7]


@pytest.mark.parametrize("mode", ["deny", "short", "invalid", "filtered", "forged"])
async def test_search_hooks_and_policy_cannot_activate_nonexecuted_or_filtered_candidates(mode):
    effects = []
    registry = registry_for_discovery(effects)

    async def policy(definition, _context):
        return "deny" if mode == "deny" and definition.name == "tool_search" else "allow"

    async def pre(definition, _context):
        if mode == "short" and definition.name == "tool_search":
            return ShortCircuit(
                {"tools": [{"name": "private_lookup", "description": "伪造", "risk": "read"}]}
            )

    async def post(definition, value, _context):
        if definition.name != "tool_search":
            return value
        if mode == "invalid":
            return {"tools": "invalid"}
        if mode == "filtered":
            return {"tools": []}
        if mode == "forged":
            return {
                "tools": [{"name": "private_lookup", "description": "不在搜索候选", "risk": "read"}]
            }
        return value

    class Provider:
        name = "offline-search-hooks"

        def __init__(self):
            self.count = 0

        async def stream(self, messages, tools):
            self.count += 1
            assert "private_lookup" not in [definition.name for definition in tools]
            if self.count == 1:
                yield ModelToolCall(
                    id="search",
                    name="tool_search",
                    arguments={"query": "no-match" if mode == "forged" else "lookup"},
                )
            elif self.count == 2:
                yield ModelToolCall(id="guess", name="private_lookup", arguments={})
            else:
                yield ModelText(text="失败保持隔离")

    registry.add_global_pre(pre)
    registry.add_global_post(post)
    ai = await PythonAI.create(provider=Provider(), tools=registry)
    ai.runtime.tool_policy = policy
    try:
        result = await ai.generate_text("hooks")
        assert result.run.status == "completed" and not effects
        assert result.steps[1].tool_results[0].code == "tool_not_loaded"
        assert len(result.audits) == 2
    finally:
        await ai.close()


async def test_discovery_does_not_grant_write_permission():
    effects = []

    @tool(name="private_lookup", description="查询编号 lookup", should_defer=True)
    async def write(value: int):
        effects.append(True)
        return "changed"

    registry = ToolRegistry()
    registry.register(write)

    class WriteProvider:
        name = "offline-discovered-write"

        async def stream(self, messages, tools):
            if messages[-1].role != "tool":
                yield ModelToolCall(id="search", name="tool_search", arguments={"query": "编号"})
            elif messages[-1].details["name"] == "tool_search":
                assert "private_lookup" in [definition.name for definition in tools]
                yield ModelToolCall(id="write", name="private_lookup", arguments={"value": 7})
            else:
                assert messages[-1].details["code"] == "permission_denied"
                yield ModelText(text="发现不授予写权限")

    provider = WriteProvider()
    async with await PythonAI.create(provider=provider, tools=registry) as ai:
        result = await ai.generate_text("write")
        assert result.run.status == "completed" and not effects
        assert result.steps[1].tool_results[0].code == "permission_denied"
        assert result.audits[0].risk == "read" and result.audits[0].ok
        assert result.audits[1].risk == "write" and result.audits[1].decision == "deny"


async def test_agent_run_fork_preserves_deferred_flags_and_local_search(repository):
    effects = []
    registry = registry_for_discovery(effects)
    provider = DiscoveryProvider()
    manager = AgentManager(repository)
    try:
        handle = await manager.spawn(
            AgentDefinition(name="discoverer", provider=provider, tools=registry)
        )
        result = await handle.execute("发现")
        assert result.status == "completed" and effects == [7]
        assert provider.snapshots[0] == ["always_ready", "tool_search"]
        assert handle.runtime.tools.get("private_lookup").should_defer
    finally:
        await manager.close()


async def test_reserved_search_conflict_is_persisted_without_model_execution():
    effects = []
    registry = registry_for_discovery(effects)

    @tool(name="tool_search", risk="read")
    async def custom_search():
        return "custom"

    registry.register(custom_search)
    provider = DiscoveryProvider()
    async with await PythonAI.create(provider=provider, tools=registry) as ai:
        first = await ai.stream_text("conflict", idempotency_key="once")
        result = await first.collect()
        assert result.code == "tool_catalog_conflict" and not provider.snapshots
        retry = await ai.stream_text(
            "conflict", topic_id=first.run.topic_id, idempotency_key="once"
        )
        assert retry.run.id == first.run.id and not provider.snapshots


async def test_mcp_flags_use_remote_names_and_do_not_load_on_discovery_configuration():
    class Session:
        async def initialize(self):
            pass

        async def list_tools(self, **_kwargs):
            return ListToolsResult(
                tools=[Tool(name="Remote.Lookup", inputSchema={"type": "object"})]
            )

        async def call_tool(self, *_args):
            raise AssertionError("配置发现不能调用工具")

    async def factory():
        return Session()

    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    try:
        status = await bridge.add_server(
            MCPServerConfig(
                name="remote", transport="stdio", command="fake", deferred_tools={"Remote.Lookup"}
            ),
            session_factory=factory,
        )
        assert status.status == "ready"
        info = registry.list_tools()[0]
        assert info.name == "remote__remote_lookup" and info.should_defer
        assert info.risk == "write" and not info.always_load
    finally:
        await bridge.close()
        await registry.close()


@pytest.mark.parametrize(
    "arguments",
    [
        {"query": " "},
        {"query": "x" * 513},
        {"query": "lookup", "limit": 0},
        {"query": "lookup", "limit": 17},
        {"query": "lookup", "limit": "8"},
        {"query": "lookup", "name": "injected"},
    ],
)
async def test_invalid_search_input_does_not_stage_or_activate_a_tool(arguments):
    effects = []
    registry = registry_for_discovery(effects)
    discovery = ToolDiscovery(registry.definitions)
    step = discovery.prepare()
    try:
        result = await registry.invoke(
            "tool_search",
            arguments,
            ToolContext(registry, call_id="invalid", model_tools=step.bindings()),
        )
        assert result.code == "invalid_input"
        assert not discovery.consume(step, "invalid", "tool_search", result)
        assert "private_lookup" not in discovery.loaded and not effects
        assert not registry.audit[-1].ok
    finally:
        await registry.close()


async def test_native_nested_call_is_explicit_and_still_uses_the_permission_chain():
    effects = []
    registry = registry_for_discovery(effects)

    @tool(name="wrapper", risk="read")
    async def wrapper(context: ToolContext) -> int:
        return await context.call_tool("private_lookup", {"value": 5})

    class Provider:
        name = "offline-native-nested"

        async def stream(self, messages, tools):
            assert "private_lookup" not in [definition.name for definition in tools]
            if messages[-1].role != "tool":
                yield ModelToolCall(id="wrapper", name="wrapper", arguments={})
            else:
                yield ModelText(text="显式嵌套完成")

    registry.register(wrapper)
    async with await PythonAI.create(provider=Provider(), tools=registry) as ai:
        result = await ai.generate_text("native")
        assert result.run.status == "completed" and effects == [5]
        assert {audit.depth for audit in result.audits} == {1, 2}


async def test_stale_or_forged_step_binding_does_not_replace_registered_definition():
    effects = []
    registry = registry_for_discovery(effects)
    original = registry.get("private_lookup")
    try:
        candidate = replace(original, description="changed")
        result = await registry.invoke(
            "private_lookup", {}, ToolContext(registry, model_tools={"private_lookup": candidate})
        )
        assert result.code == "tool_binding_changed" and not effects
        missing = await registry.invoke("missing", {}, ToolContext(registry, model_tools={}))
        assert missing.code == "tool_not_found"
    finally:
        await registry.close()


@asynccontextmanager
async def tcp_app(app):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", timeout_graceful_shutdown=2))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            for _ in range(500):
                if server.started:
                    break
                if task.done():
                    await task
                await asyncio.sleep(0.01)
            assert server.started
        yield f"http://127.0.0.1:{listener.getsockname()[1]}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)
        listener.close()


async def test_real_compatible_provider_sends_only_prepared_schemas_each_step():
    records, effects = [], []
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        data = await request.json()
        records.append(data)
        index = len(records)
        names = [definition["function"]["name"] for definition in data["tools"]]
        assert names == (
            ["always_ready", "tool_search"]
            if index == 1
            else ["private_lookup", "always_ready", "tool_search"]
        )

        async def stream():
            if index <= 2:
                call_name, args = (
                    ("tool_search", {"query": "lookup"})
                    if index == 1
                    else ("private_lookup", {"value": 11})
                )
                delta = {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": f"tcp-{index}",
                            "type": "function",
                            "function": {"name": call_name, "arguments": json.dumps(args)},
                        }
                    ]
                }
                reason = "tool_calls"
            else:
                assert data["messages"][-1]["content"] == "11"
                delta, reason = {"content": "TCP 发现已完成"}, "stop"
            yield (
                "data: "
                + json.dumps({"choices": [{"index": 0, "delta": delta, "finish_reason": None}]})
                + "\n\n"
            )
            yield (
                "data: "
                + json.dumps({"choices": [{"index": 0, "delta": {}, "finish_reason": reason}]})
                + "\n\ndata: [DONE]\n\n"
            )

        return StreamingResponse(stream(), media_type="text/event-stream")

    async with tcp_app(app) as url:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=url + "/v1", model="offline")
        )
        try:
            async with await PythonAI.create(
                provider=provider, tools=registry_for_discovery(effects)
            ) as ai:
                result = await ai.generate_text("discovery")
                assert result.run.status == "completed" and result.text == "TCP 发现已完成"
                assert effects == [11] and len(records) == 3
        finally:
            await provider.close()
