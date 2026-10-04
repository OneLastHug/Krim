import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from mcp.types import CallToolResult, ListToolsResult, TextContent, Tool
from pydantic import BaseModel, Field, RootModel, computed_field

from krim_agent.api import create_app
from krim_agent.application.workflow import WorkflowServices
from krim_agent.config import Settings
from krim_agent.mcp import MCPBridge, MCPServerConfig
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI, ToolResultPart
from krim_agent.tools import ShortCircuit, ToolBase, ToolContext, ToolRegistry, tool
from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode


class ValueOutput(StrictModel):
    value: int = Field(ge=0, alias="v")

    @computed_field
    @property
    def label(self) -> str:
        return f"value-{self.value}"


class NestedOutput(StrictModel):
    item: ValueOutput


async def test_pydantic_output_uses_wire_schema_aliases_and_computed_fields():
    original = NestedOutput(item=ValueOutput(v=3))

    @tool(risk="read", output_schema=NestedOutput)
    async def model_result() -> NestedOutput:
        return original

    registry = ToolRegistry()
    registry.register(model_result)
    try:
        assert await registry.call_tool("model_result", {}) is original
        result = await registry.invoke("model_result", {}, ToolContext(registry))
        assert result.data == {"item": {"v": 3, "label": "value-3"}}
        assert not result.is_error
        schema = model_result.schema().output_schema
        assert schema == NestedOutput.model_json_schema(mode="serialization")
        assert schema["$defs"]["ValueOutput"]["required"] == ["v", "label"]
    finally:
        await registry.close()


async def test_root_model_class_style_and_existing_position_parameters():
    class Inputs(StrictModel):
        pass

    class Result(RootModel[list[int]]):
        pass

    class ClassResult(ToolBase):
        name = "class_result"
        description = "数组结果"
        parameters = Inputs
        risk = "read"
        output_schema = Result

        async def execute(self, _arguments: BaseModel, _context: ToolContext):
            return Result([1, 2])

    registry = ToolRegistry()
    registry.register(ClassResult().definition())
    try:
        result = await registry.invoke("class_result", {}, ToolContext(registry))
        assert result.data == [1, 2] and not result.is_error
        from krim_agent.tools import ToolDefinition

        async def executor(_arguments, _context):
            return 1

        async def pre(_context):
            return ShortCircuit(2)

        async def post(value, _context):
            return value + 1

        # 新字段不能改变既有第六/第七个位置参数的 Hook 含义。
        registry.register(
            ToolDefinition("legacy", "既有位置参数", Inputs, executor, "read", pre, post)
        )
        assert (await registry.invoke("legacy", {}, ToolContext(registry))).data == 3
    finally:
        await registry.close()


async def test_schema_snapshot_and_metadata_cannot_be_mutated_to_bypass_validation():
    declared = {"type": "integer"}

    @tool(risk="read", output_schema=declared)
    async def immutable_result() -> str:
        return "private-value"

    declared["type"] = "string"
    assert replace(immutable_result).schema().output_schema == {"type": "integer"}
    public = immutable_result.schema().output_schema
    public["type"] = "string"
    registry = ToolRegistry()
    registry.register(immutable_result)
    child = registry.fork()
    try:
        for target in [registry, child]:
            result = await target.invoke("immutable_result", {}, ToolContext(target))
            assert result.code == "invalid_output_schema"
            assert "private-value" not in result.content[0].text
        assert immutable_result.schema().output_schema == {"type": "integer"}
    finally:
        await child.close()
        await registry.close()


@pytest.mark.parametrize(
    "schema",
    [
        {"$ref": "https://example.invalid/schema"},
        {"properties": {"value": {"$dynamicRef": "https://example.invalid/schema"}}},
        {"$defs": {"value": {"$ref": "file:///private/schema"}}},
        {"items": {"$recursiveRef": "https://example.invalid/schema"}},
        {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "unknown"},
        {"$schema": {"bad": "dialect"}},
    ],
)
def test_bad_or_remote_output_schemas_fail_before_execution(schema):
    with pytest.raises(ValueError):

        @tool(risk="read", output_schema=schema)
        async def never_registered() -> str:
            raise AssertionError("定义失败不能执行")


@pytest.mark.parametrize(
    "schema,value,ok",
    [
        (True, {"any": 1}, True),
        (False, {"any": 1}, False),
        ({"type": "integer"}, True, False),
        ({"type": "integer"}, "3", False),
        ({"type": "string", "format": "ipv4"}, "999.1.1.1", False),
        ({"type": "string", "format": "ipv4"}, "127.0.0.1", True),
        (
            {"enum": [{"$ref": "https://example.invalid/literal"}]},
            {"$ref": "https://example.invalid/literal"},
            True,
        ),
        ({"type": "object", "properties": {"$ref": {"type": "string"}}}, {"$ref": "literal"}, True),
        ({"$defs": {"number": {"type": "integer"}}, "$ref": "#/$defs/number"}, 3, True),
        ({"$ref": "#/$defs/missing"}, 3, False),
        ({"$ref": "#"}, 3, False),
    ],
)
async def test_schema_semantics_and_unresolvable_refs_remain_local(schema, value, ok):
    @tool(risk="read", output_schema=schema)
    async def result_value():
        return value

    registry = ToolRegistry()
    registry.register(result_value)
    try:
        result = await registry.invoke("result_value", {}, ToolContext(registry))
        assert not result.is_error if ok else result.code == "invalid_output_schema"
        assert registry.audit[-1].ok is ok
        assert registry.audit[-1].code == (None if ok else "invalid_output_schema")
    finally:
        await registry.close()


@pytest.mark.parametrize("stage", ["short", "post"])
async def test_consumed_final_value_is_validated_before_one_audit_and_result_observation(stage):
    executed = []
    observed = []
    delivered = asyncio.Event()

    @tool(risk="read", output_schema={"type": "integer"})
    async def final_value() -> int:
        executed.append(True)
        return 3

    async def pre(_context):
        return ShortCircuit("short-secret") if stage == "short" else None

    async def post(value, _context):
        return "post-secret" if stage == "post" else value

    async def observer(value):
        observed.append(value.result)
        delivered.set()

    registry = ToolRegistry()
    registry.register(replace(final_value, pre_execute=pre, post_execute=post))
    registry.add_result_observer(observer)
    try:
        result = await registry.invoke(
            "final_value", {}, ToolContext(registry, run_id="audit-output")
        )
        await asyncio.wait_for(delivered.wait(), 2)
        assert result.code == "invalid_output_schema" and result.data is None
        assert len(registry.audits_for_run("audit-output")) == 1
        assert not registry.audit[-1].ok and registry.audit[-1].code == result.code
        assert observed[0] == result
        assert "secret" not in result.model_dump_json()
        assert executed == ([] if stage == "short" else [True])
    finally:
        await registry.close()


@pytest.mark.parametrize(
    "value", [float("inf"), float("nan"), 9007199254740992, "x" * 65537, object()]
)
async def test_wire_limits_are_not_relaxed_by_output_schema(value):
    @tool(risk="read", output_schema=True)
    async def invalid_wire():
        return value

    registry = ToolRegistry()
    registry.register(invalid_wire)
    try:
        result = await registry.invoke("invalid_wire", {}, ToolContext(registry))
        assert result.code == "invalid_output" and result.data is None
        assert len(registry.audit) == 1 and not registry.audit[0].ok
    finally:
        await registry.close()


class OutputProvider:
    name = "offline-output-contract"

    def __init__(self):
        self.received = None

    async def stream(self, messages, _tools):
        if messages[-1].role != "tool":
            yield ModelToolCall(id="output-call", name="bad_output", arguments={})
        else:
            self.received = messages[-1]
            yield ModelText(text="输出错误已经进入模型下一步")


async def test_runtime_sdk_http_and_persisted_audit_receive_the_same_output_failure(repository):
    @tool(name="bad_output", risk="read", output_schema={"type": "integer"})
    async def bad_output() -> str:
        return "secret-output"

    registry = ToolRegistry()
    registry.register(bad_output)
    provider = OutputProvider()
    runtime = ChatRuntime(repository, provider, registry, Settings())
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        metadata = (await api.get("/v1/tools")).json()["tools"][0]
        assert metadata["output_schema"] == {"type": "integer"}
        ai = PythonAI(runtime)
        stream = await ai.stream_text("执行声明输出的工具")
        result = await stream.collect()
        assert (
            result.run.status == "completed"
            and result.steps[0].tool_results[0].code == "invalid_output_schema"
        )
        assert len(result.audits) == 1 and not result.audits[0].ok
        assert provider.received.details["code"] == "invalid_output_schema"
        assert "secret-output" not in provider.received.content
        audit = (await api.get(f"/v1/runs/{stream.run.id}/audits")).json()
        assert audit[0]["code"] == "invalid_output_schema" and not audit[0]["ok"]
        parts = [part async for part in stream.full_stream()]
        tool_parts = [part for part in parts if isinstance(part, ToolResultPart)]
        assert len(tool_parts) == 1 and tool_parts[0].result.code == "invalid_output_schema"
        assert "secret-output" not in json.dumps([part.model_dump(mode="json") for part in parts])
        await ai.close()


async def test_workflow_cannot_consume_an_invalid_tool_output():
    @tool(risk="read", output_schema={"type": "integer"})
    async def wrong_value() -> str:
        return "not-integer"

    registry = ToolRegistry()
    registry.register(wrong_value)
    graph = WorkflowGraph(
        nodes=[WorkflowNode(id="tool", kind="tool", config={"name": "wrong_value"})]
    )
    try:
        result = (
            await WorkflowEngine()
            .execute(graph, {}, WorkflowServices(tools=registry).bindings(graph))
            .wait()
        )
        assert result.status == "failed" and result.code == "invalid_output_schema"
        assert not registry.audit[-1].ok
    finally:
        await registry.close()


async def test_mcp_output_contract_preserves_origin_permission_and_server_isolation():
    class Session:
        def __init__(self, output_schema):
            self.output_schema = output_schema
            self.calls = []

        async def initialize(self):
            return SimpleNamespace()

        async def list_tools(self, **_kwargs):
            return ListToolsResult(
                tools=[
                    Tool(
                        name="lookup",
                        inputSchema={"type": "object"},
                        outputSchema=self.output_schema,
                    )
                ]
            )

        async def call_tool(self, name, arguments):
            self.calls.append((name, arguments))
            return CallToolResult(
                content=[TextContent(type="text", text="legacy")],
                structuredContent={"value": "remote-secret"},
            )

    async def factory(session):
        return session

    schema = {"type": "object", "properties": {"value": {"type": "integer"}}, "required": ["value"]}
    good, invalid = Session(schema), Session({"$ref": "https://example.invalid/schema"})
    registry = ToolRegistry()
    bridge = MCPBridge(registry)
    try:
        ready = await bridge.add_server(
            MCPServerConfig(
                name="good", transport="stdio", command="fake", readonly_tools={"lookup"}
            ),
            session_factory=lambda: factory(good),
        )
        failed = await bridge.add_server(
            MCPServerConfig(name="invalid", transport="stdio", command="fake"),
            session_factory=lambda: factory(invalid),
        )
        assert ready.status == "ready" and failed.status == "failed"
        assert [info.server for info in bridge.tools()] == ["good"]
        assert registry.get("good__lookup").schema().output_schema == schema
        result = await registry.invoke("good__lookup", {}, ToolContext(registry))
        assert result.code == "invalid_output_schema" and len(good.calls) == 1
        assert not registry.audit[-1].ok and "remote-secret" not in result.model_dump_json()
        assert not invalid.calls
    finally:
        await bridge.close()
        await registry.close()
