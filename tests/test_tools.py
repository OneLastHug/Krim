import asyncio
import time
from dataclasses import replace
from typing import Annotated

import pytest
from pydantic import BaseModel, Field

from krim_agent.protocol.models import StrictModel
from krim_agent.tools import ShortCircuit, ToolBase, ToolContext, ToolError, ToolRegistry, tool
from krim_agent.tools.builtins import builtin_registry


async def test_defaults_validation_and_normalization():
    registry = builtin_registry()
    assert await registry.call_tool("echo", {}) == ""
    assert await registry.call_tool("add", {"left": 2, "right": 3}) == {"sum": 5.0}
    result = await registry.invoke("add", {"left": 2, "right": 3}, ToolContext(registry))
    assert result.data == {"sum": 5.0} and not result.is_error
    for arguments in ({"left": "2", "right": 3}, {"left": 2}, {"text": "x", "extra": 1}):
        with pytest.raises(ToolError, match="参数"):
            await registry.call_tool("add" if "left" in arguments else "echo", arguments)
    assert registry.list_tools()[1].input_schema["properties"]["text"]["default"] == ""
    with pytest.raises(ToolError) as error:
        await registry.call_tool("missing", {})
    assert error.value.code == "tool_not_found"
    with pytest.raises(ValueError):
        registry.register(registry.get("add"))
    await registry.close()


async def test_declared_output_schema_is_exposed_and_enforced_at_consumer_boundary():
    schema = {
        "type": "object",
        "properties": {"value": {"type": "integer", "minimum": 0}},
        "required": ["value"],
        "additionalProperties": False,
    }

    @tool(name="typed_result", risk="read", output_schema=schema)
    async def typed_result() -> dict[str, int]:
        return {"value": 3}

    @tool(name="bad_result", risk="read", output_schema=schema)
    async def bad_result() -> dict[str, str]:
        return {"value": "three"}

    registry = ToolRegistry()
    registry.register_many(typed_result, bad_result)
    info = registry.get("typed_result").schema()
    assert info.output_schema == schema
    assert await registry.call_tool("typed_result", {}) == {"value": 3}
    accepted = await registry.invoke("typed_result", {}, ToolContext(registry))
    assert accepted.data == {"value": 3} and not accepted.is_error
    rejected = await registry.invoke("bad_result", {}, ToolContext(registry))
    assert rejected.is_error and rejected.code == "invalid_output_schema"
    # 原始 call_tool 保留 Python 嵌套调用语义；invoke 才是模型/Workflow 消费边界。
    assert await registry.call_tool("bad_result", {}) == {"value": "three"}
    await registry.close()


async def test_output_schema_rejects_remote_reference_at_definition_time():
    with pytest.raises(ValueError, match="本地引用"):

        @tool(
            name="remote_schema",
            risk="read",
            output_schema={"$ref": "https://example.invalid/schema.json"},
        )
        async def remote_schema() -> str:
            return "unsafe"


async def test_permission_runs_before_pre_short_circuit():
    seen = []

    @tool()
    async def write() -> str:
        seen.append("execute")
        return "changed"

    async def pre(_definition, _context):
        seen.append("pre")
        return ShortCircuit("bypass")

    registry = ToolRegistry()
    registry.register(write)
    registry.add_global_pre(pre)
    with pytest.raises(ToolError) as error:
        await registry.call_tool("write", {})
    assert error.value.code == "permission_denied" and not seen
    assert registry.audit[-1].decision == "deny"

    async def ask(_definition, _context):
        return "ask"

    with pytest.raises(ToolError) as error:
        await registry.call_tool("write", {}, ToolContext(registry, policy=ask))
    assert error.value.code == "approval_required" and not seen

    async def allow(_definition, _context):
        return "audit"

    assert await registry.call_tool("write", {}, ToolContext(registry, policy=allow)) == "bypass"
    assert seen == ["pre"]
    assert registry.audit[-1].decision == "audit"
    await registry.close()


async def test_middleware_order_and_mutation_validation():
    seen = []

    @tool(risk="read")
    async def sample(value: int = 1) -> int:
        seen.append("execute")
        return value

    async def global_pre(_definition, context):
        seen.append("global_pre")
        assert context.arguments == {"value": 1}
        context.arguments["value"] = 2

    async def local_pre(_context):
        seen.append("local_pre")

    async def local_post(result, _context):
        seen.append("local_post")
        return result + 1

    async def global_post(_definition, result, _context):
        seen.append("global_post")
        return result + 1

    registry = ToolRegistry()
    registry.register(replace(sample, pre_execute=local_pre, post_execute=local_post))
    registry.add_global_pre(global_pre)
    registry.add_global_post(global_post)
    assert await registry.call_tool("sample", {}) == 4
    assert seen == ["global_pre", "local_pre", "execute", "local_post", "global_post"]
    await registry.close()


async def test_raw_nested_result_and_exactly_eight_levels():
    registry = ToolRegistry()
    sentinel = object()

    @tool(risk="read")
    async def raw() -> object:
        return sentinel

    @tool(risk="read")
    async def recurse(count: int, context: ToolContext) -> object:
        assert context.run_id == "run-test" and context.topic_id == "topic-test"
        if count:
            return await context.call_tool("recurse", {"count": count - 1})
        return sentinel

    registry.register_many(raw, recurse)
    assert await registry.call_tool("raw", {}) is sentinel
    normalized = await registry.invoke("raw", {}, ToolContext(registry))
    assert normalized.is_error and normalized.code == "invalid_output"
    context = ToolContext(registry, run_id="run-test", topic_id="topic-test")
    assert await registry.call_tool("recurse", {"count": 7}, context) is sentinel
    assert max(audit.depth for audit in registry.audit) == 8
    with pytest.raises(ToolError) as error:
        await registry.call_tool("recurse", {"count": 8}, context)
    assert error.value.code == "max_depth"
    await registry.close()


async def test_timeouts_cancellation_and_observers_do_not_block():
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    observer_started = asyncio.Event()
    observer_finished = asyncio.Event()

    @tool(risk="read")
    async def wait() -> str:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return "unreachable"

    async def observer(_audit):
        observer_started.set()
        await observer_finished.wait()
        raise RuntimeError("observer failure")

    def sync_observer(_audit):
        time.sleep(0.05)
        raise RuntimeError("ignored sync observer")

    registry = builtin_registry()
    registry.register(wait)
    registry.add_observer(observer)
    registry.add_observer(sync_observer)
    assert await registry.call_tool("echo", {"text": "ok"}) == "ok"
    assert not observer_finished.is_set()
    await asyncio.wait_for(observer_started.wait(), 1)
    with pytest.raises(ToolError) as error:
        await registry.call_tool("wait", {}, ToolContext(registry, timeout=0.01))
    assert error.value.code == "timeout" and cancelled.is_set()
    entered.clear()
    cancelled.clear()
    task = asyncio.create_task(registry.call_tool("wait", {}))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set() and registry.audit[-1].code == "cancelled"
    observer_finished.set()
    await registry.close()


class ClassInput(StrictModel):
    value: int = 4


class ClassTool(ToolBase):
    name = "class_tool"
    description = "类式工具测试"
    parameters = ClassInput
    risk = "read"

    async def execute(self, arguments: BaseModel, _context: ToolContext) -> int:
        return arguments.value


async def test_class_style_tool():
    registry = ToolRegistry()
    registry.register(ClassTool().definition())
    assert await registry.call_tool("class_tool", {}) == 4
    await registry.close()


async def test_result_observer_gets_isolated_snapshot_without_blocking():
    registry = builtin_registry()
    observed = asyncio.Event()
    release = asyncio.Event()

    async def observer(snapshot):
        snapshot.result.data["sum"] = 999
        observed.set()
        await release.wait()
        raise RuntimeError("ignored")

    registry.add_result_observer(observer)
    result = await registry.invoke("add", {"left": 2, "right": 3}, ToolContext(registry))
    assert not release.is_set()
    await asyncio.wait_for(observed.wait(), 1)
    assert result.data == {"sum": 5.0}
    release.set()
    await registry.close()


async def test_changed_arguments_are_reauthorized_before_execution():
    executed = []
    registry = ToolRegistry()

    @tool(risk="write")
    async def scoped(path: str) -> str:
        executed.append(path)
        return path

    async def policy(_definition, context):
        return "allow" if context.arguments["path"] == "allowed" else "deny"

    async def pre(_definition, context):
        context.arguments["path"] = "denied"

    registry.register(scoped)
    registry.add_global_pre(pre)
    with pytest.raises(ToolError) as error:
        await registry.call_tool(
            "scoped", {"path": "allowed"}, ToolContext(registry, policy=policy)
        )
    assert error.value.code == "permission_denied" and not executed
    await registry.close()


async def test_annotated_constraints_and_aliases_are_preserved():
    @tool(risk="read")
    async def bounded(value: Annotated[str, Field(alias="v", max_length=3)]) -> str:
        return value

    registry = ToolRegistry()
    registry.register(bounded)
    assert await registry.call_tool("bounded", {"v": "abc"}) == "abc"
    with pytest.raises(ToolError) as error:
        await registry.call_tool("bounded", {"v": "abcd"})
    assert error.value.code == "invalid_input"
    await registry.close()
