from krim_agent.application.workflow import WorkflowServices
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolRegistry, tool
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode


async def test_tool_adapter_reuses_permissions_and_audits():
    services = WorkflowServices(tools=builtin_registry())
    graph = WorkflowGraph(nodes=[WorkflowNode(id="add", kind="tool", config={"name": "add"})])
    handle = WorkflowEngine().execute(graph, {"left": 2, "right": 3}, services.bindings(graph))
    result = await handle.wait()
    assert result.status == "completed" and result.output == {"sum": 5.0}

    @tool(name="write_forbidden")
    async def write_forbidden() -> str:
        return "side effect"

    registry = ToolRegistry()
    registry.register(write_forbidden)
    write_graph = WorkflowGraph(
        nodes=[WorkflowNode(id="write", kind="tool", config={"name": "write_forbidden"})]
    )
    write_result = (
        await WorkflowEngine()
        .execute(write_graph, {}, WorkflowServices(tools=registry).bindings(write_graph))
        .wait()
    )
    assert write_result.status == "failed" and write_result.code == "permission_denied"


async def test_trusted_code_adapter_requires_explicit_runner():
    graph = WorkflowGraph(nodes=[WorkflowNode(id="code", kind="code", config={"runner": "double"})])
    missing = await WorkflowEngine().execute(graph, 2).wait()
    assert missing.status == "failed" and missing.code == "runner_required"
    services = WorkflowServices(trusted_code={"double": lambda value: value * 2})
    result = await WorkflowEngine().execute(graph, 2, services.bindings(graph)).wait()
    assert result.status == "completed" and result.output == 4


async def test_model_adapter_uses_python_sdk_and_maps_incomplete_result():
    async with await PythonAI.create() as ai:
        graph = WorkflowGraph(
            nodes=[
                WorkflowNode(
                    id="model", kind="agent", config={"model": "offline", "prompt": "/echo "}
                )
            ]
        )
        services = WorkflowServices(models={"offline": ai})
        # WorkflowServices 的默认 binding 名称会被 Engine 按 kind 取用。
        result = await WorkflowEngine().execute(graph, "hello", services.bindings(graph)).wait()
        assert result.status == "completed" and "hello" in result.output
