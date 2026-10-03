import asyncio

import pytest

from krim_agent.workflow import (
    WorkflowEdge,
    WorkflowEngine,
    WorkflowError,
    WorkflowGraph,
    WorkflowNode,
    WorkflowPort,
)
from krim_agent.workflow.runners import ExecutionContext, RunnerBinding
from krim_agent.workflow.types import NodeOutput


def graph(*nodes, edges=(), output_node=None):
    return WorkflowGraph(nodes=list(nodes), edges=list(edges), output_node=output_node)


async def test_graph_ports_dag_and_production_projection():
    one = WorkflowNode(id="one", kind="constant", config={"value": 1})
    two = WorkflowNode(id="two", kind="set", input_ports=[WorkflowPort(name="in")])
    wf = graph(one, two, edges=[WorkflowEdge(source="one", target="two")], output_node="two")
    wf.validate_graph()
    assert [node.id for node in wf.production_projection().nodes] == ["one", "two"]
    with pytest.raises(WorkflowError, match="循环"):
        graph(
            WorkflowNode(id="a", kind="set"),
            WorkflowNode(id="b", kind="set"),
            edges=[WorkflowEdge(source="a", target="b"), WorkflowEdge(source="b", target="a")],
        ).validate_graph()
    with pytest.raises(WorkflowError, match="单值端口"):
        graph(
            WorkflowNode(id="a", kind="constant"),
            WorkflowNode(id="b", kind="constant"),
            WorkflowNode(id="c", kind="set"),
            edges=[WorkflowEdge(source="a", target="c"), WorkflowEdge(source="b", target="c")],
        ).validate_graph()
    developer = WorkflowNode(id="dev", kind="constant", developer_only=True)
    public = WorkflowNode(id="public", kind="set")
    with pytest.raises(WorkflowError, match="开发节点"):
        graph(
            developer, public, edges=[WorkflowEdge(source="dev", target="public")]
        ).production_projection()


async def test_constants_set_ports_merge_and_branch_selection():
    wf = graph(
        WorkflowNode(id="start", kind="constant", config={"value": {"branch": "yes", "n": 2}}),
        WorkflowNode(
            id="branch", kind="branch", config={"path": "branch", "operator": "eq", "value": "yes"}
        ),
        WorkflowNode(id="yes", kind="set", config={"values": {"answer": "yes"}}),
        WorkflowNode(id="no", kind="set", config={"values": {"answer": "no"}}),
        WorkflowNode(id="out", kind="aggregate", config={"mode": "merge"}),
        edges=[
            WorkflowEdge(source="start", target="branch"),
            WorkflowEdge(source="branch", target="yes", when="true"),
            WorkflowEdge(source="branch", target="no", when="false"),
            WorkflowEdge(source="yes", target="out"),
            WorkflowEdge(source="no", target="out"),
        ],
        output_node="out",
    )
    result = await WorkflowEngine().execute(wf, None).wait()
    assert result.status == "completed" and result.output == {
        "branch": "yes",
        "n": 2,
        "answer": "yes",
    }
    assert result.node_statuses["no"] == "skipped"
    assert any(
        event.type == "node_skip" and event.node_id == "no"
        for event in await WorkflowEngine().execute(wf, None).events()
    )


async def test_runner_retry_is_explicit_and_outputs_are_ported():
    attempts = 0

    async def runner(node, inputs, context: ExecutionContext):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("transient")
        return NodeOutput(ports={"out": {"attempt": attempts, "input": inputs["in"]}})

    wf = graph(
        WorkflowNode(id="work", kind="tool", retry={"max_attempts": 2}),
        WorkflowNode(id="out", kind="output"),
        edges=[WorkflowEdge(source="work", target="out")],
        output_node="out",
    )
    unsafe = (
        await WorkflowEngine().execute(wf, "input", RunnerBinding(runner, retry_safe=False)).wait()
    )
    assert unsafe.status == "failed" and unsafe.code == "retry_unsafe" and attempts == 0
    attempts = 0
    result = (
        await WorkflowEngine().execute(wf, "input", RunnerBinding(runner, retry_safe=True)).wait()
    )
    assert result.output == {"attempt": 2, "input": "input"} and attempts == 2


async def test_runner_missing_code_is_rejected_and_pause_resume_stop_are_real():
    code = WorkflowGraph(nodes=[WorkflowNode(id="code", kind="code")])
    result = await WorkflowEngine().execute(code, "x", RunnerBinding(lambda *_: "bad")).wait()
    assert result.status == "failed" and result.code == "code_unavailable"

    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow(node, inputs, context):
        entered.set()
        await release.wait()
        return inputs["in"]

    wf = WorkflowGraph(nodes=[WorkflowNode(id="slow", kind="tool")])
    entered.clear()
    release.clear()
    handle = WorkflowEngine().execute(wf, "x", RunnerBinding(slow), pause_before={"slow"})
    for _ in range(100):
        if handle.status == "paused":
            break
        await asyncio.sleep(0.01)
    assert handle.status == "paused"
    # pause_before 在节点调用前生效；先显式继续，再让 runner 完成。
    await handle.resume()
    await asyncio.wait_for(entered.wait(), 2)
    release.set()
    result = await handle.wait()
    assert result.status == "completed"

    entered.clear()
    release.clear()
    handle = WorkflowEngine().execute(wf, "x", RunnerBinding(slow))
    await asyncio.wait_for(entered.wait(), 2)
    stopped = await handle.stop()
    assert stopped.status == "aborted" or stopped.code == "cancelled"


async def test_rerun_with_input_override_increments_epoch_and_invalidates_output():
    calls = []

    async def runner(node, inputs, context):
        calls.append((node.id, inputs["in"]))
        return inputs["in"]

    wf = WorkflowGraph(
        nodes=[WorkflowNode(id="a", kind="tool"), WorkflowNode(id="b", kind="tool")],
        edges=[WorkflowEdge(source="a", target="b")],
        output_node="b",
    )
    handle = WorkflowEngine().execute(wf, "first", RunnerBinding(runner, retry_safe=True))
    first = await handle.wait()
    second = await handle.rerun("b", "override")
    assert first.epoch == 0 and second.epoch == 1 and second.output == "override"
    assert calls == [("a", "first"), ("b", "first"), ("b", "override")]
    assert any(event.type == "rerun" for event in await handle.events())


async def test_multi_input_aggregate_nested_fanout_loop_and_live_events():
    child = WorkflowGraph(
        nodes=[WorkflowNode(id="input", kind="input"), WorkflowNode(id="output", kind="output")],
        edges=[WorkflowEdge(source="input", target="output")],
        output_node="output",
    )
    sum_graph = WorkflowGraph(
        nodes=[
            WorkflowNode(id="a", kind="constant", config={"value": 1}),
            WorkflowNode(id="b", kind="constant", config={"value": 2}),
            WorkflowNode(
                id="sum",
                kind="aggregate",
                input_ports=[WorkflowPort(name="in", multiple=True)],
                config={"mode": "sum"},
            ),
        ],
        edges=[WorkflowEdge(source="a", target="sum"), WorkflowEdge(source="b", target="sum")],
        output_node="sum",
    )
    engine = WorkflowEngine(max_concurrency=2)
    aggregate = await engine.execute(sum_graph, None).wait()
    assert aggregate.status == "completed" and aggregate.output == 3
    fanout = WorkflowGraph(
        nodes=[
            WorkflowNode(id="fan", kind="fanout", config={"graph": child.model_dump(mode="json")})
        ]
    )
    handle = engine.execute(fanout, [3, 4])
    observed = []
    async for event in handle.stream_events(heartbeat=0.01):
        if event is not None:
            observed.append(event)
    result = await handle.wait()
    assert result.status == "completed" and result.output == [3, 4]
    assert [event.seq for event in observed] == list(range(len(observed)))
    assert any(event.type == "node_start" and event.node_id == "fan" for event in observed)
    loop = WorkflowGraph(
        nodes=[
            WorkflowNode(
                id="loop",
                kind="loop",
                config={"graph": child.model_dump(mode="json"), "max_iterations": 2},
            )
        ]
    )
    loop_result = await engine.execute(loop, "loop-value").wait()
    assert loop_result.status == "completed" and loop_result.output == "loop-value"


async def test_schema_ports_defaults_and_developer_projection_events():
    graph_with_schema = WorkflowGraph(
        nodes=[
            WorkflowNode(
                id="input",
                kind="input",
                output_ports=[WorkflowPort(name="out", json_schema={"type": "integer"})],
            ),
            WorkflowNode(
                id="output",
                kind="output",
                input_ports=[WorkflowPort(name="in", json_schema={"type": "integer"})],
            ),
        ],
        edges=[WorkflowEdge(source="input", target="output")],
        output_node="output",
    )
    engine = WorkflowEngine()
    assert (await engine.execute(graph_with_schema, 3).wait()).status == "completed"
    with pytest.raises(WorkflowError, match="端口值"):
        engine.execute(graph_with_schema, "3")
    with pytest.raises(WorkflowError, match="本地"):
        WorkflowGraph(
            nodes=[
                WorkflowNode(
                    id="a",
                    kind="constant",
                    output_ports=[WorkflowPort(name="out", json_schema={"$ref": "https://bad"})],
                )
            ]
        ).validate_graph()

    default_branch = WorkflowGraph(
        nodes=[
            WorkflowNode(id="branch", kind="branch", config={"selection": "unknown"}),
            WorkflowNode(id="fallback", kind="output"),
        ],
        edges=[WorkflowEdge(source="branch", target="fallback", when="default")],
        output_node="fallback",
    )
    result = await engine.execute(default_branch, {"x": 1}).wait()
    assert result.status == "completed" and result.output == {"x": 1}

    developer = WorkflowGraph(
        nodes=[
            WorkflowNode(id="dev", kind="constant", developer_only=True),
            WorkflowNode(id="out", kind="output"),
        ],
        edges=[WorkflowEdge(source="dev", target="out")],
        output_node="out",
    )
    with pytest.raises(WorkflowError, match="开发节点"):
        engine.execute(developer, "x", production=True)
