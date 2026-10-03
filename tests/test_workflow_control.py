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
from krim_agent.workflow.runners import RunnerBinding
from krim_agent.workflow.types import WorkflowGroup


async def wait_paused(handle, from_seq=0):
    async for event in handle.stream_events(from_seq=from_seq, heartbeat=0.01):
        if event is not None and event.type == "paused":
            return event
    raise AssertionError("未收到暂停确认")


async def test_before_after_pause_override_and_rerun_preserve_other_branches():
    seen = []

    async def runner(node, inputs, context):
        seen.append((node.id, inputs["in"]))
        return inputs["in"]

    graph = WorkflowGraph(
        nodes=[WorkflowNode(id="a", kind="tool"), WorkflowNode(id="b", kind="tool")],
        edges=[WorkflowEdge(source="a", target="b")],
        output_node="b",
    )
    handle = WorkflowEngine().execute(
        graph,
        "first",
        RunnerBinding(runner, retry_safe=True),
        pause_before={"b"},
        pause_after={"a"},
    )
    event = await asyncio.wait_for(wait_paused(handle), 2)
    assert event.node_id == "a" and seen == [("a", "first")]
    with pytest.raises(WorkflowError):
        await handle.resume("cannot change completed input")
    cursor = len(handle.events_log)
    await handle.resume()
    event = await asyncio.wait_for(wait_paused(handle, cursor), 2)
    assert event.node_id == "b"
    await handle.resume("second")
    initial = await handle.wait()
    assert initial.output == "second"
    rerun = await handle.rerun("b", "third")
    assert rerun.output == "third" and rerun.node_results["a"] == "first"
    assert seen == [("a", "first"), ("b", "second"), ("b", "third")]


async def test_invalid_override_and_side_effect_rerun_leave_existing_results():
    async def runner(node, inputs, context):
        return inputs["in"]

    graph = WorkflowGraph(
        nodes=[
            WorkflowNode(
                id="work",
                kind="tool",
                input_ports=[WorkflowPort(name="in", json_schema={"type": "integer"})],
            )
        ]
    )
    handle = WorkflowEngine().execute(graph, 1, RunnerBinding(runner), pause_before={"work"})
    await asyncio.wait_for(wait_paused(handle), 2)
    with pytest.raises(WorkflowError):
        await handle.resume("wrong type")
    assert handle.status == "paused"
    await handle.resume(2)
    assert (await handle.wait()).output == 2
    with pytest.raises(WorkflowError) as error:
        await handle.rerun("work", 3)
    assert error.value.code == "rerun_requires_confirmation"
    assert (await handle.wait()).output == 2
    with pytest.raises(WorkflowError):
        await handle.rerun("work", "wrong type", allow_side_effects=True)
    assert handle.node_outputs["work"].ports["out"] == 2
    assert (await handle.rerun("work", 3, allow_side_effects=True)).output == 3


async def test_loop_retry_budgets_and_fanout_cancel_cleanup():
    attempts = 0

    async def increment(node, inputs, context):
        nonlocal attempts
        attempts += 1
        if node.config.get("fail_once") and attempts == 1:
            raise OSError("transient")
        return inputs["in"] + 1

    child = WorkflowGraph(nodes=[WorkflowNode(id="work", kind="tool")])
    loop = WorkflowGraph(
        nodes=[
            WorkflowNode(
                id="loop",
                kind="loop",
                config={
                    "graph": child.model_dump(mode="json"),
                    "max_iterations": 3,
                    "until": {"operator": "gte", "value": 3},
                },
            )
        ]
    )
    result = (
        await WorkflowEngine().execute(loop, 0, RunnerBinding(increment, retry_safe=True)).wait()
    )
    assert result.output == 3
    limited = (
        await WorkflowEngine(max_nodes=2)
        .execute(loop, 0, RunnerBinding(increment, retry_safe=True))
        .wait()
    )
    assert limited.status == "failed" and limited.code == "node_limit"

    child.nodes[0].config["fail_once"] = True
    retry = WorkflowGraph(
        nodes=[
            WorkflowNode(
                id="retry",
                kind="retry",
                config={"graph": child.model_dump(mode="json"), "max_attempts": 2},
            )
        ]
    )
    attempts = 0
    result = (
        await WorkflowEngine().execute(retry, 1, RunnerBinding(increment, retry_safe=True)).wait()
    )
    assert result.status == "completed" and result.output == 2 and attempts == 2
    denied = await WorkflowEngine().execute(retry, 1, RunnerBinding(increment)).wait()
    assert denied.code == "retry_unsafe"

    entered = asyncio.Event()
    active = 0

    async def waiting(node, inputs, context):
        nonlocal active
        active += 1
        if active == 2:
            entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            active -= 1

    fanout = WorkflowGraph(
        nodes=[
            WorkflowNode(id="fan", kind="fanout", config={"graph": child.model_dump(mode="json")})
        ]
    )
    handle = WorkflowEngine(max_concurrency=2).execute(fanout, [1, 2, 3], RunnerBinding(waiting))
    await asyncio.wait_for(entered.wait(), 2)
    assert (await handle.stop()).status == "aborted"
    assert active == 0 and not handle._children


async def test_ports_types_groups_preview_and_production_never_emit_developer_data():
    graph = WorkflowGraph(
        nodes=[
            WorkflowNode(id="input", kind="input"),
            WorkflowNode(id="out", kind="output"),
            WorkflowNode(
                id="dev", kind="constant", config={"value": "secret"}, developer_only=True
            ),
        ],
        edges=[WorkflowEdge(source="input", target="out")],
        output_node="out",
        groups=[WorkflowGroup(id="main", node_ids=["input", "out"])],
    )
    handle = WorkflowEngine().execute(graph, "public")
    assert (await handle.wait()).output == "public"
    events = await handle.events()
    assert not any(event.node_id == "dev" or "secret" in str(event.data) for event in events)
    assert any(event.group_ids == ["main"] for event in events if event.node_id == "out")

    incompatible = WorkflowGraph(
        nodes=[
            WorkflowNode(
                id="a",
                kind="input",
                output_ports=[WorkflowPort(name="out", json_schema={"type": "string"})],
            ),
            WorkflowNode(
                id="b",
                kind="output",
                input_ports=[WorkflowPort(name="in", json_schema={"type": "integer"})],
            ),
        ],
        edges=[WorkflowEdge(source="a", target="b")],
    )
    with pytest.raises(WorkflowError) as error:
        incompatible.validate_graph()
    assert error.value.code == "port_type"


async def test_every_pure_node_and_multiple_sinks_output():
    engine = WorkflowEngine()
    enum_graph = WorkflowGraph(nodes=[WorkflowNode(id="enum", kind="enumerator")])
    assert (await engine.execute(enum_graph, ["a", "b"]).wait()).output == [
        {"index": 0, "value": "a"},
        {"index": 1, "value": "b"},
    ]
    validate = WorkflowGraph(
        nodes=[
            WorkflowNode(id="validate", kind="validator", config={"schema": {"type": "integer"}})
        ]
    )
    assert (await engine.execute(validate, 1).wait()).output == 1
    assert (await engine.execute(validate, "1").wait()).code == "validation_error"
    graph = WorkflowGraph(
        nodes=[
            WorkflowNode(id="a", kind="constant", config={"value": 1}),
            WorkflowNode(id="b", kind="constant", config={"value": 2}),
            WorkflowNode(id="preview", kind="image_preview"),
        ]
    )
    assert (await engine.execute(graph, "preview data").wait()).output == {"a": 1, "b": 2}
