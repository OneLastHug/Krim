import asyncio
import sys

import httpx
import pytest

from krim_agent.client import KrimClient
from krim_agent.workflow import WorkflowGraph, WorkflowNode


def graph():
    return WorkflowGraph(
        nodes=[WorkflowNode(id="value", kind="constant", config={"value": 7})],
    )


async def test_api_workflow_run_state_events_and_client(api):
    response = await api.post(
        "/v1/workflows/runs",
        json={
            "idempotency_key": "api-one",
            "graph": graph().model_dump(mode="json"),
            "input_value": None,
        },
    )
    assert response.status_code == 202
    run = response.json()
    assert run["status"] in {"queued", "running", "completed"}
    run_id = run["run_id"]
    events = await api.get(f"/v1/workflows/runs/{run_id}/events")
    assert events.status_code == 200 and "event: start" in events.text
    state = (await api.get(f"/v1/workflows/runs/{run_id}")).json()
    assert state["run_id"] == run_id and state["output"] == 7

    with KrimClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "run_id": run_id,
                    "status": "completed",
                    "epoch": 0,
                    "current_node": None,
                    "output": 7,
                    "node_results": {"value": 7},
                    "node_outputs": {"value": {"out": 7}},
                    "node_statuses": {"value": "completed"},
                    "event_count": 3,
                    "production": True,
                },
            )
        )
    ) as client:
        assert client.workflow(run_id).output == 7


async def test_workflow_pause_resume_override_rerun_and_stop(api):
    graph_value = WorkflowGraph(
        nodes=[WorkflowNode(id="value", kind="output")],
    ).model_dump(mode="json")
    run = (
        await api.post(
            "/v1/workflows/runs",
            json={
                "idempotency_key": "api-pause",
                "graph": graph_value,
                "input_value": "first",
                "pause_before": ["value"],
            },
        )
    ).json()
    run_id = run["run_id"]
    paused = await api.post(f"/v1/workflows/runs/{run_id}/pause")
    assert paused.status_code == 200
    resumed = await api.post(
        f"/v1/workflows/runs/{run_id}/resume",
        json={"has_input_override": True, "input_override": "second"},
    )
    assert resumed.status_code == 200
    await api.get(f"/v1/workflows/runs/{run_id}/events")
    state = (await api.get(f"/v1/workflows/runs/{run_id}")).json()
    assert state["output"] == "second"
    rerun = await api.post(
        f"/v1/workflows/runs/{run_id}/rerun",
        json={"node_id": "value", "has_input_override": True, "input_override": "third"},
    )
    assert rerun.status_code == 200 and rerun.json()["epoch"] == 1
    assert rerun.json()["output"] == "third"

    stopped = await api.post(f"/v1/workflows/runs/{run_id}/stop")
    assert stopped.status_code == 200


async def test_workflow_sse_cursor_and_error_boundary(api):
    graph_value = WorkflowGraph(
        nodes=[WorkflowNode(id="value", kind="constant", config={"value": 1})]
    ).model_dump(mode="json")
    run = (
        await api.post(
            "/v1/workflows/runs", json={"idempotency_key": "api-events", "graph": graph_value}
        )
    ).json()
    response = await api.get(f"/v1/workflows/runs/{run['run_id']}/events?from_seq=1")
    assert response.status_code == 200
    assert "id: 0" not in response.text
    invalid = await api.get(f"/v1/workflows/runs/{run['run_id']}/events?from_seq=-1")
    assert invalid.status_code == 422


async def test_cli_workflow_commands(monkeypatch, capsys):
    # CLI 命令解析覆盖会在真实 TCP 测试中执行；这里验证未知 workflow action 先被 parser 拒绝。
    from krim_agent.cli import main

    monkeypatch.setattr(sys, "argv", ["krim", "workflow", "unknown"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2


async def test_workflow_idempotency_tools_models_and_cursor_errors(api):
    tool_graph = WorkflowGraph(nodes=[WorkflowNode(id="add", kind="tool", config={"name": "add"})])
    request = {
        "idempotency_key": "real-tool",
        "graph": tool_graph.model_dump(mode="json"),
        "input_value": {"left": 2, "right": 3},
    }
    first = await api.post("/v1/workflows/runs", json=request)
    assert first.status_code == 202
    run_id = first.json()["run_id"]
    await api.get(f"/v1/workflows/runs/{run_id}/events")
    result = (await api.get(f"/v1/workflows/runs/{run_id}")).json()
    assert result["output"] == {"sum": 5.0}
    again = await api.post("/v1/workflows/runs", json=request)
    assert again.json()["run_id"] == run_id
    conflict = await api.post(
        "/v1/workflows/runs", json={**request, "input_value": {"left": 7, "right": 3}}
    )
    assert (
        conflict.status_code == 409 and conflict.json()["error"]["code"] == "idempotency_conflict"
    )
    model = await api.post(
        "/v1/workflows/runs",
        json={
            "idempotency_key": "model",
            "graph": {"nodes": [{"id": "agent", "kind": "agent"}]},
            "input_value": "model prompt",
        },
    )
    assert model.status_code == 202
    model_id = model.json()["run_id"]
    await api.get(f"/v1/workflows/runs/{model_id}/events")
    assert "[fake]" in (await api.get(f"/v1/workflows/runs/{model_id}")).json()["output"]
    denied = await api.get(f"/v1/workflows/runs/{run_id}/events", headers={"Last-Event-ID": "-1"})
    assert denied.status_code == 416
    missing = await api.get("/v1/workflows/runs/missing/events")
    assert missing.status_code == 404
    invalid = await api.get(f"/v1/workflows/runs/{run_id}/events?from_seq=999")
    assert invalid.status_code == 416


async def test_workflow_shutdown_handoff_and_cold_reads_never_restore_without_post(tmp_path):
    from krim_agent.api import create_app
    from krim_agent.config import Settings

    url = f"sqlite+aiosqlite:///{tmp_path / 'api-restart.sqlite'}"
    settings = Settings(database_url=url)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            response = await client.post(
                "/v1/workflows/runs",
                json={
                    "idempotency_key": "handoff",
                    "graph": {"nodes": [{"id": "out", "kind": "output"}]},
                    "input_value": "before",
                    "pause_before": ["out"],
                },
            )
            run_id = response.json()["run_id"]
            async with asyncio.timeout(3):
                async for event in app.state.workflow.events(run_id):
                    if event is not None and event.type == "paused":
                        break
    restarted = create_app(settings)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://127.0.0.1"
        ) as client:
            state = (await client.get(f"/v1/workflows/runs/{run_id}")).json()
            assert state["status"] == "paused"
            response = await client.get(f"/v1/workflows/runs/{run_id}/events")
            assert response.status_code == 200
            assert (await client.get(f"/v1/workflows/runs/{run_id}")).json()[
                "event_count"
            ] == state["event_count"]
            restored = await client.post(f"/v1/workflows/runs/{run_id}/restore", json={})
            assert restored.status_code == 202 and restored.json()["status"] == "paused"
            resumed = await client.post(
                f"/v1/workflows/runs/{run_id}/resume",
                json={
                    "has_input_override": True,
                    "input_override": "after",
                },
            )
            assert resumed.status_code == 200
            await client.get(f"/v1/workflows/runs/{run_id}/events")
            assert (await client.get(f"/v1/workflows/runs/{run_id}")).json()["output"] == "after"


async def test_workflow_cold_stop_does_not_execute_pending_nodes(tmp_path):
    from krim_agent.api import create_app
    from krim_agent.config import Settings

    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'cold-stop.sqlite'}")
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            created = await client.post(
                "/v1/workflows/runs",
                json={
                    "idempotency_key": "cold-stop",
                    "graph": {
                        "nodes": [{"id": "work", "kind": "tool", "config": {"name": "echo"}}]
                    },
                    "input_value": {"text": "must-not-execute"},
                    "pause_before": ["work"],
                },
            )
            run_id = created.json()["run_id"]
            async with asyncio.timeout(3):
                async for event in app.state.workflow.events(run_id):
                    if event is not None and event.type == "paused":
                        break
    restarted = create_app(settings)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://127.0.0.1"
        ) as client:
            stopped = await client.post(f"/v1/workflows/runs/{run_id}/stop")
            assert stopped.status_code == 200 and stopped.json()["status"] == "aborted"
            assert stopped.json()["node_results"] == {}
            assert not restarted.state.workflow.services.tools.audit
