import asyncio
import json
import sys
import time

from krim_agent.client import KrimClient
from krim_agent.workflow import WorkflowGraph, WorkflowNode


async def run_cli(url, *args):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        *args,
        "--url",
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == 0, stderr.decode()
    return stdout.decode()


async def test_real_tcp_workflow_client_cli_idempotency_and_sse(server_url):
    graph = WorkflowGraph(nodes=[WorkflowNode(id="value", kind="constant", config={"value": 9})])

    def scenario():
        with KrimClient(server_url) as client:
            first = client.workflow_run(graph, idempotency_key="workflow-once")
            retry = client.workflow_run(graph, idempotency_key="workflow-once")
            assert first.run_id == retry.run_id
            events = list(client.workflow_events(first.run_id))
            assert events[-1].type == "end"
            mid = list(client.workflow_events(first.run_id, 1))
            assert mid[0].seq == 1
            return first.run_id

    run_id = await asyncio.to_thread(scenario)
    state = json.loads(await run_cli(server_url, "workflow", "get", run_id))
    assert state["output"] == 9 and state["status"] == "completed"
    listing = json.loads(await run_cli(server_url, "workflow", "list"))
    assert any(item["run_id"] == run_id for item in listing)
    event_lines = await run_cli(server_url, "workflow", "events", run_id, "--from-seq", "1")
    assert '"type":"end"' in event_lines


async def test_cli_run_pause_override_and_control_use_http(tmp_path, server_url):
    graph_file = tmp_path / "workflow.json"
    graph_file.write_text(
        json.dumps(
            {
                "nodes": [{"id": "out", "kind": "output"}],
                "output_node": "out",
            }
        ),
        "utf-8",
    )
    created = json.loads(
        await run_cli(
            server_url,
            "workflow",
            "run",
            str(graph_file),
            "--input",
            '"before"',
            "--pause-before",
            "out",
            "--idempotency-key",
            "cli-control",
        )
    )
    run_id = created["run_id"]

    def with_client():
        return KrimClient(server_url)

    def paused():
        with with_client() as client:
            return client.workflow(run_id).status == "paused"

    def wait_sync():
        deadline = time.monotonic() + 4
        while not paused():
            if time.monotonic() >= deadline:
                raise AssertionError("Workflow 未在预算内暂停")
            time.sleep(0.01)

    await asyncio.wait_for(asyncio.to_thread(wait_sync), 5)
    resumed = json.loads(
        await run_cli(
            server_url,
            "workflow",
            "resume",
            run_id,
            "--override",
            '"after"',
        )
    )
    assert resumed["run_id"] == run_id
    await run_cli(server_url, "workflow", "events", run_id)
    state = json.loads(await run_cli(server_url, "workflow", "get", run_id))
    assert state["status"] == "completed" and state["output"] == "after"
    rerun = json.loads(
        await run_cli(
            server_url,
            "workflow",
            "rerun",
            run_id,
            "--node",
            "out",
            "--override",
            '"again"',
        )
    )
    assert rerun["epoch"] == 1 and rerun["output"] == "again"
    stopped = json.loads(await run_cli(server_url, "workflow", "stop", run_id))
    assert stopped["status"] == "completed"
