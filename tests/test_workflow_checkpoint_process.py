import asyncio
import json
import sys
from pathlib import Path

from krim_agent.persistence import Database, WorkflowCheckpointStore
from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode
from krim_agent.workflow.runners import RunnerBinding


async def test_checkpoint_restores_in_independent_python_process(tmp_path):
    database_path = tmp_path / "process.sqlite"
    database = Database(f"sqlite+aiosqlite:///{database_path}")
    await database.initialize()
    entered = asyncio.Event()

    async def runner(node, inputs, context):
        entered.set()
        await asyncio.Event().wait()

    try:
        store = WorkflowCheckpointStore(database)
        graph = WorkflowGraph(nodes=[WorkflowNode(id="work", kind="tool")])
        handle = WorkflowEngine().execute(
            graph, "process-input", RunnerBinding(runner, retry_safe=True), checkpoint=store
        )
        await asyncio.wait_for(entered.wait(), 3)
        run_id = handle.run_id
        await handle.suspend()
    finally:
        await database.close()

    worker = Path(__file__).parent / "fixtures" / "workflow_restore_worker.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(worker),
        f"sqlite+aiosqlite:///{database_path}",
        run_id,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == 0, stderr.decode()
    assert json.loads(stdout) == {"status": "completed", "output": "process-input"}


async def test_process_kill_recovers_after_owner_lease_expires(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'crash.sqlite'}"
    writer = Path(__file__).parent / "fixtures" / "workflow_crash_writer.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(writer),
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        line = await asyncio.wait_for(process.stdout.readline(), 8)
        if not line:
            raise AssertionError((await process.stderr.read()).decode())
        run_id = json.loads(line)["run_id"]
        process.kill()
        await process.wait()
        await asyncio.sleep(0.35)
        reader = Path(__file__).parent / "fixtures" / "workflow_restore_worker.py"
        restored = await asyncio.create_subprocess_exec(
            sys.executable,
            str(reader),
            url,
            run_id,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(restored.communicate(), 15)
        assert restored.returncode == 0, stderr.decode()
        assert json.loads(stdout) == {"status": "completed", "output": "process-input"}
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()
