"""Workflow 崩溃恢复测试 writer：临时数据库、只读虚构 runner。"""

import asyncio
import json
import sys

from krim_agent.persistence import Database, WorkflowCheckpointStore
from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode
from krim_agent.workflow.runners import RunnerBinding


async def main() -> None:
    database = Database(sys.argv[1])
    await database.initialize()
    entered = asyncio.Event()

    async def runner(node, inputs, context):
        entered.set()
        await asyncio.Event().wait()

    store = WorkflowCheckpointStore(database, lease_seconds=2)
    graph = WorkflowGraph(nodes=[WorkflowNode(id="work", kind="tool")])
    handle = WorkflowEngine().execute(
        graph, "process-input", RunnerBinding(runner, retry_safe=True), checkpoint=store
    )
    await entered.wait()
    print(json.dumps({"run_id": handle.run_id}), flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
