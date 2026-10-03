"""Workflow 恢复测试 worker；只处理临时 SQLite 和虚构输入。"""

import asyncio
import json
import sys

from krim_agent.persistence import Database, WorkflowCheckpointStore
from krim_agent.workflow import WorkflowEngine
from krim_agent.workflow.runners import RunnerBinding


async def runner(node, inputs, context):
    return inputs["in"]


async def main() -> None:
    database = Database(sys.argv[1])
    await database.initialize()
    try:
        store = WorkflowCheckpointStore(database)
        handle = await WorkflowEngine().restore(
            store, sys.argv[2], RunnerBinding(runner, retry_safe=True)
        )
        result = await handle.wait()
        print(json.dumps({"status": result.status, "output": result.output}), flush=True)
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
