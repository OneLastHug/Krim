"""Workflow SQLite 检查点、暂停与数据库重开示例；只使用内建纯节点。"""

import asyncio
import tempfile
from pathlib import Path

from krim_agent.persistence import Database, WorkflowCheckpointStore
from krim_agent.workflow import WorkflowEdge, WorkflowEngine, WorkflowGraph, WorkflowNode


async def pause_event(handle, cursor: int = 0) -> None:
    async for event in handle.stream_events(from_seq=cursor, heartbeat=0.01):
        if event is not None and event.type == "paused":
            return


async def main() -> None:
    graph = WorkflowGraph(
        nodes=[WorkflowNode(id="input", kind="input"), WorkflowNode(id="out", kind="output")],
        edges=[WorkflowEdge(source="input", target="out")],
        output_node="out",
    )
    with tempfile.TemporaryDirectory() as directory:
        url = f"sqlite+aiosqlite:///{(Path(directory) / 'workflow.sqlite').as_posix()}"
        database = Database(url)
        await database.initialize()
        try:
            store = WorkflowCheckpointStore(database)
            handle = WorkflowEngine().execute(
                graph, "原输入", checkpoint=store, pause_before={"out"}
            )
            await pause_event(handle)
            # suspend 等待生产任务退出并释放租约，保留数据库中的暂停位置。
            saved = await handle.suspend()
        finally:
            await database.close()

        reopened = Database(url)
        await reopened.initialize()
        try:
            store = WorkflowCheckpointStore(reopened)
            restored = await WorkflowEngine().restore(store, saved.run_id)
            await pause_event(restored, len(saved.events))
            await restored.resume("恢复后的输入")
            result = await restored.wait()
            print(
                f"状态：{result.status}；输出：{result.output}；"
                f"同一运行：{result.run_id == saved.run_id}"
            )
            print(f"持久事件：{len((await store.load(result.run_id)).state.events)}")
        finally:
            await reopened.close()


if __name__ == "__main__":
    asyncio.run(main())
