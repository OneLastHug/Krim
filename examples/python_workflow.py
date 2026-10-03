"""完整 Python Workflow 消费：工具权限、模型 SDK、实时事件与结果。"""

import asyncio

from krim_agent.application.workflow import WorkflowServices
from krim_agent.sdk import PythonAI
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workflow import WorkflowEdge, WorkflowEngine, WorkflowGraph, WorkflowNode


async def main() -> None:
    # 离线模型和只读内置工具；Workflow 核心不依赖 DB/模型 SDK。
    async with await PythonAI.create() as ai:
        services = WorkflowServices(tools=builtin_registry(), models={"offline": ai})
        graph = WorkflowGraph(
            nodes=[
                WorkflowNode(
                    id="numbers", kind="constant", config={"value": {"left": 2, "right": 3}}
                ),
                WorkflowNode(id="calculate", kind="tool", config={"name": "add"}),
                WorkflowNode(
                    id="describe", kind="agent", config={"model": "offline", "prompt": "/echo "}
                ),
                WorkflowNode(id="output", kind="output"),
            ],
            edges=[
                WorkflowEdge(source="numbers", target="calculate"),
                WorkflowEdge(source="calculate", target="describe"),
                WorkflowEdge(source="describe", target="output"),
            ],
            output_node="output",
        )
        handle = WorkflowEngine().execute(graph, None, services.bindings(graph))
        async for event in handle.stream_events():
            if event is not None:
                print("事件：", event.seq, event.type, event.node_id)
        result = await handle.wait()
        print("工具节点：", result.node_results["calculate"])
        print("工作流终态：", result.status)
        print("输出：", result.output)


if __name__ == "__main__":
    asyncio.run(main())
