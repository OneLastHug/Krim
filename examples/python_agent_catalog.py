"""离线模型→Agent 工具→子工具→父模型；没有付费调用或外部用户数据。"""

import asyncio
import json

from krim_agent.agents import AgentCatalog, AgentDefinition, AgentManager
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider, ModelText, ModelToolCall
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolRegistry
from krim_agent.tools.builtins import add, echo


class DelegatingOfflineModel:
    name = "offline-agent-delegation"

    async def stream(self, messages, _tools):
        response = next((message for message in reversed(messages) if message.role == "tool"), None)
        if response is None:
            yield ModelToolCall(name="agent_calculator", arguments={"prompt": "/add 2 3"})
        else:
            result = json.loads(response.content)
            yield ModelText(text="父模型接收子结果：" + result["text"])


async def main() -> None:
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    repository = KrimRepository(database)
    manager = AgentManager(repository)
    child_tools = ToolRegistry()
    child_tools.register_many(add, echo)
    catalog = AgentCatalog(manager)
    catalog.register(
        "calculator",
        AgentDefinition(name="calculator", provider=FakeProvider(delay=0), tools=child_tools),
        description="离线只读计算助手",
        risk="read",
    )
    root_tools = ToolRegistry()
    catalog.install_tools(root_tools)
    runtime = ChatRuntime(repository, DelegatingOfflineModel(), root_tools, Settings())
    ai = PythonAI(runtime)
    try:
        result = await ai.generate_text("委派计算")
        assert result.run.status == "completed" and "5.0" in result.text
        child = result.steps[0].tool_results[0].data
        assert child["parent_tool_call_id"] == result.audits[0].call_id
        assert not manager._handles
        print(
            f"目录：{catalog.descriptors()[0].name}；状态：{result.run.status}；"
            f"结果：{result.text}；父调用已绑定：True；剩余句柄：0"
        )
    finally:
        await ai.close()
        await runtime.close()
        await manager.close()
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
