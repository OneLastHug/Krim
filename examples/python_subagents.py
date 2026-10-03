"""Python SubAgent：执行、继续、事件、父调用绑定与资源回收。"""

import asyncio

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    # 使用内存 SQLite 和离线 provider，不执行外部模型或任意系统工具。
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    manager = AgentManager(KrimRepository(database), max_concurrent=2)
    try:
        definition = AgentDefinition(
            name="calculator",
            provider=FakeProvider(delay=0),
            tools=builtin_registry(),
            parent_tool_call_id="example-parent-tool",
        )
        handle = await manager.spawn(definition)
        first = await handle.execute("/add 2 3", "first")
        print("首次执行：", first.status, first.text, first.parent_tool_call_id)
        second = await handle.resume("/echo 第二轮", "second")
        print("继续执行：", second.status, second.text, "turn", second.turn)
        events = [frame async for frame in handle.events()]
        print("持久事件：", len(events), events[-1].kind)
        # 句柄销毁不会删除 topic、消息或审计，也不会关闭宿主工具注册表。
        destroyed = await handle.destroy()
        print("资源回收：", destroyed.status)
    finally:
        await manager.close()
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
