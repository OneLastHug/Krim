"""通过 Python Host SDK 验证分支、编辑和重新生成；全程离线。"""

import asyncio

from krim_agent.app import LocalApp
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ChatRequest, EditRequest, RegenerateRequest
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    # 内存数据库避免改变用户数据，离线 provider 不连接付费服务。
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.initialize()
    runtime = ChatRuntime(
        KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
    )
    app = LocalApp(runtime)
    handle = app.handle
    try:
        topic = await handle.create_topic("消息树示例")
        original = await handle.send(
            topic.id, ChatRequest(text="/add 2 3", idempotency_key="original")
        )
        async for _frame in handle.events(original.id):
            pass
        original_path = await handle.messages(topic.id)
        print("原回答：", original_path[-1].content)

        # 编辑创建新的用户兄弟节点，原问题和回答仍保存在树里。
        edited = await handle.edit(
            topic.id,
            original.input_message_id,
            EditRequest(text="/add 4 5", idempotency_key="edited"),
        )
        async for _frame in handle.events(edited.id):
            pass
        print("编辑后：", (await handle.messages(topic.id))[-1].content)

        # 从旧回答找到最近的用户祖先，复用问题而生成新的回答分支。
        regenerated = await handle.regenerate(
            topic.id, original_path[-1].id, RegenerateRequest(idempotency_key="regenerated")
        )
        async for _frame in handle.events(regenerated.id):
            pass
        print("重新生成：", (await handle.messages(topic.id))[-1].content)

        # 切回编辑分支，活动路径恢复到 9，旧分支共 11 个节点仍保留。
        await handle.select_branch(topic.id, edited.input_message_id)
        tree = await handle.tree(topic.id)
        print("全部节点：", len(tree.messages), "活动节点：", len(tree.active_path))
        print("切回编辑：", (await handle.messages(topic.id))[-1].content)
    finally:
        # 宿主借用运行时，连接、运行时和数据库分别由创建者释放。
        await app.close()
        await runtime.close()
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
