"""离线 Agent 会话示例：多轮、关闭/重开、历史事件和资源收束。"""

import asyncio

from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


async def main() -> None:
    async with await PythonAI.create(provider=FakeProvider(delay=0)) as ai:
        session = await ai.agent()
        # 相同会话保留对话历史，每次已接受执行有独立 Run。
        first = await session.execute("/add 2 3", idempotency_key="first")
        closed = await session.close()
        assert closed.status == "closed" and closed.turn == 1
        # 重开保留原轮次和预算，不重放第一轮模型或工具。
        await session.reopen(expected_revision=closed.revision)
        second = await session.resume("/echo done", idempotency_key="second")
        print(first.text)
        print(second.text)
        # 跨轮事件读取固定 run_id，避免把旧游标带到新 Run。
        frames = [frame async for frame in session.events(run_id=first.run_id)]
        assert frames[-1].kind == "end"
        assert [turn.turn for turn in await session.turns()] == [1, 2]
        await session.destroy()


asyncio.run(main())
