"""真实强制退出窗口：Run/session 已提交，handle 指针尚未保存。"""

import asyncio
import json
import os
import sys

from krim_agent.config import Settings
from krim_agent.protocol.domain import ChatRequest
from krim_agent.providers import ModelText
from krim_agent.sdk import PythonAI


class WaitingProvider:
    name = "session-process"

    def __init__(self):
        self.calls = 0
        self.entered = asyncio.Event()

    async def stream(self, _messages, _tools):
        self.calls += 1
        self.entered.set()
        yield ModelText(text="已保存的部分文本")
        await asyncio.Event().wait()


async def main():
    provider = WaitingProvider()
    ai = await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{sys.argv[1]}"), provider=provider
    )
    if sys.argv[2] == "crash":
        ai.agents().manager.store.lease_seconds = 0.2
        session = await ai.agent(persistent=True)
        handle = ai.agents().manager._handles[session.agent_id]
        run = await handle.runtime.start(
            handle.topic_id, ChatRequest(text="wait", idempotency_key="once")
        )
        # 未调用 handle.execute/persist；模拟原子 Run 创建后的进程崩溃。
        await asyncio.wait_for(provider.entered.wait(), 5)
        print(json.dumps({"agent_id": session.agent_id, "run_id": run.id}), flush=True)
        os._exit(0)
    async with ai:
        session = ai.agent_session(sys.argv[3])
        view = await session.restore()
        frames = [f async for f in session.events(run_id=view.current_run_id)]
        assert view.turn == 1 and view.status == "interrupted" and provider.calls == 0
        assert frames[-1].kind == "end" and frames[-1].data["status"] == "interrupted"
        print(
            json.dumps(
                {
                    "agent_id": view.agent_id,
                    "run_id": view.current_run_id,
                    "turn": view.turn,
                    "calls": provider.calls,
                }
            )
        )


asyncio.run(main())
