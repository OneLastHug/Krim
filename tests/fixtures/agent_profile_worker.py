"""真实跨进程 Agent 原版本恢复与零读取调用。"""

import asyncio
import json
import sys

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentPromptRequest
from krim_agent.config import Settings
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


class Counting(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for event in super().stream(messages, tools):
            yield event


async def main():
    provider = Counting()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{sys.argv[1]}"), provider=provider
    ) as ai:
        service = ai.agents()
        if sys.argv[2] == "create":
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="worker",
                        description="跨进程",
                        tool_names=["add"],
                        instructions="旧版本规则",
                    ),
                )
            )
            view = await service.spawn("worker", persistent=True)
            view = await service.execute(
                view.agent_id, AgentPromptRequest(prompt="/add 2 3", idempotency_key="first")
            )
        else:
            old = await service.profile_get("worker")
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=old.revision,
                    config=old.config.model_copy(update={"instructions": "新规则"}),
                )
            )
            assert await service.restore_persistent() == 1
            view = await service.get(sys.argv[3])
            assert view.profile_revision == 1
            assert service.manager._handles[view.agent_id].definition.instructions == "旧版本规则"
            assert provider.calls == 0
        print(
            json.dumps(
                {
                    "agent_id": view.agent_id,
                    "topic_id": view.topic_id,
                    "run_id": view.current_run_id,
                    "revision": view.profile_revision,
                    "calls": provider.calls,
                }
            )
        )


asyncio.run(main())
