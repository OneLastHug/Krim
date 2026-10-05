"""可信目录的 persistent Agent 预算跨真实进程恢复，不重放旧模型。"""

import asyncio
import json
import sys

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.config import Settings
from krim_agent.context_engine import ContextRunConfig
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import ModelText, ModelUsage
from krim_agent.tools import ToolRegistry
from krim_agent.usage import TokenUsage


class Provider:
    name = "persistent-budget-model"

    def __init__(self):
        self.calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        yield ModelText(text="first answer")
        yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))


async def main():
    database = Database(f"sqlite+aiosqlite:///{sys.argv[1]}")
    await database.initialize()
    repository = KrimRepository(database)
    await repository.recover_interrupted()
    provider = Provider()
    config = ContextRunConfig.from_settings(
        Settings(
            context_tokenizer="cl100k_base",
            context_reserved_output_tokens=16,
        )
    )
    definition = AgentDefinition(
        name="persist_budget",
        provider=provider,
        tools=ToolRegistry(),
        persistent=True,
        context_config=config,
        usage_budget=UsageBudgetConfig(max_total_tokens=100),
    )
    manager = AgentManager(repository)
    try:
        if sys.argv[2] == "first":
            handle = await manager.spawn(definition)
            result = await handle.execute("one", "one")
        else:
            stored = (await manager.store.list())[0]
            handle = await manager.restore(definition, stored)
            result = await handle.resume(
                "this is a longer next prompt that cannot fit the remaining reservation", "two"
            )
        accounts = await handle.runtime.budget_store.accounts()
        print(
            json.dumps(
                {
                    "agent_id": handle.agent_id,
                    "code": result.code,
                    "status": result.status,
                    "spent_tokens": accounts[0].spent_tokens,
                    "provider_calls": provider.calls,
                }
            ),
            flush=True,
        )
    finally:
        await manager.close()
        await database.close()


asyncio.run(main())
