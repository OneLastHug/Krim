"""旧0030包创建Agent/命令，新包恢复原定义与无origin的原请求。"""

import asyncio
import json
import sys
from pathlib import Path

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.execution.records import CommandRequest
from krim_agent.execution.service import DurableCommandService
from krim_agent.persistence.commands import CommandStore
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


class Counter(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for event in super().stream(messages, tools):
            yield event


class Runner(LinuxSandboxRunner):
    def __init__(self):
        super().__init__()
        self.calls = 0

    async def start(self, *args, **kwargs):
        self.calls += 1
        return await super().start(*args, **kwargs)


async def main():
    directory, mode = sys.argv[1:]
    root = Path(directory)
    await asyncio.to_thread(root.mkdir, parents=True, exist_ok=True)
    settings = Settings(database_url=f"sqlite+aiosqlite:///{root / 'legacy.sqlite'}")
    state_path = root / "state.json"
    provider, runner = Counter(), Runner()
    async with await PythonAI.create(settings=settings, provider=provider) as ai:
        service = ai.agents()
        if mode == "old":
            profile = await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="plain",
                        description="旧定义",
                        tool_names=["echo"],
                        persistent=True,
                    ),
                )
            )
            agent = await ai.agent("plain")
            result = await agent.execute("/echo retained", idempotency_key="run")
            assert result.status == "completed"
            topic = await ai.runtime.repository.create_topic("old command")
            state = {
                "agent_id": agent.agent_id,
                "run_id": result.run_id,
                "topic_id": topic.id,
                "profile_hash": profile.config_hash,
                "definition_hash": service.manager.definition_hash(
                    await service.definition("plain")
                ),
            }
        else:
            state = json.loads(await asyncio.to_thread(state_path.read_text))
            assert await service.restore_persistent() == 1 and provider.calls == 0
            assert (await service.profile_get("plain")).config_hash == state["profile_hash"]
            assert (
                service.manager.definition_hash(await service.definition("plain"))
                == state["definition_hash"]
            )
            assert "execution_key" not in (await service.profile_get("plain")).config.model_dump()
            assert (await ai.agent_session(state["agent_id"]).state()).current_run_id == state[
                "run_id"
            ]
        store = CommandStore(ai.runtime.repository.database)
        async with DurableCommandService(store, runner, owned=True) as commands:
            request = CommandRequest(
                request_key="raw",
                command=CommandSpec(argv=["/usr/bin/python3", "-I", "-c", "print('legacy')"]),
            )
            handle = await commands.start("topic:" + state["topic_id"], request)
            result = await handle.wait()
            assert result.outcome == "completed" and await handle.output() == (b"legacy\n", b"")
            if mode == "old":
                state["command_id"] = handle.command_id
                await asyncio.to_thread(state_path.write_text, json.dumps(state))
            else:
                assert handle.command_id == state["command_id"] and runner.calls == 0
                assert await store.origin("topic:" + state["topic_id"], handle.command_id) is None
        assert "/site-packages/" in krim_agent.__file__
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "mode": mode,
                    "model_calls": provider.calls,
                    "command_calls": runner.calls,
                    **state,
                }
            )
        )


asyncio.run(main())
