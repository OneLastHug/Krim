"""实际进程间恢复Agent命令、原产物与冻结资源版本；适用于源码和wheel。"""

import asyncio
import json
import sys
from pathlib import Path

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.sdk import AgentCommandRequest, CommandLookup, CommandRead, PythonAI


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
    files, state_file = root / "files", root / "state.json"
    if mode == "create":
        await asyncio.to_thread(files.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread((files / "input.txt").write_text, "original input\n")
    settings = Settings(database_url=f"sqlite+aiosqlite:///{root / 'agent.sqlite'}")
    runner = Runner()
    async with await PythonAI.create(settings=settings) as ai:

        async def policy(definition, _context):
            return "ask" if definition.risk == "write" else "allow"

        ai.runtime.tool_policy = policy
        service = ai.agents()
        service.register_workspace("files", files, version="1", writable=False)
        service.register_execution(
            "sandbox", runner, version="2" if mode == "wrong" else "1", owned=True
        )
        request = AgentCommandRequest(
            request_key="command",
            input_paths=["input.txt"],
            command=CommandSpec(
                argv=[
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    "from pathlib import Path;print(Path('input.txt').read_text(),end='');"
                    "Path('output.txt').write_text('artifact')",
                ]
            ),
        )
        if mode == "create":
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="worker",
                        description="跨进程命令",
                        persistent=True,
                        workspace_key="files",
                        execution_key="sandbox",
                        tool_names=[
                            "workspace_read",
                            "command_execute",
                            "command_get",
                            "command_read",
                            "command_cancel",
                        ],
                    ),
                )
            )
            agent = await ai.agent("worker")
            result = await agent.execute_command(request, confirmed=True)
            assert result.outcome == "completed" and result.stdout_text == "original input\n"
            assert not await asyncio.to_thread((files / "output.txt").exists)
            await asyncio.to_thread((files / "input.txt").write_text, "external change\n")
            state = {"agent_id": agent.agent_id, "command_id": result.command_id}
            await asyncio.to_thread(state_file.write_text, json.dumps(state))
            restored = 0
        else:
            state = json.loads(await asyncio.to_thread(state_file.read_text))
            restored = await service.restore_persistent()
            if mode == "wrong":
                assert restored == 0 and runner.calls == 0
                inventory = await service.inventory()
                assert inventory[0].error_code == "agent_definition_changed"
            else:
                assert restored == 1
                agent = ai.agent_session(state["agent_id"])
                result = await agent.execute_command(request, confirmed=True)
                assert (
                    result.command_id == state["command_id"]
                    and result.stdout_text == "original input\n"
                )
                artifact = await agent.read_command(
                    CommandRead(command_id=result.command_id, source="artifact", path="output.txt")
                )
                assert artifact.text == "artifact" and runner.calls == 0
                assert (
                    await agent.command(CommandLookup(request_key="command"))
                ).command_id == result.command_id
                assert (
                    await asyncio.to_thread((files / "input.txt").read_text) == "external change\n"
                )
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "mode": mode,
                    "restored": restored,
                    "calls": runner.calls,
                    **state,
                }
            )
        )


asyncio.run(main())
