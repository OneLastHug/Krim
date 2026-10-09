"""Krim Agent命令SDK演练：显式资源、审批、只读输入、原产物与冷恢复。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.sdk import AgentCommandRequest, CommandLookup, CommandRead, PythonAI


async def main() -> None:
    with TemporaryDirectory(prefix="krim-agent-command-") as directory:
        root = Path(directory)
        files = root / "files"
        await asyncio.to_thread(files.mkdir)
        await asyncio.to_thread((files / "input.txt").write_text, "Krim Agent\n")
        settings = Settings(database_url=f"sqlite+aiosqlite:///{root / 'agent.sqlite'}")
        request = AgentCommandRequest(
            request_key="trial",
            input_paths=["input.txt"],
            command=CommandSpec(
                argv=[
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    "from pathlib import Path; value=Path('input.txt').read_text().upper();"
                    "Path('output.txt').write_text(value); print(value,end='')",
                ]
            ),
        )

        async def policy(definition, _context):
            # 写工具需要调用者明确确认，读取工具仍走自己的权限与Hook。
            return "ask" if definition.risk == "write" else "allow"

        async def configure(ai: PythonAI) -> None:
            ai.runtime.tool_policy = policy
            service = ai.agents()
            service.register_workspace("files", files, version="1", writable=False)
            service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)

        async with await PythonAI.create(settings=settings) as ai:
            await configure(ai)
            await ai.agents().profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="command-worker",
                        description="命令演练",
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
            agent = await ai.agent("command-worker")
            result = await agent.execute_command(request, confirmed=True)
            assert result.outcome == "completed" and result.processes_reaped
            artifact = await agent.read_command(
                CommandRead(command_id=result.command_id, source="artifact", path="output.txt")
            )
            assert artifact.text == "KRIM AGENT\n"
            assert not await asyncio.to_thread((files / "output.txt").exists)
            agent_id, command_id = agent.agent_id, result.command_id
            await asyncio.to_thread((files / "input.txt").write_text, "later external change\n")

        # 新SDK实例复供相同版本资源，恢复原Agent后只读取原任务。
        async with await PythonAI.create(settings=settings) as cold:
            await configure(cold)
            assert await cold.agents().restore_persistent() == 1
            restored = cold.agent_session(agent_id)
            same = await restored.execute_command(request, confirmed=True)
            assert same.command_id == command_id and same.stdout_text == "KRIM AGENT\n"
            queried = await restored.command(CommandLookup(request_key="trial"))
            assert queried.command_id == command_id
            print(
                json.dumps(
                    {
                        "agent_id": agent_id,
                        "command_id": command_id,
                        "outcome": same.outcome,
                        "stdout": same.stdout_text,
                        "artifact": artifact.text,
                        "workspace_unchanged_by_command": True,
                        "restored_original_command": True,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
