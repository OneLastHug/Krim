"""真实进程的命令产物提案、发布骤停、原操作核实与零重放。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.sdk import (
    AgentCommandRequest,
    CommandProposalRequest,
    PythonAI,
    WorkspaceChangeAction,
)
from krim_agent.workspace_writer import WorkspaceWriter


class Runner(LinuxSandboxRunner):
    def __init__(self):
        super().__init__()
        self.calls = 0

    async def start(self, *args, **kwargs):
        self.calls += 1
        return await super().start(*args, **kwargs)


async def main():
    directory, mode = sys.argv[1:3]
    binary = "--binary" in sys.argv[3:]
    root = Path(directory)
    files, state_path = root / "files", root / "state.json"
    fresh = mode in {"preview", "crash-before", "crash-after"}
    if fresh:
        await asyncio.to_thread(files.mkdir, parents=True, exist_ok=True)
        if binary:
            await asyncio.to_thread((files / "file.txt").write_bytes, b"\0\xffbefore")
        else:
            await asyncio.to_thread((files / "file.txt").write_text, "before")
    runner = Runner()
    settings = Settings(database_url=f"sqlite+aiosqlite:///{root / 'agent.sqlite'}")
    async with await PythonAI.create(settings=settings) as ai:

        async def allow(*_args):
            return "allow"

        ai.runtime.tool_policy = allow
        service = ai.agents()
        service.register_workspace("files", files, version="1", writable=True)
        service.register_execution("sandbox", runner, version="1", owned=True)
        if fresh:
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="publisher",
                        description="publication process",
                        persistent=True,
                        workspace_key="files",
                        execution_key="sandbox",
                        tool_names=[
                            "workspace_read",
                            "workspace_change",
                            "workspace_reconcile",
                            "workspace_finalize",
                            "command_execute",
                            "command_changes",
                            "command_propose",
                        ],
                    ),
                )
            )
            agent = await ai.agent("publisher")
            result = await agent.execute_command(
                AgentCommandRequest(
                    request_key="command",
                    input_paths=["file.txt"],
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "open('file.txt','wb').write(bytes([0,128])+b'artifact')"
                            if binary
                            else "open('file.txt','w').write('artifact')",
                        ]
                    ),
                )
            )
            proposal = await agent.propose_command_change(
                CommandProposalRequest(
                    command_id=result.command_id, path="file.txt", request_key="proposal"
                )
            )
            state = {
                "agent_id": agent.agent_id,
                "command_id": result.command_id,
                "proposal_id": proposal.proposal.proposal_id,
            }
            await asyncio.to_thread(state_path.write_text, json.dumps(state))
        else:
            state = json.loads(await asyncio.to_thread(state_path.read_text))
            assert await service.restore_persistent() == 1
            agent = ai.agent_session(state["agent_id"])
            assert runner.calls == 0
        apply = WorkspaceChangeAction(
            proposal_id=state["proposal_id"],
            expected_revision=0,
            action="apply",
            request_key="apply",
        )
        status = None
        if mode in {"apply", "crash-before", "crash-after"}:
            if mode.startswith("crash"):
                original = WorkspaceWriter.apply

                def crash(*args, **kwargs):
                    if mode == "crash-after":
                        result = original(*args, **kwargs)
                        assert result.receipt.status == "done", result
                    os._exit(23)

                WorkspaceWriter.apply = crash
            operation = await agent.change_file(apply)
            assert operation.status == "done"
            state["operation_id"] = operation.operation_id
            await asyncio.to_thread(state_path.write_text, json.dumps(state))
            status = operation.status
        elif mode == "recover":
            operation = (await agent.file_change_history(state["proposal_id"]))[0]
            resolved = await agent.reconcile_file_change(operation.operation_id)
            status = resolved.status
            state["operation_id"] = resolved.operation_id
            assert status in {"done", "rejected"}
            await agent.finalize_file_change(resolved.operation_id)
            assert (await agent.change_file(apply)).operation_id == resolved.operation_id
        elif mode == "replay":
            operation = await agent.change_file(apply)
            assert operation.operation_id == state["operation_id"]
            original = await agent.propose_command_change(
                CommandProposalRequest(
                    command_id=state["command_id"], path="file.txt", request_key="proposal"
                )
            )
            assert original.proposal.proposal_id == state["proposal_id"]
            status = operation.status
        elif mode == "undo":
            proposal = await agent.file_change(state["proposal_id"])
            operation = await agent.change_file(
                WorkspaceChangeAction(
                    proposal_id=proposal.proposal_id,
                    expected_revision=proposal.revision,
                    action="undo",
                    request_key="undo",
                )
            )
            assert operation.status == "done"
            await agent.finalize_file_change(operation.operation_id)
            status = operation.status
        content = (
            (await asyncio.to_thread((files / "file.txt").read_bytes)).hex()
            if binary
            else await asyncio.to_thread((files / "file.txt").read_text)
        )
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "mode": mode,
                    "calls": runner.calls,
                    "status": status,
                    "content": content,
                    **state,
                }
            ),
            flush=True,
        )


asyncio.run(main())
