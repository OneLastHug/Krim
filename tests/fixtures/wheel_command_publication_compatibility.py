"""0030旧安装包创建原Agent/文件提案，新包升级并发布产物，旧模型解析原线协议。"""

import asyncio
import json
import sys
from pathlib import Path

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.sdk import (
    AgentCommandRequest,
    CommandLookup,
    FileChangeSpec,
    PythonAI,
    WorkspaceChangeAction,
)
from krim_agent.workspace_change import WorkspaceAttemptView, WorkspaceChangeView


async def main():
    directory, mode = sys.argv[1:]
    root = Path(directory)
    files, state_file = root / "files", root / "state.json"
    if mode == "parse":
        state = json.loads(await asyncio.to_thread(state_file.read_text))
        proposal = WorkspaceChangeView.model_validate_json(json.dumps(state["new_proposal"]))
        operation = WorkspaceAttemptView.model_validate_json(json.dumps(state["new_operation"]))
        assert proposal.proposal_id == operation.proposal_id and operation.receipt.status == "done"
        print(
            json.dumps(
                {"mode": mode, "package": krim_agent.__file__, "strict_receipts_parsed": True}
            )
        )
        return
    if mode == "old":
        await asyncio.to_thread(files.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread((files / "input.txt").write_text, "before")
    settings = Settings(database_url=f"sqlite+aiosqlite:///{root / 'agent.sqlite'}")
    async with await PythonAI.create(settings=settings) as ai:

        async def allow(*_args):
            return "allow"

        ai.runtime.tool_policy = allow
        service = ai.agents()
        service.register_workspace("files", files, version="1", writable=True)
        service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)
        if mode == "old":
            profile = await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="old-agent",
                        description="preserved",
                        persistent=True,
                        workspace_key="files",
                        execution_key="sandbox",
                        tool_names=[
                            "workspace_read",
                            "workspace_preview",
                            "workspace_change",
                            "command_execute",
                            "command_get",
                        ],
                    ),
                )
            )
            agent = await ai.agent("old-agent")
            result = await agent.execute_command(
                AgentCommandRequest(
                    request_key="old-command",
                    input_paths=["input.txt"],
                    command=CommandSpec(
                        argv=["/usr/bin/python3", "-c", "print(open('input.txt').read())"]
                    ),
                )
            )
            proposal = await agent.preview_file_change(
                FileChangeSpec(
                    path="old.txt", operation="write", expected_sha256=None, content="old proposal"
                ),
                request_key="old-proposal",
            )
            state = {
                "agent_id": agent.agent_id,
                "command_id": result.command_id,
                "old_proposal_id": proposal.proposal_id,
                "profile_hash": profile.config_hash,
                "definition_hash": service.manager.definition_hash(
                    await service.definition("old-agent")
                ),
            }
        else:
            from krim_agent.sdk import CommandProposalRequest

            state = json.loads(await asyncio.to_thread(state_file.read_text))
            assert await service.restore_persistent() == 1
            assert (await service.profile_get("old-agent")).config_hash == state["profile_hash"]
            assert (
                service.manager.definition_hash(await service.definition("old-agent"))
                == state["definition_hash"]
            )
            old = ai.agent_session(state["agent_id"])
            assert (
                await old.command(CommandLookup(request_key="old-command"))
            ).command_id == state["command_id"]
            applied = await old.change_file(
                WorkspaceChangeAction(
                    proposal_id=state["old_proposal_id"],
                    expected_revision=0,
                    action="apply",
                    request_key="old-apply",
                )
            )
            assert (
                applied.status == "done"
                and await asyncio.to_thread((files / "old.txt").read_text) == "old proposal"
            )
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="publisher",
                        description="new capability",
                        persistent=True,
                        workspace_key="files",
                        execution_key="sandbox",
                        tool_names=[
                            "workspace_read",
                            "workspace_change",
                            "command_execute",
                            "command_propose",
                        ],
                    ),
                )
            )
            agent = await ai.agent("publisher")
            result = await agent.execute_command(
                AgentCommandRequest(
                    request_key="new-command",
                    input_paths=["input.txt"],
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-c",
                            "open('input.txt','w').write('published artifact')",
                        ]
                    ),
                )
            )
            proposed = await agent.propose_command_change(
                CommandProposalRequest(
                    command_id=result.command_id, path="input.txt", request_key="new-proposal"
                )
            )
            published = await agent.change_file(
                WorkspaceChangeAction(
                    proposal_id=proposed.proposal.proposal_id,
                    expected_revision=0,
                    action="apply",
                    request_key="new-apply",
                )
            )
            assert (
                published.status == "done"
                and await asyncio.to_thread((files / "input.txt").read_text) == "published artifact"
            )
            state["new_proposal"] = proposed.proposal.model_dump(mode="json")
            state["new_operation"] = published.model_dump(mode="json")
        await asyncio.to_thread(state_file.write_text, json.dumps(state))
        assert "/site-packages/" in krim_agent.__file__
        print(
            json.dumps(
                {
                    "mode": mode,
                    "package": krim_agent.__file__,
                    "preserved_agent": state["agent_id"],
                    "old_command": state["command_id"],
                }
            )
        )


asyncio.run(main())
