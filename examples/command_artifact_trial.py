"""Krim Agent命令成果演练：真实差异、独立批准、条件应用和撤销。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.sdk import (
    AgentCommandRequest,
    CommandChangesRequest,
    CommandProposalRequest,
    PythonAI,
    WorkspaceChangeAction,
)


async def main() -> None:
    with TemporaryDirectory(prefix="krim-command-artifact-") as directory:
        root = Path(directory)
        files = root / "files"
        await asyncio.to_thread(files.mkdir)
        await asyncio.to_thread((files / "input.txt").write_text, "before\n")
        settings = Settings(database_url=f"sqlite+aiosqlite:///{root / 'agent.sqlite'}")
        async with await PythonAI.create(settings=settings) as ai:

            async def policy(definition, _context):
                return "ask" if definition.risk == "write" else "allow"

            ai.runtime.tool_policy = policy
            service = ai.agents()
            service.register_workspace("files", files, version="1", writable=True)
            service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="publisher",
                        description="命令成果审阅",
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
                    input_paths=["input.txt"],
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "from pathlib import Path;Path('input.txt').write_text('after\\n');"
                            "Path('generated.txt').write_text('new file\\n')",
                        ]
                    ),
                ),
                confirmed=True,
            )
            assert result.outcome == "completed"
            changes = await agent.command_changes(
                CommandChangesRequest(command_id=result.command_id)
            )
            proposals = [
                await agent.propose_command_change(
                    CommandProposalRequest(
                        command_id=result.command_id,
                        path=item.path,
                        request_key="proposal:" + item.path,
                    )
                )
                for item in changes.entries
            ]
            # 生成提案只保存差异，原文件此时仍保持原内容。
            assert await asyncio.to_thread((files / "input.txt").read_text) == "before\n"
            assert not await asyncio.to_thread((files / "generated.txt").exists)
            operations = []
            for item in proposals:
                operation = await agent.change_file(
                    WorkspaceChangeAction(
                        proposal_id=item.proposal.proposal_id,
                        action="apply",
                        expected_revision=0,
                        request_key="apply:" + item.change.path,
                    ),
                    confirmed=True,
                )  # 独立批准当前文件动作，命令批准不会代替发布批准。
                assert operation.status == "done"
                operations.append(operation)
            assert await asyncio.to_thread((files / "input.txt").read_text) == "after\n"
            assert await asyncio.to_thread((files / "generated.txt").read_text) == "new file\n"
            for item in proposals:
                current = await agent.file_change(item.proposal.proposal_id)
                undone = await agent.change_file(
                    WorkspaceChangeAction(
                        proposal_id=current.proposal_id,
                        action="undo",
                        expected_revision=current.revision,
                        request_key="undo:" + item.change.path,
                    ),
                    confirmed=True,
                )
                assert undone.status == "done"
                for history in await agent.file_change_history(current.proposal_id):
                    await agent.finalize_file_change(history.operation_id, confirmed=True)
            assert await asyncio.to_thread((files / "input.txt").read_text) == "before\n"
            assert not await asyncio.to_thread((files / "generated.txt").exists)
            print(
                json.dumps(
                    {
                        "command_id": result.command_id,
                        "changes": [item.model_dump(mode="json") for item in changes.entries],
                        "proposals": [item.proposal.proposal_id for item in proposals],
                        "published_operations": [item.operation_id for item in operations],
                        "review_kept_original_files": True,
                        "published": True,
                        "undo_restored_original": True,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
