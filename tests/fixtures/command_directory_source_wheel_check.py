"""项目外安装包执行真实隔离命令目录来源，再按原目录提案发布与撤销。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import func, select, text

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.persistence.database import CommandDirectoryProposalRow
from krim_agent.sdk import (
    AgentCommandRequest,
    AgentSession,
    CommandDirectoryProposalRequest,
    DirectoryChangeAction,
    DirectoryQueryRequest,
    PythonAI,
)


async def main() -> None:
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(
        prefix="krim-command-directory-source-", dir="/data/tmp/root"
    ) as folder:
        root = Path(folder) / "workspace"
        root.mkdir()
        settings = Settings(
            database_url=f"sqlite+aiosqlite:///{Path(folder) / 'state.sqlite'}",
            interactive_tools=True,
        )
        async with await PythonAI.create(settings=settings) as ai:
            service = ai.agents()
            service.register_workspace("files", root, version="1", writable=True)
            runner = LinuxSandboxRunner()
            service.register_execution("sandbox", runner, version="1", owned=True)
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="source-wheel",
                        description="安装包目录来源",
                        workspace_key="files",
                        execution_key="sandbox",
                        tool_names=[
                            "command_execute",
                            "command_get",
                            "directory_source_preview",
                            "directory_change",
                            "directory_query",
                            "directory_finalize",
                        ],
                    ),
                )
            )
            view = await service.spawn("source-wheel")
            agent = AgentSession(service, view.agent_id)
            result = await agent.execute_command(
                AgentCommandRequest(
                    request_key="command",
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "import os;os.mkdir('empty');os.chmod('empty',0o750)",
                        ]
                    ),
                ),
                confirmed=True,
            )
            assert result.outcome == "completed" and not (root / "empty").exists()

            async def forbidden(*_args, **_kwargs):
                raise AssertionError("目录提案与查询不得重跑原命令")

            runner.start = forbidden
            intent = CommandDirectoryProposalRequest(
                command_id=result.command_id, path="empty", request_key="proposal"
            )
            proposal = await agent.propose_command_directory(intent)
            repeated = await agent.propose_command_directory(intent)
            assert repeated.proposal.proposal_id == proposal.proposal.proposal_id
            applied = await agent.change_directory(
                DirectoryChangeAction(
                    proposal_id=proposal.proposal.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="apply",
                ),
                confirmed=True,
            )
            assert applied.status == "done" and (root / "empty").stat().st_mode & 0o777 == 0o750
            current = await agent.query_directory(
                DirectoryQueryRequest(kind="proposal", identifier=proposal.proposal.proposal_id)
            )
            undone = await agent.change_directory(
                DirectoryChangeAction(
                    proposal_id=proposal.proposal.proposal_id,
                    action="undo",
                    expected_revision=current.proposal.revision,
                    request_key="undo",
                ),
                confirmed=True,
            )
            assert undone.status == "done" and not (root / "empty").exists()
            await agent.finalize_directory_change(undone.operation_id, confirmed=True)
            database = ai.runtime.repository.database
            async with database.write_lock, database.sessions() as sql:
                assert (
                    await sql.scalar(select(func.count()).select_from(CommandDirectoryProposalRow))
                    == 1
                )
                assert (
                    await sql.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0041_workspace_tree_plans"
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "schema": "0041_workspace_tree_plans",
                        "source_links": 1,
                        "created": True,
                        "undone": True,
                        "no_command_replay": True,
                    }
                )
            )


asyncio.run(main())
