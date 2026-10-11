"""项目外wheel验证二进制命令成果沿原提案/文件账本发布。"""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import (
    AgentCommandRequest,
    AgentSession,
    CommandProposalRequest,
    WorkspaceChangeAction,
)
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-binary-wheel-") as directory:
        root = Path(directory)
        workspace = root / "workspace"
        workspace.mkdir()
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        runtime = ChatRuntime(
            KrimRepository(database),
            FakeProvider(delay=0),
            builtin_registry(),
            Settings(
                database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}", interactive_tools=True
            ),
        )

        async def host_policy(definition, _context):
            return "ask" if definition.risk == "write" else "allow"

        runtime.tool_policy = host_policy
        app = create_app(
            runtime=runtime,
            workspace_root=workspace,
            workspace_writable=True,
            execution_runner=LinuxSandboxRunner(),
            execution_owned=True,
        )
        async with app.router.lifespan_context(app):
            service = app.state.agents
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="binary-wheel",
                        description="wheel binary artifact",
                        workspace_key="local",
                        execution_key="local",
                        tool_names=[
                            "workspace_read",
                            "workspace_preview",
                            "workspace_change",
                            "workspace_reconcile",
                            "workspace_finalize",
                            "command_execute",
                            "command_get",
                            "command_read",
                            "command_history",
                            "command_inspect",
                            "command_changes",
                            "command_propose",
                        ],
                    ),
                )
            )
            view = await service.spawn("binary-wheel")
            session = AgentSession(service, view.agent_id)
            result = await session.execute_command(
                AgentCommandRequest(
                    request_key="wheel-binary",
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "open('out.bin','wb').write(b'\\0\\xff')",
                        ]
                    ),
                ),
                confirmed=True,
            )
            assert result.outcome == "completed"
            proposal = await session.propose_command_change(
                CommandProposalRequest(
                    command_id=result.command_id, path="out.bin", request_key="wheel-proposal"
                )
            )
            assert (
                proposal.proposal.preview.truncated and "binary" in proposal.proposal.preview.diff
            )
            applied = await session.change_file(
                WorkspaceChangeAction(
                    proposal_id=proposal.proposal.proposal_id,
                    action="apply",
                    expected_revision=proposal.proposal.revision,
                    request_key="wheel-apply",
                ),
                confirmed=True,
            )
            assert applied.status == "done" and (workspace / "out.bin").read_bytes() == b"\0\xff"
            second = await session.execute_command(
                AgentCommandRequest(
                    request_key="wheel-binary-input",
                    input_paths=["out.bin"],
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "data=open('out.bin','rb').read();print(data.hex());"
                            "open('out.bin','wb').write(data+bytes([128]))",
                        ]
                    ),
                ),
                confirmed=True,
            )
            assert second.outcome == "completed" and second.stdout_text == "00ff\n"
            binary = await session.propose_command_change(
                CommandProposalRequest(
                    command_id=second.command_id,
                    path="out.bin",
                    request_key="binary-input-proposal",
                )
            )
            for action in ("apply", "keep", "undo"):
                current = await session.file_change(binary.proposal.proposal_id)
                receipt = await session.change_file(
                    WorkspaceChangeAction(
                        proposal_id=current.proposal_id,
                        expected_revision=current.revision,
                        action=action,
                        request_key="binary-input-" + action,
                    ),
                    confirmed=True,
                )
                assert receipt.status == "done"
                assert (workspace / "out.bin").read_bytes() == (
                    b"\0\xff" if action == "undo" else b"\0\xff\x80"
                )
                await session.finalize_file_change(receipt.operation_id, confirmed=True)
            print(
                {
                    "package": krim_agent.__file__,
                    "command": result.outcome,
                    "file": "binary-published",
                    "input": "binary-captured",
                    "undo": "exact-bytes-restored",
                }
            )


asyncio.run(main())
