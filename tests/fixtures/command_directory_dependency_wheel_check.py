"""项目外wheel生成真实隔离目录成果，再独立进程冷读原来源依赖计划。"""

import asyncio
import hashlib
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import func, select

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.execution.directory_publication import CommandDirectoryDependencyRequest
from krim_agent.persistence.command_directory_dependencies import CommandDirectoryDependencyStore
from krim_agent.persistence.database import (
    Database,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
)
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.sdk import AgentCommandRequest, AgentSession, PythonAI


async def read(root):
    assert "/site-packages/" in str(krim_agent.__file__)
    frozen = json.loads((root / "source.json").read_text())
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    try:
        await database.initialize()

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("冷读依赖计划不得执行命令")

        LinuxSandboxRunner.start = forbidden
        store = CommandDirectoryDependencyStore(WorkspaceEditStore(database))
        request = CommandDirectoryDependencyRequest(
            command_id=frozen["command_id"], source_hash=frozen["source_hash"]
        )
        value = await store.plan(frozen["scope_id"], request)
        assert value.model_dump(mode="json") == frozen["view"]
        assert [step.path for step in value.plan.steps] == ["tree", "tree/sub", "tree/sub/data.bin"]
        assert [step.mode for step in value.plan.steps] == [0o750, 0o700, None]
        assert value.files[0].sha256 == hashlib.sha256(b"\x00\xff").hexdigest()
        assert list((root / "files").iterdir()) == []
        async with database.sessions() as sql:
            for model in (
                WorkspaceDirectoryProposalRow,
                WorkspaceDirectoryAttemptRow,
                WorkspaceEditProposalRow,
                WorkspaceEditAttemptRow,
            ):
                assert await sql.scalar(select(func.count()).select_from(model)) == 0
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "source_hash_bound": True,
                    "real_nested_directory_binary": True,
                    "separate_process_cold_read": True,
                    "zero_command_replay": True,
                    "zero_proposal_attempts": True,
                    "zero_host_publication": True,
                }
            )
        )
    finally:
        await database.close()


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-directory-source-plan-", dir="/data/tmp/root") as folder:
        root = Path(folder)
        files = root / "files"
        files.mkdir()
        async with await PythonAI.create(
            settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        ) as ai:

            async def allow(_definition, _context):
                return "allow"

            ai.runtime.tool_policy = allow
            service = ai.agents()
            service.register_workspace("files", files, version="1", writable=True)
            service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="dependency-wheel",
                        description="目录依赖来源安装包",
                        workspace_key="files",
                        execution_key="sandbox",
                        tool_names=["command_execute", "command_get"],
                    ),
                )
            )
            agent = AgentSession(service, (await service.spawn("dependency-wheel")).agent_id)
            command = await agent.execute_command(
                AgentCommandRequest(
                    request_key="command",
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "import os;os.makedirs('tree/sub');os.chmod('tree',0o750);"
                            "os.chmod('tree/sub',0o700);open('tree/sub/data.bin','wb').write(bytes([0,255]))",
                        ]
                    ),
                ),
                confirmed=True,
            )
            assert command.outcome == "completed" and command.artifacts_complete
            store = CommandDirectoryDependencyStore(service.workspace_edits)
            value = await store.plan(
                "session:" + agent.session_id,
                CommandDirectoryDependencyRequest(command_id=command.command_id),
            )
            (root / "source.json").write_text(
                json.dumps(
                    {
                        "scope_id": value.scope_id,
                        "command_id": value.command_id,
                        "source_hash": value.source_hash,
                        "view": value.model_dump(mode="json"),
                    }
                )
            )
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            __file__,
            "read",
            str(root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(child.communicate(), 30)
            assert child.returncode == 0, stderr.decode()
            print(stdout.decode().strip())
        finally:
            if child.returncode is None:
                child.kill()
                await child.communicate()


asyncio.run(read(Path(sys.argv[2])) if len(sys.argv) == 3 else main())
