"""项目外wheel冻结真实目录树根/步骤，再新进程冷读并取消，零宿主发布。"""

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
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.sdk import AgentCommandRequest, AgentSession, PythonAI
from krim_agent.workspace_directory import ArtifactDirectorySpec, WorkspaceDirectoryWriter
from krim_agent.workspace_tree import TreePlanRequest


async def read(root):
    assert "/site-packages/" in str(krim_agent.__file__)
    frozen = json.loads((root / "source.json").read_text())
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    try:
        await database.initialize()

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("冷读依赖计划不得执行命令")

        LinuxSandboxRunner.start = forbidden
        files = WorkspaceEditStore(database)
        store = WorkspaceTreePlanStore(
            files, WorkspaceDirectoryStore(database, owner_id=files.owner_id)
        )
        value = await store.get(frozen["scope_id"], frozen["plan_id"])
        assert value.model_dump(mode="json") == frozen["view"]
        assert value.status == "planned" and value.step_count == 3
        page = await store.steps(value.scope_id, value.plan_id)
        assert [step.path for step in page.items] == ["tree", "tree/sub", "tree/sub/data.bin"]
        assert [step.parent_position for step in page.items] == [None, 0, 1]
        assert [step.mode for step in page.items] == [0o750, 0o700, 0o644]
        assert page.items[-1].sha256 == hashlib.sha256(b"\x00\xff").hexdigest()
        cancelled = await store.cancel(value.scope_id, value.plan_id)
        assert cancelled.status == "cancelled"
        assert await store.by_key(value.scope_id, value.request_key) == cancelled
        assert all(
            step.status == "skipped"
            for step in (await store.steps(value.scope_id, value.plan_id)).items
        )
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
                    "schema": "0041_workspace_tree_plans",
                    "frozen_root_steps": True,
                    "separate_process_cold_read": True,
                    "cancel_zero_reopen": True,
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
    with TemporaryDirectory(prefix="krim-workspace-tree-store-", dir="/data/tmp/root") as folder:
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
            source = await CommandDirectoryDependencyStore(service.workspace_edits).plan(
                "session:" + agent.session_id,
                CommandDirectoryDependencyRequest(command_id=command.command_id),
            )
            binding = service._handle(agent.agent_id).definition.workspace
            assert binding is not None
            prepared = WorkspaceDirectoryWriter(binding.require()).prepare_artifact(
                ArtifactDirectorySpec(
                    path="tree",
                    operation="create",
                    mode=0o750,
                    command_id=command.command_id,
                    source_hash=source.source_hash,
                )
            )
            store = WorkspaceTreePlanStore(
                service.workspace_edits, service.manager.directory_changes.store
            )
            value, created = await store.reserve(
                "session:" + agent.session_id,
                binding,
                TreePlanRequest(
                    command_id=command.command_id,
                    source_hash=source.source_hash,
                    root_path="tree",
                    request_key="tree-plan",
                ),
                prepared,
            )
            assert created and value.status == "planned"
            (root / "source.json").write_text(
                json.dumps(
                    {
                        "scope_id": value.scope_id,
                        "plan_id": value.plan_id,
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
