"""真正Host后置Hook强杀；辅助JSON不代替原产品SQL材料或原观察资格。"""

import asyncio
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

from sqlalchemy import func, select

import krim_agent
import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.application.agents import AgentService
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreAttemptRow as Attempt,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestorePlanRow as Plan,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreFence
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_missing_directory_restore_control import (  # noqa: E402
    checkpoint,
    drain,
    execute_request,
    preview_request,
    setup,
)


async def main():
    phase, point, directory = sys.argv[1:4]
    root = Path(directory)
    files = root / "files"
    files.mkdir(exist_ok=True)
    url = f"sqlite+aiosqlite:///{root / 'state.sqlite'}"
    database = Database(url)
    await database.initialize()
    runtime = ChatRuntime(
        KrimRepository(database),
        FakeProvider(delay=0),
        builtin_registry(),
        Settings(database_url=url, provider="fake"),
    )
    service = None
    try:
        if phase == "produce":
            service, agent, binding = await setup(runtime, files, persistent=True)
            service.missing_directory_restore_store.lease_seconds = 5
            target = await checkpoint(agent)
            (files / "tree").rename(files / "retained-original")
            metadata = {}

            async def hold(definition, value, context):
                desired = MISSING_DIRECTORY_RESTORE_STEP_TOOLS[
                    "prepare" if point == "absence" else "execute"
                ]
                if definition.name != desired:
                    return value
                fence = context.directory_restore
                async with database.sessions() as sql:
                    plan = await sql.get(Plan, fence.plan_id)
                    attempt = await sql.get(Attempt, fence.operation_id)
                    metadata.update(
                        agent_id=agent.agent_id,
                        scope="session:" + agent.session_id,
                        plan_id=plan.plan_id,
                        operation_id=fence.operation_id,
                        target_id=target.checkpoint_id,
                        fence=asdict(fence),
                        absence=plan.absence_fingerprint,
                        lease=(attempt or plan).lease_expires_at,
                        owner_id=service.missing_directory_restore_store.owner_id,
                        root_inode=(files / "tree").stat().st_ino if point == "effect" else None,
                        root_mode=(files / "tree").stat().st_mode & 0o7777
                        if point == "effect"
                        else None,
                    )
                with (root / "metadata.json").open("w") as output:
                    json.dump(metadata, output)
                    output.flush()
                    os.fsync(output.fileno())
                print(
                    json.dumps(
                        {"pid": os.getpid(), "point": point, "package": str(krim_agent.__file__)}
                    ),
                    flush=True,
                )
                sys.stdin.readline()
                raise AssertionError("应在真实叶后置Hook由父进程SIGKILL")

            service._handle(agent.agent_id).runtime.tools.add_global_post(hold)
            planned = await service.workspace_preview_missing_directory_restore(
                agent.agent_id, preview_request(target.checkpoint_id)
            )
            if point == "effect":
                planned = await service.workspace_missing_directory_restore_wait(
                    agent.agent_id, planned.plan_id
                )
                await drain(service)
                assert planned.phase == "ready"
                await service.workspace_start_missing_directory_restore(
                    agent.agent_id, execute_request(planned)
                )
            await drain(service)
            raise AssertionError("原生产者不应越过真实强杀点")

        metadata = json.loads((root / "metadata.json").read_text())
        await asyncio.sleep(max(0, metadata["lease"] - time.time()) + 0.02)

        async def allow(_definition, _context):
            return "allow"

        runtime.tool_policy = allow
        service = AgentService(runtime, runtime.settings)
        binding = service.register_workspace(
            "files", files, version="1", writable=True, default=True
        )
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        await service.restore(metadata["agent_id"])
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("冷Host不得重放Model/Reader/Writer/namespace")

        WorkspaceDirectoryAbsenceReader.capture = forbidden
        WorkspaceDirectoryAbsenceReader.verify = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
        WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
        physical.rename_at = forbidden
        FakeProvider.stream = forbidden
        await service.recover_workspace_edits()
        state = await service.workspace_missing_directory_restore(
            metadata["agent_id"], metadata["plan_id"]
        )
        assert state.phase == ("interrupted" if point == "absence" else "uncertain")
        assert state.baseline.fingerprint == metadata["absence"]
        assert state.operation_id == metadata["operation_id"]
        assert (
            await service.workspace_preview_missing_directory_restore(
                metadata["agent_id"], preview_request(metadata["target_id"])
            )
            == state
        )
        assert (
            await service.workspace_missing_directory_restore_by_key(
                metadata["agent_id"], "preview"
            )
            == state
        )
        old = MissingDirectoryRestoreFence(**metadata["fence"])
        try:
            await service.missing_directory_restore_store.renew(
                metadata["scope"], old, binding, qualify=lambda: None
            )
        except ServiceError:
            pass
        else:
            raise AssertionError("原Owner/token不能在冷Host续租")
        if point == "effect":
            await service.workspace_verify_missing_directory_restore(
                metadata["agent_id"], state.operation_id
            )
            await drain(service)
            actual = await service.workspace_missing_directory_restore(
                metadata["agent_id"], state.plan_id
            )
            assert actual.phase == "uncertain" and actual.operation_id == state.operation_id
            assert actual.receipt.error_code == "workspace_missing_directory_restore_unverifiable"
            assert (files / "tree").stat().st_ino == metadata["root_inode"]
            assert (files / "tree").stat().st_mode & 0o7777 == metadata["root_mode"]
            assert (files / "tree/nested/nul").read_bytes() == b"\0\xff"
        else:
            assert not (files / "tree").exists()
        async with database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(Plan)) == 1
            assert await sql.scalar(select(func.count()).select_from(Attempt)) == int(
                point == "effect"
            )
        assert not replays and database.engine.sync_engine.pool.checkedout() == 0
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "point": point,
                    "plan_id": state.plan_id,
                    "operation_id": state.operation_id,
                    "phase": state.phase,
                    "natural_restore_lease_expiry": time.time() > metadata["lease"],
                    "provider_reader_writer_namespace_replays": len(replays),
                    "binary_hex": (files / "tree/nested/nul").read_bytes().hex()
                    if point == "effect"
                    else None,
                    "package": str(krim_agent.__file__),
                }
            )
        )
    finally:
        if service is not None:
            await service.close()
        await runtime.close()
        await database.close()


asyncio.run(main())
