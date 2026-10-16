"""真实Model来源Store/SQL/物理强杀；GC call合成，不冒称已接模型父工具。"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import krim_agent
import krim_agent.workspace_empty_container_retention as physical
import krim_agent.workspace_missing_directory_writer as missing
from krim_agent.application.agents import AgentService
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import AgentHandleRow
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.protocol.domain import ChatRequest
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_container_retention import ContainerRetentionRequest
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_container_retention_host import completed, drain, request, setup  # noqa: E402
from test_container_retention_store import remove  # noqa: E402


def save(path, value):
    with path.open("w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())


async def main():
    phase, point, folder = sys.argv[1:4]
    root = Path(folder)
    files = root / "files"
    files.mkdir(exist_ok=True)
    url = f"sqlite+aiosqlite:///{root / 'state.sqlite'}"
    database = Database(url)
    await database.initialize()
    runtime = ChatRuntime(
        KrimRepository(database),
        FakeProvider(delay=0),
        builtin_registry(),
        Settings(database_url=url),
    )
    service = None
    try:
        if phase == "produce":
            service, agent, binding = await setup(runtime, files, persistent=True, agent_lease=0.8)
            store = service.container_retention_store
            store.lease_seconds = 1
            original = await completed(service, agent, binding)
            preview = await service.workspace_preview_container_retention(
                agent.agent_id, original.plan_id
            )
            wanted = request(preview)
            handle = service._handle(agent.agent_id)
            run, _ = await runtime.repository.begin_run(
                handle.topic_id,
                ChatRequest(text="合成GC call持久来源", idempotency_key="gc-origin"),
                agent_session_id=agent.session_id,
                agent_id=agent.agent_id,
                agent_owner_id=service.manager.owner_id,
            )
            source = CheckpointModelSource(run.id, "original-current-gc-call")
            scope = "session:" + agent.session_id
            view, _ = await store.reserve(
                scope, binding, wanted, qualify=lambda: None, model_source=source
            )
            fence = await store.execution(scope, view.operation_id)
            origin = await store.inputs(scope, fence, binding, qualify=lambda: None)
            core = WorkspaceEmptyContainerRetention(binding.workspace)
            async with database.sessions() as sql:
                row = await sql.get(Row, view.operation_id)
                model_metadata = row.model_sources_metadata
            metadata = {
                "agent_id": agent.agent_id,
                "operation_id": view.operation_id,
                "request": wanted.model_dump(mode="json"),
                "source": preview.source.model_dump(mode="json"),
                "original": original.model_dump(mode="json"),
                "source_plan_id": original.plan_id,
                "run_id": run.id,
                "call_id": source.call_id,
                "model_metadata": model_metadata,
                "inode": (files / "tree").stat().st_ino,
            }
            save(root / "original.json", metadata)

            def pause():
                print(
                    json.dumps(
                        {"pid": os.getpid(), "point": point, "package": str(krim_agent.__file__)}
                    ),
                    flush=True,
                )
                sys.stdin.readline()
                raise AssertionError("原模型来源Store进程应真正SIGKILL")

            if point == "admitted":
                pause()
            actual_rmdir = physical.os.rmdir

            def namespace(name, *, dir_fd):
                actual_rmdir(name, dir_fd=dir_fd)
                if point == "namespace":
                    pause()

            physical.os.rmdir = namespace
            context = (store, scope, binding, origin, original, core, wanted, view, fence)
            effect = await remove(context)
            assert effect.status == "removed"
            final = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
            assert final.status == "done"
            if point == "receipt":
                pause()
            raise AssertionError("原强杀暂停点未到达")

        metadata = json.loads((root / "original.json").read_text())
        service = AgentService(runtime, runtime.settings)
        service.register_workspace("files", files, version="1", writable=True, default=True)
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_container_retentions()
        store = service.container_retention_store
        wanted = ContainerRetentionRequest.model_validate(metadata["request"])
        agent_id, identifier = metadata["agent_id"], metadata["operation_id"]
        scope = "session:" + agent_id
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("当前Model来源冷读不能重新生产旧Run/Reader/Writer/rmdir")

        runtime.provider.stream = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
        WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
        WorkspaceEmptyContainerRetention.remove = forbidden
        physical.os.rmdir = forbidden
        missing.rename_at = forbidden
        cold = await service.workspace_container_retention_by_key(agent_id, wanted.request_key)
        assert await service.workspace_start_container_retention(agent_id, wanted) == cold
        async with database.sessions() as sql:
            row = await sql.get(Row, identifier)
            actor = await sql.get(AgentHandleRow, agent_id)
            expiry = max(row.lease_expires_at or 0, actor.lease_expires_at or 0)
            assert row.model_sources_metadata == metadata["model_metadata"]
        remaining = expiry + 0.02 - time.time()
        if remaining > 0:
            await asyncio.sleep(remaining)
        assert time.time() > expiry
        # 与产品启动相同：有效Actor已自然到期，先收束旧Run及其interrupted end。
        await service.assert_inventory_available()
        await runtime.repository.recover_interrupted()
        assert await service.recover_workspace_edits() == (0 if point == "receipt" else 1)
        await service.restore(agent_id)
        current = await service.workspace_container_retention(agent_id, identifier)
        if point == "namespace":
            assert current.status == "unknown"
            # 新进程当前Host显式只读核实，不能替换原Model remove来源。
            await service.workspace_verify_container_retention(
                agent_id,
                identifier,
                expected_revision=current.revision,
                request_key="cold-host-verify",
            )
            await drain(service)
        final = await service.workspace_container_retention(agent_id, identifier)
        assert final.status == ("interrupted" if point == "admitted" else "done")
        if point != "admitted":
            assert final.receipt.outcome == (
                "absence_verified" if point == "namespace" else "removed"
            )
        origins = await store.model_sources(scope, identifier)
        assert origins.remove == CheckpointModelSource(metadata["run_id"], metadata["call_id"])
        assert origins.verify is None
        async with database.sessions() as sql:
            row = await sql.get(Row, identifier)
            assert row.model_sources_metadata == metadata["model_metadata"]
        assert final.source.model_dump(mode="json") == metadata["source"]
        assert (
            await service.workspace_missing_directory_restore(agent_id, metadata["source_plan_id"])
        ).model_dump(mode="json") == metadata["original"]
        assert (files / "tree").stat().st_ino == metadata["inode"]
        assert (files / "tree/nested/nul").read_bytes() == b"\0\xff" and not replays
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "operation_id": identifier,
                    "status": final.status,
                    "original_model_source": {
                        "run_id": origins.remove.run_id,
                        "call_id": origins.remove.call_id,
                    },
                    "natural_actor_gc_expiry": True,
                    "production_replays": len(replays),
                    "package": str(krim_agent.__file__),
                    "scope": scope,
                }
            ),
            flush=True,
        )
    finally:
        if service is not None:
            await service.close()
        await runtime.close()
        await database.close()


asyncio.run(main())
