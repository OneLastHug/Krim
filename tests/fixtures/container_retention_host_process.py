"""真实Host Registry/SQL/rmdir强杀；自然租约冷恢复，原键不重放。"""

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
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_container_retention import ContainerRetentionRequest
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_container_retention_host import completed, drain, request, setup  # noqa: E402


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
            service.container_retention_store.lease_seconds = 1
            original = await completed(service, agent, binding)
            preview = await service.workspace_preview_container_retention(
                agent.agent_id, original.plan_id
            )
            wanted = request(preview)
            metadata = {
                "agent_id": agent.agent_id,
                "source_plan_id": original.plan_id,
                "request": wanted.model_dump(mode="json"),
                "source": preview.source.model_dump(mode="json"),
                "original": original.model_dump(mode="json"),
                "inode": (files / "tree").stat().st_ino,
                "producer": os.getpid(),
                "point": point,
            }

            def pause(identifier):
                metadata["operation_id"] = identifier
                save(root / "original.json", metadata)
                print(
                    json.dumps(
                        {"pid": os.getpid(), "point": point, "package": str(krim_agent.__file__)}
                    ),
                    flush=True,
                )
                sys.stdin.readline()
                raise AssertionError("原生产进程必须被真正SIGKILL")

            record = service.container_retention_store.record_intent

            async def intent(scope, fence, *args, **kwargs):
                if point == "before_intent":
                    pause(fence.operation_id)
                await record(scope, fence, *args, **kwargs)
                if point == "intent":
                    pause(fence.operation_id)

            service.container_retention_store.record_intent = intent
            settle = service.container_retention_store.settle

            async def receipt(scope, fence, *args, **kwargs):
                value = await settle(scope, fence, *args, **kwargs)
                if point == "receipt":
                    pause(fence.operation_id)
                return value

            service.container_retention_store.settle = receipt
            rmdir = physical.os.rmdir

            def namespace(name, *, dir_fd):
                rmdir(name, dir_fd=dir_fd)
                if point == "namespace":
                    pause(metadata["operation_id"])

            physical.os.rmdir = namespace
            started = await service.workspace_start_container_retention(agent.agent_id, wanted)
            metadata["operation_id"] = started.operation_id
            await drain(service)
            raise AssertionError("强杀暂停点没有到达")

        metadata = json.loads((root / "original.json").read_text())
        service = AgentService(runtime, runtime.settings)
        service.register_workspace("files", files, version="1", writable=True, default=True)
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_container_retentions()
        service.container_retention_store.lease_seconds = 1
        agent_id = metadata["agent_id"]
        identifier = metadata["operation_id"]
        wanted = ContainerRetentionRequest.model_validate(metadata["request"])
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("冷历史/核实禁止旧Provider/捕获/准备/发布/rmdir重放")

        runtime.provider.stream = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
        WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
        WorkspaceEmptyContainerRetention.remove = forbidden
        physical.os.rmdir = forbidden
        missing.rename_at = forbidden
        before = await service.workspace_container_retention_by_key(agent_id, wanted.request_key)
        assert before.operation_id == identifier
        assert await service.workspace_start_container_retention(agent_id, wanted) == before
        async with database.sessions() as sql:
            gc = await sql.get(Row, identifier)
            actor = await sql.get(AgentHandleRow, agent_id)
            expiry = max(gc.lease_expires_at or 0, actor.lease_expires_at or 0)
        remaining = expiry + 0.02 - time.time()
        if remaining > 0:
            await asyncio.sleep(remaining)
        assert time.time() > expiry
        assert await service.recover_workspace_edits() == (0 if point == "receipt" else 1)
        await service.restore(agent_id)
        cold = await service.workspace_container_retention(agent_id, identifier)
        if point == "before_intent":
            assert cold.status == "interrupted" and not cold.intent_recorded
        elif point != "receipt":
            assert cold.status == "unknown" and cold.intent_recorded
            await service.workspace_verify_container_retention(
                agent_id,
                identifier,
                expected_revision=cold.revision,
                request_key="explicit-after-restart",
            )
            await drain(service)
        final = await service.workspace_container_retention(agent_id, identifier)
        expected = {
            "before_intent": ("interrupted", None),
            "intent": ("rejected", "rejected"),
            "namespace": ("done", "absence_verified"),
            "receipt": ("done", "removed"),
        }[point]
        assert (final.status, final.receipt.outcome if final.receipt else None) == expected
        assert final.source.model_dump(mode="json") == metadata["source"]
        assert (
            await service.workspace_missing_directory_restore(agent_id, metadata["source_plan_id"])
        ).model_dump(mode="json") == metadata["original"]
        assert await service.workspace_start_container_retention(agent_id, wanted) == final
        assert (files / "tree").stat().st_ino == metadata["inode"]
        assert (files / "tree/nested/nul").read_bytes() == b"\0\xff" and not replays
        assert bool(list(files.glob("._krim-tx-*"))) is (point in {"before_intent", "intent"})
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "point": point,
                    "operation_id": identifier,
                    "status": final.status,
                    "outcome": expected[1],
                    "binary": "00ff",
                    "natural_actor_gc_lease_expiry": True,
                    "production_replays": len(replays),
                    "package": str(krim_agent.__file__),
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
