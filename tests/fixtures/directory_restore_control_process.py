"""原Host Registry备份/namespace后真正SIGKILL；另进程同SQL操作零重放。"""

import asyncio
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

import krim_agent
import krim_agent.workspace_directory_restore_writer as physical
from krim_agent.config import Settings
from krim_agent.persistence.database import WorkspaceDirectoryRestorePlanRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI
from krim_agent.workspace_directory_restore import DirectoryRestoreFence
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_restore_control import (  # noqa: E402
    checkpoint,
    drain,
    execute_request,
    preview_request,
    setup,
)


async def main():
    phase, point, folder = sys.argv[1:4]
    root = Path(folder)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        if phase == "produce":
            (root / "files").mkdir()
            service, agent, binding = await setup(ai.runtime, root / "files", persistent=True)
            service.directory_restore_store.lease_seconds = 5
            target = await checkpoint(agent)
            (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\x80\0")
            handle = service._handle(agent.agent_id)

            async def hold(definition, value, context):
                expected = DIRECTORY_RESTORE_STEP_TOOLS[
                    "prepare" if point == "backup" else "execute"
                ]
                if definition.name == expected:
                    fence = context.directory_restore
                    store = service.directory_restore_store
                    async with store.database.sessions() as sql:
                        plan = await sql.get(WorkspaceDirectoryRestorePlanRow, fence.plan_id)
                        before = (await store._baseline(sql, plan)).snapshot
                        metadata = {
                            "pid": os.getpid(),
                            "point": point,
                            "agent_id": agent.agent_id,
                            "scope": "session:" + agent.session_id,
                            "plan_id": plan.plan_id,
                            "operation_id": fence.operation_id,
                            "target_id": target.checkpoint_id,
                            "fence": asdict(fence),
                            "backup_fingerprint": before.fingerprint,
                            "before_inode": before.entries[0].directory.inode,
                            "lease": plan.lease_expires_at,
                        }
                        if point == "effect":
                            from krim_agent.persistence.database import (
                                WorkspaceDirectoryRestoreAttemptRow,
                            )

                            operation = await sql.get(
                                WorkspaceDirectoryRestoreAttemptRow, fence.operation_id
                            )
                            metadata["lease"] = operation.lease_expires_at
                            metadata["staged_inode"] = (
                                store._prepared(plan).staged.entries[0].directory.inode
                            )
                    (root / "metadata.json").write_text(json.dumps(metadata))
                    print(
                        json.dumps(
                            {
                                "pid": os.getpid(),
                                "point": point,
                                "package": str(krim_agent.__file__),
                            }
                        ),
                        flush=True,
                    )
                    # 人工强杀窗口在原后置Hook，产品材料/意图均由原真实工具写入SQL。
                    sys.stdin.readline()
                    raise AssertionError("应在原后置Hook被宿主SIGKILL")
                return value

            handle.runtime.tools.add_global_post(hold)
            planned = await service.workspace_preview_directory_restore(
                agent.agent_id, preview_request(target.checkpoint_id)
            )
            planned = await service.workspace_directory_restore_wait(
                agent.agent_id, planned.plan_id, timeout_seconds=10
            )
            if point == "effect":
                assert planned.phase == "ready"
                await drain(service)
                planned = await service.workspace_start_directory_restore(
                    agent.agent_id, execute_request(planned)
                )
                await service.workspace_directory_restore_wait(
                    agent.agent_id, planned.plan_id, timeout_seconds=10
                )
            raise AssertionError("原生产者必须在暂停点被强杀")

        metadata = json.loads((root / "metadata.json").read_text())
        service = ai.agents()
        binding = service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_directory_checkpoints()
        service.register_directory_restores()
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("原Host冷恢复不得重放Reader/Writer/Model/namespace")

        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceTreeSnapshotReader.verify = forbidden
        WorkspaceDirectoryRestoreWriter.prepare = forbidden
        WorkspaceDirectoryRestoreWriter.apply = forbidden
        WorkspaceDirectoryRestoreWriter._write_tree = forbidden
        physical.rename_at = forbidden
        FakeProvider.stream = forbidden
        # 原恢复租约自然到期；仅AgentHandle为死进程合成库做受控过期，见父测试。
        await asyncio.sleep(max(0, metadata["lease"] - time.time()) + 0.02)
        assert await service.restore_persistent() == 1, service.restore_errors
        view = await service.workspace_directory_restore(metadata["agent_id"], metadata["plan_id"])
        assert view.phase == ("interrupted" if point == "backup" else "uncertain")
        assert view.backup.snapshot_fingerprint == metadata["backup_fingerprint"]
        assert view.operation_id == metadata["operation_id"]
        assert await service.directory_restore_store.recover_interrupted() == 0
        try:
            await service.directory_restore_store.renew(
                metadata["scope"],
                DirectoryRestoreFence(**metadata["fence"]),
                binding,
                qualify=lambda: None,
            )
        except ServiceError:
            pass
        else:
            raise AssertionError("旧owner/token不得继续操作")
        if point == "effect":
            await service.workspace_verify_directory_restore(
                metadata["agent_id"], view.operation_id
            )
            await drain(service)
            view = await service.workspace_directory_restore(metadata["agent_id"], view.plan_id)
            assert view.phase == "finished" and view.operation_id == metadata["operation_id"]
            async with service.directory_restore_store.database.sessions() as sql:
                row = await sql.get(WorkspaceDirectoryRestorePlanRow, view.plan_id)
                original = service.directory_restore_store._prepared(row)
                from krim_agent.persistence.database import WorkspaceDirectoryRestoreAttemptRow
                from krim_agent.persistence.directory_restore_codec import decode_effect

                operation = await sql.get(WorkspaceDirectoryRestoreAttemptRow, view.operation_id)
                effect = decode_effect(original, operation.effect_metadata)
                assert effect.observed_after.entries[0].directory.inode == metadata["staged_inode"]
                assert effect.retained_before.entries[0].directory.inode == metadata["before_inode"]
        assert (
            await service.workspace_directory_restore_by_key(metadata["agent_id"], "preview")
            == view
        )
        assert (
            await service.workspace_preview_directory_restore(
                metadata["agent_id"], preview_request(metadata["target_id"])
            )
            == view
        )
        assert not replays
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "point": point,
                    "plan_id": view.plan_id,
                    "operation_id": view.operation_id,
                    "status": view.status,
                    "replays": len(replays),
                    "binary_hex": (binding.workspace.root / "tree/nested/nul").read_bytes().hex(),
                    "original_restore_lease_unchanged": True,
                    "synthetic_dead_handle_expired": True,
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )


asyncio.run(main())
