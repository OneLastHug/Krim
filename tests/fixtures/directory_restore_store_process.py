"""真实专用SQL Store五个强杀窗口；冷进程不借合成JSON充当产品材料。"""

import asyncio
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

from sqlalchemy import func, select

import krim_agent
import krim_agent.workspace_directory_restore_writer as physical
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    WorkspaceDirectoryCheckpointRow,
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestoreLockRow,
    WorkspaceDirectoryRestorePlanRow,
)
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_directory_restores import WorkspaceDirectoryRestoreStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_directory_restore import DirectoryRestoreFence
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_checkpoint_capture import make_tree  # noqa: E402
from test_directory_restore_execution import apply, prepared  # noqa: E402
from test_directory_restore_plans import request, target  # noqa: E402


async def main():
    phase, point, directory = sys.argv[1:4]
    root = Path(directory)
    files = root / "files"
    files.mkdir(exist_ok=True)
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    await database.initialize()
    with Workspace(files, writable=True) as workspace:
        binding = WorkspaceBinding("files", "1", workspace, "a" * 64)
        edits = WorkspaceEditStore(database)
        store = WorkspaceDirectoryRestoreStore(
            WorkspaceDirectoryCaptureStore(WorkspaceDirectoryCheckpointStore(edits)),
            lease_seconds=5,
        )
        metadata = {}

        def hold():
            # 仅存父进程识别点和旧Fence；真实BLOB/准备/意图全部读取产品SQL表。
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
            raise AssertionError("应由宿主真实SIGKILL")

        async def identify(scope, fence):
            async with database.sessions() as sql:
                plan = await sql.get(WorkspaceDirectoryRestorePlanRow, fence.plan_id)
                attempt = await sql.get(WorkspaceDirectoryRestoreAttemptRow, fence.operation_id)
                metadata.update(
                    scope=scope,
                    owner_id=store.owner_id,
                    fence=asdict(fence),
                    plan_id=plan.plan_id,
                    operation_id=fence.operation_id,
                    target_id=plan.target_checkpoint_id,
                    before=plan.baseline_fingerprint,
                    stage_token=plan.stage_token,
                    target=plan.target_fingerprint,
                    lease=(attempt or plan).lease_expires_at,
                )

        if phase == "produce":
            make_tree(files)
            topic = await KrimRepository(database).create_topic("专用Store真实强杀")
            scope = "topic:" + topic.id
            snapshot = WorkspaceTreeSnapshotReader(workspace).capture("tree")
            from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate

            context = (
                store.captures,
                scope,
                binding,
                snapshot,
                DirectoryCheckpointMaterialCreate(root_path="tree", request_key="original"),
            )
            identifier = await target(context)
            (files / "tree/nested/nul").write_bytes(b"current\x80\0")
            planned, _ = await store.reserve(
                scope, binding, request(identifier), qualify=lambda: None
            )
            fence = await store.preparation(scope, planned.plan_id)
            before = WorkspaceTreeSnapshotReader(workspace).capture("tree")
            await store.record_baseline(scope, fence, binding, before, qualify=lambda: None)
            metadata["before_inode"] = before.entries[0].directory.inode
            if point == "baseline":
                await identify(scope, fence)
                hold()
            planned = await store.ready(scope, fence, binding, qualify=lambda: None)
            if point == "stage":
                original_stage = store.record_stage

                async def record_stage(scope, fence, *args, **kwargs):
                    await original_stage(scope, fence, *args, **kwargs)
                    await identify(scope, fence)
                    hold()

                store.record_stage = record_stage
            op, fence, writer, change, authorize = await prepared(store, scope, binding, planned)
            metadata["staged_inode"] = change.staged.entries[0].directory.inode
            await identify(scope, fence)
            if point == "prepared":
                hold()
            if point == "intent":
                await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
                hold()
            assert point in {"namespace", "namespace_mutated"}
            rename = physical.rename_at

            def publish(*args, **kwargs):
                rename(*args, **kwargs)
                hold()

            physical.rename_at = publish
            await apply(store, scope, binding, fence, writer, change, authorize)
            raise AssertionError("原生产者不应越过强杀点")

        metadata = json.loads((root / "metadata.json").read_text())
        scope = metadata["scope"]
        old_fence = DirectoryRestoreFence(**metadata["fence"])
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("冷查询/核实不得重放Reader/Model/Writer/namespace")

        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceTreeSnapshotReader.verify = forbidden
        WorkspaceDirectoryRestoreWriter.prepare = forbidden
        WorkspaceDirectoryRestoreWriter.apply = forbidden
        WorkspaceDirectoryRestoreWriter._write_tree = forbidden
        physical.rename_at = forbidden
        FakeProvider.stream = forbidden
        # 使用真实原租约自然到期，不改SQL中的租约或提升产品期限。
        await asyncio.sleep(max(0, metadata["lease"] - time.time()) + 0.02)
        assert await store.recover_interrupted() == 1
        state = await store.get(scope, metadata["plan_id"])
        assert state.operation_id == metadata["operation_id"]
        assert state.phase == (
            "uncertain" if point.startswith("namespace") or point == "intent" else "interrupted"
        )
        assert state.backup.snapshot_fingerprint == metadata["before"]
        assert await store.by_key(scope, "preview") == state
        assert await store.reserve(
            scope, binding, request(metadata["target_id"]), qualify=forbidden
        ) == (state, False)
        assert await store.recover_interrupted() == 0
        try:
            await store.renew(scope, old_fence, binding, qualify=lambda: None)
        except ServiceError:
            pass
        else:
            raise AssertionError("新owner不能使用原owner/token")
        async with database.sessions() as sql:
            row = await sql.get(WorkspaceDirectoryRestorePlanRow, metadata["plan_id"])
            before = await store._baseline(sql, row)
            assert any(
                entry.file is not None and entry.file.data == b"current\x80\0"
                for entry in before.snapshot.entries
            )
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryCheckpointRow))
                == 2
            )
            assert (
                await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryRestorePlanRow))
                == 1
            )
        status = state.phase
        if state.phase == "uncertain":
            _, fence = await store.claim_verify(
                scope, state.operation_id, binding, qualify=lambda: None
            )
            assert fence.operation_id == old_fence.operation_id and fence.token != old_fence.token
            actual = await store.verification_input(scope, fence, binding, qualify=lambda: None)
            effect = await asyncio.to_thread(
                WorkspaceDirectoryRestoreWriter(workspace).reconcile, actual
            )
            expected = {
                "intent": "rejected",
                "namespace": "done",
                "namespace_mutated": "uncertain",
            }[point]
            assert effect.receipt.status == expected
            result = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
            status = result.status
            if point == "namespace":
                assert effect.observed_after.entries[0].directory.inode == metadata["staged_inode"]
                assert effect.retained_before.entries[0].directory.inode == metadata["before_inode"]
        async with database.sessions() as sql:
            locks = await sql.scalar(
                select(func.count()).select_from(WorkspaceDirectoryRestoreLockRow)
            )
        assert bool(locks) == (point == "namespace_mutated")
        assert not replays and database.engine.sync_engine.pool.checkedout() == 0
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "point": point,
                    "status": status,
                    "plan_id": state.plan_id,
                    "operation_id": state.operation_id,
                    "replays": len(replays),
                    "package": str(krim_agent.__file__),
                    "binary_hex": (files / "tree/nested/nul").read_bytes().hex(),
                    "natural_lease_expiry": True,
                }
            ),
            flush=True,
        )
    await database.close()


asyncio.run(main())
