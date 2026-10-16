"""专用缺失账本真实SIGKILL；恢复只读产品SQL材料，辅助JSON仅识别原操作。"""

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
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    WorkspaceDirectoryCheckpointRow,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreAttemptRow as Attempt,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreLockRow as Lock,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestorePlanRow as Plan,
)
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_missing_directory_restores import (
    WorkspaceMissingDirectoryRestoreStore,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreFence
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_checkpoint_capture import make_tree  # noqa: E402
from test_directory_restore_plans import target  # noqa: E402
from test_missing_directory_restore_store import apply, prepared, preview  # noqa: E402


async def main():
    phase, point, directory = sys.argv[1:4]
    root = Path(directory)
    files = root / "files"
    files.mkdir(exist_ok=True)
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    await database.initialize()
    try:
        with (
            Workspace(files, writable=True) as workspace,
            WorkspaceDirectoryAbsenceReader(workspace) as reader,
        ):
            binding = WorkspaceBinding("files", "1", workspace, "a" * 64)
            store = WorkspaceMissingDirectoryRestoreStore(
                WorkspaceDirectoryCaptureStore(
                    WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(database))
                ),
                lease_seconds=5,
            )
            metadata = {}

            def hold():
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
                raise AssertionError("须由父进程实际SIGKILL")

            async def identify(scope, fence):
                async with database.sessions() as sql:
                    plan = await sql.get(Plan, fence.plan_id)
                    attempt = await sql.get(Attempt, fence.operation_id)
                    metadata.update(
                        scope=scope,
                        owner_id=store.owner_id,
                        fence=asdict(fence),
                        plan_id=plan.plan_id,
                        operation_id=fence.operation_id,
                        target_id=plan.target_checkpoint_id,
                        before=plan.absence_fingerprint,
                        stage_token=plan.stage_token,
                        target=plan.target_fingerprint,
                        lease=(attempt or plan).lease_expires_at,
                    )

            if phase == "produce":
                make_tree(files)
                topic = await KrimRepository(database).create_topic("缺失恢复强杀")
                scope = "topic:" + topic.id
                snapshot = WorkspaceTreeSnapshotReader(workspace).capture("tree")
                original = (
                    store.captures,
                    scope,
                    binding,
                    snapshot,
                    DirectoryCheckpointMaterialCreate(root_path="tree", request_key="original"),
                )
                identifier = await target(original)
                (files / "tree").rename(files / "retained-original")
                context = (store, scope, binding, snapshot, identifier, reader)
                planned, _ = await store.reserve(
                    scope, binding, preview(identifier), qualify=lambda: None
                )
                fence = await store.preparation(scope, planned.plan_id)
                before = reader.capture("tree")
                await store.record_absence(
                    scope, fence, binding, before, observation_reader=reader, qualify=lambda: None
                )
                if point == "absence":
                    await identify(scope, fence)
                    hold()
                planned = await store.ready(scope, fence, binding, qualify=lambda: None)
                if point == "ready":
                    await identify(scope, fence)
                    hold()
                if point == "stage":
                    record_stage = store.record_stage

                    async def staged(scope, fence, *args, **kwargs):
                        await record_stage(scope, fence, *args, **kwargs)
                        await identify(scope, fence)
                        hold()

                    store.record_stage = staged
                operation, fence, writer, change, authorize = await prepared(context, planned)
                metadata["staged_inode"] = change.staged.entries[0].directory.inode
                metadata["staged_mode"] = change.staged.entries[0].directory.mode
                await identify(scope, fence)
                if point == "intent":
                    await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
                    hold()
                if point == "namespace":
                    rename = physical.rename_at

                    def renamed(*args, **kwargs):
                        rename(*args, **kwargs)
                        hold()

                    physical.rename_at = renamed
                effect = await apply(context, fence, writer, change, authorize)
                assert effect.receipt.status == "done"
                assert point == "settled"
                await store.settle(scope, fence, binding, effect, qualify=lambda: None)
                await identify(scope, fence)
                hold()

            metadata = json.loads((root / "metadata.json").read_text())
            scope = metadata["scope"]
            old_fence = MissingDirectoryRestoreFence(**metadata["fence"])
            replays = []

            def forbidden(*_args, **_kwargs):
                replays.append(True)
                raise AssertionError("冷恢复不得重放Model/Reader/Writer/namespace")

            WorkspaceTreeSnapshotReader.capture = forbidden
            WorkspaceTreeSnapshotReader.verify = forbidden
            WorkspaceDirectoryAbsenceReader.capture = forbidden
            WorkspaceDirectoryAbsenceReader.verify = forbidden
            WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
            WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
            physical.rename_at = forbidden
            FakeProvider.stream = forbidden
            lease = metadata["lease"]
            if lease is not None:
                await asyncio.sleep(max(0, lease - time.time()) + 0.02)
            assert await store.recover_interrupted() == int(point != "settled")
            state = await store.get(scope, metadata["plan_id"])
            assert state.operation_id == metadata["operation_id"]
            expected = (
                "finished"
                if point == "settled"
                else "uncertain"
                if point in {"intent", "namespace"}
                else "interrupted"
            )
            assert state.phase == expected and state.baseline.fingerprint == metadata["before"]
            assert await store.by_key(scope, "missing-preview") == state
            assert await store.reserve(
                scope, binding, preview(metadata["target_id"]), qualify=forbidden
            ) == (state, False)
            assert await store.recover_interrupted() == 0
            try:
                await store.renew(scope, old_fence, binding, qualify=lambda: None)
            except ServiceError:
                pass
            else:
                raise AssertionError("新Owner不能借原token续租")
            if expected == "uncertain":
                _, verify = await store.claim_verify(
                    scope, state.operation_id, binding, qualify=lambda: None
                )
                actual = await store.verification_input(
                    scope, verify, binding, qualify=lambda: None
                )
                effect = await asyncio.to_thread(
                    WorkspaceMissingDirectoryRestoreWriter(workspace, reader).reconcile, actual
                )
                assert effect.receipt.status == "uncertain"
                assert (
                    effect.receipt.error_code == "workspace_missing_directory_restore_unverifiable"
                )
                state = await store.settle(scope, verify, binding, effect, qualify=lambda: None)
                assert state.phase == "uncertain" and state.operation_id == metadata["operation_id"]
            async with database.sessions() as sql:
                assert await sql.scalar(select(func.count()).select_from(Plan)) == 1
                assert (
                    await sql.scalar(
                        select(func.count()).select_from(WorkspaceDirectoryCheckpointRow)
                    )
                    == 1
                )
                assert bool(await sql.scalar(select(func.count()).select_from(Lock))) == (
                    expected == "uncertain"
                )
            current = files / "tree"
            if point in {"namespace", "settled"}:
                assert current.stat().st_ino == metadata["staged_inode"]
                assert current.stat().st_mode & 0o7777 == metadata["staged_mode"]
                assert (current / "nested/nul").read_bytes() == b"\0\xff"
            else:
                assert not current.exists()
            assert (files / "retained-original/nested/nul").read_bytes() == b"\0\xff"
            assert not replays and database.engine.sync_engine.pool.checkedout() == 0
            print(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "point": point,
                        "plan_id": state.plan_id,
                        "operation_id": state.operation_id,
                        "phase": state.phase,
                        "replays": len(replays),
                        "natural_lease_expiry": lease is not None and time.time() > lease,
                        "lease_not_applicable": lease is None,
                        "binary_hex": (current / "nested/nul").read_bytes().hex()
                        if current.exists()
                        else None,
                        "package": str(krim_agent.__file__),
                    }
                )
            )
    finally:
        await database.close()


asyncio.run(main())
