"""真实原Missing SQL完成后，回收SQL/命名空间前后SIGKILL，冷原键只读核实。"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import krim_agent
import krim_agent.workspace_directory_restore_writer as full
import krim_agent.workspace_empty_container_retention as physical
import krim_agent.workspace_missing_directory_writer as missing_writer
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.persistence.database import WorkspaceMissingDirectoryRestorePlanRow as Plan
from krim_agent.persistence.workspace_container_retentions import WorkspaceContainerRetentionStore
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_missing_directory_restores import (
    WorkspaceMissingDirectoryRestoreStore,
)
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_container_retention import ContainerRetentionRequest
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_checkpoint_capture import make_tree  # noqa: E402
from test_directory_restore_execution import bridge  # noqa: E402
from test_directory_restore_plans import target  # noqa: E402
from test_missing_directory_restore_store import apply, prepared, ready  # noqa: E402


def save(path, value):
    with path.open("w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())


async def main():
    phase, point, folder = sys.argv[1:4]
    root = Path(folder)
    files = root / "files"
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    repository = KrimRepository(database)
    await database.initialize()
    if phase == "produce":
        files.mkdir()
        make_tree(files)
    workspace = Workspace(files, writable=True)
    binding = WorkspaceBinding("files", "1", workspace, "a" * 64)
    captures = WorkspaceDirectoryCaptureStore(
        WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(database))
    )
    restores = WorkspaceMissingDirectoryRestoreStore(captures)
    store = WorkspaceContainerRetentionStore(restores, lease_seconds=1)
    core = WorkspaceEmptyContainerRetention(workspace)
    try:
        if phase == "produce":
            topic = await repository.create_topic("原Host恢复及回收强杀")
            scope = "topic:" + topic.id
            snapshot = WorkspaceTreeSnapshotReader(workspace).capture("tree")
            request = DirectoryCheckpointMaterialCreate(root_path="tree", request_key="target")
            identifier = await target((captures, scope, binding, snapshot, request))
            (files / "tree").rename(files / "retained-original")
            with WorkspaceDirectoryAbsenceReader(workspace) as reader:
                context = (restores, scope, binding, snapshot, identifier, reader)
                planned, _before = await ready(context)
                _operation, fence, writer, change, authorize = await prepared(context, planned)
                effect = await apply(context, fence, writer, change, authorize)
                original = await restores.settle(
                    scope, fence, binding, effect, qualify=lambda: None
                )
            assert original.phase == "finished"
            async with database.sessions() as sql:
                source = await store.sources.read(
                    sql,
                    scope,
                    planned.plan_id,
                    workspace_key=binding.key,
                    resource_fingerprint=binding.fingerprint,
                    workspace_fingerprint=workspace.fingerprint,
                )
            observed = core.inspect(source.stage, authorize=lambda: None)
            request = ContainerRetentionRequest(
                source_plan_id=planned.plan_id,
                expected_source_hash=source.view.source_hash,
                expected_container_fingerprint=observed.fingerprint,
                request_key="original-retention",
            )
            view, _ = await store.reserve(scope, binding, request, qualify=lambda: None)
            fence = await store.execution(scope, view.operation_id)
            metadata = {
                "scope": scope,
                "source_plan_id": planned.plan_id,
                "operation_id": view.operation_id,
                "request_key": request.request_key,
                "request": request.model_dump(mode="json"),
                "original": original.model_dump(mode="json"),
                "source_hash": source.view.source_hash,
                "stage": source.metadata["stage_metadata"],
                "inode": (files / "tree").stat().st_ino,
                "producer": os.getpid(),
                "point": point,
            }
            save(root / "original.json", metadata)

            def pause():
                print(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "point": point,
                            "operation_id": view.operation_id,
                            "package": str(krim_agent.__file__),
                        }
                    ),
                    flush=True,
                )
                sys.stdin.readline()
                raise AssertionError("本进程必须被真正SIGKILL")

            if point == "admitted":
                pause()

            async def record(value):
                if point == "before_intent":
                    pause()
                await store.record_intent(scope, fence, binding, value, qualify=lambda: None)
                if point == "intent":
                    pause()

            loop = asyncio.get_running_loop()
            authorize = bridge(
                loop, lambda: store.assert_execution(scope, fence, binding, qualify=lambda: None)
            )
            intent = bridge(loop, record)
            rmdir = os.rmdir

            def namespace(name, *, dir_fd):
                rmdir(name, dir_fd=dir_fd)
                if point == "namespace":
                    pause()

            physical.os.rmdir = namespace
            result = await asyncio.to_thread(
                core.remove,
                source.stage,
                expected_fingerprint=observed.fingerprint,
                confirmed=True,
                authorize=authorize,
                record_intent=intent,
            )
            assert result.status == "removed"
            final = await store.settle(scope, fence, binding, result, qualify=lambda: None)
            assert final.status == "done"
            if point == "receipt":
                pause()
            raise AssertionError("强杀暂停点未到达")
        metadata = json.loads((root / "original.json").read_text())
        scope = metadata["scope"]
        if phase == "join":
            request = ContainerRetentionRequest.model_validate(metadata["request"])

            def no_readmission():
                raise AssertionError("跨进程同键只能读回原操作")

            view, created = await store.reserve(scope, binding, request, qualify=no_readmission)
            assert not created and view.operation_id == metadata["operation_id"]
            print(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "producer": metadata["producer"],
                        "operation_id": view.operation_id,
                        "created": created,
                        "status": view.status,
                        "package": str(krim_agent.__file__),
                    }
                ),
                flush=True,
            )
            return
        async with database.sessions() as sql:
            row = await sql.get(Row, metadata["operation_id"])
            deadline = row.lease_expires_at
        if deadline is not None:
            await asyncio.sleep(max(0, deadline - time.time()) + 0.05)
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("冷回收只读，不重放rmdir/Reader/prepare/apply/rename")

        physical.os.rmdir = forbidden
        full.rename_at = forbidden
        missing_writer.rename_at = forbidden
        full.WorkspaceDirectoryRestoreWriter.prepare = forbidden
        full.WorkspaceDirectoryRestoreWriter.apply = forbidden
        missing_writer.WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
        missing_writer.WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        recovered = await store.recover()
        current = await store.by_key(scope, metadata["request_key"])
        async with database.sessions() as sql:
            source = await store.sources.read(
                sql,
                scope,
                metadata["source_plan_id"],
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=workspace.fingerprint,
            )
        assert source.view.source_hash == metadata["source_hash"]
        assert source.metadata["stage_metadata"] == metadata["stage"]
        if current.status == "unknown":
            _view, fence = await store.claim_verify(
                scope,
                current.operation_id,
                binding,
                expected_revision=current.revision,
                request_key="explicit-cold-inspection",
                qualify=lambda: None,
            )
            assert fence is not None
            authorize = bridge(
                asyncio.get_running_loop(),
                lambda: store.assert_execution(scope, fence, binding, qualify=lambda: None),
            )
            observed = await asyncio.to_thread(core.inspect, source.stage, authorize=authorize)
            final = await store.finish_verify(scope, fence, binding, observed, qualify=lambda: None)
        else:
            final = current
        original = await restores.get(scope, metadata["source_plan_id"])
        assert original.model_dump(mode="json") == metadata["original"]
        assert (files / "tree").stat().st_ino == metadata["inode"]
        assert (files / "tree/nested/nul").read_bytes() == b"\0\xff"
        assert await store.by_key(scope, metadata["request_key"]) == final
        async with database.sessions() as sql:
            original_row = await sql.get(Plan, metadata["source_plan_id"])
            entries = original_row.staging_reserved_entries
        assert (
            final.status
            == {
                "before_intent": "interrupted",
                "intent": "rejected",
                "namespace": "done",
                "receipt": "done",
            }[point]
        )
        assert entries == (0 if point in {"namespace", "receipt"} else 1)
        assert not replays
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "producer": metadata["producer"],
                    "point": point,
                    "operation_id": final.operation_id,
                    "recovered": recovered,
                    "status": final.status,
                    "outcome": final.receipt.outcome if final.receipt else None,
                    "inode": metadata["inode"],
                    "binary_hex": "00ff",
                    "original_source_same": True,
                    "original_finished_same": True,
                    "reserved_entries": entries,
                    "replays": 0,
                    "natural_retention_lease_expiry": deadline is not None,
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )
    finally:
        workspace.close()
        await database.close()


asyncio.run(main())
