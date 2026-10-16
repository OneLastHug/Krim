"""缺失来源回退的真实SIGKILL窗口；原SQL材料在新进程只读核实。"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import krim_agent
import krim_agent.workspace_tree_writer as physical
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import WorkspaceTreeMoveAttemptRow
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_missing_directory_restores import (
    WorkspaceMissingDirectoryRestoreStore,
)
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_missing_directory_rollback import MissingDirectoryRollbackSource
from krim_agent.workspace_tree_move import TreeMovePreviewRequest
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_checkpoint_capture import make_tree  # noqa: E402
from test_directory_restore_plans import target  # noqa: E402
from test_missing_directory_restore_store import apply, prepared, ready  # noqa: E402
from test_workspace_tree_move_ledger import action, publish  # noqa: E402


async def main():
    phase, point, folder = sys.argv[1:4]
    root = Path(folder)
    files = root / "files"
    files.mkdir(exist_ok=True)
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    await database.initialize()
    try:
        with Workspace(files, writable=True) as workspace:
            binding = WorkspaceBinding("files", "1", workspace, "a" * 64)
            edits = WorkspaceEditStore(database)
            moves = WorkspaceTreeMoveStore(edits, lease_seconds=5)
            writer = WorkspaceTreeWriter(workspace)
            if phase == "produce":
                make_tree(files)
                scope = "topic:" + (await KrimRepository(database).create_topic("回退强杀")).id
                captures = WorkspaceDirectoryCaptureStore(WorkspaceDirectoryCheckpointStore(edits))
                snapshot = WorkspaceTreeSnapshotReader(workspace).capture("tree")
                identifier = await target(
                    (
                        captures,
                        scope,
                        binding,
                        snapshot,
                        DirectoryCheckpointMaterialCreate(root_path="tree", request_key="origin"),
                    )
                )
                (files / "tree").rename(files / "original-retained")
                restores = WorkspaceMissingDirectoryRestoreStore(captures)
                with WorkspaceDirectoryAbsenceReader(workspace) as observer:
                    context = (restores, scope, binding, snapshot, identifier, observer)
                    planned, _ = await ready(context)
                    _, fence, missing_writer, change, authorize = await prepared(context, planned)
                    effect = await apply(context, fence, missing_writer, change, authorize)
                    assert effect.receipt.status == "done"
                    finished = await restores.settle(
                        scope, fence, binding, effect, qualify=lambda: None
                    )
                (files / "tree/user-extra").write_bytes(b"user\0after\xff")
                current = writer.reader.capture("tree")
                change = writer.prepare(
                    "tree", "preserved", expected_fingerprint=current.fingerprint
                )
                request = TreeMovePreviewRequest(
                    source_path="tree",
                    target_path="preserved",
                    expected_fingerprint=current.fingerprint,
                    request_key="rollback-preview",
                )
                proposal, _ = await moves.preview(
                    scope,
                    binding,
                    request,
                    change,
                    missing_origin=MissingDirectoryRollbackSource(finished.plan_id),
                )
                attempt, _ = await moves.reserve(
                    scope, action(proposal.view), binding.fingerprint, change
                )
                if point == "intent":
                    await moves.record_intent(scope, attempt.view.operation_id, attempt.prepared)
                else:
                    result = await publish(moves, writer, scope, attempt)
                    assert result.receipt.status == "done"
                    if point == "settled":
                        await moves.settle(scope, attempt.view.operation_id, result)
                async with database.sessions() as sql:
                    row = await sql.get(WorkspaceTreeMoveAttemptRow, attempt.view.operation_id)
                    lease = row.lease_expires_at
                metadata = {
                    "scope": scope,
                    "proposal_id": proposal.view.proposal_id,
                    "operation_id": attempt.view.operation_id,
                    "source_plan_id": finished.plan_id,
                    "source_operation_id": finished.operation_id,
                    "owner_id": moves.owner_id,
                    "before": current.fingerprint,
                    "lease": lease,
                    "request": request.model_dump(mode="json"),
                }
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
                await asyncio.to_thread(sys.stdin.readline)
                raise AssertionError("必须由父进程真实SIGKILL")
            metadata = json.loads((root / "metadata.json").read_text())
            if metadata["lease"] is not None:
                await asyncio.sleep(max(0, metadata["lease"] - time.time()) + 0.05)
            replays = 0

            def forbidden(*_args, **_kwargs):
                nonlocal replays
                replays += 1
                raise AssertionError("冷查询/原键/核实禁止重发副作用")

            physical.rename_at = forbidden
            WorkspaceTreeWriter.apply = forbidden
            WorkspaceTreeWriter.prepare = forbidden
            scope, proposal_id, operation_id = (
                metadata[k] for k in ("scope", "proposal_id", "operation_id")
            )
            operation = await moves.operation(scope, operation_id)
            assert (
                operation.intent_recorded
                and operation.prepared.before.fingerprint == metadata["before"]
            )
            history = await moves.missing_rollback_proposal(scope, proposal_id)
            assert history.source_plan_id == metadata["source_plan_id"]
            assert history.source_operation_id == metadata["source_operation_id"]
            stored = await moves.preview(
                scope,
                binding,
                TreeMovePreviewRequest.model_validate(metadata["request"]),
                None,
                missing_origin=MissingDirectoryRollbackSource(metadata["source_plan_id"]),
            )
            assert stored[0].view.proposal_id == proposal_id and not stored[1]
            stale = WorkspaceTreeMoveStore(edits)
            stale.owner_id = metadata["owner_id"]
            try:
                await stale.assert_execution(scope, operation_id)
            except ServiceError:
                pass
            else:
                raise AssertionError("过期原Owner不得继续")
            if point != "settled":
                assert operation.view.status == "uncertain"
                actual = writer.reconcile(operation.prepared)
                result = await moves.settle(scope, operation_id, actual, resolving=True)
                assert result.view.status == ("rejected" if point == "intent" else "done")
            else:
                assert operation.view.status == "done"
            final = await moves.missing_rollback_proposal(scope, proposal_id)
            assert final.rollback_state == (
                "awaiting_approval" if point == "intent" else "preserved"
            )
            assert replays == 0
            print(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "replays": replays,
                        "source_plan_id": final.source_plan_id,
                        "operation_id": operation_id,
                        "state": final.rollback_state,
                        "natural_lease_expiry": point != "settled",
                        "package": str(krim_agent.__file__),
                    }
                ),
                flush=True,
            )
    finally:
        await database.close()


asyncio.run(main())
