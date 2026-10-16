"""新旧恢复与文件/目录/树的双向物理锁、跨家族暂存及材料回执总量。"""

import pytest
from sqlalchemy import text
from test_directory_checkpoint_capture import make_tree
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import execute_request
from test_directory_restore_plans import request
from test_missing_directory_restore_store import (
    execute,
    prepared,
    ready,
)
from test_missing_directory_restore_store import (
    missing as missing,
)

from krim_agent.persistence.database import WorkspaceMissingDirectoryRestorePlanRow
from krim_agent.persistence.directory_restore_capacity import material_usage, staging_usage
from krim_agent.persistence.directory_restore_codec import MAX_METADATA
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_directory_restores import WorkspaceDirectoryRestoreStore
from krim_agent.persistence.workspace_tree_exchanges import WorkspaceTreeExchangeStore
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    WorkspaceDirectoryWriter,
)
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_exchange_types import TreeExchangeAction, TreeExchangePreviewRequest
from krim_agent.workspace_tree_move import TreeMoveAction, TreeMovePreviewRequest
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter
from krim_agent.workspace_writer import WorkspaceWriter


async def captured(captures, scope, binding, root, key):
    snapshot = WorkspaceTreeSnapshotReader(binding.workspace).capture(root)
    view, _ = await captures.reserve(
        scope, binding, DirectoryCheckpointMaterialCreate(root_path=root, request_key=key)
    )
    fence = await captures.execution(scope, view.checkpoint_id)
    value = await captures.record(scope, fence, binding, snapshot, qualify=lambda: None)
    await captures.finish(scope, fence, binding, value, qualify=lambda: None)
    return view.checkpoint_id, snapshot


async def full_ready(captures, scope, binding, root="tree", key="full"):
    identifier, snapshot = await captured(captures, scope, binding, root, key)
    store = WorkspaceDirectoryRestoreStore(captures)
    view, _ = await store.reserve(scope, binding, request(identifier, key), qualify=lambda: None)
    fence = await store.preparation(scope, view.plan_id)
    await store.record_baseline(scope, fence, binding, snapshot, qualify=lambda: None)
    return store, await store.ready(scope, fence, binding, qualify=lambda: None)


@pytest.mark.parametrize("kind", ["file", "directory", "move", "exchange", "full"])
@pytest.mark.parametrize("first", ["missing", "neighbor"])
async def test_all_families_physical_alias_locks_in_both_admission_orders(missing, kind, first):
    store, scope, binding, *_ = missing
    if kind == "exchange":
        (binding.workspace.root / "other").mkdir()
        (binding.workspace.root / "other/raw").write_bytes(b"\0\xff")
    planned, _ = await ready(missing)
    with Workspace(binding.workspace.root, writable=True) as workspace:
        alias = WorkspaceBinding("alias", "2", workspace, "b" * 64)
        if kind == "file":
            neighbor = store.edits
            change = WorkspaceWriter(workspace).prepare(
                FileChangeSpec(
                    path="neighbor", operation="write", expected_sha256=None, content="new"
                )
            )
            proposal, _ = await neighbor.preview(
                scope, alias.key, alias.fingerprint, "neighbor", change
            )
            action = WorkspaceChangeAction(
                proposal_id=proposal.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="neighbor",
            )
        elif kind == "directory":
            neighbor = WorkspaceDirectoryStore(store.database)
            change = WorkspaceDirectoryWriter(workspace).prepare(
                DirectoryChangeSpec(path="neighbor", operation="create", mode=0o750)
            )
            proposal, _ = await neighbor.preview(
                scope, alias.key, alias.fingerprint, "neighbor", change
            )
            action = DirectoryChangeAction(
                proposal_id=proposal.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="neighbor",
            )
        elif kind == "move":
            writer = WorkspaceTreeWriter(workspace)
            before = writer.reader.capture("retained-original")
            change = writer.prepare(
                "retained-original", "moved", expected_fingerprint=before.fingerprint
            )
            neighbor = WorkspaceTreeMoveStore(store.edits)
            proposal, _ = await neighbor.preview(
                scope,
                alias,
                TreeMovePreviewRequest(
                    source_path="retained-original",
                    target_path="moved",
                    expected_fingerprint=before.fingerprint,
                    request_key="neighbor",
                ),
                change,
            )
            action = TreeMoveAction(
                proposal_id=proposal.view.proposal_id,
                action="apply",
                expected_revision=proposal.view.revision,
                request_key="neighbor",
            )
        elif kind == "exchange":
            writer = WorkspaceTreeExchangeWriter(workspace)
            before = writer.trees.reader.capture("retained-original")
            other = writer.trees.reader.capture("other")
            change = writer.prepare(
                "retained-original",
                "other",
                expected_left=before.fingerprint,
                expected_right=other.fingerprint,
            )
            neighbor = WorkspaceTreeExchangeStore(store.edits)
            proposal, _ = await neighbor.preview(
                scope,
                alias,
                TreeExchangePreviewRequest(
                    left_path="retained-original",
                    right_path="other",
                    expected_left=before.fingerprint,
                    expected_right=other.fingerprint,
                    request_key="neighbor",
                ),
                change,
            )
            action = TreeExchangeAction(
                proposal_id=proposal.view.proposal_id,
                action="apply",
                expected_revision=proposal.view.revision,
                request_key="neighbor",
            )
        else:
            neighbor, proposal = await full_ready(store.captures, scope, alias, "retained-original")

        async def admit_neighbor():
            if kind == "full":
                return await neighbor.start(
                    scope, alias, execute_request(proposal), qualify=lambda: None
                )
            return await neighbor.reserve(scope, action, alias.fingerprint, change)

        if first == "missing":
            await store.start(scope, binding, execute(planned), qualify=lambda: None)
            with pytest.raises(ServiceError) as denied:
                await admit_neighbor()
            assert denied.value.code == "workspace_missing_directory_restore_busy"
        else:
            await admit_neighbor()
            with pytest.raises(ServiceError) as denied:
                await store.start(scope, binding, execute(planned), qualify=lambda: None)
            assert (
                denied.value.code
                == {
                    "file": "workspace_file_busy",
                    "directory": "workspace_directory_busy",
                    "move": "workspace_tree_move_busy",
                    "exchange": "workspace_tree_exchange_busy",
                    "full": "workspace_directory_restore_busy",
                }[kind]
            )
        assert not (workspace.root / "tree").exists()
        assert not (workspace.root / "neighbor").exists()
        assert not (workspace.root / "moved").exists()


@pytest.mark.parametrize("first", ["missing", "full"])
@pytest.mark.parametrize("dimension", ["bytes", "entries"])
async def test_staging_is_shared_both_directions_and_cancel_releases_only_unallocated(
    missing, tmp_path, first, dimension
):
    store, scope, binding, *_ = missing
    other_path = tmp_path / "independent-workspace"
    other_path.mkdir()
    make_tree(other_path)
    with Workspace(other_path, writable=True) as workspace:
        other_binding = WorkspaceBinding("other", "1", workspace, "b" * 64)
        complete, full = await full_ready(store.captures, scope, other_binding)
        planned, _ = await ready(missing)
        left = (store, binding, planned, execute)
        right = (complete, other_binding, full, execute_request)
        if first == "full":
            left, right = right, left
        first_store, first_binding, first_plan, first_request = left
        second_store, second_binding, second_plan, second_request = right
        await first_store.start(
            scope, first_binding, first_request(first_plan), qualify=lambda: None
        )
        if dimension == "bytes":
            second_store.max_staging_bytes = (
                first_plan.target_snapshot.byte_size + second_plan.target_snapshot.byte_size - 1
            )
        else:
            second_store.max_staging_entries = (
                first_plan.target_snapshot.entry_count + second_plan.target_snapshot.entry_count + 1
            )
        with pytest.raises(ServiceError) as denied:
            await second_store.start(
                scope, second_binding, second_request(second_plan), qualify=lambda: None
            )
        assert denied.value.code.endswith("_capacity")
        async with store.database.sessions() as sql:
            assert await staging_usage(sql) == (
                first_plan.target_snapshot.byte_size,
                first_plan.target_snapshot.entry_count + 1,
            )
        assert (await second_store.get(scope, second_plan.plan_id)).phase == "ready"
        await first_store.cancel(scope, first_plan.plan_id)
        async with store.database.sessions() as sql:
            assert await staging_usage(sql) == (0, 0)
        assert (
            await second_store.start(
                scope, second_binding, second_request(second_plan), qualify=lambda: None
            )
        )[1]


async def test_shared_material_counts_single_blob_actual_json_and_reserved_unknown_receipt(missing):
    store, scope, binding, *_ = missing
    complete, full = await full_ready(store.captures, scope, binding, "retained-original")
    planned, _ = await ready(missing)
    _, _, _, _, _ = await prepared(missing, planned)
    async with store.database.sessions() as sql:
        total = await material_usage(sql, sqlite=True)
        row = await sql.get(WorkspaceMissingDirectoryRestorePlanRow, planned.plan_id)
        expected_missing = len(row.prepared_payload) + MAX_METADATA
        for name in ("absence_metadata", "target_summary", "stage_metadata", "prepared_metadata"):
            expected_missing += (
                await sql.execute(
                    text(
                        "SELECT length(CAST("
                        + name
                        + " AS BLOB)) FROM workspace_missing_directory_restore_plans "
                        "WHERE plan_id=:id"
                    ),
                    {"id": planned.plan_id},
                )
            ).scalar_one()
        expected_full = (
            await sql.execute(
                text(
                    "SELECT length(CAST(target_summary AS BLOB)) + "
                    "length(CAST(baseline_summary AS BLOB)) "
                    "FROM workspace_directory_restore_plans WHERE plan_id=:id"
                ),
                {"id": full.plan_id},
            )
        ).scalar_one()
        assert total == expected_missing + expected_full
    assert expected_missing < total - 1 and expected_full < total - 1
    # 两种调用方必须以同一合计拒绝，即使各自表单独未超限。
    store.max_snapshot_bytes = complete.max_snapshot_bytes = total - 1
    for selected in (store, complete):
        async with selected._transaction() as sql:
            with pytest.raises(ServiceError) as denied:
                await selected._material_capacity(sql)
            assert denied.value.code.endswith("_capacity")


async def test_original_full_baseline_target_works_even_when_original_target_blob_is_bad(missing):
    store, scope, binding, *_ = missing
    complete, full = await full_ready(store.captures, scope, binding, "retained-original")
    async with store.database.sessions() as sql, sql.begin():
        await sql.execute(
            text("UPDATE workspace_directory_checkpoints SET payload=:bad WHERE checkpoint_id=:id"),
            {
                "bad": b"bad-target",
                "id": (
                    await sql.execute(
                        text(
                            "SELECT target_material_id FROM workspace_directory_restore_plans "
                            "WHERE plan_id=:id"
                        ),
                        {"id": full.plan_id},
                    )
                ).scalar_one(),
            },
        )
    from test_missing_directory_restore_store import preview

    view, created = await store.reserve(
        scope,
        binding,
        preview(full.plan_id, "full-baseline", kind="baseline"),
        qualify=lambda: None,
    )
    assert created and view.target.kind == "baseline"
    assert view.root_path == "retained-original"
    assert view.target_snapshot == full.backup
