"""缺失来源沿原Move参与共享namespace互斥，分页不遗漏损坏事实。"""

import pytest
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_missing_baseline_rollback_store import preview
from test_missing_baseline_rollback_store import rollback_origin as rollback_origin
from test_missing_directory_restore_store import missing as missing
from test_workspace_tree_move_ledger import action

from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    WorkspaceDirectoryWriter,
)
from krim_agent.workspace_tree_move import TreeMovePreviewRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter
from krim_agent.workspace_writer import WorkspaceWriter


@pytest.mark.parametrize("family", ["file", "directory", "move"])
@pytest.mark.parametrize("first", ["rollback", "other"])
async def test_missing_rollback_and_old_namespace_actions_block_each_other(
    rollback_origin, family, first
):
    store, scope, binding, *_ = rollback_origin
    writer, _, change, proposal = await preview(rollback_origin)
    if family == "file":
        other = store.files
        prepared = WorkspaceWriter(binding.workspace).prepare(
            FileChangeSpec(
                path="tree/nested/user-new",
                operation="write",
                content="preserve",
                expected_sha256=None,
            )
        )
        view, _ = await other.preview(
            scope, binding.key, binding.fingerprint, "other-file", prepared
        )
        request = WorkspaceChangeAction(
            proposal_id=view.view.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="file",
        )
    elif family == "directory":
        other = WorkspaceDirectoryStore(store.database, owner_id=store.owner_id)
        prepared = WorkspaceDirectoryWriter(binding.workspace).prepare(
            DirectoryChangeSpec(
                path="tree/nested/user-new",
                operation="create",
                mode=0o750,
            )
        )
        view, _ = await other.preview(
            scope, binding.key, binding.fingerprint, "other-dir", prepared
        )
        request = DirectoryChangeAction(
            proposal_id=view.view.proposal_id,
            action="apply",
            expected_revision=0,
            request_key="dir",
        )
    else:
        other = store
        prepared = writer.prepare(
            "tree", "another-preserved", expected_fingerprint=change.before.fingerprint
        )
        view, _ = await other.preview(
            scope,
            binding,
            TreeMovePreviewRequest(
                source_path="tree",
                target_path="another-preserved",
                expected_fingerprint=change.before.fingerprint,
                request_key="other-move",
            ),
            prepared,
        )
        request = action(view.view, key="other-action")

    async def rollback():
        return await store.reserve(scope, action(proposal.view), binding.fingerprint, change)

    async def old():
        return await other.reserve(scope, request, binding.fingerprint, prepared)

    successful, blocked = (rollback, old) if first == "rollback" else (old, rollback)
    attempt, _ = await successful()
    with pytest.raises(ServiceError) as denied:
        await blocked()
    assert denied.value.code in {
        "workspace_file_busy",
        "workspace_directory_busy",
        "workspace_tree_move_busy",
    }
    if first == "rollback":
        await store.assert_execution(scope, attempt.view.operation_id)
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    assert not (binding.workspace.root / "rollback-preserved").exists()


async def test_pagination_checks_general_rows_and_resumes_after_empty_page(
    rollback_origin, monkeypatch
):
    import krim_agent.persistence.workspace_tree_moves as module

    store, scope, binding, *_ = rollback_origin
    values = iter(
        [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
    )
    monkeypatch.setattr(module, "uuid4", lambda: next(values))
    writer = WorkspaceTreeWriter(binding.workspace)
    current = writer.reader.capture("tree")
    change = writer.prepare("tree", "ordinary", expected_fingerprint=current.fingerprint)
    generic, _ = await store.preview(
        scope,
        binding,
        TreeMovePreviewRequest(
            source_path="tree",
            target_path="ordinary",
            expected_fingerprint=current.fingerprint,
            request_key="generic-first",
        ),
        change,
    )
    _, _, _, special = await preview(rollback_origin)
    page = await store.missing_rollback_list(scope, limit=1)
    assert page.items == [] and page.next_after == generic.view.proposal_id
    following = await store.missing_rollback_list(scope, limit=1, after=page.next_after)
    assert [item.move.proposal_id for item in following.items] == [special.view.proposal_id]
    assert following.next_after is None
    with pytest.raises(ServiceError):
        await store.missing_rollback_list("topic:foreign", after=page.next_after)
