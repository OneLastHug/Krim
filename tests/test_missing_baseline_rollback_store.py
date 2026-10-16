"""原缺失恢复完成后的完整保留/反向恢复，冷历史复用同一Move账本。"""

import pytest
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_missing_directory_restore_store import apply, prepared, ready
from test_missing_directory_restore_store import missing as missing
from test_workspace_tree_move_ledger import action, publish

from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRollbackRow,
    WorkspaceTreeMoveProposalRow,
)
from krim_agent.persistence.workspace_edits import digest
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_missing_directory_rollback import MissingDirectoryRollbackSource
from krim_agent.workspace_tree_move import TreeMovePreviewRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


@pytest.fixture
async def rollback_origin(missing):
    restores, scope, binding, *_ = missing
    planned, _ = await ready(missing)
    operation, fence, writer, change, authorize = await prepared(missing, planned)
    effect = await apply(missing, fence, writer, change, authorize)
    assert effect.receipt.status == "done"
    finished = await restores.settle(scope, fence, binding, effect, qualify=lambda: None)
    yield WorkspaceTreeMoveStore(restores.edits), scope, binding, finished, effect


async def preview(context, *, key="rollback-preview", source=None, model_source=None):
    store, scope, binding, finished, _ = context
    writer = WorkspaceTreeWriter(binding.workspace)
    snapshot = writer.reader.capture("tree")
    change = writer.prepare("tree", "rollback-preserved", expected_fingerprint=snapshot.fingerprint)
    request = TreeMovePreviewRequest(
        source_path="tree",
        target_path="rollback-preserved",
        expected_fingerprint=snapshot.fingerprint,
        request_key=key,
    )
    proposal, created = await store.preview(
        scope,
        binding,
        request,
        change,
        missing_origin=source
        if source is not None
        else MissingDirectoryRollbackSource(finished.plan_id),
        model_source=model_source,
    )
    assert created
    return writer, request, change, proposal


async def test_real_preserve_current_changes_keep_and_reverse_use_original_move_facts(
    rollback_origin, monkeypatch
):
    store, scope, binding, original, effect = rollback_origin
    root = binding.workspace.root
    (root / "tree/nested/nul").write_bytes(b"user\0change\xff")
    (root / "tree/extra-user-file").write_bytes(b"preserve me")
    writer, request, change, proposal = await preview(rollback_origin)
    actual = await store.missing_rollback_proposal(scope, proposal.view.proposal_id)
    assert actual.source_plan_id == original.plan_id
    assert actual.source_operation_id == original.operation_id
    assert actual.baseline == original.baseline and actual.rollback_state == "awaiting_approval"
    assert actual.published_fingerprint == effect.observed_after.fingerprint
    assert actual.approved_fingerprint == change.before.fingerprint != actual.published_fingerprint
    attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
    result = await publish(store, writer, scope, attempt)
    assert result.receipt.status == "done"
    await store.settle(scope, attempt.view.operation_id, result)
    assert not (root / "tree").exists()
    assert (root / "rollback-preserved/nested/nul").read_bytes() == b"user\0change\xff"
    assert (root / "rollback-preserved/extra-user-file").read_bytes() == b"preserve me"
    preserved = await store.missing_rollback_proposal(scope, proposal.view.proposal_id)
    assert preserved.rollback_state == "preserved"
    current = await store.proposal(scope, proposal.view.proposal_id)
    keep, _ = await store.reserve(
        scope, action(current.view, "keep", "keep-rollback"), binding.fingerprint, change
    )
    kept = writer.verify_keep(change, result)
    await store.settle(scope, keep.view.operation_id, kept)
    current = await store.proposal(scope, proposal.view.proposal_id)
    reverse = writer.prepare_undo(change, result)
    undo, _ = await store.reserve(
        scope, action(current.view, "undo", "restore-tree"), binding.fingerprint, reverse
    )
    reversed_effect = await publish(store, writer, scope, undo)
    assert reversed_effect.receipt.status == "done"
    await store.settle(scope, undo.view.operation_id, reversed_effect)
    assert (root / "tree/nested/nul").read_bytes() == b"user\0change\xff"
    assert not (root / "rollback-preserved").exists()
    finished = await store.missing_rollback_proposal(scope, proposal.view.proposal_id)
    assert finished.rollback_state == "restored"
    async with store.database.sessions() as sql:
        link = await sql.get(WorkspaceMissingDirectoryRollbackRow, proposal.view.proposal_id)
        assert link.source_plan_id == original.plan_id

    def forbidden(*_args, **_kwargs):
        pytest.fail("冷历史/原键不得重放Reader/Writer")

    monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
    monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
    monkeypatch.setattr(type(writer.reader), "capture", forbidden)
    assert await store.preview(
        scope,
        binding,
        request,
        None,
        missing_origin=MissingDirectoryRollbackSource(original.plan_id),
    ) == (await store.proposal(scope, proposal.view.proposal_id), False)
    assert (await store.missing_rollback_list(scope)).items == [finished]


@pytest.mark.parametrize(
    "fault",
    [
        "source",
        "marker",
        "hash",
        "both-markers",
        "extra",
        "wrong-plan",
        "wrong-scope",
        "approved",
        "bool-version",
    ],
)
async def test_lost_or_rehashed_source_is_not_downgraded_through_generic_entries(
    rollback_origin, fault
):
    store, scope, binding, _, _ = rollback_origin
    _, request, change, proposal = await preview(rollback_origin)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceTreeMoveProposalRow, proposal.view.proposal_id)
        link = await sql.get(WorkspaceMissingDirectoryRollbackRow, proposal.view.proposal_id)
        if fault == "source":
            await sql.delete(link)
        elif fault == "marker":
            row.missing_restore_plan_id = row.missing_restore_hash = None
        elif fault == "hash":
            row.missing_restore_hash = "f" * 64
        elif fault == "both-markers":
            row.missing_restore_plan_id = row.missing_restore_hash = None
            await sql.delete(link)
        else:
            metadata = dict(link.source_metadata)
            if fault == "extra":
                metadata["untrusted_token"] = "no"
            elif fault == "wrong-plan":
                metadata["source_plan_id"] = "other-plan"
            elif fault == "wrong-scope":
                metadata["scope_id"] = "topic:other"
            elif fault == "approved":
                metadata["approved_fingerprint"] = "f" * 64
            else:
                metadata["version"] = True
            link.source_metadata = metadata
            link.source_hash = row.missing_restore_hash = digest(metadata)
            row.request_hash = store._wanted(
                request,
                binding.fingerprint,
                missing_plan_id=row.missing_restore_plan_id,
                missing_source_hash=row.missing_restore_hash,
            )
    for read in (
        lambda: store.proposal(scope, proposal.view.proposal_id),
        lambda: store.proposal_by_key(scope, request.request_key),
        lambda: store.reserve(scope, action(proposal.view), binding.fingerprint, change),
        lambda: store.list(scope),
        lambda: store.missing_rollback_list(scope),
    ):
        with pytest.raises(ServiceError):
            await read()
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    assert not (binding.workspace.root / "rollback-preserved").exists()


async def test_replaced_root_is_rejected_even_when_current_bytes_and_expected_fingerprint_match(
    rollback_origin,
):
    import shutil

    store, scope, binding, _, _ = rollback_origin
    root = binding.workspace.root
    (root / "tree").rename(root / "actual-original-published-root")
    shutil.copytree(root / "actual-original-published-root", root / "tree")
    assert (root / "tree").stat().st_ino != (root / "actual-original-published-root").stat().st_ino
    with pytest.raises(ServiceError) as denied:
        await preview(rollback_origin)
    assert denied.value.code == "workspace_missing_directory_rollback_root_changed"
    assert (await store.missing_rollback_list(scope)).items == []
    assert (root / "tree/nested/nul").read_bytes() == b"\0\xff"


@pytest.mark.parametrize("fault", ["scope", "key", "fingerprint", "source-type"])
async def test_original_scope_resource_and_source_class_cannot_be_borrowed(rollback_origin, fault):
    from dataclasses import replace

    store, scope, binding, finished, _ = rollback_origin
    writer = WorkspaceTreeWriter(binding.workspace)
    snapshot = writer.reader.capture("tree")
    change = writer.prepare("tree", "rollback-preserved", expected_fingerprint=snapshot.fingerprint)
    request = TreeMovePreviewRequest(
        source_path="tree",
        target_path="rollback-preserved",
        expected_fingerprint=snapshot.fingerprint,
        request_key="foreign",
    )
    selected = (
        replace(binding, key="alias")
        if fault == "key"
        else replace(binding, fingerprint="f" * 64)
        if fault == "fingerprint"
        else binding
    )
    with pytest.raises(ServiceError):
        await store.preview(
            "topic:foreign" if fault == "scope" else scope,
            selected,
            request,
            change,
            missing_origin=False
            if fault == "source-type"
            else MissingDirectoryRollbackSource(finished.plan_id),
        )
    assert (await store.missing_rollback_list(scope)).items == []
