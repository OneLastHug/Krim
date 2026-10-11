"""目录Writer的真实条件发布、外部竞争、原材料恢复与零重放核实。"""

import errno
import os
import sys
from dataclasses import replace

import pytest

import krim_agent.workspace_directory as module
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Linux目录renameat2事务"
)


def create(path="empty", mode=0o750):
    return DirectoryChangeSpec(path=path, operation="create", mode=mode)


def deletion(writer, path="empty"):
    snapshot = writer.capture(path)
    assert snapshot is not None
    return DirectoryChangeSpec(
        path=path, operation="delete", expected_fingerprint=snapshot.fingerprint
    )


def test_real_create_delete_and_original_inode_restore_are_separate_effects(tmp_path):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(create())
        assert not (tmp_path / "empty").exists()
        created = writer.apply(plan)
        assert created.receipt.status == "done" and created.receipt.changed
        assert (tmp_path / "empty").stat().st_mode & 0o777 == 0o750
        directory = writer.capture("empty")
        removed_plan = writer.prepare(deletion(writer))
        removed = writer.apply(removed_plan)
        assert removed.receipt.status == "done" and removed.recovery is not None
        assert not (tmp_path / "empty").exists()
        restored_plan = writer.prepare_undo(removed_plan, removed)
        restored = writer.apply(restored_plan)
        assert restored.receipt.status == "done"
        after = writer.capture("empty")
        assert after.identity == directory.identity and after.mode == directory.mode
        assert (after.uid, after.gid) == (directory.uid, directory.gid)
        assert not list(tmp_path.glob("._krim-tx-*"))


def test_create_undo_is_bound_to_recorded_after_identity_and_empty_state(tmp_path):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        original = writer.prepare(create())
        effect = writer.apply(original)
        undo = writer.prepare_undo(original, effect)
        undone = writer.apply(undo)
        assert undone.receipt.status == "done" and not (tmp_path / "empty").exists()
        assert undone.recovery is not None
        writer.finalize(undo, undone.recovery)
        assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("change", ["identity", "mode", "contents"])
def test_create_undo_refuses_new_external_directory_or_changes(tmp_path, change):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        original = writer.prepare(create())
        effect = writer.apply(original)
        target = tmp_path / "empty"
        if change == "identity":
            target.rename(tmp_path / "saved")
            target.mkdir()
            target.chmod(0o750)
        elif change == "mode":
            target.chmod(0o700)
        else:
            (target / "external").write_text("external")
        with pytest.raises(ServiceError) as rejected:
            writer.prepare_undo(original, effect)
        assert rejected.value.code == "workspace_directory_conflict"
        assert target.is_dir()


def test_missing_parent_root_sensitive_links_and_special_modes_remain_rejected(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "link").symlink_to(outside, target_is_directory=True)
    (tmp_path / "special").mkdir()
    (tmp_path / "special").chmod(0o1777)
    (tmp_path / "ordinary-file").write_text("file")
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        for path in [".", "../escape", ".git", ".env"]:
            with pytest.raises((ValueError, ServiceError)):
                writer.prepare(create(path))
        for path, code in [
            ("missing/child", "workspace_not_found"),
            ("link", "workspace_directory_type"),
            ("special", "workspace_directory_mode"),
            ("ordinary-file", "workspace_directory_type"),
        ]:
            with pytest.raises(ServiceError) as rejected:
                writer.prepare(create(path))
            assert rejected.value.code == code
        assert list(outside.iterdir()) == []


def test_nonempty_delete_never_recurses_and_readonly_never_publishes(tmp_path):
    target = tmp_path / "empty"
    target.mkdir()
    (target / "file").write_bytes(b"\0\xff")
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        with pytest.raises(ServiceError) as nonempty:
            writer.prepare(deletion(writer))
        assert nonempty.value.code == "workspace_directory_nonempty"
        assert (target / "file").read_bytes() == b"\0\xff"
    with Workspace(tmp_path) as readonly:
        writer = WorkspaceDirectoryWriter(readonly)
        prepared = writer.prepare(create("new"))
        with pytest.raises(ServiceError) as rejected:
            writer.apply(prepared)
        assert rejected.value.code == "workspace_read_only" and not (tmp_path / "new").exists()


@pytest.mark.parametrize("kind", ["parent", "mode", "identity", "contents"])
def test_prepare_apply_rechecks_parent_and_full_original_snapshot(tmp_path, kind):
    parent = tmp_path / "parent"
    parent.mkdir()
    target = parent / "empty"
    target.mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        prepared = writer.prepare(deletion(writer, "parent/empty"))
        if kind == "parent":
            parent.rename(tmp_path / "saved")
            parent.mkdir()
            target.mkdir()
            with pytest.raises(ServiceError) as rejected:
                writer.apply(prepared)
            assert rejected.value.code == "workspace_parent_changed"
        else:
            if kind == "mode":
                target.chmod(0o700)
            elif kind == "identity":
                target.rename(parent / "saved")
                target.mkdir()
            else:
                (target / "file").write_text("external")
            effect = writer.apply(prepared)
            assert effect.receipt.status == "rejected"
            assert effect.receipt.error_code == "workspace_directory_conflict"
        assert target.exists()


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("action", ["create", "delete"])
def test_atomic_rename_exception_keeps_material_and_verifies_without_replay(
    tmp_path, monkeypatch, after, action
):
    if action == "delete":
        (tmp_path / "empty").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(create() if action == "create" else deletion(writer))
        records = []
        original = module.rename_at
        calls = 0

        def fail(fd, source, target, **kwargs):
            nonlocal calls
            calls += 1
            if after:
                original(fd, source, target, **kwargs)
            raise OSError(errno.EIO, "lost syscall result")

        monkeypatch.setattr(module, "rename_at", fail)
        effect = writer.apply(plan, record_intent=records.append)
        assert effect.receipt.status == "uncertain" and effect.recovery is not None
        assert len(records) == 1
        verified = writer.verify(plan, records[0])
        assert verified.receipt.status == ("done" if after else "rejected")
        assert calls == 1
        if after and action == "delete":
            writer.finalize(plan, records[0])
        elif not after and action == "create":
            writer.finalize(plan, records[0], rejected=True)
        assert not list(tmp_path.glob("._krim-tx-*"))


def test_cancel_or_authority_loss_before_publish_preserves_target_and_cleans_own_stage(tmp_path):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        prepared = writer.prepare(create())
        cancelled = False

        def record(_reference):
            nonlocal cancelled
            cancelled = True

        result = writer.apply(prepared, abort=lambda: cancelled, record_intent=record)
        assert (
            result.receipt.status == "rejected"
            and result.receipt.error_code == "workspace_write_cancelled"
        )
        assert not list(tmp_path.iterdir())
        calls = 0

        def authorize():
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ServiceError("authority_lost", "lease expired", 409)

        result = writer.apply(prepared, authorize=authorize)
        assert result.receipt.status == "rejected" and result.receipt.error_code == "authority_lost"
        assert not list(tmp_path.iterdir())


def test_source_swap_at_atomic_delete_preserves_foreign_directory(tmp_path, monkeypatch):
    (tmp_path / "empty").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(deletion(writer))
        original = module.rename_at
        swapped = False

        def swap(fd, source, target, **kwargs):
            nonlocal swapped
            if source == "empty" and not swapped:
                swapped = True
                (tmp_path / "empty").rename(tmp_path / "original")
                (tmp_path / "empty").mkdir()
                (tmp_path / "empty" / "external").write_text("foreign")
            return original(fd, source, target, **kwargs)

        monkeypatch.setattr(module, "rename_at", swap)
        effect = writer.apply(plan)
        assert effect.receipt.status == "rejected"
        assert (tmp_path / "empty" / "external").read_text() == "foreign"
        assert (tmp_path / "original").is_dir()


def test_wrong_after_receipt_and_changed_recovery_reference_cannot_undo_or_finalize(tmp_path):
    (tmp_path / "empty").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(deletion(writer))
        effect = writer.apply(plan)
        with pytest.raises(ServiceError) as bad_receipt:
            writer.prepare_undo(
                plan, replace(effect, receipt=replace(effect.receipt, operation_token="0" * 32))
            )
        assert bad_receipt.value.code == "workspace_directory_receipt"
        with pytest.raises(ServiceError) as bad_reference:
            writer.finalize(plan, replace(effect.recovery, stage_name="outside"))
        assert bad_reference.value.code == "workspace_directory_reference"
        (tmp_path / effect.recovery.stage_name / "external").write_text("keep")
        with pytest.raises(ServiceError) as changed:
            writer.finalize(plan, effect.recovery)
        assert changed.value.code == "workspace_directory_stage_changed"
        assert (tmp_path / effect.recovery.stage_name / "external").read_text() == "keep"


def test_restore_reference_is_checked_before_any_unscoped_directory_read(tmp_path, monkeypatch):
    (tmp_path / "empty").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(deletion(writer))
        effect = writer.apply(plan)
        bad = replace(effect, recovery=replace(effect.recovery, stage_name="../outside"))
        snapshot = writer._snapshot

        def guarded(fd, name, **kwargs):
            assert name != "../outside", "恢复引用须在打开目录前拒绝"
            return snapshot(fd, name, **kwargs)

        monkeypatch.setattr(writer, "_snapshot", guarded)
        with pytest.raises(ServiceError) as rejected:
            writer.prepare_undo(plan, bad)
        assert rejected.value.code == "workspace_directory_reference"


@pytest.mark.parametrize("mode", [0o500, 0o555, 0o700, 0o750])
def test_delete_restore_preserves_original_normal_readonly_modes(tmp_path, mode):
    target = tmp_path / "empty"
    target.mkdir()
    target.chmod(mode)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        before = writer.capture("empty")
        plan = writer.prepare(deletion(writer))
        deleted = writer.apply(plan)
        restore = writer.prepare_undo(plan, deleted)
        result = writer.apply(restore)
        assert result.receipt.status == "done"
        actual = writer.capture("empty")
        assert (actual.identity, actual.mode, actual.uid, actual.gid) == (
            before.identity,
            before.mode,
            before.uid,
            before.gid,
        )


def test_private_snapshot_spec_or_resource_substitution_fails_before_mutation(tmp_path):
    (tmp_path / "empty").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(deletion(writer))
        wrong = replace(
            plan,
            spec_json=DirectoryChangeSpec(
                path="empty", operation="delete", expected_fingerprint="0" * 64
            ).model_dump_json(),
        )
        with pytest.raises(ServiceError) as rejected:
            writer.apply(wrong)
        assert rejected.value.code == "workspace_change_invalid"
        applied = writer.apply(plan)
        with pytest.raises(ServiceError) as resource:
            writer.verify(replace(plan, workspace_fingerprint="0" * 64), applied.recovery)
        assert resource.value.code == "workspace_binding_changed"


@pytest.mark.parametrize("mutation", ["new_target", "stage_file"])
def test_after_staging_foreign_target_or_material_never_publishes(tmp_path, mutation):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(create())

        def mutate(reference):
            if mutation == "new_target":
                (tmp_path / "empty").mkdir()
                (tmp_path / "empty" / "external").write_text("foreign")
            else:
                (tmp_path / reference.stage_name / "external").write_text("foreign")

        effect = writer.apply(plan, record_intent=mutate)
        assert effect.receipt.status != "done"
        if mutation == "new_target":
            assert (tmp_path / "empty" / "external").read_text() == "foreign"
        else:
            assert not (tmp_path / "empty").exists()
            assert effect.receipt.status == "uncertain"
            assert (tmp_path / effect.recovery.stage_name / "external").read_text() == "foreign"


def test_failed_stage_setup_retains_unknown_identity_and_never_blindly_cleans(
    tmp_path, monkeypatch
):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        plan = writer.prepare(create())

        def partial(fd, stage, mode):
            os.mkdir(stage, 0o700, dir_fd=fd)
            raise OSError(errno.EIO, "stage setup interrupted")

        monkeypatch.setattr(writer, "_create_stage", partial)
        effect = writer.apply(plan)
        assert effect.receipt.status == "uncertain" and effect.recovery.expected is None
        assert not (tmp_path / "empty").exists()
        assert writer.verify(plan, effect.recovery).receipt.status == "uncertain"
        with pytest.raises(ServiceError) as rejected:
            writer.finalize(plan, effect.recovery, rejected=True)
        assert rejected.value.code == "workspace_directory_stage_unknown"
        assert (tmp_path / effect.recovery.stage_name).is_dir()
