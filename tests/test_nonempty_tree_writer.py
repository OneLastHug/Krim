"""真实非空树原inode/字节移动、原条件/取消/unknown核实与反向恢复。"""

import sys
from dataclasses import replace

import pytest
from test_workspace_tree_snapshot import fd_count, seed

import krim_agent.workspace_tree_writer as module
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="需要真实renameat2与双父锁"
)


def prepare(writer, source="tree", target="dest/tree"):
    return writer.prepare(
        source, target, expected_fingerprint=writer.reader.capture(source).fingerprint
    )


def test_original_nonempty_tree_move_and_reverse_preserve_every_inode_byte_and_mode(tmp_path):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        descriptors = fd_count()
        change = prepare(writer)
        assert change.before.entries[0].directory.empty is False
        captured = []
        effect = writer.apply(change, record_intent=captured.append)
        assert effect.receipt.status == "done" and effect.receipt.changed
        assert effect.receipt.operation_token == change.token and captured == [change]
        assert not (tmp_path / "tree").exists()
        assert (tmp_path / "dest/tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
        assert writer._moved(change, effect.observed_after)
        reverse = writer.prepare_undo(change, effect)
        undone = writer.apply(reverse)
        assert undone.receipt.status == "done" and not (tmp_path / "dest/tree").exists()
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
        for old, current in zip(change.before.entries, undone.observed_after.entries, strict=True):
            assert old.path == current.path
            if old.file is not None:
                assert old.file == current.file
            else:
                assert old.directory.identity == current.directory.identity
                assert old.directory.mode == current.directory.mode
        assert fd_count() == descriptors


@pytest.mark.parametrize(
    "target", ["tree", "tree/sub/moved", "../moved", "/tmp/moved", ".env/moved"]
)
def test_relative_target_boundaries_and_self_embedding_do_not_move_original_tree(tmp_path, target):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        with pytest.raises(ServiceError):
            prepare(writer, target=target)
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"


@pytest.mark.parametrize("change", ["bytes", "mode", "extra", "target", "parent"])
def test_admitted_move_rejects_changed_entire_tree_or_target_or_parent(tmp_path, change):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        original = prepare(writer)
        if change == "bytes":
            (tmp_path / "tree/sub/data.bin").write_bytes(b"outside change")
        elif change == "mode":
            (tmp_path / "tree/sub").chmod(0o750)
        elif change == "extra":
            (tmp_path / "tree/extra").write_bytes(b"other")
        elif change == "target":
            (tmp_path / "dest/tree").mkdir()
        else:
            (tmp_path / "dest").rename(tmp_path / "original-dest")
            (tmp_path / "dest").mkdir()
        descriptors = fd_count()
        result = writer.apply(original)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert (tmp_path / "tree").is_dir() and fd_count() == descriptors


@pytest.mark.parametrize("phase", ["cancel", "authorize", "late-authorize", "intent"])
def test_cancel_or_late_authorization_failure_or_intent_change_keeps_source(tmp_path, phase):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)
        calls = 0
        stopped = False

        def authorize():
            nonlocal calls
            calls += 1
            if (phase == "authorize" and calls == 2) or (phase == "late-authorize" and calls == 3):
                raise ServiceError("synthetic_run_stopped", "合成原资格停止", 403)

        def intent(_change):
            nonlocal stopped
            if phase == "cancel":
                stopped = True
            elif phase == "intent":
                (tmp_path / "tree/sub/data.bin").write_bytes(b"late outside change")

        effect = writer.apply(
            change, authorize=authorize, record_intent=intent, cancelled=lambda: stopped
        )
        assert effect.receipt.status == "rejected" and not effect.receipt.changed
        assert (tmp_path / "tree").is_dir() and not (tmp_path / "dest/tree").exists()


def test_noreplace_preserves_racing_target_and_every_original_source(tmp_path, monkeypatch):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    original = module.rename_at
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)

        def racing(*args, **kwargs):
            (tmp_path / "dest/tree").mkdir()
            (tmp_path / "dest/tree/outside.txt").write_text("must keep")
            return original(*args, **kwargs)

        monkeypatch.setattr(module, "rename_at", racing)
        result = writer.apply(change)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert (tmp_path / "dest/tree/outside.txt").read_text() == "must keep"
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"


def test_unknown_original_move_reconciles_without_second_rename_and_undo_uses_real_after(
    tmp_path, monkeypatch
):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    original = module.rename_at
    calls = 0
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)

        def lost(*args, **kwargs):
            nonlocal calls
            calls += 1
            original(*args, **kwargs)
            raise OSError("synthetic original rename return lost")

        monkeypatch.setattr(module, "rename_at", lost)
        unknown = writer.apply(change)
        assert unknown.receipt.status == "uncertain" and unknown.receipt.recovery_required
        assert not (tmp_path / "tree").exists() and calls == 1
        resolved = writer.reconcile(change)
        assert resolved.receipt.status == "done" and calls == 1
        assert resolved.receipt.operation_token == change.token
        reverse = writer.prepare_undo(change, resolved)
        monkeypatch.setattr(module, "rename_at", original)
        assert writer.apply(reverse).receipt.status == "done"
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"


def test_cancellation_after_original_rename_keeps_actual_done_and_never_rolls_back(
    tmp_path, monkeypatch
):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    original = module.rename_at
    stopped = False
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)

        def stop_after(*args, **kwargs):
            nonlocal stopped
            original(*args, **kwargs)
            stopped = True

        monkeypatch.setattr(module, "rename_at", stop_after)
        result = writer.apply(change, cancelled=lambda: stopped)
        assert result.receipt.status == "done" and result.receipt.changed
        assert not (tmp_path / "tree").exists() and (tmp_path / "dest/tree").is_dir()


def test_undo_refuses_changed_target_or_forged_original_receipt(tmp_path):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change)
        assert effect.receipt.status == "done"
        fake = replace(effect, receipt=replace(effect.receipt, operation_token="0" * 32))
        with pytest.raises(ServiceError) as denied:
            writer.prepare_undo(change, fake)
        assert denied.value.code == "workspace_tree_move_receipt"
        (tmp_path / "dest/tree/sub/data.bin").write_bytes(b"external after move")
        with pytest.raises(ServiceError) as denied:
            writer.prepare_undo(change, effect)
        assert denied.value.code == "workspace_tree_move_conflict"
        assert not (tmp_path / "tree").exists()


def test_read_only_and_other_resource_cannot_publish_full_tree(tmp_path):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)
        result = writer.apply(change)
        assert (
            result.receipt.status == "rejected"
            and result.receipt.error_code == "workspace_read_only"
        )
    with Workspace(tmp_path, writable=True) as different:
        with pytest.raises(ServiceError) as denied:
            WorkspaceTreeWriter(different).apply(change)
        assert denied.value.code == "workspace_binding_changed"
    assert (tmp_path / "tree").is_dir() and not (tmp_path / "dest/tree").exists()


def test_unknown_changed_target_stays_uncertain_and_never_recognizes_copied_bytes(tmp_path):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)
        assert writer.apply(change).receipt.status == "done"
        (tmp_path / "dest/tree/sub/data.bin").write_bytes(b"external")
        result = writer.reconcile(change)
        assert result.receipt.status == "uncertain" and result.receipt.recovery_required
        assert not (tmp_path / "tree").exists()


def test_unavailable_atomic_capability_is_rejected_before_intent_or_namespace(
    tmp_path, monkeypatch
):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)
        intents = []

        def missing():
            raise ServiceError("workspace_write_platform", "合成renameat2不可用", 503)

        monkeypatch.setattr(module, "_rename_function", missing)
        result = writer.apply(change, record_intent=intents.append)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == "workspace_write_platform" and intents == []
        assert (tmp_path / "tree").is_dir() and not (tmp_path / "dest/tree").exists()


def test_reconcile_confirms_original_source_without_any_rename(tmp_path, monkeypatch):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)

        def forbidden(*_args, **_kwargs):
            raise AssertionError("核实不能移动")

        monkeypatch.setattr(module, "rename_at", forbidden)
        result = writer.reconcile(change)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == "workspace_tree_move_not_observed"
