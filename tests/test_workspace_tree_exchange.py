"""真实一次exchange、原全部bytes/inode、双树/父条件与unknown零重放。"""

import os
import sys
from dataclasses import replace
from threading import Event

import pytest

import krim_agent.workspace_tree_exchange as module
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="原Linux renameat2")


def seed(root):
    (root / "holder/live/sub/empty").mkdir(parents=True)
    (root / "new-parent/prepared/sub/empty").mkdir(parents=True)
    (root / "holder/live/sub/data.bin").write_bytes(b"\0\xff\x80")
    (root / "new-parent/prepared/sub/data.bin").write_bytes(b"\x80\x00new")
    (root / "new-parent/prepared/new.txt").write_text("完整新树\n")
    (root / "holder/live").chmod(0o750)
    (root / "new-parent/prepared").chmod(0o700)
    (root / "holder/live/sub/data.bin").chmod(0o640)
    (root / "new-parent/prepared/sub/data.bin").chmod(0o600)


def prepare(writer):
    left = writer.trees.reader.capture("holder/live")
    right = writer.trees.reader.capture("new-parent/prepared")
    return writer.prepare(
        left.root_path,
        right.root_path,
        expected_left=left.fingerprint,
        expected_right=right.fingerprint,
    )


def test_real_atomic_exchange_open_old_dirfd_inode_bytes_modes_actual_dual_after_and_undo(
    tmp_path, monkeypatch
):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        old_fd = os.open(tmp_path / "holder/live", os.O_RDONLY | os.O_DIRECTORY)
        calls, intents = [], []
        actual = module.rename_at

        def record(*args, **kwargs):
            calls.append((args, kwargs))
            actual(*args, **kwargs)

        monkeypatch.setattr(module, "rename_at", record)
        try:
            effect = writer.apply(change, record_intent=intents.append)
            assert effect.receipt.status == "done" and effect.receipt.changed
            assert len(calls) == 1 and calls[0][1]["exchange"] is True
            assert intents == [change]
            assert (
                effect.observed_left.entries[0].directory.inode
                == change.right.entries[0].directory.inode
            )
            assert (
                effect.observed_right.entries[0].directory.inode
                == change.left.entries[0].directory.inode
            )
            assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\x80\x00new"
            assert (tmp_path / "new-parent/prepared/sub/data.bin").read_bytes() == b"\0\xff\x80"
            fd = os.open("sub/data.bin", os.O_RDONLY, dir_fd=old_fd)
            try:
                assert os.read(fd, 20) == b"\0\xff\x80"
            finally:
                os.close(fd)
            assert effect.receipt.left_after_fingerprint == effect.observed_left.fingerprint
            assert effect.receipt.right_after_fingerprint == effect.observed_right.fingerprint
            assert effect.receipt.left_after_fingerprint != change.right.fingerprint
            reverse = writer.prepare_undo(change, effect)
            assert reverse.token != change.token and reverse.left == effect.observed_left
            restored = writer.apply(reverse)
            assert restored.receipt.status == "done" and len(calls) == 2
            assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"
            assert (tmp_path / "new-parent/prepared/sub/data.bin").read_bytes() == b"\x80\x00new"
            assert (tmp_path / "holder/live").stat().st_ino == change.left.entries[
                0
            ].directory.inode
            assert (tmp_path / "new-parent/prepared").stat().st_ino == change.right.entries[
                0
            ].directory.inode
            assert (tmp_path / "holder/live/sub/data.bin").stat().st_mode & 0o777 == 0o640
        finally:
            os.close(old_fd)


@pytest.mark.parametrize(
    "fault", ["left_bytes", "right_bytes", "left_mode", "right_identity", "left_added", "parent"]
)
def test_any_original_tree_or_parent_change_refuses_before_namespace(tmp_path, monkeypatch, fault):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        if fault.endswith("bytes"):
            location = "holder/live" if fault.startswith("left") else "new-parent/prepared"
            (tmp_path / location / "sub/data.bin").write_bytes(b"changed")
        elif fault == "left_mode":
            (tmp_path / "holder/live/sub/data.bin").chmod(0o600)
        elif fault == "right_identity":
            file = tmp_path / "new-parent/prepared/sub/data.bin"
            file.rename(tmp_path / "retained.bin")
            file.write_bytes(b"\x80\x00new")
            file.chmod(0o600)
        elif fault == "left_added":
            (tmp_path / "holder/live/extra.txt").write_text("new")
        else:
            (tmp_path / "holder").rename(tmp_path / "old-holder")
            (tmp_path / "holder").mkdir()

        def forbidden(*_args, **_kwargs):
            pytest.fail("原条件已变化仍namespace交换")

        monkeypatch.setattr(module, "rename_at", forbidden)
        result = writer.apply(change)
        assert result.receipt.status == "rejected" and not result.receipt.changed


@pytest.mark.parametrize("fault", ["cancel", "last_authority", "intent_mutation", "capability"])
def test_last_original_authority_cancel_intent_and_no_unsafe_fallback(tmp_path, monkeypatch, fault):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        abort, seen = Event(), []
        calls = 0

        def authorize():
            nonlocal calls
            calls += 1
            if fault == "last_authority" and calls == 3:
                raise ServiceError("original_authority_lost", "原最后资格失效", 409)

        def intent(value):
            seen.append(value)
            if fault == "cancel":
                abort.set()
            if fault == "intent_mutation":
                (tmp_path / "new-parent/prepared/new.txt").write_text("raced")

        def forbidden(*_args, **_kwargs):
            pytest.fail("最后条件/能力拒绝仍namespace")

        monkeypatch.setattr(module, "rename_at", forbidden)
        if fault == "capability":
            monkeypatch.setattr(
                module,
                "_rename_function",
                lambda: (_ for _ in ()).throw(
                    ServiceError("workspace_write_platform", "无原子交换", 503)
                ),
            )
        result = writer.apply(
            change, cancelled=abort.is_set, authorize=authorize, record_intent=intent
        )
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert len(seen) == (0 if fault == "capability" else 1)
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"


@pytest.mark.parametrize("observed", [False, True])
def test_unknown_before_or_after_real_effect_reconciles_same_material_without_exchange(
    tmp_path, monkeypatch, observed
):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        actual = module.rename_at

        def ambiguous(*args, **kwargs):
            if observed:
                actual(*args, **kwargs)
            raise OSError("合成回程中断")

        monkeypatch.setattr(module, "rename_at", ambiguous)
        unknown = writer.apply(change)
        assert unknown.receipt.status == "uncertain" and unknown.receipt.recovery_required

        def forbidden(*_args, **_kwargs):
            pytest.fail("unknown再次交换原树")

        monkeypatch.setattr(module, "rename_at", forbidden)
        resolved = writer.reconcile(change)
        assert resolved.receipt.status == ("done" if observed else "rejected")
        assert resolved.receipt.operation_token == change.token
        if observed:
            assert resolved.observed_left is not None and resolved.observed_right is not None
            (tmp_path / "holder/live/sub/data.bin").write_bytes(b"unexpected")
            assert writer.reconcile(change).receipt.status == "uncertain"


def test_cancel_after_real_effect_reads_actual_dual_after_not_false_rollback(tmp_path, monkeypatch):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        abort, actual = Event(), module.rename_at

        def cancelled_after(*args, **kwargs):
            actual(*args, **kwargs)
            abort.set()

        monkeypatch.setattr(module, "rename_at", cancelled_after)
        result = writer.apply(change, cancelled=abort.is_set)
        assert result.receipt.status == "done" and result.receipt.changed
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\x80\x00new"


@pytest.mark.parametrize(
    "field",
    [
        "operation_token",
        "left_path",
        "right_path",
        "left_after_fingerprint",
        "right_before_fingerprint",
    ],
)
def test_undo_rejects_substituted_token_paths_or_fingerprints(tmp_path, field):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        result = writer.apply(change)
        assert result.receipt.status == "done"
        forged = replace(result, receipt=replace(result.receipt, **{field: "other"}))
        with pytest.raises(ServiceError) as error:
            writer.prepare_undo(change, forged)
        assert error.value.code == "workspace_tree_exchange_receipt"


def test_combined_capacity_read_only_and_paths_do_not_expand_original_authorization(tmp_path):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        for limited in (
            WorkspaceTreeExchangeWriter(workspace, max_entries=4),
            WorkspaceTreeExchangeWriter(workspace, max_total_bytes=4),
        ):
            with pytest.raises(ServiceError):
                limited.prepare(
                    change.left.root_path,
                    change.right.root_path,
                    expected_left=change.left.fingerprint,
                    expected_right=change.right.fingerprint,
                )
        with pytest.raises(ServiceError):
            writer.prepare(
                "holder/live",
                "holder/live/sub",
                expected_left=change.left.fingerprint,
                expected_right=change.right.fingerprint,
            )
    with Workspace(tmp_path, writable=False) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        before = prepare(writer)
        assert writer.apply(before).receipt.status == "rejected"
        assert writer.reconcile(before).receipt.status == "rejected"
