"""真实原Missing/Full容器、最后资格与取消、未知及只读absence；绝不递归。"""

import os
import sys
import time
from dataclasses import replace
from threading import Event, Thread

import pytest
from test_directory_restore_writer import prepared
from test_missing_directory_writer import TOKEN, original

import krim_agent.workspace_empty_container_retention as module
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="原Linux目录能力")


def allow():
    return None


def published(writer, target, before):
    change = writer.prepare(target, before, token=TOKEN)
    assert writer.apply(change, record_intent=lambda _change: None).receipt.status == "done"
    return change.stage


def remove(core, stage, *, authorize=allow, record_intent=lambda _value: None, **options):
    observed = core.inspect(stage, authorize=allow)
    return core.remove(
        stage,
        expected_fingerprint=observed.fingerprint,
        confirmed=True,
        authorize=authorize,
        record_intent=record_intent,
        **options,
    )


def test_real_missing_empty_container_removed_once_target_binary_user_changes_untouched(
    tmp_path, monkeypatch
):
    with original(tmp_path) as (workspace, _reader, writer, target, before, tree):
        stage = published(writer, target, before)
        (tree / "user-added").write_bytes(b"user\0later\xff")
        original_tree = WorkspaceTreeSnapshotReader(workspace).capture(stage.root_path)
        core = WorkspaceEmptyContainerRetention(workspace)
        observed = core.inspect(stage, authorize=allow)
        assert observed.status == "empty" and observed.error_code is None
        calls, auth, intents = [], [], []
        rmdir = os.rmdir

        def effect(name, *, dir_fd):
            calls.append(name)
            assert name == "._krim-tx-" + stage.token
            rmdir(name, dir_fd=dir_fd)

        monkeypatch.setattr(module.os, "rmdir", effect)
        result = core.remove(
            stage,
            expected_fingerprint=observed.fingerprint,
            confirmed=True,
            authorize=lambda: auth.append(True),
            record_intent=intents.append,
        )
        assert result.status == "removed" and result.changed and not result.recovery_required
        assert len(calls) == 1 and len(auth) == 3 and intents == [observed]
        assert WorkspaceTreeSnapshotReader(workspace).capture(stage.root_path) == original_tree
        assert (tree / "sub/raw.bin").read_bytes() == b"\0\xff"
        assert stage.token not in repr(result) + repr(observed)
        again = core.inspect(stage, authorize=allow)
        assert again.status == "absent" and again.fingerprint == result.observed_fingerprint
        receipt = core.remove(
            stage,
            expected_fingerprint=observed.fingerprint,
            confirmed=True,
            authorize=allow,
            record_intent=lambda _value: pytest.fail("absence不再记录新删除意图"),
        )
        assert receipt.status == "already_absent" and not receipt.changed
        assert len(calls) == 1


def test_full_exchange_retained_tree_never_deleted(tmp_path, monkeypatch):
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryRestoreWriter(workspace)
        _target, before, change = prepared(tmp_path, writer)
        assert writer.apply(change, record_intent=lambda _value: None).receipt.status == "done"
        private = tmp_path / "holder" / ("._krim-tx-" + change.stage.token)
        old = (private / "payload/sub/raw.bin").read_bytes()
        assert old == b"\x80current"
        core = WorkspaceEmptyContainerRetention(workspace)
        observed = core.inspect(change.stage, authorize=allow)
        assert observed.status == "not_empty"
        monkeypatch.setattr(
            module.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("不可删除旧树")
        )
        result = remove(core, change.stage)
        assert result.status == "rejected" and not result.changed
        assert result.error_code == "workspace_empty_container_not_empty"
        assert (private / "payload/sub/raw.bin").read_bytes() == old
        assert writer.reconcile(change).receipt.before_fingerprint == before.fingerprint


@pytest.mark.parametrize("entry", ["file", "directory", "symlink", "fifo"])
def test_new_private_entries_are_not_opened_followed_or_deleted(tmp_path, monkeypatch, entry):
    with original(tmp_path) as (workspace, _reader, writer, target, before, _tree):
        stage = published(writer, target, before)
        private = tmp_path / "parent" / ("._krim-tx-" + TOKEN)
        outside = tmp_path / "outside"
        outside.write_bytes(b"keep-outside\0\xff")
        child = private / "new"
        if entry == "file":
            child.write_bytes(b"keep-private")
        elif entry == "directory":
            child.mkdir()
        elif entry == "symlink":
            child.symlink_to(outside)
        else:
            os.mkfifo(child)
        core = WorkspaceEmptyContainerRetention(workspace)
        assert core.inspect(stage, authorize=allow).status == "not_empty"
        monkeypatch.setattr(
            module.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("非空不得rmdir")
        )
        value = remove(core, stage)
        assert value.status == "rejected" and not value.changed
        assert child.lstat() and outside.read_bytes() == b"keep-outside\0\xff"


@pytest.mark.parametrize(
    "fault", ["child", "aba", "mode", "parent", "link", "authority", "cancel", "timeout"]
)
def test_last_authority_changes_are_rechecked_before_namespace_call(tmp_path, monkeypatch, fault):
    with original(tmp_path) as (workspace, _reader, writer, target, before, tree):
        stage = published(writer, target, before)
        private = tree.parent / ("._krim-tx-" + TOKEN)
        core = WorkspaceEmptyContainerRetention(
            workspace, lock_timeout=0.02 if fault == "timeout" else 5
        )
        stop = Event()
        calls, intents = [], []

        def authorize():
            calls.append(True)
            if len(calls) != 3:
                return
            if fault == "child":
                (private / "user").write_bytes(b"untouched")
            elif fault == "aba":
                item = private / "once"
                item.write_bytes(b"temporary")
                item.unlink()
            elif fault == "mode":
                private.chmod(0o750)
            elif fault == "parent":
                tree.parent.rename(tmp_path / "old-parent")
                tree.parent.mkdir()
            elif fault == "link":
                private.rename(tree.parent / "original-private")
                private.symlink_to(tree, target_is_directory=True)
            elif fault == "authority":
                raise ServiceError("test_owner_revoked", "原资格已撤销", 409)
            elif fault == "cancel":
                stop.set()
            else:
                time.sleep(0.03)

        monkeypatch.setattr(
            module.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("最后条件失败")
        )
        result = remove(
            core, stage, authorize=authorize, record_intent=intents.append, cancelled=stop.is_set
        )
        assert result.status == "rejected" and not result.changed and not result.recovery_required
        assert len(calls) == 3 and len(intents) == 1
        actual_tree = (tmp_path / "old-parent/tree") if fault == "parent" else tree
        assert actual_tree.stat().st_ino and (actual_tree / "sub/raw.bin").read_bytes() == b"\0\xff"


def test_falsey_callable_authority_and_cancellation_are_not_replaced(tmp_path):
    class Falsey:
        calls = 0

        def __bool__(self):
            return False

        def __call__(self):
            self.calls += 1

    class Cancel(Falsey):
        def __call__(self):
            self.calls += 1
            return True

    with original(tmp_path) as (workspace, _reader, writer, target, before, _tree):
        stage = published(writer, target, before)
        core = WorkspaceEmptyContainerRetention(workspace)
        authority = Falsey()
        observed = core.inspect(stage, authorize=authority)
        assert observed.status == "empty" and authority.calls == 2
        cancel = Cancel()
        result = remove(core, stage, authorize=authority, cancelled=cancel)
        assert (
            result.status == "rejected"
            and result.error_code == "workspace_empty_container_cancelled"
        )
        assert cancel.calls and authority.calls == 3


@pytest.mark.parametrize(
    "fault", ["wrong-type", "path", "token", "bool-identity", "owner", "inode"]
)
def test_bad_original_stage_never_becomes_delete_authority(tmp_path, monkeypatch, fault):
    with original(tmp_path) as (workspace, _reader, writer, target, before, tree):
        stage = published(writer, target, before)
        core = WorkspaceEmptyContainerRetention(workspace)
        monkeypatch.setattr(
            module.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("无可信Stage")
        )
        if fault == "wrong-type":
            bad = {"token": TOKEN}
        elif fault == "path":
            bad = replace(stage, root_path="../outside")
        elif fault == "token":
            bad = replace(stage, token="not-a-token")
        elif fault == "bool-identity":
            bad = replace(stage, parent_identity=(True, stage.parent_identity[1]))
        elif fault == "owner":
            bad = replace(
                stage, container_owner=(stage.container_owner[0] + 1, stage.container_owner[1])
            )
        else:
            bad = replace(
                stage,
                container_identity=(stage.container_identity[0], stage.container_identity[1] + 1),
            )
        if fault in {"owner", "inode"}:
            assert core.inspect(bad, authorize=allow).status == "unverifiable"
        else:
            with pytest.raises(ServiceError) as error:
                core.inspect(bad, authorize=allow)
            assert error.value.status == 422
        assert (tree.parent / ("._krim-tx-" + TOKEN)).exists()


@pytest.mark.parametrize("when", ["before-effect", "after-effect", "fsync", "after-cancel"])
def test_namespace_failure_remains_unknown_then_readonly_observation(tmp_path, monkeypatch, when):
    with original(tmp_path) as (workspace, _reader, writer, target, before, tree):
        stage = published(writer, target, before)
        core = WorkspaceEmptyContainerRetention(workspace)
        rmdir = os.rmdir
        calls = []
        stop = Event()

        def fail(name, *, dir_fd):
            calls.append(name)
            if when != "before-effect":
                rmdir(name, dir_fd=dir_fd)
            if when == "after-cancel":
                stop.set()
            elif when != "fsync":
                raise OSError("synthetic unknown")

        monkeypatch.setattr(module.os, "rmdir", fail)
        if when == "fsync":
            monkeypatch.setattr(
                module.os, "fsync", lambda _fd: (_ for _ in ()).throw(OSError("sync"))
            )
        result = remove(core, stage, cancelled=stop.is_set)
        assert result.status == "unknown" and result.changed and result.recovery_required
        monkeypatch.setattr(
            module.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("只读不能重删")
        )
        observed = WorkspaceEmptyContainerRetention(workspace).inspect(stage, authorize=allow)
        assert observed.status == ("empty" if when == "before-effect" else "absent")
        assert len(calls) == 1 and (tree / "sub/raw.bin").read_bytes() == b"\0\xff"


def test_lock_wait_and_callbacks_share_original_physical_deadline(tmp_path):
    with original(tmp_path) as (workspace, _reader, writer, target, before, _tree):
        stage = published(writer, target, before)
        core = WorkspaceEmptyContainerRetention(workspace, lock_timeout=0.08)
        observed = core.inspect(stage, authorize=allow)
        entered, released = Event(), Event()

        def hold():
            with workspace._write_lock:
                entered.set()
                released.wait(1)

        thread = Thread(target=hold)
        thread.start()
        assert entered.wait(1)
        started = time.monotonic()
        try:
            result = core.remove(
                stage,
                expected_fingerprint=observed.fingerprint,
                confirmed=True,
                authorize=lambda: time.sleep(0.05),
                record_intent=lambda _value: pytest.fail("原预算内未取得锁"),
            )
        finally:
            elapsed = time.monotonic() - started
            released.set()
            thread.join(1)
        assert (
            result.status == "rejected" and result.error_code == "workspace_empty_container_timeout"
        )
        assert 0.075 <= elapsed < 0.12


def test_confirmation_readonly_and_intent_failure_never_remove(tmp_path, monkeypatch):
    with original(tmp_path) as (workspace, _reader, writer, target, before, tree):
        stage = published(writer, target, before)
        core = WorkspaceEmptyContainerRetention(workspace)
        fingerprint = core.inspect(stage, authorize=allow).fingerprint
        monkeypatch.setattr(module.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("不能删除"))
        for confirmation in [1, "true", False]:
            with pytest.raises(ServiceError):
                core.remove(
                    stage,
                    expected_fingerprint=fingerprint,
                    confirmed=confirmation,
                    authorize=allow,
                    record_intent=lambda _value: None,
                )
        failure = core.remove(
            stage,
            expected_fingerprint=fingerprint,
            confirmed=True,
            authorize=allow,
            record_intent=lambda _value: (_ for _ in ()).throw(OSError("intent not saved")),
        )
        assert failure.status == "rejected" and not failure.changed
        wrong = core.remove(
            stage,
            expected_fingerprint="f" * 64,
            confirmed=True,
            authorize=allow,
            record_intent=lambda _value: pytest.fail("旧版本"),
        )
        assert (
            wrong.status == "rejected" and wrong.error_code == "workspace_empty_container_version"
        )
        with Workspace(tmp_path, writable=False) as readonly:
            read_core = WorkspaceEmptyContainerRetention(readonly)
            observed = read_core.inspect(stage, authorize=allow)
            assert observed.status == "empty"
            result = read_core.remove(
                stage,
                expected_fingerprint=observed.fingerprint,
                confirmed=True,
                authorize=allow,
                record_intent=lambda _value: pytest.fail("只读不能意图"),
            )
            assert result.status == "rejected" and result.error_code == "workspace_read_only"
        assert (tree.parent / ("._krim-tx-" + TOKEN)).is_dir()
