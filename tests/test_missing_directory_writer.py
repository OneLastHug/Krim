"""真实缺失完整树发布、自身事件、拒绝与未知零重放；无公开控制旁路。"""

import json
import selectors
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要原Linux能力")
TOKEN = "a" * 32


@contextmanager
def original(tmp_path, *, writable=True, timeout=5):
    tree = tmp_path / "parent/tree"
    (tree / "sub").mkdir(parents=True)
    (tree / "empty-dir").mkdir()
    (tree / "text.txt").write_bytes(b"original directory\n")
    (tree / "sub/raw.bin").write_bytes(b"\0\xff")
    (tree / "sub/raw.bin").chmod(0o640)
    (tree / "empty.bin").write_bytes(b"")
    tree.chmod(0o750)
    with Workspace(tmp_path, writable=writable) as workspace:
        target = WorkspaceTreeSnapshotReader(workspace).capture("parent/tree")
        shutil.rmtree(tree)
        with WorkspaceDirectoryAbsenceReader(workspace, lock_timeout=timeout) as reader:
            before = reader.capture("parent/tree")
            writer = WorkspaceMissingDirectoryRestoreWriter(workspace, reader, lock_timeout=timeout)
            yield workspace, reader, writer, target, before, tree


def forbidden(*_args, **_kwargs):
    raise AssertionError("原操作不允许再次构树/发布或记录意图")


def test_real_missing_tree_prepare_own_event_publish_and_duplicate_calls_are_cold(
    tmp_path, monkeypatch
):
    with original(tmp_path) as (_workspace, reader, writer, target, before, tree):
        records, intents = [], []
        change = writer.prepare(target, before, token=TOKEN, record_stage=records.append)
        assert not tree.exists()
        assert len(records) == 1
        assert change.before == before
        assert change.current_absence.fingerprint != before.fingerprint
        with pytest.raises(ServiceError):
            reader.verify(before)
        reader.verify(change.current_absence)
        assert writer.prepare(target, before, token=TOKEN, record_stage=forbidden) == change
        stage = tree.parent / ("._krim-tx-" + TOKEN)
        assert stage.stat().st_mode & 0o777 == 0o700
        value = writer.apply(change, record_intent=intents.append)
        assert value.receipt.status == "done" and value.receipt.changed
        assert value.receipt.before_absence_fingerprint == before.fingerprint
        assert value.receipt.target_fingerprint == target.fingerprint
        assert (tree / "sub/raw.bin").read_bytes() == b"\0\xff"
        assert (tree / "sub/raw.bin").stat().st_mode & 0o777 == 0o640
        assert tree.stat().st_mode & 0o777 == 0o750
        assert (tree / "empty.bin").read_bytes() == b""
        assert list((tree / "empty-dir").iterdir()) == []
        assert not (stage / "payload").exists()
        assert len(intents) == 1
        monkeypatch.setattr(physical, "rename_at", forbidden)
        monkeypatch.setattr(writer.base, "_write_tree", forbidden)
        monkeypatch.setattr(writer.base, "_capture", forbidden)
        assert writer.apply(change, record_intent=forbidden) == value
        (tree / "text.txt").write_bytes(b"later external")
        assert writer.reconcile(change) == value
        assert (tree / "text.txt").read_bytes() == b"later external"


@pytest.mark.parametrize("fault", ["sibling", "aba", "parent-mode", "stage-mode"])
def test_owned_stage_cannot_hide_other_events_or_authority_changes(tmp_path, monkeypatch, fault):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):

        def changed(_stage):
            if fault == "sibling":
                (tree.parent / "external").write_bytes(b"unrelated namespace")
            elif fault == "aba":
                tree.mkdir()
                tree.rmdir()
            elif fault == "parent-mode":
                tree.parent.chmod(0o700)
            else:
                stage = tree.parent / ("._krim-tx-" + TOKEN)
                stage.chmod(0o750)
                stage.chmod(0o700)

        monkeypatch.setattr(writer.base, "_write_tree", forbidden)
        with pytest.raises(ServiceError):
            writer.prepare(target, before, token=TOKEN, record_stage=changed)
        assert not tree.exists()
        assert not (tree.parent / ("._krim-tx-" + TOKEN) / "payload").exists()
        with pytest.raises(ServiceError) as closed:
            writer.prepare(target, before, token=TOKEN)
        assert closed.value.code == "workspace_missing_directory_restore_prepare_closed"


@pytest.mark.parametrize("kind", ["file", "directory", "symlink", "dangling"])
def test_any_new_target_prevents_publication_and_closed_request_never_revives(
    tmp_path, monkeypatch, kind
):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        if kind == "file":
            tree.write_bytes(b"external\0\xff")
        elif kind == "directory":
            tree.mkdir()
        else:
            tree.symlink_to(tree.parent / "external" if kind == "dangling" else tmp_path)
        inode = tree.lstat().st_ino
        monkeypatch.setattr(physical, "rename_at", forbidden)
        value = writer.apply(change, record_intent=forbidden)
        assert value.receipt.status == "rejected" and not value.receipt.changed
        assert tree.lstat().st_ino == inode
        if kind == "directory":
            tree.rmdir()
        else:
            tree.unlink()
        assert writer.apply(change, record_intent=forbidden) == value
        assert not tree.exists()


def test_unknown_after_real_noreplace_verifies_same_tree_without_producer_replay(
    tmp_path, monkeypatch
):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        native, renamed = physical.rename_at, 0

        def lost(*args, **kwargs):
            nonlocal renamed
            renamed += 1
            native(*args, **kwargs)
            raise OSError("synthetic real publication reply lost")

        monkeypatch.setattr(physical, "rename_at", lost)
        first = writer.apply(change, record_intent=lambda _value: None)
        assert first.receipt.status == "uncertain" and first.receipt.changed
        assert (tree / "sub/raw.bin").read_bytes() == b"\0\xff"
        inode = tree.stat().st_ino
        monkeypatch.setattr(physical, "rename_at", forbidden)
        monkeypatch.setattr(writer.base, "_write_tree", forbidden)
        final = writer.reconcile(change)
        assert final.receipt.status == "done" and final.receipt.changed
        assert tree.stat().st_ino == inode
        assert renamed == 1
        assert writer.apply(change, record_intent=forbidden) == final


def test_new_reader_and_writer_cannot_fabricate_lost_namespace_history(tmp_path, monkeypatch):
    with original(tmp_path) as (workspace, reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        native = physical.rename_at

        def lost(*args, **kwargs):
            native(*args, **kwargs)
            raise OSError("synthetic after namespace")

        monkeypatch.setattr(physical, "rename_at", lost)
        assert writer.apply(change, record_intent=lambda _value: None).receipt.status == "uncertain"
        reader.close()
        with WorkspaceDirectoryAbsenceReader(workspace) as cold:
            restored = WorkspaceMissingDirectoryRestoreWriter(workspace, cold)
            monkeypatch.setattr(physical, "rename_at", forbidden)
            monkeypatch.setattr(restored.base, "_write_tree", forbidden)
            monkeypatch.setattr(restored.base, "_capture", forbidden)
            value = restored.reconcile(change)
            assert value.receipt.status == "uncertain"
            assert value.receipt.error_code == "workspace_missing_directory_restore_unverifiable"
            assert (tree / "sub/raw.bin").read_bytes() == b"\0\xff"


def test_real_last_qualify_deadline_keeps_target_missing(tmp_path):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        # 只缩短待验证的发布阶段；原准备仍使用其5秒上限。
        writer.base.lock_timeout = 0.02
        calls = 0

        def late():
            nonlocal calls
            calls += 1
            if calls == 3:
                time.sleep(0.04)

        result = writer.apply(change, record_intent=lambda _value: None, authorize=late)
        assert result.receipt.status == "rejected"
        assert "timeout" in result.receipt.error_code
        assert not tree.exists()
        assert writer.apply(change, record_intent=forbidden).receipt == result.receipt


def test_cancel_before_namespace_closes_original_request_without_reviving(tmp_path, monkeypatch):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        monkeypatch.setattr(physical, "rename_at", forbidden)
        value = writer.apply(change, record_intent=forbidden, cancelled=lambda: True)
        assert value.receipt.status == "rejected" and not value.receipt.changed
        assert not tree.exists()
        assert writer.apply(change, record_intent=forbidden, cancelled=lambda: False) == value


def test_read_only_workspace_never_allocates_private_stage(tmp_path):
    with original(tmp_path, writable=False) as (_workspace, _reader, writer, target, before, tree):
        with pytest.raises(ServiceError) as denied:
            writer.prepare(target, before, token=TOKEN)
        assert denied.value.code == "workspace_read_only"
        assert not tree.exists()
        assert not (tree.parent / ("._krim-tx-" + TOKEN)).exists()


def test_callback_material_copies_cannot_redirect_private_stage_or_intent(tmp_path):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):

        def stage_alias(value):
            object.__setattr__(value, "token", "b" * 32)

        change = writer.prepare(target, before, token=TOKEN, record_stage=stage_alias)
        assert change.stage.token == TOKEN

        def intent_alias(value):
            object.__setattr__(value.stage, "root_path", "other")
            object.__setattr__(value.current_absence.parent, "mode", 0o700)

        result = writer.apply(change, record_intent=intent_alias)
        assert result.receipt.status == "done"
        assert (tree / "sub/raw.bin").read_bytes() == b"\0\xff"
        assert not (tmp_path / "other").exists()


def test_intent_failure_closes_original_request_before_any_namespace_attempt(tmp_path, monkeypatch):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        calls = 0

        def failed(_value):
            nonlocal calls
            calls += 1
            raise ServiceError("test_intent_unavailable", "原意图回程故障", 503)

        monkeypatch.setattr(physical, "rename_at", forbidden)
        value = writer.apply(change, record_intent=failed)
        assert value.receipt.status == "rejected" and not value.receipt.changed
        assert value.receipt.error_code == "test_intent_unavailable"
        assert calls == 1 and not tree.exists()
        assert writer.apply(change, record_intent=forbidden) == value


def test_real_noreplace_race_preserves_competing_file_and_never_retries(tmp_path, monkeypatch):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        native, calls = physical.rename_at, 0

        def compete(*args, **kwargs):
            nonlocal calls
            calls += 1
            assert not kwargs.get("exchange", False)
            tree.write_bytes(b"competing exact bytes\0\xff")
            native(*args, **kwargs)

        monkeypatch.setattr(physical, "rename_at", compete)
        value = writer.apply(change, record_intent=lambda _value: None)
        # 已进入系统调用的错误保守保留unknown；真实竞争者必须保持且不能被覆盖。
        assert value.receipt.status == "uncertain"
        assert tree.read_bytes() == b"competing exact bytes\0\xff"
        assert (
            tree.parent / ("._krim-tx-" + TOKEN) / "payload/sub/raw.bin"
        ).read_bytes() == b"\0\xff"
        assert writer.apply(change, record_intent=forbidden) == value
        assert calls == 1


@pytest.mark.parametrize("fault", ["aba", "staged-bytes", "revoke", "close-observer"])
def test_last_authority_cannot_publish_changed_material_or_stale_observation(tmp_path, fault):
    with original(tmp_path) as (_workspace, reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        calls = 0

        def changed():
            nonlocal calls
            calls += 1
            if calls != 3:
                return
            if fault == "aba":
                tree.mkdir()
                tree.rmdir()
            elif fault == "staged-bytes":
                (tree.parent / ("._krim-tx-" + TOKEN) / "payload/sub/raw.bin").write_bytes(
                    b"\x01\xfe"
                )
            elif fault == "revoke":
                raise ServiceError("test_write_revoked", "最后写权限撤销", 403)
            else:
                reader.close()

        result = writer.apply(change, record_intent=lambda _value: None, authorize=changed)
        assert calls == 3
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert not tree.exists()
        assert writer.apply(change, record_intent=forbidden) == result


def test_post_namespace_changed_bytes_remain_unknown_and_are_not_repaired(tmp_path, monkeypatch):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        native = physical.rename_at

        def changed(*args, **kwargs):
            native(*args, **kwargs)
            (tree / "sub/raw.bin").write_bytes(b"post-external")

        monkeypatch.setattr(physical, "rename_at", changed)
        result = writer.apply(change, record_intent=lambda _value: None)
        assert result.receipt.status == "uncertain"
        monkeypatch.setattr(physical, "rename_at", forbidden)
        monkeypatch.setattr(writer.base, "_write_tree", forbidden)
        assert writer.reconcile(change).receipt.status == "uncertain"
        assert (tree / "sub/raw.bin").read_bytes() == b"post-external"


def test_fake_namespace_success_never_creates_a_done_receipt(tmp_path, monkeypatch):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        calls = 0

        def fake(*_args, **_kwargs):
            nonlocal calls
            calls += 1

        monkeypatch.setattr(physical, "rename_at", fake)
        result = writer.apply(change, record_intent=lambda _value: None)
        assert result.receipt.status == "uncertain"
        assert writer.reconcile(change).receipt.status == "uncertain"
        assert writer.apply(change, record_intent=forbidden).receipt.status == "uncertain"
        assert not tree.exists() and calls == 1


def test_returned_effect_mutation_cannot_rewrite_original_cached_receipt(tmp_path):
    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        result = writer.apply(change, record_intent=lambda _value: None)
        digest = result.receipt.after_fingerprint
        object.__setattr__(result.receipt, "status", "uncertain")
        object.__setattr__(result.observed_after.entries[0].directory, "mode", 0o700)
        cached = writer.apply(change, record_intent=forbidden)
        assert cached.receipt.status == "done" and cached.receipt.after_fingerprint == digest
        assert cached.observed_after.entries[0].directory.mode == 0o750
        assert tree.stat().st_mode & 0o777 == 0o750


def test_falsey_cancel_and_invalid_callbacks_do_not_bypass_or_poison_context(tmp_path):
    class Cancel:
        def __bool__(self):
            return False

        def __call__(self):
            return True

    with original(tmp_path) as (_workspace, _reader, writer, target, before, tree):
        change = writer.prepare(target, before, token=TOKEN)
        with pytest.raises(ServiceError) as invalid:
            writer.apply(change, record_intent=forbidden, cancelled=False)
        assert invalid.value.code == "workspace_missing_directory_restore_context"
        result = writer.apply(change, record_intent=forbidden, cancelled=Cancel())
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert not tree.exists()


@pytest.mark.parametrize("point", ["intent", "effect"])
def test_actual_sigkill_then_new_process_preserves_material_and_unknown_zero_replay(
    tmp_path, point
):
    script = Path(__file__).parent / "fixtures/missing_directory_restore_process.py"
    process = subprocess.Popen(
        [sys.executable, "-I", str(script), "produce", point, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=10), "生产者必须到达原暂停点"
        line = process.stdout.readline()
        if not line:
            _out, errors = process.communicate(timeout=2)
            pytest.fail(errors.decode())
        signal = json.loads(line)
        assert signal == {"pid": process.pid, "point": point}
        selected = tmp_path / (
            "files/tree" if point == "effect" else "files/._krim-tx-" + "c" * 32 + "/payload"
        )
        inode, binary_inode = selected.stat().st_ino, (selected / "raw.bin").stat().st_ino
        process.kill()
        _out, errors = process.communicate(timeout=5)
        assert process.returncode == -9, errors.decode()
        restored = subprocess.run(
            [sys.executable, "-I", str(script), "restore", point, str(tmp_path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        result = json.loads(restored.stdout)
        assert result["status"] == "uncertain" and result["replays"] == 0
        assert result["root_inode"] == inode and result["binary_inode"] == binary_inode
        assert result["binary"] == "00ff"
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
