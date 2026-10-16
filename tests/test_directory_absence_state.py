"""真实缺失、父版本和无副作用边界；不把私有观察当公开恢复已完成。"""

import json
import os
import subprocess
import sys
import time
from dataclasses import FrozenInstanceError, asdict, replace

import pytest

from krim_agent import workspace_directory_absence as absence_module
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_absence import (
    WorkspaceDirectoryAbsence,
    WorkspaceDirectoryAbsenceReader,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要原Linux物理锁")


def fd_count():
    return len(os.listdir("/proc/self/fd"))


def test_absent_read_cold_history_and_lost_epoch_refusal_keep_zero_filesystem_effect(
    tmp_path, monkeypatch
):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("缺失观察不得生产或验证完整树内容")

    for method in ("capture", "verify", "_observe"):
        monkeypatch.setattr(absence_module.WorkspaceTreeSnapshotReader, method, forbidden)
    (tmp_path / "parent").mkdir()
    (tmp_path / "parent/raw.bin").write_bytes(b"\0\xff")
    parent = tmp_path / "parent"
    before = parent.stat()
    with Workspace(tmp_path, writable=False) as workspace:
        descriptors = fd_count()
        with WorkspaceDirectoryAbsenceReader(workspace) as reader:
            value = reader.capture("parent/missing")
            assert value == reader.capture("parent/missing")
            reader.verify(value)
            assert fd_count() == descriptors + 1
            assert value.parent.identity == (before.st_dev, before.st_ino)
            assert value.parent.mtime_ns == before.st_mtime_ns
            assert value.parent.ctime_ns == before.st_ctime_ns
            assert value.workspace_fingerprint == workspace.fingerprint
            script = """
import json,sys
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_absence import (
    DirectoryAbsenceParent, WorkspaceDirectoryAbsence, WorkspaceDirectoryAbsenceReader,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
def forbidden(*_args, **_kwargs):
    raise AssertionError('冷缺失观察不得生产或验证完整树内容')
for method in ('capture','verify','_observe'):
    setattr(WorkspaceTreeSnapshotReader,method,forbidden)
data=json.loads(sys.argv[2])
data['parent']=DirectoryAbsenceParent(**data['parent'])
old=WorkspaceDirectoryAbsence(**data)
with Workspace(sys.argv[1]) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
    try:
        reader.verify(old)
    except ServiceError as error:
        code=error.code
    else:
        raise AssertionError('新进程不得冒领原观察epoch')
    fresh=reader.capture('parent/missing')
    print(json.dumps({'old_fingerprint':old.fingerprint, 'fresh_fingerprint':fresh.fingerprint,
                      'parent':fresh.parent.metadata(), 'error':code}))
"""
            child = subprocess.run(
                [sys.executable, "-I", "-c", script, str(tmp_path), json.dumps(asdict(value))],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
            cold = json.loads(child.stdout)
            assert cold["old_fingerprint"] == value.fingerprint
            assert cold["fresh_fingerprint"] != value.fingerprint
            assert cold["parent"] == value.parent.metadata()
            assert cold["error"] == "workspace_directory_absence_unverifiable"
            reader.verify(value)
            with pytest.raises(FrozenInstanceError):
                value.root_path = "other"
        assert fd_count() == descriptors
    assert not (parent / "missing").exists()
    assert sorted(path.name for path in parent.iterdir()) == ["raw.bin"]
    assert (parent / "raw.bin").read_bytes() == b"\0\xff"
    after = parent.stat()
    assert (after.st_ino, after.st_mtime_ns, after.st_ctime_ns) == (
        before.st_ino,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )


@pytest.mark.parametrize("kind", ["file", "empty-dir", "tree", "symlink", "dangling"])
def test_existing_leaf_is_never_missing_even_for_dangling_link(tmp_path, kind):
    leaf = tmp_path / "leaf"
    if kind == "file":
        leaf.write_bytes(b"\0\xff")
    elif kind in {"empty-dir", "tree"}:
        leaf.mkdir()
        if kind == "tree":
            (leaf / "data").write_text("retained")
    else:
        (tmp_path / "other").mkdir()
        leaf.symlink_to(tmp_path / ("other" if kind == "symlink" else "not-created"))
    inode = leaf.lstat().st_ino
    with Workspace(tmp_path) as workspace:
        with pytest.raises(ServiceError) as denied:
            WorkspaceDirectoryAbsenceReader(workspace).capture("leaf")
        assert denied.value.code == "workspace_directory_absence_present"
    assert leaf.lstat().st_ino == inode


@pytest.mark.parametrize("change", ["create", "aba", "sibling", "mode", "replace-parent"])
def test_original_absence_rejects_namespace_aba_and_parent_changes(tmp_path, change):
    parent = tmp_path / "parent"
    parent.mkdir()
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceDirectoryAbsenceReader(workspace)
        before = reader.capture("parent/missing")
        if change in {"create", "aba"}:
            (parent / "missing").write_bytes(b"new")
            if change == "aba":
                (parent / "missing").unlink()
        elif change == "sibling":
            (parent / "sibling").mkdir()
        elif change == "mode":
            parent.chmod(0o700 if before.parent.mode != 0o700 else 0o750)
        else:
            parent.rename(tmp_path / "retained-parent")
            parent.mkdir()
        with pytest.raises(ServiceError):
            reader.verify(before)


def test_existing_sibling_content_is_not_claimed_as_frozen_parent_namespace(tmp_path):
    (tmp_path / "data.bin").write_bytes(b"before")
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceDirectoryAbsenceReader(workspace)
        before = reader.capture("missing")
        (tmp_path / "data.bin").write_bytes(b"after\0\xff")
        reader.verify(before)
    assert (tmp_path / "data.bin").read_bytes() == b"after\0\xff"


@pytest.mark.parametrize("path", ["../outside", "/absolute", "a/../b", ".env/item", "a\\b", ""])
def test_unsafe_path_is_rejected_without_creating_any_target(tmp_path, path):
    with Workspace(tmp_path) as workspace:
        with pytest.raises(ServiceError):
            WorkspaceDirectoryAbsenceReader(workspace).capture(path)
    assert list(tmp_path.iterdir()) == []


def test_missing_and_symlink_parent_are_not_target_absence(tmp_path):
    (tmp_path / "original").mkdir()
    (tmp_path / "link").symlink_to(tmp_path / "original", target_is_directory=True)
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceDirectoryAbsenceReader(workspace)
        for path in ("not-created/leaf", "link/leaf"):
            with pytest.raises(ServiceError):
                reader.capture(path)
    assert list((tmp_path / "original").iterdir()) == []


@pytest.mark.parametrize("change", ["create", "aba", "replace-parent", "revoke"])
def test_last_authority_callback_change_cannot_return_stale_absence(tmp_path, change):
    parent = tmp_path / "parent"
    parent.mkdir()
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceDirectoryAbsenceReader(workspace)
        calls = 0

        def qualify():
            nonlocal calls
            calls += 1
            if calls != 2:
                return
            if change in {"create", "aba"}:
                (parent / "missing").write_text("external")
                if change == "aba":
                    (parent / "missing").unlink()
            elif change == "replace-parent":
                parent.rename(tmp_path / "retained")
                parent.mkdir()
            else:
                raise ServiceError("test_revoked", "原授权撤销", 403)

        descriptors = fd_count()
        with pytest.raises(ServiceError):
            reader.capture("parent/missing", authorize=qualify)
        assert calls == 2
        assert fd_count() == descriptors


def test_cancel_and_real_authority_delay_keep_original_physical_deadline(tmp_path):
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceDirectoryAbsenceReader(workspace, lock_timeout=0.02)
        with pytest.raises(ServiceError) as cancelled:
            reader.capture("missing", cancelled=lambda: True)
        assert cancelled.value.code == "workspace_directory_absence_cancelled"
        with pytest.raises(ServiceError) as late:
            reader.capture("missing", authorize=lambda: time.sleep(0.04))
        assert late.value.code == "workspace_directory_absence_timeout"
    assert list(tmp_path.iterdir()) == []


def test_closed_workspace_and_different_resource_never_verify_old_absence(tmp_path):
    (tmp_path / "other").mkdir()
    with Workspace(tmp_path) as workspace, Workspace(tmp_path / "other") as other:
        reader = WorkspaceDirectoryAbsenceReader(workspace)
        value = reader.capture("missing")
        with pytest.raises(ServiceError) as mismatch:
            WorkspaceDirectoryAbsenceReader(other).verify(value)
        assert mismatch.value.code == "workspace_directory_absence_material"
        workspace.close()
        with pytest.raises(ServiceError):
            reader.verify(value)


@pytest.mark.parametrize("field,value", [("inode", True), ("mode", -1), ("nlink", 0)])
def test_broken_frozen_parent_cannot_be_trusted_material(tmp_path, field, value):
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceDirectoryAbsenceReader(workspace)
        before = reader.capture("missing")
        object.__setattr__(before.parent, field, value)
        with pytest.raises(ServiceError) as denied:
            reader.verify(before)
        assert denied.value.code == "workspace_directory_absence_material"


def test_strict_identity_material_and_no_falsey_callback_downgrade(tmp_path):
    class FalseyCallable:
        def __bool__(self):
            return False

        def __call__(self):
            return True

    with Workspace(tmp_path) as workspace:
        reader = WorkspaceDirectoryAbsenceReader(workspace)
        before = reader.capture("missing")
        with pytest.raises(ValueError):
            replace(before, workspace_fingerprint="wrong")
        with pytest.raises(ServiceError) as denied:
            reader.capture("missing", cancelled=FalseyCallable())
        assert denied.value.code == "workspace_directory_absence_cancelled"
        for invalid in (False, {}, []):
            with pytest.raises(ServiceError):
                reader.capture("missing", cancelled=invalid)
        with pytest.raises(ServiceError):
            reader.verify({})
        assert type(before) is WorkspaceDirectoryAbsence


def test_managed_watch_capacity_idempotent_release_and_stale_release_cannot_close_new(tmp_path):
    import fcntl

    with Workspace(tmp_path) as workspace:
        descriptors = fd_count()
        with WorkspaceDirectoryAbsenceReader(workspace, max_observations=2) as reader:
            first, second = reader.capture("first"), reader.capture("second")
            assert fd_count() == descriptors + 2
            for observation in reader._observations.values():
                descriptor = observation.watch.descriptor
                assert fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_NONBLOCK
                assert fcntl.fcntl(descriptor, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
            with pytest.raises(ServiceError) as full:
                reader.capture("third")
            assert full.value.code == "workspace_directory_absence_limit"
            reader.verify(second)
            assert reader.release(first)
            assert not reader.release(first)
            fresh = reader.capture("first")
            assert fresh.fingerprint != first.fingerprint
            assert not reader.release(first)
            reader.verify(fresh)
            with pytest.raises(ServiceError) as stale:
                reader.verify(first)
            assert stale.value.code == "workspace_directory_absence_unverifiable"
        assert fd_count() == descriptors
        reader.close()
        assert not reader.release(fresh)
        with pytest.raises(ServiceError):
            reader.capture("after-close")


@pytest.mark.parametrize("fault", ["overflow", "empty", "io"])
def test_lost_or_overflow_watch_never_silently_falls_back_to_stat(tmp_path, monkeypatch, fault):
    import struct

    with Workspace(tmp_path) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
        value = reader.capture("missing")
        descriptor = reader._observations["missing"].watch.descriptor
        original = os.read

        def broken(fd, size):
            if fd == descriptor:
                if fault == "io":
                    raise OSError("synthetic watch loss")
                return struct.pack("iIII", -1, 0x4000, 0, 0) if fault == "overflow" else b""
            return original(fd, size)

        monkeypatch.setattr(absence_module.os, "read", broken)
        with pytest.raises(ServiceError) as lost:
            reader.verify(value)
        assert lost.value.code == "workspace_directory_absence_conflict"
        assert not reader._observations
        with pytest.raises(ServiceError) as again:
            reader.verify(value)
        assert again.value.code == "workspace_directory_absence_unverifiable"
        assert not (tmp_path / "missing").exists()


def test_watch_capability_failure_rejects_without_stat_only_or_descriptor_leak(
    tmp_path, monkeypatch
):
    def unavailable(_parent):
        raise ServiceError("workspace_directory_absence_watch_unavailable", "合成缺失能力", 409)

    monkeypatch.setattr(absence_module, "_watch_fd", unavailable)
    with Workspace(tmp_path) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
        descriptors = fd_count()
        with pytest.raises(ServiceError) as denied:
            reader.capture("missing")
        assert denied.value.code == "workspace_directory_absence_watch_unavailable"
        assert fd_count() == descriptors
        assert not reader._observations


@pytest.mark.parametrize("magic", [0x6969, 0x65735546, 0x794C7630, 0])
def test_remote_userspace_overlay_or_unknown_filesystem_is_not_assumed_observable(
    tmp_path, monkeypatch, magic
):
    monkeypatch.setattr(absence_module, "_filesystem_kind", lambda _library, _parent: magic)
    with Workspace(tmp_path) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
        descriptors = fd_count()
        with pytest.raises(ServiceError) as denied:
            reader.capture("missing")
        assert denied.value.code == "workspace_directory_absence_watch_filesystem"
        assert fd_count() == descriptors
        assert not reader._observations


def test_close_during_last_qualify_reclaims_unpublished_watch_and_rejects(tmp_path):
    with Workspace(tmp_path) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
        descriptors, calls = fd_count(), 0

        def close():
            nonlocal calls
            calls += 1
            if calls == 2:
                reader.close()

        with pytest.raises(ServiceError) as closed:
            reader.capture("missing", authorize=close)
        assert closed.value.code == "workspace_directory_absence_closed"
        assert calls == 2
        assert fd_count() == descriptors
        assert not reader._observations


def test_other_thread_can_close_during_qualify_without_leaking_unpublished_watch(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    with Workspace(tmp_path) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
        descriptors, calls = fd_count(), 0
        entered, release = Event(), Event()

        def qualify():
            nonlocal calls
            calls += 1
            if calls == 2:
                entered.set()
                assert release.wait(1), "测试必须释放原授权回调"

        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(reader.capture, "missing", authorize=qualify)
            try:
                assert entered.wait(1)
                reader.close()
            finally:
                release.set()
            with pytest.raises(ServiceError) as closed:
                pending.result(timeout=2)
            assert closed.value.code == "workspace_directory_absence_closed"
        assert calls == 2
        assert fd_count() == descriptors
        assert not reader._observations


def test_fork_child_cannot_reuse_or_drain_original_process_observation(tmp_path):
    with Workspace(tmp_path) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
        original = reader.capture("missing")
        child = os.fork()
        if child == 0:
            # 子进程仍是合成外部命名空间变化；不能消费父进程原观察队列。
            try:
                try:
                    reader.verify(original)
                except ServiceError as error:
                    outcome = 0 if error.code == "workspace_directory_absence_process" else 2
                else:
                    outcome = 3
                (tmp_path / "missing").write_bytes(b"brief external child")
                (tmp_path / "missing").unlink()
                try:
                    reader.verify(original)
                except ServiceError as error:
                    if error.code != "workspace_directory_absence_process":
                        outcome = max(outcome, 2)
                else:
                    outcome = 3
            except BaseException:
                outcome = 4
            os._exit(outcome)
        _pid, status = os.waitpid(child, 0)
        try:
            reader.verify(original)
        except ServiceError as error:
            parent_error = error.code
        else:
            parent_error = None
        assert (os.waitstatus_to_exitcode(status), parent_error) == (
            0,
            "workspace_directory_absence_conflict",
        ), "fork子进程不得冒领或消费父观察，父进程仍须发现原ABA"
        assert not (tmp_path / "missing").exists()


def test_fork_child_close_avoids_inherited_lock_and_preserves_parent_watch(tmp_path):
    import signal
    from threading import Event, Thread

    with Workspace(tmp_path) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
        original = reader.capture("missing")
        held, release = Event(), Event()

        def hold():
            with reader._state_lock:
                held.set()
                assert release.wait(3), "测试必须释放原父线程锁"

        worker = Thread(target=hold)
        worker.start()
        assert held.wait(1)
        child = os.fork()
        if child == 0:
            try:
                reader.close()
                for operation in (
                    lambda: reader.capture("missing"),
                    lambda: reader.verify(original),
                    lambda: reader.release(original),
                ):
                    try:
                        operation()
                    except ServiceError as error:
                        if error.code != "workspace_directory_absence_process":
                            os._exit(2)
                    else:
                        os._exit(3)
            except BaseException:
                os._exit(4)
            os._exit(0)
        deadline, status = time.monotonic() + 2, None
        try:
            while time.monotonic() < deadline:
                found, result = os.waitpid(child, os.WNOHANG)
                if found == child:
                    status = result
                    break
                time.sleep(0.005)
            if status is None:
                os.kill(child, signal.SIGKILL)
                os.waitpid(child, 0)
                pytest.fail("子进程close不得等待已不存在的父线程锁")
        finally:
            release.set()
            worker.join(timeout=1)
        assert not worker.is_alive()
        assert os.waitstatus_to_exitcode(status) == 0
        reader.verify(original)


def test_valid_looking_returned_material_mutation_does_not_rewrite_private_epoch(tmp_path):
    with Workspace(tmp_path) as workspace, WorkspaceDirectoryAbsenceReader(workspace) as reader:
        original = reader.capture("missing")
        returned = reader.capture("missing")
        assert returned == original and returned.parent is not original.parent
        object.__setattr__(returned.parent, "mtime_ns", original.parent.mtime_ns + 1)
        with pytest.raises(ServiceError) as mutated:
            reader.verify(returned)
        assert mutated.value.code == "workspace_directory_absence_unverifiable"
        reader.verify(original)


@pytest.mark.parametrize("limit", [False, 0, 129, 2.0])
def test_watch_limit_cannot_disable_or_expand_original_resource_bound(tmp_path, limit):
    with Workspace(tmp_path) as workspace:
        with pytest.raises(ValueError):
            WorkspaceDirectoryAbsenceReader(workspace, max_observations=limit)
