"""真实已有全树版本、原字节、两轮竞争与有界拒绝，不执行树写入。"""

import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要原Linux物理锁")


def seed(root):
    (root / "tree/sub/空目录").mkdir(parents=True)
    (root / "tree/empty.txt").write_bytes(b"")
    (root / "tree/sub/data.bin").write_bytes(b"\0\xff\x80")
    (root / "tree/sub/text.txt").write_text("目录树原字节\n")
    (root / "tree").chmod(0o750)
    (root / "tree/sub").chmod(0o700)
    (root / "tree/sub/data.bin").chmod(0o640)


def fd_count():
    return len(os.listdir("/proc/self/fd"))


def test_full_read_only_tree_snapshot_keeps_bytes_modes_identity_and_stable_fingerprint(tmp_path):
    seed(tmp_path)
    with Workspace(tmp_path, writable=False) as workspace:
        reader = WorkspaceTreeSnapshotReader(workspace)
        descriptors = fd_count()
        first = reader.capture("tree")
        assert first == reader.capture("tree")
        assert first.fingerprint == reader.capture("tree").fingerprint
        reader.verify(first)
        assert fd_count() == descriptors
        assert first.parent_identity == (tmp_path.stat().st_dev, tmp_path.stat().st_ino)
        assert first.workspace_fingerprint == workspace.fingerprint
        entries = {entry.path: entry for entry in first.entries}
        assert len(entries) == 6 and entries["tree"].directory.mode == 0o750
        assert entries["tree/sub"].directory.mode == 0o700
        assert entries["tree/sub/空目录"].directory.empty
        assert entries["tree/sub/data.bin"].file.data == b"\0\xff\x80"
        assert entries["tree/sub/data.bin"].file.mode == 0o640
        assert entries["tree/empty.txt"].file.data == b""
        assert first.total_bytes == 3 + len("目录树原字节\n".encode())
        assert "data" not in entries["tree/sub/data.bin"].metadata()
        with pytest.raises(FrozenInstanceError):
            first.root_path = "other"


@pytest.mark.parametrize("change", ["bytes", "mode", "add", "delete", "identity"])
def test_verify_refuses_any_original_node_change(tmp_path, change):
    seed(tmp_path)
    file = tmp_path / "tree/sub/data.bin"
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceTreeSnapshotReader(workspace)
        snapshot = reader.capture("tree")
        if change == "bytes":
            file.write_bytes(b"changed")
        elif change == "mode":
            file.chmod(0o600)
        elif change == "add":
            (tmp_path / "tree/new.txt").write_text("new")
        elif change == "delete":
            file.unlink()
        else:
            file.rename(tmp_path / "retained-original.bin")
            file.write_bytes(b"\0\xff\x80")
            file.chmod(0o640)
        with pytest.raises(ServiceError) as denied:
            reader.verify(snapshot)
        assert denied.value.code == "workspace_tree_snapshot_changed"


def test_root_mtime_does_not_hide_file_change_between_complete_observations(tmp_path, monkeypatch):
    seed(tmp_path)
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceTreeSnapshotReader(workspace)
        original = reader._observe
        observed = 0
        root_mtime = (tmp_path / "tree").stat().st_mtime_ns
        descriptors = fd_count()

        def racing(*args):
            nonlocal observed
            result = original(*args)
            observed += 1
            if observed == 1:
                (tmp_path / "tree/sub/data.bin").write_bytes(b"changed after first full tree")
            return result

        monkeypatch.setattr(reader, "_observe", racing)
        with pytest.raises(ServiceError) as denied:
            reader.capture("tree")
        assert denied.value.code == "workspace_tree_snapshot_changed" and observed == 2
        assert (tmp_path / "tree").stat().st_mtime_ns == root_mtime
        assert fd_count() == descriptors


def test_replaced_parent_entry_cannot_return_snapshot_of_detached_tree(tmp_path, monkeypatch):
    base = tmp_path / "base"
    base.mkdir()
    seed(base)
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceTreeSnapshotReader(workspace)
        original = reader._observe
        observed = 0
        descriptors = fd_count()

        def racing(*args):
            nonlocal observed
            result = original(*args)
            observed += 1
            if observed == 1:
                base.rename(tmp_path / "original-base")
                base.mkdir()
                shutil.copytree(tmp_path / "original-base/tree", base / "tree")
            return result

        monkeypatch.setattr(reader, "_observe", racing)
        with pytest.raises(ServiceError) as denied:
            reader.capture("base/tree")
        assert denied.value.code == "workspace_parent_changed"
        assert fd_count() == descriptors


@pytest.mark.parametrize(
    "kind", ["symlink-file", "symlink-directory", "hardlink", "fifo", "sensitive"]
)
def test_untrusted_descendant_closes_whole_snapshot_instead_of_skipping(tmp_path, kind):
    seed(tmp_path)
    if kind == "symlink-file":
        (tmp_path / "outside.txt").write_text("outside")
        (tmp_path / "tree/outside.txt").symlink_to(tmp_path / "outside.txt")
    elif kind == "symlink-directory":
        (tmp_path / "outside").mkdir()
        (tmp_path / "tree/outside").symlink_to(tmp_path / "outside", target_is_directory=True)
    elif kind == "hardlink":
        os.link(tmp_path / "tree/sub/data.bin", tmp_path / "outside.bin")
    elif kind == "fifo":
        os.mkfifo(tmp_path / "tree/pipe")
    else:
        (tmp_path / "tree/.env").write_text("synthetic secret")
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceTreeSnapshotReader(workspace)
        descriptors = fd_count()
        with pytest.raises(ServiceError) as denied:
            reader.capture("tree")
        assert denied.value.code == (
            "workspace_file_type"
            if kind == "fifo"
            else "workspace_sensitive"
            if kind == "sensitive"
            else "workspace_link"
        )
        assert fd_count() == descriptors


@pytest.mark.parametrize(
    "options,code",
    [
        ({"max_entries": 5}, "workspace_tree_snapshot_limit"),
        ({"max_total_bytes": 2}, "workspace_tree_snapshot_bytes"),
        ({"max_depth": 2}, "workspace_tree_snapshot_depth"),
    ],
)
def test_capacity_rejection_keeps_all_files_and_releases_original_lock(tmp_path, options, code):
    seed(tmp_path)
    with Workspace(tmp_path) as workspace:
        descriptors = fd_count()
        with pytest.raises(ServiceError) as denied:
            WorkspaceTreeSnapshotReader(workspace, **options).capture("tree")
        assert denied.value.code == code and fd_count() == descriptors
        assert WorkspaceTreeSnapshotReader(workspace).capture("tree").entries
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff\x80"


@pytest.mark.parametrize("path", [".", "/tmp/tree", "../tree", "tree//sub", "tree/./sub"])
def test_root_requires_canonical_authorized_relative_directory(tmp_path, path):
    seed(tmp_path)
    with Workspace(tmp_path) as workspace:
        with pytest.raises(ServiceError) as denied:
            WorkspaceTreeSnapshotReader(workspace).capture(path)
        assert denied.value.code == "workspace_path"


def test_cancellation_and_authorization_failure_release_all_descriptors(tmp_path, monkeypatch):
    seed(tmp_path)
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceTreeSnapshotReader(workspace)
        descriptors = fd_count()
        with pytest.raises(ServiceError) as denied:
            reader.capture("tree", cancelled=lambda: True)
        assert denied.value.code == "workspace_tree_snapshot_cancelled"

        def forbid():
            raise ServiceError("synthetic_authorization_revoked", "合成原资格拒绝", 403)

        with pytest.raises(ServiceError) as denied:
            reader.capture("tree", authorize=forbid)
        assert denied.value.code == "synthetic_authorization_revoked"
        original = reader._observe
        observed = False

        def after_first(*args):
            nonlocal observed
            result = original(*args)
            observed = True
            return result

        monkeypatch.setattr(reader, "_observe", after_first)
        with pytest.raises(ServiceError) as denied:
            reader.capture("tree", cancelled=lambda: observed)
        assert denied.value.code == "workspace_tree_snapshot_cancelled"
        assert fd_count() == descriptors


def test_physical_deadline_is_total_and_releases_lock_after_expiry(tmp_path, monkeypatch):
    seed(tmp_path)
    with Workspace(tmp_path) as workspace:
        reader = WorkspaceTreeSnapshotReader(workspace, lock_timeout=0.02)
        original = reader._observe
        descriptors = fd_count()

        def slow(*args):
            time.sleep(0.03)
            return original(*args)

        monkeypatch.setattr(reader, "_observe", slow)
        with pytest.raises(ServiceError) as denied:
            reader.capture("tree")
        assert denied.value.code == "workspace_tree_snapshot_timeout" and fd_count() == descriptors
        assert WorkspaceTreeSnapshotReader(workspace).capture("tree").entries


def test_other_workspace_cannot_verify_original_tree_even_with_same_bytes(tmp_path):
    seed(tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    seed(other)
    with Workspace(tmp_path) as first, Workspace(other) as second:
        snapshot = WorkspaceTreeSnapshotReader(first).capture("tree")
        with pytest.raises(ServiceError) as denied:
            WorkspaceTreeSnapshotReader(second).verify(snapshot)
        assert denied.value.code == "workspace_binding_changed"


def test_separate_process_keeps_original_full_tree_fingerprint_without_writing(tmp_path):
    seed(tmp_path)
    with Workspace(tmp_path) as workspace:
        snapshot = WorkspaceTreeSnapshotReader(workspace).capture("tree")
        code = (
            "import json,sys;from krim_agent.workspace import Workspace;"
            "from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader;"
            "workspace=Workspace(sys.argv[1]);snapshot=WorkspaceTreeSnapshotReader(workspace).capture('tree');"
            "print(json.dumps({'fingerprint':snapshot.fingerprint,'bytes':snapshot.total_bytes,"
            "'nodes':len(snapshot.entries)}));workspace.close()"
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", code, str(tmp_path)],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        assert json.loads(result.stdout) == {
            "fingerprint": snapshot.fingerprint,
            "bytes": snapshot.total_bytes,
            "nodes": 6,
        }
        WorkspaceTreeSnapshotReader(workspace).verify(snapshot)


def test_remaining_budget_limits_actual_read_if_file_grows_after_original_stat(
    tmp_path, monkeypatch
):
    (tmp_path / "tree").mkdir()
    target = tmp_path / "tree/data.bin"
    target.write_bytes(b"x")
    with Workspace(tmp_path, max_file_bytes=1024) as workspace:
        reader = WorkspaceTreeSnapshotReader(workspace, max_total_bytes=2)
        original_read = os.read
        consumed = 0

        def counted(fd, amount):
            nonlocal consumed
            if consumed == 0:
                target.write_bytes(b"x" * 100)
            data = original_read(fd, amount)
            consumed += len(data)
            return data

        monkeypatch.setattr(os, "read", counted)
        with pytest.raises(ServiceError) as denied:
            reader.capture("tree")
        assert denied.value.code == "workspace_file_limit" and consumed == 3
        assert target.read_bytes() == b"x" * 100


def test_foreign_device_observation_closes_whole_tree_before_reading(tmp_path, monkeypatch):
    seed(tmp_path)
    original = os.stat

    def foreign(path, **kwargs):
        info = original(path, **kwargs)
        if path == "data.bin" and kwargs.get("dir_fd") is not None:
            return SimpleNamespace(st_dev=info.st_dev + 1)
        return info

    with Workspace(tmp_path) as workspace:
        descriptors = fd_count()
        monkeypatch.setattr(os, "stat", foreign)
        with pytest.raises(ServiceError) as denied:
            WorkspaceTreeSnapshotReader(workspace).capture("tree")
        assert denied.value.code == "workspace_tree_snapshot_device"
        assert fd_count() == descriptors


def test_zero_byte_budget_allows_empty_files_and_directories(tmp_path):
    (tmp_path / "tree/empty").mkdir(parents=True)
    (tmp_path / "tree/zero.bin").write_bytes(b"")
    with Workspace(tmp_path) as workspace:
        snapshot = WorkspaceTreeSnapshotReader(workspace, max_total_bytes=0).capture("tree")
        assert len(snapshot.entries) == 3 and snapshot.total_bytes == 0
