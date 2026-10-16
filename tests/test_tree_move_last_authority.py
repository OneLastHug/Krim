"""最后授权可正常返回但改变原树；发布前拒绝且不改写真实保留事实。"""

import os
import sys
import time

import pytest
from test_nonempty_tree_writer import prepare
from test_workspace_tree_snapshot import seed

import krim_agent.workspace_tree_writer as physical
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="原Linux renameat2")


@pytest.mark.parametrize("phase", ["apply", "keep"])
@pytest.mark.parametrize("fault", ["bytes", "restored-mtime", "mode", "file-inode", "root-inode"])
def test_last_authorization_cannot_move_changed_tree_or_attest_stale_keep(
    tmp_path, monkeypatch, phase, fault
):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change) if phase == "keep" else None
        if effect is not None:
            assert effect.receipt.status == "done"
        root = tmp_path / ("dest/tree" if phase == "keep" else "tree")
        data = root / "sub/data.bin"
        original_inode = data.stat().st_ino
        authorize_calls = []
        namespace_calls = []
        rename = physical.rename_at

        def recorded(*args, **kwargs):
            namespace_calls.append(True)
            return rename(*args, **kwargs)

        def authorize():
            authorize_calls.append(True)
            if len(authorize_calls) != (3 if phase == "apply" else 2):
                return
            if fault == "bytes":
                data.write_bytes(b"bad")
            elif fault == "restored-mtime":
                info = data.stat()
                data.write_bytes(b"bad")
                os.utime(data, ns=(info.st_atime_ns, info.st_mtime_ns))
            elif fault == "mode":
                (root / "sub").chmod(0o711)
            elif fault == "file-inode":
                alternate = root / "sub/alternate"
                alternate.write_bytes(data.read_bytes())
                alternate.chmod(data.stat().st_mode & 0o7777)
                alternate.replace(data)
                assert data.stat().st_ino != original_inode
            else:
                root.rename(root.with_name("retained-original"))
                root.mkdir()
                (root / "new-user-file").write_bytes(b"keep this version")

        monkeypatch.setattr(physical, "rename_at", recorded)
        result = (
            writer.apply(change, authorize=authorize)
            if phase == "apply"
            else writer.verify_keep(change, effect, authorize=authorize)
        )
        assert len(authorize_calls) == (3 if phase == "apply" else 2)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == "workspace_tree_move_conflict"
        assert namespace_calls == []
        assert root.is_dir()
        assert not (tmp_path / ("tree" if phase == "keep" else "dest/tree")).exists()
        if fault in {"bytes", "restored-mtime"}:
            assert data.read_bytes() == b"bad"
        if fault == "root-inode":
            assert (root / "new-user-file").read_bytes() == b"keep this version"
            assert (
                root.with_name("retained-original") / "sub/data.bin"
            ).read_bytes() == b"\0\xff\x80"


@pytest.mark.parametrize("phase", ["apply", "keep"])
def test_falsey_callable_cancellation_remains_authoritative(tmp_path, monkeypatch, phase):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()

    class Cancelled:
        def __bool__(self):
            return False

        def __call__(self):
            return True

    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change) if phase == "keep" else None

        def forbidden(*_args, **_kwargs):
            pytest.fail("已取消不能执行第二个namespace效果")

        monkeypatch.setattr(physical, "rename_at", forbidden)
        result = (
            writer.apply(change, cancelled=Cancelled())
            if phase == "apply"
            else writer.verify_keep(change, effect, cancelled=Cancelled())
        )
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == "workspace_write_cancelled"
        assert (tmp_path / ("dest/tree" if phase == "keep" else "tree")).is_dir()


@pytest.mark.parametrize("phase", ["apply", "keep"])
def test_last_authorization_delay_keeps_original_physical_deadline(tmp_path, monkeypatch, phase):
    seed(tmp_path)
    (tmp_path / "dest").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change) if phase == "keep" else None
        # 准备沿原默认5秒；仅被测发布/保留缩至0.02秒，原0.04秒阻塞不变。
        writer.lock_timeout = 0.02
        calls = []

        def authorize():
            calls.append(True)
            if len(calls) == (3 if phase == "apply" else 2):
                time.sleep(0.04)

        def forbidden(*_args, **_kwargs):
            pytest.fail("原物理期限已过，不能发布namespace")

        monkeypatch.setattr(physical, "rename_at", forbidden)
        result = (
            writer.apply(change, authorize=authorize)
            if phase == "apply"
            else writer.verify_keep(change, effect, authorize=authorize)
        )
        assert len(calls) == (3 if phase == "apply" else 2)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == "workspace_tree_move_timeout"
        assert (tmp_path / ("dest/tree" if phase == "keep" else "tree")).is_dir()
