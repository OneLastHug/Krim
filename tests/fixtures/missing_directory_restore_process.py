"""真正缺失发布前后SIGKILL；仅合成可信私有材料，不作为公开序列化协议。"""

import json
import pickle
import shutil
import sys
from pathlib import Path

import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


def produce(root: Path, point: str) -> None:
    files = root / "files"
    tree = files / "tree"
    (tree / "empty").mkdir(parents=True)
    (tree / "raw.bin").write_bytes(b"\0\xff")
    (tree / "raw.bin").chmod(0o640)
    tree.chmod(0o750)
    with Workspace(files, writable=True) as workspace:
        target = WorkspaceTreeSnapshotReader(workspace).capture("tree")
        shutil.rmtree(tree)
        with WorkspaceDirectoryAbsenceReader(workspace) as reader:
            before = reader.capture("tree")
            writer = WorkspaceMissingDirectoryRestoreWriter(workspace, reader)
            value = writer.prepare(target, before, token="c" * 32)
            # pickle只跨本测试自身父子进程传递可信合成材料，产品不读取此格式。
            (root / "prepared.pickle").write_bytes(pickle.dumps(value))

            def signal() -> None:
                import os

                print(json.dumps({"pid": os.getpid(), "point": point}), flush=True)
                sys.stdin.readline()

            native = physical.rename_at

            def published(*args, **kwargs):
                native(*args, **kwargs)
                signal()

            if point == "effect":
                physical.rename_at = published
            writer.apply(
                value, record_intent=lambda _value: signal() if point == "intent" else None
            )
            raise AssertionError("父进程必须在原暂停点SIGKILL生产者")


def restore(root: Path, point: str) -> None:
    with Workspace(root / "files", writable=True) as workspace:
        with WorkspaceDirectoryAbsenceReader(workspace) as reader:
            value = pickle.loads((root / "prepared.pickle").read_bytes())
            writer = WorkspaceMissingDirectoryRestoreWriter(workspace, reader)
            calls = []

            def forbidden(*_args, **_kwargs):
                calls.append(True)
                raise AssertionError("冷恢复不得生产完整材料或再次发布")

            physical.rename_at = forbidden
            writer.base._write_tree = forbidden
            writer.base._capture = forbidden
            WorkspaceTreeSnapshotReader.capture = forbidden
            WorkspaceTreeSnapshotReader.verify = forbidden
            WorkspaceTreeSnapshotReader._observe = forbidden
            result = writer.reconcile(value)
            assert result.receipt.status == "uncertain"
            assert result.receipt.error_code == "workspace_missing_directory_restore_unverifiable"
            assert not calls
            tree = root / "files/tree"
            payload = root / ("files/._krim-tx-" + "c" * 32) / "payload"
            selected = payload if point == "intent" else tree
            assert (selected / "raw.bin").read_bytes() == b"\0\xff"
            assert (selected / "raw.bin").stat().st_mode & 0o777 == 0o640
            assert selected.stat().st_mode & 0o777 == 0o750
            assert tree.exists() == (point == "effect")
            assert payload.exists() == (point == "intent")
            print(
                json.dumps(
                    {
                        "status": result.receipt.status,
                        "before": result.receipt.before_absence_fingerprint,
                        "target": result.receipt.target_fingerprint,
                        "root_inode": selected.stat().st_ino,
                        "binary_inode": (selected / "raw.bin").stat().st_ino,
                        "binary": "00ff",
                        "replays": len(calls),
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    action, point, folder = sys.argv[1:]
    assert point in {"intent", "effect"}
    (produce if action == "produce" else restore)(Path(folder), point)
