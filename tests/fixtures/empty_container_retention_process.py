"""实际Missing发布后的空容器回收前/后强杀；另进程只读原Stage和目标。"""

import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

import krim_agent
import krim_agent.workspace_directory_restore_writer as full
import krim_agent.workspace_empty_container_retention as physical
import krim_agent.workspace_missing_directory_writer as missing
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_restore_writer import DirectoryRestoreStage
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_empty_container_retention import allow, published  # noqa: E402
from test_missing_directory_writer import original  # noqa: E402

phase, point, folder = sys.argv[1:4]
root = Path(folder)
files = root / "files"


def save(value):
    with (root / "stage.json").open("w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())


if phase == "produce":
    with original(files) as (workspace, _reader, writer, target, before, tree):
        stage = published(writer, target, before)
        core = WorkspaceEmptyContainerRetention(workspace)
        observed = core.inspect(stage, authorize=allow)
        metadata = {
            "stage": asdict(stage),
            "expected_fingerprint": observed.fingerprint,
            "inode": tree.stat().st_ino,
            "binary_hex": (tree / "sub/raw.bin").read_bytes().hex(),
            "producer": os.getpid(),
            "point": point,
        }
        save(metadata)

        def pause():
            print(
                json.dumps(
                    {"pid": os.getpid(), "point": point, "package": str(krim_agent.__file__)}
                ),
                flush=True,
            )
            sys.stdin.readline()
            raise AssertionError("本进程必须被真正SIGKILL")

        def record_intent(value):
            assert value == observed
            metadata["intent_saved"] = True
            save(metadata)
            if point == "before":
                pause()

        rmdir = os.rmdir

        def unlink_container(name, *, dir_fd):
            rmdir(name, dir_fd=dir_fd)
            if point == "after":
                pause()

        physical.os.rmdir = unlink_container
        core.remove(
            stage,
            expected_fingerprint=observed.fingerprint,
            confirmed=True,
            authorize=allow,
            record_intent=record_intent,
        )
        raise AssertionError("回收强杀暂停点未到达")
else:
    metadata = json.loads((root / "stage.json").read_text())
    assert metadata["intent_saved"]
    value = metadata["stage"]
    for key in ("parent_identity", "container_identity", "container_owner"):
        value[key] = tuple(value[key])
    stage = DirectoryRestoreStage(**value)
    replays = []

    def forbidden(*_args, **_kwargs):
        replays.append(True)
        raise AssertionError("只读核对禁止重新发布、删除、读树或新生产")

    physical.os.rmdir = forbidden
    full.rename_at = forbidden
    missing.rename_at = forbidden
    full.WorkspaceDirectoryRestoreWriter.prepare = forbidden
    full.WorkspaceDirectoryRestoreWriter.apply = forbidden
    missing.WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
    missing.WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
    WorkspaceTreeSnapshotReader.capture = forbidden
    with Workspace(files, writable=False) as workspace:
        observed = WorkspaceEmptyContainerRetention(workspace).inspect(stage, authorize=allow)
        assert observed.status == ("empty" if point == "before" else "absent")
        tree = files / stage.root_path
        assert tree.stat().st_ino == metadata["inode"]
        assert (tree / "sub/raw.bin").read_bytes().hex() == metadata["binary_hex"]
        assert not replays
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "producer": metadata["producer"],
                    "point": point,
                    "status": observed.status,
                    "inode": tree.stat().st_ino,
                    "binary_hex": metadata["binary_hex"],
                    "intent_saved": True,
                    "replays": len(replays),
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )
