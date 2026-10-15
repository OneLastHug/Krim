"""真实目录namespace后SIGKILL；合成材料文件不是产品持久恢复协议。"""

import base64
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import krim_agent
import krim_agent.workspace_directory_restore_writer as module
from krim_agent.persistence.tree_snapshot_codec import decode_snapshot, encode_snapshot
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory_restore_writer import (
    DirectoryRestoreStage,
    PreparedDirectoryRestore,
    WorkspaceDirectoryRestoreWriter,
)

phase, folder = sys.argv[1:3]
root = Path(folder)
with Workspace(root / "files", writable=True) as workspace:
    writer = WorkspaceDirectoryRestoreWriter(workspace)
    if phase == "produce":
        target = writer.reader.capture("holder/live")
        (root / "files/holder/live/sub/raw.bin").write_bytes(b"\x80current")
        before = writer.reader.capture("holder/live")
        change = writer.prepare(target, expected_before=before.fingerprint, token=uuid4().hex)

        def intent(original):
            records = []
            for tree in (original.before, original.staged):
                metadata, blob = encode_snapshot(tree)
                records.append({"metadata": metadata, "blob": base64.b64encode(blob).decode()})
            value = {
                "stage": asdict(original.stage),
                "target_fingerprint": original.target_fingerprint,
                "records": records,
            }
            with (root / "material.json").open("w") as output:
                json.dump(value, output)
                output.flush()
                os.fsync(output.fileno())

        rename = module.rename_at

        def hold(*args, **kwargs):
            rename(*args, **kwargs)
            print(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "token": change.stage.token,
                        "package": str(krim_agent.__file__),
                        "before_inode": change.before.entries[0].directory.inode,
                        "staged_inode": change.staged.entries[0].directory.inode,
                        "namespace_before_receipt": True,
                    }
                ),
                flush=True,
            )
            sys.stdin.readline()
            raise AssertionError("应由宿主真实SIGKILL")

        module.rename_at = hold
        writer.apply(change, record_intent=intent)
        raise AssertionError("必须在原namespace后被强杀")
    else:
        assert phase == "recover"
        value = json.loads((root / "material.json").read_text())
        stage = value["stage"]
        original = DirectoryRestoreStage(
            stage["root_path"],
            stage["token"],
            tuple(stage["parent_identity"]),
            tuple(stage["container_identity"]),
            tuple(stage["container_owner"]),
        )
        snapshots = [
            decode_snapshot(item["metadata"], base64.b64decode(item["blob"], validate=True))
            for item in value["records"]
        ]
        change = PreparedDirectoryRestore(original, value["target_fingerprint"], *snapshots)
        replays = 0

        def forbidden(*_args, **_kwargs):
            global replays
            replays += 1
            raise AssertionError("另进程核实不得再prepare/apply/write/rename")

        module.rename_at = forbidden
        WorkspaceDirectoryRestoreWriter.prepare = forbidden
        WorkspaceDirectoryRestoreWriter.apply = forbidden
        WorkspaceDirectoryRestoreWriter._write_tree = forbidden
        result = writer.reconcile(change)
        assert replays == 0
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "token": original.token,
                    "status": result.receipt.status,
                    "package": str(krim_agent.__file__),
                    "after_inode": result.observed_after.entries[0].directory.inode
                    if result.observed_after
                    else None,
                    "retained_inode": result.retained_before.entries[0].directory.inode
                    if result.retained_before
                    else None,
                    "binary_hex": (root / "files/holder/live/sub/raw.bin").read_bytes().hex(),
                    "replays": replays,
                }
            ),
            flush=True,
        )
