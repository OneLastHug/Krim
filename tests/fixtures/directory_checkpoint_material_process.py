"""两个真实进程存同键材料；新进程按原键冷读不访问生产者。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import krim_agent
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence import Database
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.providers import FakeProvider
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_writer import WorkspaceWriter


async def main():
    phase, database_url, folder, scope, key = sys.argv[1:]
    database = Database(database_url)
    store = WorkspaceDirectoryCheckpointStore(WorkspaceEditStore(database))
    try:
        await database.initialize()
        if phase == "save":
            with Workspace(Path(folder)) as workspace:
                binding = WorkspaceBinding("files", "1", workspace, "a" * 64)
                snapshot = WorkspaceTreeSnapshotReader(workspace).capture("tree")
                print(
                    json.dumps({"ready": os.getpid(), "snapshot": snapshot.fingerprint}), flush=True
                )
                assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "save"
                material, created = await store.save_captured(
                    scope,
                    binding,
                    DirectoryCheckpointMaterialCreate(root_path="tree", request_key=key),
                    snapshot,
                )
                print(
                    json.dumps(
                        {
                            "id": material.view.checkpoint_id,
                            "created": created,
                            "snapshot": material.snapshot.fingerprint,
                            "package": str(krim_agent.__file__),
                        }
                    ),
                    flush=True,
                )
        else:
            assert phase == "read"
            replays = 0

            def forbidden(*_args, **_kwargs):
                nonlocal replays
                replays += 1
                raise AssertionError("原目录材料冷读不得启动Reader/Writer/Provider/命令")

            WorkspaceTreeSnapshotReader.capture = forbidden
            WorkspaceTreeSnapshotReader.verify = forbidden
            WorkspaceWriter.apply = forbidden
            FakeProvider.stream = forbidden
            LinuxSandboxRunner.start = forbidden
            material = await store.by_key(
                scope, key, workspace_key="files", resource_fingerprint="a" * 64
            )
            assert material is not None
            raw = next(e.file for e in material.snapshot.entries if e.path == "tree/raw.bin")
            assert raw is not None and raw.data == b"\0\xff" and raw.mode == 0o640
            assert any(e.path == "tree/empty" and e.directory for e in material.snapshot.entries)
            assert replays == 0
            print(
                json.dumps(
                    {
                        "id": material.view.checkpoint_id,
                        "snapshot": material.snapshot.fingerprint,
                        "binary_hex": raw.data.hex(),
                        "mode": raw.mode,
                        "replay_calls": replays,
                        "package": str(krim_agent.__file__),
                    }
                ),
                flush=True,
            )
    finally:
        await database.close()


asyncio.run(main())
