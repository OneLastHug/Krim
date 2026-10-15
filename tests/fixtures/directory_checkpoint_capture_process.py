"""真实公开目录生产者在record/finish提交后强杀，另进程冷恢复。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx

import krim_agent
import krim_agent.api.server as server_module
from krim_agent.config import Settings
from krim_agent.persistence.database import WorkspaceDirectoryCaptureRow
from krim_agent.providers import FakeProvider
from krim_agent.sdk import DirectoryCheckpointCreate, PythonAI
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_writer import WorkspaceWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_checkpoint_capture import counts, setup  # noqa: E402


async def main():
    phase, stage, directory = sys.argv[1:4]
    root = Path(directory)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        if phase == "produce":
            service, agent, _ = await setup(ai.runtime, root / "files", persistent=True)
            original = getattr(service.directory_checkpoint_store, stage)

            async def hold(scope, fence, *args, **kwargs):
                await original(scope, fence, *args, **kwargs)
                metadata = {
                    "pid": os.getpid(),
                    "agent_id": agent.agent_id,
                    "scope": scope,
                    "checkpoint_id": fence.checkpoint_id,
                    "inode": (root / "files/tree/nested/nul").stat().st_ino,
                    "package": str(krim_agent.__file__),
                }
                (root / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
                print(json.dumps(metadata), flush=True)
                await asyncio.to_thread(sys.stdin.readline)
                raise AssertionError("原目录生产者应已被强杀")

            setattr(service.directory_checkpoint_store, stage, hold)
            started = await agent.create_directory_checkpoint(
                DirectoryCheckpointCreate(root_path="tree", request_key="original-public-directory")
            )
            await agent.wait_directory_checkpoint(started.checkpoint_id, timeout_seconds=30)
            return

        metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
        service = ai.agents()
        service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_directory_checkpoints()
        calls = 0

        def forbidden(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise AssertionError("目录冷恢复不得重跑Provider/Reader/Writer")

        FakeProvider.stream = forbidden
        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceTreeSnapshotReader.verify = forbidden
        WorkspaceWriter.apply = forbidden
        assert await service.restore_persistent() == 1, service.restore_errors
        async with service.directory_checkpoint_store.database.sessions() as sql:
            row = await sql.get(WorkspaceDirectoryCaptureRow, metadata["checkpoint_id"])
            assert row.status == ("interrupted" if stage == "record" else "available")
            material = await service.directory_checkpoint_store._material(sql, row)
            files = {
                entry.path: entry.file
                for entry in material.snapshot.entries
                if entry.file is not None
            }
            assert files["tree/nested/nul"].data == b"\0\xff"
            assert files["tree/nested/nul"].mode == 0o640
            assert len(material.snapshot.entries) == 6
        assert await service.directory_checkpoint_store.recover_interrupted() == 0
        agent = ai.agent_session(metadata["agent_id"])
        original = await agent.directory_checkpoint(metadata["checkpoint_id"])
        assert original.snapshot.entry_count == 6
        request = DirectoryCheckpointCreate(
            root_path="tree", request_key="original-public-directory"
        )
        assert await agent.create_directory_checkpoint(request) == original
        assert await agent.directory_checkpoint_by_key(request.request_key) == original
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/directory-checkpoints/by-key",
                params={"request_key": request.request_key},
            )
            assert response.status_code == 200 and response.json() == original.model_dump(
                mode="json"
            )
        assert await counts(service) == (1, 1)
        assert (root / "files/tree/nested/nul").stat().st_ino == metadata["inode"]
        assert (root / "files/tree/nested/nul").read_bytes() == b"\0\xff" and calls == 0
        print(
            json.dumps(
                {
                    "checkpoint_id": original.checkpoint_id,
                    "status": original.status,
                    "package": str(krim_agent.__file__),
                    "replay_calls": calls,
                    "inode": metadata["inode"],
                    "same_sdk_http": True,
                }
            ),
            flush=True,
        )


asyncio.run(main())
