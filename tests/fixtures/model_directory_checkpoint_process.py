"""原模型目录record/finish真正提交后强杀，另进程冷读原来源/材料。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx
from sqlalchemy import func, select

import krim_agent
import krim_agent.api.server as server_module
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    WorkspaceDirectoryCaptureModelSourceRow,
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryCheckpointRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelToolCall
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_checkpoint import (
    DirectoryCheckpointCaptureFence,
    DirectoryCheckpointMaterialCreate,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_writer import WorkspaceWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_model_directory_checkpoints import CREATE, setup  # noqa: E402


class Provider:
    name = "offline-directory-model-process"

    def __init__(self, phase):
        self.phase, self.calls = phase, 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        if self.phase != "produce" or self.calls != 1:
            raise AssertionError("冷恢复不得调用原模型或捕获")
        yield ModelToolCall(
            id="process-directory-original",
            name=CREATE,
            arguments={"root_path": "tree", "request_key": "model-directory-process"},
        )


async def main():
    phase, stage, folder = sys.argv[1:4]
    root = Path(folder)
    provider = Provider(phase)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        if phase == "produce":
            service, agent = await setup(ai.runtime, root / "files", provider, persistent=True)
            store = service.directory_checkpoint_store
            original = getattr(store, stage)

            async def hold(scope, fence, *args, **kwargs):
                await original(scope, fence, *args, **kwargs)
                view = await store.get(scope, fence.checkpoint_id)
                source = await store.source(scope, view.checkpoint_id)
                assert source.run_id == fence.model_source.run_id
                assert view.status == ("capturing" if stage == "record" else "available")
                metadata = {
                    "pid": os.getpid(),
                    "agent_id": agent.agent_id,
                    "scope": scope,
                    "checkpoint_id": view.checkpoint_id,
                    "run_id": source.run_id,
                    "call_id": source.call_id,
                    "view": view.model_dump(mode="json"),
                    "provider_calls": provider.calls,
                    "inode": (root / "files/tree/nested/nul").stat().st_ino,
                    "package": str(krim_agent.__file__),
                }
                (root / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
                (root / "private-fence.json").write_text(
                    json.dumps({"token": fence.token, "owner": store.owner_id}), encoding="utf-8"
                )
                print(json.dumps(metadata), flush=True)
                await asyncio.Event().wait()

            setattr(store, stage, hold)
            await agent.execute("原模型目录提交后暂停", idempotency_key="process-model-directory")
            raise AssertionError("原模型目录生产者应被真正强杀")

        metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
        private = json.loads((root / "private-fence.json").read_text(encoding="utf-8"))
        service = ai.agents()
        service.register_workspace(
            "files", root / "files", writable=True, version="1", default=True
        )
        service.register_directory_checkpoints()
        service.register_directory_checkpoint_controls()
        store = service.directory_checkpoint_store
        replays = 0

        def forbidden(*_args, **_kwargs):
            nonlocal replays
            replays += 1
            raise AssertionError("原模型目录冷读不得启动Reader/Writer/producer")

        WorkspaceTreeSnapshotReader.capture = forbidden
        WorkspaceTreeSnapshotReader.verify = forbidden
        WorkspaceWriter.apply = forbidden
        service.directory_checkpoints.create = forbidden
        service.directory_checkpoints.create_model = forbidden
        assert await service.restore_persistent() == 1, service.restore_errors
        assert await store.recover_interrupted() == 0
        agent = AgentSession(service, metadata["agent_id"])
        view = await agent.directory_checkpoint(metadata["checkpoint_id"])
        assert view.status == ("interrupted" if stage == "record" else "available")
        source = await store.source(view.scope_id, view.checkpoint_id)
        assert source == CheckpointModelSource(metadata["run_id"], metadata["call_id"])
        assert await agent.directory_checkpoint_by_key("model-directory-process") == view
        request = DirectoryCheckpointMaterialCreate(
            root_path="tree", request_key="model-directory-process"
        )
        assert await store.reserve(
            view.scope_id, service.resources.workspace("files"), request, model_source=source
        ) == (view, False)
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceDirectoryCaptureRow, view.checkpoint_id)
            material = await store._material(sql, row)
            files = {
                entry.path: entry.file
                for entry in material.snapshot.entries
                if entry.file is not None
            }
            assert (
                files["tree/nested/nul"].data == b"\0\xff"
                and files["tree/nested/nul"].mode == 0o640
            )
            assert len(material.snapshot.entries) == 6
            counts = [
                await sql.scalar(select(func.count()).select_from(table))
                for table in (
                    WorkspaceDirectoryCaptureRow,
                    WorkspaceDirectoryCaptureModelSourceRow,
                    WorkspaceDirectoryCheckpointRow,
                )
            ]
        assert counts == [1, 1, 1]
        old = DirectoryCheckpointCaptureFence(view.checkpoint_id, private["token"], source)
        with_owner = store.owner_id
        store.owner_id = private["owner"]
        try:
            await store.renew(view.scope_id, old)
        except ServiceError as error:
            assert error.code == "workspace_directory_capture_fenced"
        else:
            raise AssertionError("旧模型生产者不得重获资格")
        finally:
            store.owner_id = with_owner
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/directory-checkpoints/by-key",
                params={"request_key": "model-directory-process"},
            )
            assert response.status_code == 200 and response.json() == view.model_dump(mode="json")
        assert (root / "files/tree/nested/nul").stat().st_ino == metadata["inode"]
        assert provider.calls == replays == 0
        print(
            json.dumps(
                {
                    "checkpoint_id": view.checkpoint_id,
                    "status": view.status,
                    "source": {"run_id": source.run_id, "call_id": source.call_id},
                    "counts": counts,
                    "provider_calls": provider.calls,
                    "replays": replays,
                    "inode": metadata["inode"],
                    "old_owner_fenced": True,
                    "same_sdk_http": True,
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )


asyncio.run(main())
