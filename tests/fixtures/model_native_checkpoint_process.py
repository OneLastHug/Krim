"""真实模型捕获Blob提交后SIGKILL；另进程只读原ID/来源与字节，零生产者重放。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx
from sqlalchemy import func, select

import krim_agent
import krim_agent.api.server as server_module
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    NativeCheckpointModelSourceRow,
    NativeWorkspaceCheckpointRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelToolCall
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_writer import WorkspaceWriter

NAMES = [
    "workspace_read",
    "workspace_checkpoint_capture",
    "workspace_checkpoint_create",
    "workspace_checkpoint_query",
    "workspace_checkpoint_cancel",
]


class Provider:
    name = "offline-native-model-process"

    def __init__(self, phase):
        self.phase, self.calls = phase, 0

    async def stream(self, messages, tools):
        self.calls += 1
        if self.phase != "produce" or self.calls != 1:
            raise AssertionError("冷恢复不得调用模型或重放原捕获")
        yield ModelToolCall(
            id="process-create-root",
            name="workspace_checkpoint_create",
            arguments={
                "paths": ["binary.bin"],
                "capture_format": "bytes",
                "request_key": "model-process",
            },
        )


async def main():
    phase, folder = sys.argv[1:3]
    root = Path(folder)
    provider = Provider(phase)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:

        async def allow(*_args):
            return "allow"

        ai.runtime.tool_policy = allow
        service = ai.agents()
        service.register_workspace(
            "files", root / "files", writable=True, version="1", default=True
        )
        service.register_checkpoint_bytes()
        service.register_checkpoint_controls()
        store = service.workspace_checkpoint_store
        if phase == "produce":
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="process-model-checkpoint",
                        description="真实模型原捕获强杀",
                        workspace_key="files",
                        tool_names=NAMES,
                        persistent=True,
                        tool_timeout_seconds=30,
                    ),
                )
            )
            agent = AgentSession(
                service, (await service.spawn("process-model-checkpoint")).agent_id
            )
            original = store.record

            async def record(scope, fence, capture, **kwargs):
                await original(scope, fence, capture, **kwargs)
                view = await store.get(scope, fence.checkpoint_id)
                source = await store.source(scope, fence.checkpoint_id)
                assert view.status == "capturing" and view.files[0].captured
                assert (
                    source.call_id == "process-create-root" and capture.snapshot.data == b"\0\xff"
                )
                metadata = {
                    "package": str(krim_agent.__file__),
                    "pid": os.getpid(),
                    "agent_id": agent.agent_id,
                    "scope": scope,
                    "checkpoint_id": view.checkpoint_id,
                    "run_id": source.run_id,
                    "call_id": source.call_id,
                    "old_owner": store.owner_id,
                    "view": view.model_dump(mode="json"),
                    "provider_calls": provider.calls,
                    "record_committed": True,
                }
                (root / "metadata.json").write_text(json.dumps(metadata))
                print(json.dumps(metadata), flush=True)
                await asyncio.Event().wait()

            store.record = record
            await agent.execute("捕获真实二进制并在SQL后暂停", idempotency_key="process-create")
            raise AssertionError("原生产者应由测试真正SIGKILL")
        assert phase == "recover"
        metadata = json.loads((root / "metadata.json").read_text())
        replay = 0

        def forbidden(*_args, **_kwargs):
            nonlocal replay
            replay += 1
            raise AssertionError("原检查点冷读取不能重新捕获")

        WorkspaceWriter.capture_file = forbidden
        service.workspace_checkpoints.create = forbidden
        service.workspace_checkpoints.create_model = forbidden
        assert await service.restore_persistent() == 1
        agent = AgentSession(service, metadata["agent_id"])
        view = await agent.file_checkpoint(metadata["checkpoint_id"])
        assert view.scope_id == metadata["scope"] and view.status == "interrupted"
        assert view.files[0].captured and view.files[0].content_kind == "binary"
        assert view.files[0].sha256 == metadata["view"]["files"][0]["sha256"]
        assert await agent.file_checkpoint_by_key("model-process") == view
        source = await store.source(view.scope_id, view.checkpoint_id)
        assert source == CheckpointModelSource(metadata["run_id"], metadata["call_id"])
        assert (await store.captures(view.scope_id, view.checkpoint_id))[
            0
        ].snapshot.data == b"\0\xff"
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/native-checkpoints/{view.checkpoint_id}"
            )
            assert response.status_code == 200 and response.json() == view.model_dump(mode="json")
        original_owner = store.owner_id
        store.owner_id = metadata["old_owner"]
        try:
            await store.execution(view.scope_id, view.checkpoint_id)
        except ServiceError as error:
            assert error.code == "workspace_checkpoint_fenced"
        else:
            raise AssertionError("旧生产者不能重获原捕获资格")
        finally:
            store.owner_id = original_owner
        async with store.database.sessions() as sql:
            counts = [
                await sql.scalar(select(func.count()).select_from(table))
                for table in (
                    NativeWorkspaceCheckpointRow,
                    NativeCheckpointModelSourceRow,
                )
            ]
        assert counts == [1, 1] and replay == provider.calls == 0
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "view": view.model_dump(mode="json"),
                    "source": {"run_id": source.run_id, "call_id": source.call_id},
                    "counts": counts,
                    "provider_calls": provider.calls,
                    "capture_replays": replay,
                    "old_owner_fenced": True,
                }
            ),
            flush=True,
        )


asyncio.run(main())
