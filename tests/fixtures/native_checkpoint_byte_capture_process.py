"""真实公共bytes producer落盘后SIGKILL，冷恢复经原SDK/HTTP读取同事实。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx

import krim_agent
import krim_agent.api.server as server_module
from krim_agent.config import Settings
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI, WorkspaceCheckpointCreate
from krim_agent.workspace_writer import WorkspaceWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_native_checkpoint_byte_capture import setup  # noqa: E402


async def main():
    phase, directory = sys.argv[1:3]
    root = Path(directory)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        if phase == "produce":
            service, agent, _ = await setup(ai.runtime, root / "files", persistent=True)
            original = service.workspace_checkpoint_store.record

            async def hold(scope, fence, capture):
                await original(scope, fence, capture)
                value = {
                    "pid": os.getpid(),
                    "agent_id": agent.agent_id,
                    "scope": scope,
                    "token": fence.token,
                    "checkpoint_id": fence.checkpoint_id,
                    "inode": (root / "files/nul").stat().st_ino,
                    "package": str(krim_agent.__file__),
                }
                (root / "metadata.json").write_text(json.dumps(value))
                print(json.dumps(value), flush=True)
                await asyncio.to_thread(sys.stdin.readline)
                raise AssertionError("应在公共bytes实际record后被强杀")

            service.workspace_checkpoint_store.record = hold
            request = WorkspaceCheckpointCreate(
                paths=["nul", "missing"],
                request_key="public-bytes-original",
                capture_format="bytes",
            )
            started = await agent.create_file_checkpoint(request)
            await agent.wait_file_checkpoint(started.checkpoint_id, timeout_seconds=30)
            return
        metadata = json.loads((root / "metadata.json").read_text())
        service = ai.agents()
        service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_checkpoint_bytes()
        calls = 0

        def forbidden(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise AssertionError("公共bytes冷恢复不得重跑Model/capture/verify/apply")

        FakeProvider.stream = forbidden
        WorkspaceWriter.capture_file = forbidden
        WorkspaceWriter.verify_captured = forbidden
        WorkspaceWriter.apply = forbidden
        assert await service.restore_persistent() == 1, service.restore_errors
        agent = ai.agent_session(metadata["agent_id"])
        original = await agent.file_checkpoint(metadata["checkpoint_id"])
        assert original.status == "interrupted" and original.capture_format == "bytes"
        assert original.files[0].captured and original.files[0].content_kind == "binary"
        assert not original.files[1].captured
        request = WorkspaceCheckpointCreate(
            paths=["nul", "missing"], request_key="public-bytes-original", capture_format="bytes"
        )
        assert await agent.create_file_checkpoint(request) == original
        assert await agent.file_checkpoint_by_key(request.request_key) == original
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/native-checkpoints/by-key",
                params={"request_key": request.request_key},
            )
            assert response.status_code == 200 and response.json() == original.model_dump(
                mode="json"
            )
        from krim_agent.workspace_checkpoint import CheckpointCaptureFence

        saved = await service.workspace_checkpoint_store.captured(
            metadata["scope"],
            CheckpointCaptureFence(original.checkpoint_id, 0, metadata["token"], "bytes"),
        )
        assert saved.snapshot.data == b"\0\xff"
        assert (root / "files/nul").stat().st_ino == metadata["inode"]
        assert (root / "files/nul").read_bytes() == b"\0\xff" and calls == 0
        print(
            json.dumps(
                {
                    "checkpoint_id": original.checkpoint_id,
                    "capture_format": original.capture_format,
                    "package": str(krim_agent.__file__),
                    "replay_calls": calls,
                    "inode": metadata["inode"],
                    "same_sdk_http": True,
                }
            ),
            flush=True,
        )


asyncio.run(main())
