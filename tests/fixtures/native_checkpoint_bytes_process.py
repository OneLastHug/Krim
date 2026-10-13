"""原生bytes存储record/available后真实强杀；冷重开不捕获或模型重放。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import krim_agent
from krim_agent.config import Settings
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI, WorkspaceCheckpointCreate
from krim_agent.workspace_writer import WorkspaceWriter


async def main():
    phase, stage, directory = sys.argv[1:4]
    root = Path(directory)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        service = ai.agents()
        binding = service.register_workspace(
            "files", root / "files", version="1", writable=True, default=True
        )
        store = service.workspace_checkpoint_store
        request = WorkspaceCheckpointCreate(paths=["raw", "missing"], request_key="original-bytes")
        if phase == "produce":
            agent = await ai.agent(persistent=True)
            scope = f"session:{agent.session_id}"
            view, created = await store.reserve(
                scope,
                request,
                binding.key,
                binding.fingerprint,
                binding.workspace.fingerprint,
                capture_format="bytes",
            )
            assert created
            fences = await store.execution(scope, view.checkpoint_id)
            writer = WorkspaceWriter(binding.workspace)
            await store.record(scope, fences[0], writer.capture_file("raw", allow_binary=True))
            if stage == "available":
                await store.record(
                    scope, fences[1], writer.capture_file("missing", allow_binary=True)
                )
                await store.finish(scope, fences[0])
            metadata = {
                "pid": os.getpid(),
                "agent_id": agent.agent_id,
                "scope": scope,
                "checkpoint_id": view.checkpoint_id,
                "owner": store.owner_id,
                "token": fences[0].token,
                "inode": (root / "files/raw").stat().st_ino,
                "package": str(krim_agent.__file__),
            }
            (root / "metadata.json").write_text(json.dumps(metadata))
            print(json.dumps(metadata), flush=True)
            await asyncio.to_thread(sys.stdin.readline)
            raise AssertionError("父进程应在SQL落盘后强杀")
        metadata = json.loads((root / "metadata.json").read_text())
        calls = 0

        def forbidden(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise AssertionError("字节存储冷恢复不能调用模型或捕获/发布")

        WorkspaceWriter.capture_file = forbidden
        WorkspaceWriter.verify_captured = forbidden
        WorkspaceWriter.apply = forbidden
        FakeProvider.stream = forbidden
        assert await service.restore_persistent() == 1, service.restore_errors
        view = await store.get(metadata["scope"], metadata["checkpoint_id"])
        assert view.capture_format == "bytes" and view.files[0].content_kind == "binary"
        assert view.status == ("available" if stage == "available" else "interrupted")
        again, created = await store.reserve(
            metadata["scope"],
            request,
            binding.key,
            binding.fingerprint,
            binding.workspace.fingerprint,
            capture_format="bytes",
        )
        assert again == view and not created
        assert (await store.by_key(metadata["scope"], request.request_key)) == view
        from krim_agent.workspace_checkpoint import CheckpointCaptureFence

        fence = CheckpointCaptureFence(view.checkpoint_id, 0, metadata["token"], "bytes")
        assert (await store.captured(metadata["scope"], fence)).snapshot.data == b"\0\xff"
        with_owner = store.owner_id
        store.owner_id = metadata["owner"]
        try:
            try:
                await store.renew(metadata["scope"], fence)
            except ServiceError:
                pass
            else:
                raise AssertionError("迟到owner仍能续租")
        finally:
            store.owner_id = with_owner
        assert (root / "files/raw").stat().st_ino == metadata["inode"]
        assert (root / "files/raw").read_bytes() == b"\0\xff" and calls == 0
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "checkpoint_id": view.checkpoint_id,
                    "status": view.status,
                    "capture_format": view.capture_format,
                    "replay_calls": calls,
                    "inode": metadata["inode"],
                    "old_owner_fenced": True,
                }
            ),
            flush=True,
        )


asyncio.run(main())
