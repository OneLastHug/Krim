"""复用真实模型生产者SQL提交后SIGKILL，新服务让浏览器冷读原目录事实。"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from pydantic import SecretStr
from sqlalchemy import func, select, update

import krim_agent
import krim_agent.api.server as server_module
from krim_agent.config import Settings
from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    AgentHandleRow,
    WorkspaceDirectoryCaptureModelSourceRow,
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryCheckpointRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointCaptureFence
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader
from krim_agent.workspace_writer import WorkspaceWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_checkpoint_capture import make_tree  # noqa: E402


class Provider:
    # 与原独立生产者的公开name/class一致；恢复仍核对原冻结定义。
    name = "offline-directory-model-process"

    def stream(self, *_args, **_kwargs):
        raise AssertionError("冷读不能调用原Provider")


async def serve(root: Path) -> None:
    stage = os.environ["KRIM_TEST_DIRECTORY_RECOVERY_STAGE"]
    assert stage in {"record", "finish"}
    files = root / "files"
    files.mkdir()
    make_tree(files)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(Path(__file__).with_name("model_directory_checkpoint_process.py")),
        "produce",
        stage,
        str(root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(process.stdout.readline(), 25)
        if not line:
            _, errors = await process.communicate()
            raise AssertionError(errors.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == process.pid and metadata["provider_calls"] == 1
        process.kill()
        _, errors = await asyncio.wait_for(process.communicate(), 10)
        assert process.returncode == -9, errors.decode()
        # 只对本夹具的合成库制造租约到期；不冒称自然到期或生产多worker。
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            if stage == "record":
                await sql.execute(
                    update(WorkspaceDirectoryCaptureRow)
                    .where(WorkspaceDirectoryCaptureRow.checkpoint_id == metadata["checkpoint_id"])
                    .values(lease_expires_at=time.time() - 1)
                )
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
        await database.close()

    replays = 0

    def forbidden(*_args, **_kwargs):
        nonlocal replays
        replays += 1
        raise AssertionError("浏览器冷读不能重放模型、Reader、Writer或producer")

    Provider.stream = forbidden
    WorkspaceTreeSnapshotReader.capture = forbidden
    WorkspaceTreeSnapshotReader.verify = forbidden
    WorkspaceWriter.apply = forbidden
    async with await PythonAI.create(
        settings=Settings(
            database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}",
            api_key=SecretStr("browser-test-key"),
        ),
        provider=Provider(),
    ) as ai:
        service = ai.agents()
        service.register_workspace("files", files, version="1", writable=True, default=True)
        service.register_directory_checkpoints()
        service.register_directory_checkpoint_controls()
        service.directory_checkpoints.create = forbidden
        service.directory_checkpoints.create_model = forbidden
        assert await service.restore_persistent() == 1, service.restore_errors
        store = service.directory_checkpoint_store
        agent = AgentSession(service, metadata["agent_id"])
        view = await service.get(agent.agent_id)
        private = json.loads((root / "private-fence.json").read_text())
        owner = store.owner_id
        store.owner_id = private["owner"]
        try:
            try:
                await store.renew(
                    metadata["scope"],
                    DirectoryCheckpointCaptureFence(
                        metadata["checkpoint_id"],
                        private["token"],
                        CheckpointModelSource(metadata["run_id"], metadata["call_id"]),
                    ),
                )
            except ServiceError as error:
                assert error.code == "workspace_directory_capture_fenced"
            else:
                raise AssertionError("旧owner必须失效")
        finally:
            store.owner_id = owner
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)

        @app.get("/_test/directory-recovery")
        async def facts() -> dict:
            checkpoint = await agent.directory_checkpoint(metadata["checkpoint_id"])
            assert checkpoint == await agent.directory_checkpoint_by_key("model-directory-process")
            assert checkpoint.status == ("interrupted" if stage == "record" else "available")
            source = await store.source(checkpoint.scope_id, checkpoint.checkpoint_id)
            assert source == CheckpointModelSource(metadata["run_id"], metadata["call_id"])
            async with store.database.sessions() as sql:
                row = await sql.get(WorkspaceDirectoryCaptureRow, checkpoint.checkpoint_id)
                material = await store._material(sql, row)
                binary = next(
                    entry.file
                    for entry in material.snapshot.entries
                    if entry.path == "tree/nested/nul"
                )
                counts = [
                    await sql.scalar(select(func.count()).select_from(table))
                    for table in (
                        WorkspaceDirectoryCaptureRow,
                        WorkspaceDirectoryCaptureModelSourceRow,
                        WorkspaceDirectoryCheckpointRow,
                    )
                ]
            assert binary.data == b"\0\xff" and binary.mode == 0o640
            assert (files / "tree/nested/nul").stat().st_ino == metadata["inode"]
            assert replays == 0 and counts == [1, 1, 1]
            return {
                "agent_id": agent.agent_id,
                "topic_id": view.topic_id,
                "checkpoint": checkpoint.model_dump(mode="json"),
                "source": {"run_id": source.run_id, "call_id": source.call_id},
                "binary_hex": binary.data.hex(),
                "mode": binary.mode,
                "inode": metadata["inode"],
                "counts": counts,
                "replays": replays,
                "old_owner_fenced": True,
                "producer_exit": process.returncode,
                "package": str(krim_agent.__file__),
            }

        await uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=8878, log_level="error")
        ).serve()


with TemporaryDirectory(prefix="krim-directory-browser-recovery-", dir="/data/tmp/root") as folder:
    asyncio.run(serve(Path(folder)))
