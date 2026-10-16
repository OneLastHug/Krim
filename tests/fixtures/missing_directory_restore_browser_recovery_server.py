"""原真实模型目录恢复生产者强杀后，新服务让浏览器/SDK/CLI核实同事实。"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from pydantic import SecretStr
from sqlalchemy import update

import krim_agent
import krim_agent.api.server as server_module
import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.config import Settings
from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    AgentHandleRow,
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestorePlanRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreFence
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_model_missing_directory_restores import Provider  # noqa: E402


async def serve(root: Path) -> None:
    point = os.environ["KRIM_TEST_MISSING_DIRECTORY_RESTORE_RECOVERY_POINT"]
    assert point in {"absence", "effect"}
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(Path(__file__).with_name("model_missing_directory_restore_process.py")),
        "produce",
        point,
        str(root),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(process.stdout.readline(), 25)
        if not line:
            _, errors = await process.communicate()
            raise AssertionError(errors.decode())
        signal = json.loads(line)
        assert signal["pid"] == process.pid and signal["origin"] == "model"
        process.kill()
        _, errors = await asyncio.wait_for(process.communicate(), 5)
        assert process.returncode == -9, errors.decode()
        metadata = json.loads((root / "metadata.json").read_text())
        async with database.sessions() as sql, sql.begin():
            table = (
                WorkspaceMissingDirectoryRestorePlanRow
                if point == "absence"
                else WorkspaceMissingDirectoryRestoreAttemptRow
            )
            row = await sql.get(table, metadata["operation_id"])
            lease = row.lease_expires_at
            # 仅死AgentHandle合成记录到期；原恢复租约读回后自然到期，不改值。
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
        await database.close()
    await asyncio.sleep(max(0, lease - time.time()) + 0.02)
    replays = []

    def forbidden(*_args, **_kwargs):
        replays.append(True)
        raise AssertionError("浏览器恢复不得重放原Model/Reader/Writer/namespace")

    WorkspaceDirectoryAbsenceReader.capture = forbidden
    Provider.stream = forbidden
    WorkspaceTreeSnapshotReader.capture = forbidden
    WorkspaceTreeSnapshotReader.verify = forbidden
    WorkspaceMissingDirectoryRestoreWriter.prepare = forbidden
    WorkspaceMissingDirectoryRestoreWriter.apply = forbidden
    physical.rename_at = forbidden
    async with await PythonAI.create(
        settings=Settings(
            database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}",
            api_key=SecretStr("browser-test-key"),
        ),
        provider=Provider(),
    ) as ai:
        service = ai.agents()
        binding = service.register_workspace(
            "files", root / "files", version="1", writable=True, default=True
        )
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_missing_directory_checkpoint_restore_controls()
        assert await service.restore_persistent() == 1, service.restore_errors
        agent = AgentSession(service, metadata["agent_id"])
        view = await service.get(agent.agent_id)
        original = await agent.missing_directory_checkpoint_restore(metadata["plan_id"])
        assert original.phase == ("interrupted" if point == "absence" else "uncertain"), original
        try:
            await service.missing_directory_restore_store.renew(
                metadata["scope"],
                MissingDirectoryRestoreFence(
                    **{
                        **metadata["fence"],
                        "model_source": CheckpointModelSource(**metadata["fence"]["model_source"]),
                    }
                ),
                binding,
                qualify=lambda: None,
            )
        except ServiceError:
            pass
        else:
            raise AssertionError("旧owner/token必须失效")
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)

        @app.get("/_test/missing-directory-restore-recovery")
        async def facts() -> dict:
            planned = await agent.missing_directory_checkpoint_restore(metadata["plan_id"])
            assert planned == await agent.missing_directory_checkpoint_restore_by_key(
                "model-preview"
            )
            assert (
                planned.operation_id == metadata["operation_id"]
                and planned.baseline.fingerprint == metadata["absence"]
            )
            phase = "preview" if point == "absence" else "execute"
            source = await service.missing_directory_restore_store.source(
                planned.scope_id, planned.plan_id, phase
            )
            assert source == CheckpointModelSource(metadata["run_id"], metadata["call_id"])
            file = root / "files/tree/nested/nul"
            assert replays == []
            return {
                "point": point,
                "agent_id": agent.agent_id,
                "topic_id": view.topic_id,
                "plan": planned.model_dump(mode="json"),
                "source": {"run_id": source.run_id, "call_id": source.call_id},
                "binary_hex": file.read_bytes().hex() if file.exists() else None,
                "mode": file.stat().st_mode & 0o777 if file.exists() else None,
                "root_inode": (root / "files/tree").stat().st_ino
                if (root / "files/tree").exists()
                else None,
                "replays": len(replays),
                "old_owner_fenced": True,
                "producer_exit": process.returncode,
                "restore_lease_natural": True,
                "synthetic_handle_expired": True,
                "package": str(krim_agent.__file__),
            }

        await uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=8879, log_level="error")
        ).serve()


with TemporaryDirectory(
    prefix="krim-missing-directory-restore-browser-recovery-", dir="/data/tmp/root"
) as folder:
    asyncio.run(serve(Path(folder)))
