"""实际SIGKILL原目录生产者，另进程同原任务/材料/终态且零重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update
from test_directory_checkpoint_capture import make_tree

from krim_agent.persistence import Database
from krim_agent.persistence.database import AgentHandleRow, WorkspaceDirectoryCaptureRow


@pytest.mark.parametrize("stage", ["record", "finish"])
async def test_public_capture_sigkill_and_restart_preserve_original_material_and_zero_replay(
    tmp_path, stage
):
    files = tmp_path / "files"
    files.mkdir()
    make_tree(files)
    fixture = Path(__file__).parent / "fixtures/directory_checkpoint_capture_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixture),
        "produce",
        stage,
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 25)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == producer.pid
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 10)
        assert producer.returncode == -9, error.decode()
        async with database.sessions() as sql, sql.begin():
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
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(fixture),
            "recover",
            stage,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, error = await asyncio.wait_for(recovery.communicate(), 25)
        assert recovery.returncode == 0, error.decode()
        result = json.loads(out)
        assert result["checkpoint_id"] == metadata["checkpoint_id"]
        assert result["status"] == ("interrupted" if stage == "record" else "available")
        assert result["same_sdk_http"] and result["replay_calls"] == 0
        assert result["inode"] == metadata["inode"]
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
