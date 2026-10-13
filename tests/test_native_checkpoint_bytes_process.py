"""字节存储基础真实record/available后SIGKILL与冷恢复；公共producer另单元。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence.database import AgentHandleRow, Database, NativeWorkspaceCheckpointRow


@pytest.mark.parametrize("stage", ["recorded", "available"])
async def test_bytes_ledger_sigkill_keeps_scope_blob_kind_inode_and_zero_capture_replay(
    tmp_path, stage
):
    (tmp_path / "files").mkdir()
    (tmp_path / "files/raw").write_bytes(b"\0\xff")
    fixture = Path(__file__).parent / "fixtures/native_checkpoint_bytes_process.py"
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
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            if stage == "recorded":
                await sql.execute(
                    update(NativeWorkspaceCheckpointRow)
                    .where(NativeWorkspaceCheckpointRow.checkpoint_id == metadata["checkpoint_id"])
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
        actual = json.loads(out)
        assert (
            actual["checkpoint_id"] == metadata["checkpoint_id"]
            and actual["inode"] == metadata["inode"]
        )
        assert actual["replay_calls"] == 0 and actual["old_owner_fenced"]
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
