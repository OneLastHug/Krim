"""原公共SDK producer SQL record后真正强杀，跨进程原key/Scope/bytes/零重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path

from sqlalchemy import update

from krim_agent.persistence.database import AgentHandleRow, Database, NativeWorkspaceCheckpointRow


async def test_public_bytes_capture_sigkill_restart_same_sdk_http_fact_and_zero_replay(tmp_path):
    (tmp_path / "files").mkdir()
    (tmp_path / "files/nul").write_bytes(b"\0\xff")
    fixture = Path(__file__).parent / "fixtures/native_checkpoint_byte_capture_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixture),
        "produce",
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
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, error = await asyncio.wait_for(recovery.communicate(), 25)
        assert recovery.returncode == 0, error.decode()
        result = json.loads(out)
        assert (
            result["checkpoint_id"] == metadata["checkpoint_id"]
            and result["inode"] == metadata["inode"]
        )
        assert result["same_sdk_http"] and result["replay_calls"] == 0
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
