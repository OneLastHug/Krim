"""真实公共字节恢复两SQL点强杀与同原Scope/备份/operation冷查询。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence.database import AgentHandleRow, Database, WorkspaceRestorePlanRow


@pytest.mark.parametrize("point", ["ready", "effect"])
async def test_byte_restore_sql_sigkill_restart_preserves_original_facts_without_replay(
    tmp_path, point
):
    (tmp_path / "files").mkdir()
    (tmp_path / "files/file").write_bytes(b"\0\xfftarget")
    fixture = Path(__file__).parent / "fixtures/native_checkpoint_byte_restore_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixture),
        "produce",
        point,
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 60)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == producer.pid and metadata["point"] == point
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 10)
        (tmp_path / "producer-stderr.log").write_bytes(error)
        assert producer.returncode == -9, error.decode()
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            if point == "effect":
                await sql.execute(
                    update(WorkspaceRestorePlanRow)
                    .where(WorkspaceRestorePlanRow.plan_id == metadata["plan"]["plan_id"])
                    .values(lease_expires_at=time.time() - 1)
                )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(fixture),
            "recover",
            point,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, error = await asyncio.wait_for(recovery.communicate(), 30)
        assert recovery.returncode == 0, error.decode()
        result = json.loads(out)
        assert result["plan_id"] == metadata["plan"]["plan_id"]
        assert result["baseline_checkpoint_id"] == metadata["plan"]["baseline_checkpoint_id"]
        assert (
            result["inode"] == metadata["inode"]
            and result["same_sdk_http"]
            and result["replay_calls"] == 0
        )
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                out, error = await process.communicate()
                label = "producer" if process is producer else "recovery"
                (tmp_path / f"{label}-remaining-stdout.log").write_bytes(out)
                (tmp_path / f"{label}-stderr.log").write_bytes(error)
        await database.close()
