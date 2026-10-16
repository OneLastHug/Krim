"""真实Host/模型两个强杀窗口，新进程公开消费者不重放原生产。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence import Database
from krim_agent.persistence.database import AgentHandleRow

SCRIPT = Path(__file__).resolve().parent / "fixtures/missing_directory_rollback_public_process.py"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")
@pytest.mark.parametrize("origin,point", [("model", "namespace"), ("host", "receipt")])
async def test_missing_rollback_sigkill_new_process_all_public_consumers_zero_replay(
    tmp_path, origin, point
):
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(SCRIPT),
        "produce",
        origin,
        point,
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
        signal = json.loads(line)
        assert (
            signal["pid"] == producer.pid
            and signal["origin"] == origin
            and signal["point"] == point
        )
        inode = (tmp_path / "files/preserved").stat().st_ino
        producer.kill()
        _, errors = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, errors.decode()
        metadata = json.loads((tmp_path / "metadata.json").read_text())
        async with database.sessions() as sql, sql.begin():
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        (tmp_path / "observed.json").write_text(json.dumps({"inode": inode}))
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(SCRIPT),
            "recover",
            origin,
            point,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 30)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert proof["pid"] != producer.pid
        assert proof["natural_move_lease_expiry"] == (point == "namespace")
        assert (
            proof["operation_id"] == metadata["operation_id"]
            and proof["source_plan_id"] == metadata["source_plan_id"]
        )
        assert (
            proof["source_operation_id"] == metadata["source_operation_id"]
            and proof["inode"] == inode
        )
        assert proof["consumers_same"] and proof["replays"] == proof["provider_calls"] == 0
        print(json.dumps({"producer": signal, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
