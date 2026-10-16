"""真实原模型崩溃后公开消费者同分支，原工具unknown与来源不变。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence import Database
from krim_agent.persistence.database import AgentHandleRow

SCRIPT = Path(__file__).resolve().parent / "fixtures/conversation_recovery_public_process.py"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")
async def test_real_model_death_public_recovery_then_new_read_preserves_original_unknown(tmp_path):
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(SCRIPT),
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
        signal = json.loads(line)
        assert signal["pid"] == producer.pid and signal["origin"] == "model"
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, error.decode()
        metadata = json.loads((tmp_path / "metadata.json").read_text())
        async with database.sessions() as sql, sql.begin():
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(SCRIPT),
            "recover",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 30)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert proof["pid"] != producer.pid and proof["natural_effect_lease_expiry"]
        assert (
            proof["public_consumers_same"]
            and proof["original_sources_same"]
            and proof["original_events_same"]
        )
        assert (
            proof["operation_id"] == metadata["operation_id"]
            and proof["new_query_calls"] == 2
            and proof["replays"] == 0
        )
        assert proof["binary_hex"] == "00ff" and proof["inode"] == metadata["inode"]
        print(json.dumps({"producer": signal, "recovery": proof}))
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(SCRIPT),
            "readback",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 20)
        assert recovery.returncode == 0, error.decode()
        warm = json.loads(output)
        assert (
            warm["pid"] != proof["pid"]
            and warm["warm_readback"]
            and warm["replays"] == warm["new_query_calls"] == 0
        )
        assert warm["recovery_message_id"] == proof["recovery_message_id"]
        print(json.dumps({"warm_readback": warm}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
