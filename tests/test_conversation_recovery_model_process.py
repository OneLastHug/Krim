"""原模型真实效果后SIGKILL，新进程显式恢复上下文再只读同原unknown。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence import Database
from krim_agent.persistence.database import AgentHandleRow

SCRIPT = Path(__file__).resolve().parent / "fixtures/conversation_recovery_model_process.py"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")
async def test_true_model_effect_death_explicit_branch_then_new_model_read_no_old_replay(tmp_path):
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
        assert (
            signal["pid"] == producer.pid
            and signal["origin"] == "model"
            and signal["point"] == "effect"
        )
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
        assert proof["new_provider_calls"] == 2 and proof["new_run_id"] != proof["original_run_id"]
        assert proof["operation_id"] == metadata["operation_id"] and proof["source_unchanged"]
        assert proof["replays"] == 0 and proof["unknown_preserved"] and proof["turn"] == 2
        print(json.dumps({"producer": signal, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
