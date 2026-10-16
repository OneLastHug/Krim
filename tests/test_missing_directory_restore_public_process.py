"""两处原模型真实强杀/另进程自然租约后恢复，同原来源且零重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    AgentHandleRow,
    WorkspaceMissingDirectoryRestoreAttemptRow,
    WorkspaceMissingDirectoryRestorePlanRow,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")


@pytest.mark.parametrize("point", ["absence", "effect"])
async def test_missing_model_sigkill_preserves_original_source_and_unknown_without_replay(
    tmp_path, point
):
    script = Path(__file__).parent / "fixtures/missing_directory_restore_public_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
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
        line = await asyncio.wait_for(producer.stdout.readline(), 25)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        signal = json.loads(line)
        assert signal["pid"] == producer.pid and signal["origin"] == "model"
        assert signal["call_id"] == (
            "original-missing-preview" if point == "absence" else "original-missing-start"
        )
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, error.decode()
        metadata = json.loads((tmp_path / "metadata.json").read_text())
        async with database.sessions() as sql, sql.begin():
            table = (
                WorkspaceMissingDirectoryRestorePlanRow
                if point == "absence"
                else WorkspaceMissingDirectoryRestoreAttemptRow
            )
            row = await sql.get(table, metadata["operation_id"])
            metadata["lease"] = row.lease_expires_at
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        (tmp_path / "metadata.json").write_text(json.dumps(metadata))
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            point,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 25)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert proof["pid"] != signal["pid"] and proof["natural_lease_expiry"]
        assert (
            proof["plan_id"] == metadata["plan_id"]
            and proof["operation_id"] == metadata["operation_id"]
        )
        assert proof["replays"] == 0 and proof["original_source"]
        assert proof["new_verify_calls"] == 0 and proof["public_consumers_same"]
        print(json.dumps({"producer": signal, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
