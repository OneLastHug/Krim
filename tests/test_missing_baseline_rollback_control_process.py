"""真正Host/模型强杀、原Move自然租约、原调用事实和另进程0重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence.database import AgentHandleRow, Database

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")


@pytest.mark.parametrize("origin", ["host", "model"])
@pytest.mark.parametrize("point", ["namespace", "settled"])
async def test_real_controlled_rollback_process_death_and_zero_replay(tmp_path, origin, point):
    script = Path(__file__).parent / "fixtures/missing_baseline_rollback_control_process.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
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
        line = await asyncio.wait_for(child.stdout.readline(), 20)
        if not line:
            _, errors = await child.communicate()
            raise AssertionError(errors.decode())
        signal = json.loads(line)
        assert signal["pid"] == child.pid and signal["point"] == point
        child.kill()
        _, errors = await asyncio.wait_for(child.communicate(), 5)
        assert child.returncode == -9, errors.decode()
        metadata = json.loads((tmp_path / "metadata.json").read_text())
        assert (metadata["lease"] is None) == (point == "settled")
        # 只对已被证实死亡的原handle控制过期；Move的原5秒租约由恢复进程自然等待。
        async with database.sessions() as sql, sql.begin():
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            origin,
            point,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(recovery.communicate(), 20)
        assert recovery.returncode == 0, errors.decode()
        proof = json.loads(output)
        assert proof["pid"] != signal["pid"] and proof["operation_id"] == metadata["operation_id"]
        assert proof["source_plan_id"] == metadata["source_plan_id"]
        assert proof["provider_calls"] == proof["replays"] == 0
        assert proof["natural_move_lease_expiry"] == (point == "namespace")
        print(json.dumps({"producer": signal, "recovery": proof}))
    finally:
        for process in (child, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
