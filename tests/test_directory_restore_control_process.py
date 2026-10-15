"""真正Host工具后置Hook强杀与另一服务恢复，不冒作Store合成材料测试。"""

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
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestorePlanRow,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")


@pytest.mark.parametrize("point", ["backup", "effect"])
async def test_real_host_restore_sigkill_cold_original_operation_and_zero_replay(tmp_path, point):
    script = Path(__file__).parent / "fixtures/directory_restore_control_process.py"
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
            _, errors = await producer.communicate()
            raise AssertionError(errors.decode())
        signal = json.loads(line)
        assert signal["pid"] == producer.pid and signal["point"] == point
        producer.kill()
        _, errors = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, errors.decode()
        metadata = json.loads((tmp_path / "metadata.json").read_text())
        async with database.sessions() as sql, sql.begin():
            model = (
                WorkspaceDirectoryRestorePlanRow
                if point == "backup"
                else WorkspaceDirectoryRestoreAttemptRow
            )
            original = await sql.get(model, metadata["operation_id"])
            # 只读回原进程真正最后提交的租约，避免metadata前后一次heartbeat的差异。
            metadata["lease"] = original.lease_expires_at
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
        output, errors = await asyncio.wait_for(recovery.communicate(), 25)
        assert recovery.returncode == 0, errors.decode()
        result = json.loads(output)
        assert (
            result["plan_id"] == metadata["plan_id"]
            and result["operation_id"] == metadata["operation_id"]
        )
        assert result["pid"] != signal["pid"] and result["replays"] == 0
        assert result["status"] == ("interrupted" if point == "backup" else "done")
        assert result["binary_hex"] == (b"current\x80\0".hex() if point == "backup" else "00ff")
        assert (
            result["original_restore_lease_unchanged"] and result["synthetic_dead_handle_expired"]
        )
        print(json.dumps({"producer": signal, "recovery": result}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
