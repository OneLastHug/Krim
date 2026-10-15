"""原整树命令SQL效果后SIGKILL，原Agent/请求/限额/统计/Blob零重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path

from sqlalchemy import update
from test_execution_tree_quotas import tree_root as tree_root

from krim_agent.persistence.database import AgentHandleRow, Database


async def test_actual_tree_receipt_sigkill_then_same_original_sdk_http_kernel_stats_and_blob(
    tmp_path, tree_root
):
    fixture = Path(__file__).parent / "fixtures/execution_tree_ledger_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixture),
        "produce",
        str(tmp_path),
        str(tree_root.path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 30)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["host_pid"] == producer.pid
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 10)
        assert producer.returncode == -9, error.decode()
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(fixture),
            "recover",
            str(tmp_path),
            str(tree_root.path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, error = await asyncio.wait_for(recovery.communicate(), 30)
        assert recovery.returncode == 0, error.decode()
        value = json.loads(out)
        assert value["command_id"] == metadata["command_id"] and value["replay_calls"] == 0
        assert value["same_sdk_http"] and value["usage"] == metadata["resource"]["usage"]
    finally:
        for child in (producer, recovery):
            if child is not None and child.returncode is None:
                child.kill()
                await child.communicate()
        await database.close()
