"""实际来源绑定SQL交换效果后SIGKILL，原scope/operation/封存源新进程0重放。"""

import asyncio
import json
import shutil
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence.database import (
    AgentHandleRow,
    Database,
    WorkspaceTreeExchangeAttemptRow,
)

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="真实Linux隔离/rename/SIGKILL",
)


async def test_original_bound_source_sql_sigkill_unknown_new_process_same_origin_and_zero_replay(
    tmp_path,
):
    (tmp_path / "files").mkdir()
    script = Path(__file__).parent / "fixtures/tree_exchange_origin_process.py"
    produce = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "produce",
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recover = None
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(produce.stdout.readline(), 25)
        if not line:
            _, err = await produce.communicate()
            raise AssertionError(err.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == produce.pid
        produce.kill()
        _, err = await asyncio.wait_for(produce.communicate(), 5)
        assert produce.returncode == -9, err.decode()
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            await sql.execute(
                update(WorkspaceTreeExchangeAttemptRow)
                .where(WorkspaceTreeExchangeAttemptRow.operation_id == metadata["operation_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        recover = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(recover.communicate(), 20)
        assert recover.returncode == 0, err.decode()
        result = json.loads(out)
        assert (
            result["operation_id"] == metadata["operation_id"]
            and result["source_plan_id"] == metadata["source_plan_id"]
        )
        assert (
            result["status"] == "done"
            and result["origin"] == "completed_tree"
            and result["replay_calls"] == 0
            and result["old_owner_fenced"]
            and result["startup_recovered"]
            and result["idempotent_recovery"] == 0
        )
        print(json.dumps({"produced": metadata, "recovered": result}))
    finally:
        for process in (produce, recover):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
