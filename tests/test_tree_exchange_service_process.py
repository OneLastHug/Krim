"""实际模型与来源SQL效果后SIGKILL，公共同operation核实/原双树0重放。"""

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
    reason="真实Linux隔离来源/交换/SIGKILL",
)


@pytest.mark.parametrize("kind", ["apply", "keep"])
async def test_original_source_model_sql_sigkill_public_reconcile_keeps_same_fact_and_zero_replay(
    tmp_path, kind
):
    (tmp_path / "files").mkdir()
    script = Path(__file__).parent / "fixtures/tree_exchange_service_process.py"
    produce = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "produce",
        kind,
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recover = None
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(produce.stdout.readline(), 35)
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
            kind,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(recover.communicate(), 25)
        assert recover.returncode == 0, err.decode()
        result = json.loads(out)
        assert result["operation_id"] == metadata["operation_id"]
        assert (
            result["source_plan_id"] == metadata["source_plan_id"]
            and result["origin"] == "completed_tree"
        )
        assert result["status"] == "done" and result["changed"] == (kind == "apply")
        assert result["provider_calls"] == result["replays"] == 0 and result["old_owner_fenced"]
        print(json.dumps({"produced": metadata, "recovered": result}))
    finally:
        for process in (produce, recover):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
