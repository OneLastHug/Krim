"""实际0043模型来源/原材料/锁，原effect后强杀和另进程0重放核实。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence.database import AgentHandleRow, Database, WorkspaceTreeMoveAttemptRow

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="需要实际rename/SIGKILL"
)


async def test_original_sql_tree_move_effect_sigkill_sources_recovery_and_zero_replay(tmp_path):
    files = tmp_path / "files"
    (files / "tree/sub/empty").mkdir(parents=True)
    (files / "dest").mkdir()
    (files / "tree/sub/data.bin").write_bytes(b"\0\xff")
    script = Path(__file__).parent / "fixtures/tree_move_ledger_process.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "after-move",
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    recovery = None
    try:
        line = await asyncio.wait_for(child.stdout.readline(), 20)
        if not line:
            _output, error = await child.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == child.pid and metadata["effect_before_receipt"]
        assert metadata["view"]["status"] == "running" and metadata["counts"][3] == 2
        child.kill()
        _output, error = await asyncio.wait_for(child.communicate(), 10)
        assert child.returncode == -9, error.decode()
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            await sql.execute(
                update(WorkspaceTreeMoveAttemptRow)
                .where(WorkspaceTreeMoveAttemptRow.operation_id == metadata["operation_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 20)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert proof["view"]["operation_id"] == metadata["operation_id"]
        assert proof["view"]["status"] == "done" and proof["counts"] == [1, 1, 0, 2]
        assert proof["replay_calls"] == proof["provider_calls"] == 0 and proof["old_owner_fenced"]
        assert proof["original_sql_blob"] and proof["sources_original_run_call"]
        assert (files / "dest/tree/sub/data.bin").read_bytes() == b"\0\xff"
        assert not (files / "tree").exists()
    finally:
        for process in (child, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
