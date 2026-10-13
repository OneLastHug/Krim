"""实际合成模型工具effect后SIGKILL，另进程公共核实同一0043事实。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence.database import AgentHandleRow, Database, WorkspaceTreeMoveAttemptRow

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="实际Linux SIGKILL")


@pytest.mark.parametrize("kind", ["apply", "keep"])
async def test_original_model_tree_move_sigkill_public_reconcile_and_no_replay(tmp_path, kind):
    files = tmp_path / "files"
    (files / "tree/sub/empty").mkdir(parents=True)
    (files / "dest").mkdir()
    (files / "tree/sub/data.bin").write_bytes(b"\0\xff\x80")
    script = Path(__file__).parent / "fixtures/tree_move_service_process.py"
    child = await asyncio.create_subprocess_exec(
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
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    recovery = None
    try:
        line = await asyncio.wait_for(child.stdout.readline(), 35)
        if not line:
            _output, error = await child.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == child.pid and metadata["effect_before_receipt"]
        assert metadata["provider_calls"] == (3 if kind == "apply" else 5)
        assert metadata["view"]["status"] == "running"
        child.kill()
        _output, error = await asyncio.wait_for(child.communicate(), 10)
        assert child.returncode == -9, error.decode()
        async with database.sessions.begin() as sql:
            for table, predicate in (
                (AgentHandleRow, AgentHandleRow.agent_id == metadata["agent_id"]),
                (
                    WorkspaceTreeMoveAttemptRow,
                    WorkspaceTreeMoveAttemptRow.operation_id == metadata["operation_id"],
                ),
            ):
                await sql.execute(
                    update(table).where(predicate).values(lease_expires_at=time.time() - 1)
                )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            kind,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovery.communicate(), 35)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(output)
        assert (
            proof["view"]["operation_id"] == metadata["operation_id"]
            and proof["view"]["status"] == "done"
        )
        assert proof["provider_calls"] == proof["replay_calls"] == 0
        assert (
            proof["original_run_call"] and proof["original_sql_blob"] and proof["old_owner_fenced"]
        )
        print(json.dumps({"kind": kind, "metadata": metadata, "recovered": proof}))
    finally:
        for process in (child, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
