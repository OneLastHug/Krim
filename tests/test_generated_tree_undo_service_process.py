"""真正Model根撤销/keep SQL效果后强杀、新进程公开工具同operation零重放。"""

import asyncio
import json
import shutil
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence.database import AgentHandleRow, Database, WorkspaceTreeMoveAttemptRow

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="真实隔离/Model/SIGKILL",
)


@pytest.mark.parametrize("kind", ["remove_root", "keep"])
async def test_actual_model_generated_root_sigkill_public_same_operation_recovery_zero_replay(
    tmp_path, kind
):
    (tmp_path / "files").mkdir()
    script = Path(__file__).parent / "fixtures/generated_tree_undo_service_process.py"
    producer = await asyncio.create_subprocess_exec(
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
    recovery = None
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 35)
        if not line:
            _, error = await producer.communicate()
            raise AssertionError(error.decode())
        value = json.loads(line)
        assert value["pid"] == producer.pid and value["provider_calls"] == (
            2 if kind == "remove_root" else 4
        )
        assert value["status_before_receipt"] == "running"
        producer.kill()
        _, error = await asyncio.wait_for(producer.communicate(), 10)
        assert producer.returncode == -9, error.decode()
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == value["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            await sql.execute(
                update(WorkspaceTreeMoveAttemptRow)
                .where(WorkspaceTreeMoveAttemptRow.operation_id == value["operation_id"])
                .values(lease_expires_at=time.time() - 1)
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
        out, error = await asyncio.wait_for(recovery.communicate(), 35)
        assert recovery.returncode == 0, error.decode()
        proof = json.loads(out)
        assert (
            proof["operation_id"] == value["operation_id"] and proof["plan_id"] == value["plan_id"]
        )
        assert (
            proof["status"] == "done" and proof["replay_calls"] == 0 and proof["old_owner_fenced"]
        )
        assert (
            proof["root_inode"] == value["root_inode"]
            and proof["file_inode"] == value["file_inode"]
        )
        print(json.dumps({"kind": kind, "produced": value, "recovered": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
