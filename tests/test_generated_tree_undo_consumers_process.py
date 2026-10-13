"""原Model SQL根效果后SIGKILL，管理SDK/HTTP/真实TCP/CLI同事实零重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path

from sqlalchemy import update

from krim_agent.persistence.database import AgentHandleRow, Database, WorkspaceTreeMoveAttemptRow


async def test_actual_model_root_sigkill_all_management_consumers_same_operation_zero_replay(
    tmp_path,
):
    (tmp_path / "files").mkdir()
    fixture = Path(__file__).parent / "fixtures"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixture / "generated_tree_undo_service_process.py"),
        "produce",
        "remove_root",
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(child.stdout.readline(), 35)
        if not line:
            _, error = await child.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == child.pid and metadata["provider_calls"] == 2
        child.kill()
        _, error = await asyncio.wait_for(child.communicate(), 10)
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
            str(fixture / "generated_tree_undo_consumers_process.py"),
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, error = await asyncio.wait_for(recovery.communicate(), 35)
        assert recovery.returncode == 0, error.decode()
        result = json.loads(out)
        assert (
            result["operation_id"] == metadata["operation_id"]
            and result["plan_id"] == metadata["plan_id"]
        )
        assert (
            result["consumers_same_fact"]
            and result["replay_calls"] == 0
            and result["old_owner_fenced"]
        )
        print(json.dumps({"produced": metadata, "recovered": result}))
    finally:
        for process in (child, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
