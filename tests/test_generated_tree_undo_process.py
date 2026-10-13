"""真实SQL原根撤销效果后SIGKILL、另进程同operation/来源/inode与零重放。"""

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
    reason="真实Linux隔离/原子根移动/SIGKILL",
)


async def test_generated_root_undo_sql_sigkill_new_process_same_source_and_zero_replay(tmp_path):
    (tmp_path / "files").mkdir()
    script = Path(__file__).parent / "fixtures/generated_tree_undo_process.py"
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
        line = await asyncio.wait_for(produce.stdout.readline(), 30)
        if not line:
            _, err = await produce.communicate()
            raise AssertionError(err.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == produce.pid and metadata["counts"][:3] == [1, 1, 1]
        assert not (tmp_path / "files/tree").exists()
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
                update(WorkspaceTreeMoveAttemptRow)
                .where(WorkspaceTreeMoveAttemptRow.operation_id == metadata["operation_id"])
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
            and result["plan_id"] == metadata["plan_id"]
        )
        assert (
            result["status"] == "done"
            and result["replay_calls"] == 0
            and result["old_owner_fenced"]
        )
        assert (
            result["root_inode"] == metadata["root_inode"]
            and result["file_inode"] == metadata["file_inode"]
        )
        assert (tmp_path / "files/retained/sub/data.bin").read_bytes() == b"\0\xff"
        print(json.dumps({"produced": metadata, "recovered": result}))
    finally:
        for process in (produce, recover):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
