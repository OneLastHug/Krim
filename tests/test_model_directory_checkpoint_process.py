"""两点原模型目录SQL后SIGKILL，跨进程原Scope/来源/材料/0重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import update
from test_directory_checkpoint_capture import make_tree

from krim_agent.persistence import Database
from krim_agent.persistence.database import AgentHandleRow, WorkspaceDirectoryCaptureRow


@pytest.mark.parametrize("stage", ["record", "finish"])
async def test_model_directory_sigkill_recovers_source_bytes_without_replay(tmp_path, stage):
    files = tmp_path / "files"
    files.mkdir()
    make_tree(files)
    fixture = Path(__file__).parent / "fixtures/model_directory_checkpoint_process.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixture),
        "produce",
        stage,
        str(tmp_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    recovery = None
    try:
        line = await asyncio.wait_for(child.stdout.readline(), 25)
        if not line:
            _, errors = await child.communicate()
            raise AssertionError(errors.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == child.pid and metadata["provider_calls"] == 1
        assert metadata["view"]["snapshot"]["entry_count"] == 6
        assert metadata["view"]["status"] == ("capturing" if stage == "record" else "available")
        child.kill()
        _, errors = await asyncio.wait_for(child.communicate(), 10)
        assert child.returncode == -9, errors.decode()
        async with database.sessions() as sql, sql.begin():
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            if stage == "record":
                await sql.execute(
                    update(WorkspaceDirectoryCaptureRow)
                    .where(WorkspaceDirectoryCaptureRow.checkpoint_id == metadata["checkpoint_id"])
                    .values(lease_expires_at=time.time() - 1)
                )
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(fixture),
            "recover",
            stage,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, errors = await asyncio.wait_for(recovery.communicate(), 25)
        assert recovery.returncode == 0, errors.decode()
        proof = json.loads(out)
        assert proof["checkpoint_id"] == metadata["checkpoint_id"]
        assert proof["source"] == {"run_id": metadata["run_id"], "call_id": metadata["call_id"]}
        assert proof["status"] == ("interrupted" if stage == "record" else "available")
        assert proof["counts"] == [1, 1, 1] and proof["replays"] == proof["provider_calls"] == 0
        assert (
            proof["same_sdk_http"]
            and proof["old_owner_fenced"]
            and proof["inode"] == metadata["inode"]
        )
    finally:
        for process in (child, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
        await database.close()
