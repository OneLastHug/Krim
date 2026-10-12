"""真实树原事务强杀后冷读和核实原回执，不重发发布。"""

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
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
    WorkspaceTreePlanRow,
)

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要Linux真实隔离及SIGKILL测试进程",
)


@pytest.mark.parametrize("source", ["directory", "file"])
async def test_sigkill_after_original_effect_recovers_same_operation_without_replay(
    tmp_path, source
):
    (tmp_path / "files").mkdir()
    script = Path(__file__).parent / "fixtures" / "workspace_tree_fence_process.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        "after-write",
        source,
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(child.stdout.readline(), 35)
        assert line
        value = json.loads(line)
        assert value["pid"] == child.pid and value["provider_calls"] == 0
        position = 0 if source == "directory" else 2
        original = value["steps"]["items"][position]["operation"]
        assert original["status"] == "running"
        child.kill()
        _, errors = await asyncio.wait_for(child.communicate(), 10)
        assert child.returncode == -9, errors.decode()
        async with database.sessions.begin() as sql:
            model = WorkspaceEditAttemptRow if source == "file" else WorkspaceDirectoryAttemptRow
            for current, predicate in (
                (AgentHandleRow, AgentHandleRow.agent_id == value["agent_id"]),
                (WorkspaceTreePlanRow, WorkspaceTreePlanRow.plan_id == value["plan_id"]),
                (model, model.operation_id == original["operation_id"]),
            ):
                await sql.execute(
                    update(current).where(predicate).values(lease_expires_at=time.time() - 1)
                )
        recovered = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script),
            "recover",
            source,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, errors = await asyncio.wait_for(recovered.communicate(), 45)
            assert recovered.returncode == 0, errors.decode()
            cold = json.loads(stdout)
            assert cold["replay_calls"] == cold["provider_calls"] == 0
            assert cold["view"]["done_count"] == position + 1
        finally:
            if recovered.returncode is None:
                recovered.kill()
                await recovered.communicate()
    finally:
        if child.returncode is None:
            child.kill()
            await child.communicate()
        await database.close()
