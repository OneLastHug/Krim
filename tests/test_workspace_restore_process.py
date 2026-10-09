"""恢复计划跨进程取消、执行前重启与发布后崩溃核实。"""

import asyncio
import json
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy import update

import krim_agent
from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    AgentHandleRow,
    WorkspaceEditAttemptRow,
    WorkspaceRestorePlanRow,
)
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_restores import WorkspaceRestoreStore

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


@asynccontextmanager
async def worker(directory, mode):
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).parent / "fixtures/workspace_restore_worker.py"),
        mode,
        str(directory),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        yield child
    finally:
        if child.returncode is None:
            child.kill()
            await asyncio.wait_for(child.communicate(), 10)


async def ready(child):
    raw = await asyncio.wait_for(child.stdout.readline(), 35)
    if not raw:
        _, error = await child.communicate()
        pytest.fail(error.decode())
    result = json.loads(raw)
    assert result["pid"] == child.pid
    assert Path(result["package"]) == Path(krim_agent.__file__)
    return result


def files(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("A")
    return root


@pytest.mark.parametrize("stop", ["cancel", "expire"])
async def test_other_process_stops_restore_before_publication(tmp_path, stop):
    root = files(tmp_path)
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'restore.sqlite'}")
    store = WorkspaceRestoreStore(WorkspaceEditStore(database))
    try:
        async with worker(tmp_path, "before-lock") as child:
            metadata = await ready(child)
            assert metadata["phase"] == "paused"
            scope, identifier = metadata["scope_id"], metadata["plan_id"]
            if stop == "cancel":
                view = await store.cancel(scope, identifier)
            else:
                async with database.sessions.begin() as sql:
                    await sql.execute(
                        update(WorkspaceRestorePlanRow)
                        .where(WorkspaceRestorePlanRow.plan_id == identifier)
                        .values(lease_expires_at=time.time() - 1)
                    )
                view = await store.get(scope, identifier)
            assert view.phase == ("cancelled" if stop == "cancel" else "interrupted")
            child.stdin.write(b"release\n")
            await child.stdin.drain()
            output, errors = await asyncio.wait_for(child.communicate(), 25)
            assert child.returncode == 0, errors.decode()
            result = json.loads(output)["view"]
            assert result["phase"] == view.phase
            assert result["items"][0]["operation"]["status"] == "rejected", result
            assert (root / "file.txt").read_text() == "B"
            assert not list(root.glob("._krim-tx-*"))
    finally:
        await database.close()


@pytest.mark.parametrize("mode", ["prepare", "before-save"])
async def test_restore_reopens_original_plan_and_never_replays_unknown_effect(tmp_path, mode):
    root = files(tmp_path)
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'restore.sqlite'}")
    try:
        async with worker(tmp_path, mode) as child:
            metadata = await ready(child)
            if mode == "before-save":
                child.stdin.write(b"release\n")
                await child.stdin.drain()
            _, errors = await asyncio.wait_for(child.communicate(), 20)
            assert child.returncode == (0 if mode == "prepare" else 23), errors.decode()
        if mode == "before-save":
            assert (root / "file.txt").read_text() == "A"
            async with database.sessions.begin() as sql:
                await sql.execute(update(AgentHandleRow).values(lease_expires_at=time.time() - 1))
                await sql.execute(
                    update(WorkspaceEditAttemptRow).values(lease_expires_at=time.time() - 1)
                )
                await sql.execute(
                    update(WorkspaceRestorePlanRow).values(lease_expires_at=time.time() - 1)
                )
        else:
            assert (root / "file.txt").read_text() == "B"
        async with worker(tmp_path, "recover") as child:
            output, errors = await asyncio.wait_for(child.communicate(), 35)
            assert child.returncode == 0, errors.decode()
            result = json.loads(output)
            assert (
                result["view"]["plan_id"] == metadata["plan_id"]
                and result["view"]["status"] == "done"
            )
            assert Path(result["package"]) == Path(krim_agent.__file__)
        assert (root / "file.txt").read_text() == "A"
    finally:
        await database.close()
