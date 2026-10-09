"""独立进程证明捕获fence与崩溃后的原键只读行为。"""

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
from krim_agent.persistence.database import AgentHandleRow, NativeWorkspaceCheckpointRow
from krim_agent.persistence.workspace_checkpoints import WorkspaceCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件锁")


@asynccontextmanager
async def worker(directory, mode):
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).parent / "fixtures/workspace_checkpoint_worker.py"),
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
    raw = await asyncio.wait_for(child.stdout.readline(), 25)
    if not raw:
        _, error = await child.communicate()
        pytest.fail(error.decode())
    result = json.loads(raw)
    assert result["phase"] == "paused" and result["pid"] == child.pid
    assert Path(result["package"]) == Path(krim_agent.__file__)
    return result


@pytest.mark.parametrize("action", ["cancel", "expire"])
async def test_other_process_fences_late_checkpoint_reader(tmp_path, action):
    root = tmp_path / "files"
    root.mkdir()
    (root / "first.txt").write_text("original")
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'capture.sqlite'}")
    store = WorkspaceCheckpointStore(WorkspaceEditStore(database))
    try:
        async with worker(tmp_path, "before-lock") as child:
            metadata = await ready(child)
            scope, identifier = metadata["scope_id"], metadata["checkpoint_id"]
            if action == "cancel":
                view = await store.cancel(scope, identifier)
            else:
                async with database.sessions.begin() as sql:
                    await sql.execute(
                        update(NativeWorkspaceCheckpointRow)
                        .where(NativeWorkspaceCheckpointRow.checkpoint_id == identifier)
                        .values(lease_expires_at=time.time() - 1)
                    )
                view = await store.get(scope, identifier)
            assert view.status == ("cancelled" if action == "cancel" else "interrupted")
            child.stdin.write(b"release\n")
            await child.stdin.drain()
            output, errors = await asyncio.wait_for(child.communicate(), 20)
            assert child.returncode == 0, errors.decode()
            finished = json.loads(output)
            assert finished["view"]["status"] == view.status
            assert all(not item["captured"] for item in finished["view"]["files"])
            assert (root / "first.txt").read_text() == "original"
            assert not (root / "missing.txt").exists()
            assert not list(root.glob("._krim-tx-*"))
    finally:
        await database.close()


async def test_crashed_partial_checkpoint_reopens_as_unavailable_without_recapture(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "first.txt").write_text("original")
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'capture.sqlite'}")
    try:
        async with worker(tmp_path, "after-record") as child:
            metadata = await ready(child)
            child.stdin.write(b"release\n")
            await child.stdin.drain()
            _, errors = await asyncio.wait_for(child.communicate(), 15)
            assert child.returncode == 23, errors.decode()
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(NativeWorkspaceCheckpointRow).values(lease_expires_at=time.time() - 1)
            )
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
        async with worker(tmp_path, "recover") as child:
            output, errors = await asyncio.wait_for(child.communicate(), 25)
            assert child.returncode == 0, errors.decode()
            result = json.loads(output)
            assert result["phase"] == "recovered"
            assert Path(result["package"]) == Path(krim_agent.__file__)
            assert result["view"]["checkpoint_id"] == metadata["checkpoint_id"]
        assert (root / "first.txt").read_text() == "original"
        assert not (root / "missing.txt").exists()
    finally:
        await database.close()
