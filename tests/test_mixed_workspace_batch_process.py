"""独立进程关闭混合父准入，以及SIGKILL后原结果恢复和零发布重放。"""

import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import update

from krim_agent.persistence.database import (
    AgentHandleRow,
    Database,
    MixedWorkspaceBatchRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="需要Linux条件事务与SIGKILL"
)


@asynccontextmanager
async def process(root, phase, source):
    script = Path(__file__).parent / "fixtures" / "mixed_batch_worker_process.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        phase,
        source,
        str(root),
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


async def handshake(child):
    line = await asyncio.wait_for(child.stdout.readline(), 35)
    if not line:
        _out, errors = await child.communicate()
        pytest.fail(errors.decode())
    value = json.loads(line)
    assert value["pid"] == child.pid and child.pid != os.getpid()
    assert value["phase"] == "paused" and value["provider_calls"] == 0
    return value


def folder():
    root = Path(".cache/project-completion/p04-mixed-worker/processes") / str(uuid4())
    (root / "files").mkdir(parents=True)
    return root.resolve()


@pytest.mark.parametrize("source", ["file", "directory"])
@pytest.mark.parametrize("phase", ["before-reserve", "before-lock"])
@pytest.mark.parametrize("stop", ["cancel", "expiry"])
async def test_other_process_closes_parent_before_late_original_reserve_or_publish(
    source, phase, stop
):
    root = folder()
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    store = MixedBatchStore(
        WorkspaceEditStore(database, owner_id="observer"),
        WorkspaceDirectoryStore(database, owner_id="observer"),
    )
    try:
        async with process(root, phase, source) as child:
            ready = await handshake(child)
            if stop == "cancel":
                await store.cancel(ready["scope_id"], ready["batch_id"])
            else:
                async with database.sessions.begin() as sql:
                    await sql.execute(
                        update(MixedWorkspaceBatchRow)
                        .where(MixedWorkspaceBatchRow.batch_id == ready["batch_id"])
                        .values(lease_expires_at=time.time() - 1)
                    )
                await store.get(ready["scope_id"], ready["batch_id"])
            child.stdin.write(b"release\n")
            await child.stdin.drain()
            child.stdin.close()
            stdout, stderr = await asyncio.wait_for(child.communicate(), 35)
            assert child.returncode == 0, stderr.decode()
            final = json.loads(stdout)["batch"]
            assert final["producer_status"] == ("cancelled" if stop == "cancel" else "interrupted")
            assert (
                final["items"][1]["operation"] is None and final["items"][1]["status"] == "skipped"
            )
            assert (
                not (root / "files" / "file.txt").exists()
                and not (root / "files" / "empty").exists()
            )
    finally:
        await database.close()


@pytest.mark.parametrize("source", ["file", "directory"])
@pytest.mark.parametrize("phase", ["after-write", "after-done"])
async def test_sigkill_then_new_process_reads_original_effect_and_reconciles_without_replay(
    source, phase
):
    root = folder()
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    try:
        async with process(root, phase, source) as child:
            ready = await handshake(child)
            first = ready["batch"]["items"][0]["operation"]
            assert first["status"] == ("running" if phase == "after-write" else "done")
            child.kill()
            _stdout, stderr = await asyncio.wait_for(child.communicate(), 10)
            assert child.returncode == -9, stderr.decode()
        assert (root / "files" / "file.txt").exists() == (source == "file")
        assert (root / "files" / "empty").exists() == (source == "directory")
        # 实际SIGKILL已确认后，只加速原租约到期，不接管活跃生产者。
        operation_model = (
            WorkspaceEditAttemptRow if source == "file" else WorkspaceDirectoryAttemptRow
        )
        async with database.sessions.begin() as sql:
            for model, predicate in (
                (AgentHandleRow, AgentHandleRow.agent_id == ready["agent_id"]),
                (MixedWorkspaceBatchRow, MixedWorkspaceBatchRow.batch_id == ready["batch_id"]),
                (operation_model, operation_model.operation_id == first["operation_id"]),
            ):
                await sql.execute(
                    update(model)
                    .where(predicate, model.lease_expires_at.is_not(None))
                    .values(lease_expires_at=time.time() - 1)
                )
        async with process(root, "recover", source) as child:
            stdout, stderr = await asyncio.wait_for(child.communicate(), 45)
            assert child.returncode == 0, stderr.decode()
            value = json.loads(stdout)
            assert (
                value["phase"] == "recovered" and value["replays"] == value["provider_calls"] == 0
            )
            assert value["batch"]["items"][0]["operation"]["operation_id"] == first["operation_id"]
            assert value["batch"]["items"][1]["operation"] is None
    finally:
        await database.close()
