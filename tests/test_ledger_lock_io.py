"""无变化的公共查询不改数据库；原写意图仍阻止真正另进程越过最新资格。"""

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import text

from krim_agent.execution import CommandSpec, SandboxLimits
from krim_agent.execution.records import CommandRequest, CommandResource
from krim_agent.persistence import Database
from krim_agent.persistence.command_models import CommandRow
from krim_agent.persistence.commands import CommandStore
from krim_agent.persistence.workspace_checkpoints import WorkspaceCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore


async def writer(database, session, kind):
    if kind == "command":
        await CommandStore._writer(session)
    else:
        await WorkspaceEditStore(database)._writer(session)


async def child(payload):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(Path(__file__).parent / "fixtures/ledger_lock_process.py"),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        process.stdin.write(json.dumps(payload).encode() + b"\n")
        await process.stdin.drain()
        process.stdin.close()
        assert await asyncio.wait_for(process.stdout.readline(), 10) == b"WAITING\n"
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    return process


async def reserve(repository):
    store = CommandStore(repository.database)
    topic = await repository.create_topic("实际账本写锁")
    scope = "topic:" + topic.id
    accepted = await store.reserve(
        scope,
        CommandRequest(request_key="lock-source", command=CommandSpec(argv=["/usr/bin/true"])),
        CommandResource(key="test", fingerprint="a" * 64),
        SandboxLimits(),
    )
    return store, scope, accepted


@pytest.mark.parametrize("kind", ["command", "checkpoint"])
async def test_unchanged_public_history_and_authority_do_not_change_database(repository, kind):
    database = repository.database
    store, scope, accepted = await reserve(repository)
    checkpoints = WorkspaceCheckpointStore(WorkspaceEditStore(database))
    await checkpoints.by_key(scope, "missing")
    observer = Database(str(database.engine.url))
    try:
        async with observer.engine.connect() as connection:
            before = await connection.scalar(text("PRAGMA data_version"))
            await connection.commit()
            for _ in range(3):
                if kind == "command":
                    assert (await store.get(scope, accepted.view.command_id)) == accepted.view
                    await store.authorize(scope, accepted.view.command_id, accepted.token)
                    resources = await store.resources(scope, accepted.view.command_id)
                    assert resources.command_id == accepted.view.command_id
                else:
                    assert await checkpoints.by_key(scope, "missing") is None
            after = await connection.scalar(text("PRAGMA data_version"))
            await connection.commit()
            assert after == before, "无变化的历史/资格查询不应提交额外的数据库写入"
    finally:
        await observer.close()


@pytest.mark.parametrize("holding", ["command", "workspace"])
@pytest.mark.parametrize("competing", ["command", "workspace"])
async def test_unchanged_writer_intent_still_blocks_real_other_process(
    repository, holding, competing
):
    database = repository.database
    async with database.write_lock, database.sessions.begin() as session:
        await writer(database, session, "command")
        await writer(database, session, "workspace")
    process = pending = None
    try:
        async with database.write_lock, database.sessions.begin() as session:
            await writer(database, session, holding)
            process = await child(
                {"url": str(database.engine.url), "operation": "lock", "kind": competing}
            )
            pending = asyncio.create_task(process.stdout.readline())
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(pending), 0.15)
            assert not pending.done() and process.returncode is None
        response = json.loads(await asyncio.wait_for(pending, 10))
        assert response == {"locked": True, "changes": 0}
        assert await asyncio.wait_for(process.wait(), 10) == 0
    finally:
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("revoked", ["cancel", "lease"])
async def test_waiting_original_producer_reads_new_cancel_or_expiry_after_real_lock_release(
    repository, revoked
):
    database = repository.database
    store, scope, accepted = await reserve(repository)
    process = pending = None
    try:
        async with database.write_lock, database.sessions.begin() as session:
            await store._writer(session)
            process = await child(
                {
                    "url": str(database.engine.url),
                    "operation": "authorize",
                    "owner": store.owner_id,
                    "scope": scope,
                    "identifier": accepted.view.command_id,
                    "token": accepted.token,
                }
            )
            pending = asyncio.create_task(process.stdout.readline())
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(pending), 0.15)
            assert not pending.done()
            row = await session.get(CommandRow, accepted.view.command_id)
            if revoked == "cancel":
                row.cancel_requested = True
            else:
                row.lease_expires_at = time.time() - 1
        response = json.loads(await asyncio.wait_for(pending, 10))
        expected = "execution_cancelled" if revoked == "cancel" else "execution_lease_lost"
        assert response == {"error": expected}
        assert await asyncio.wait_for(process.wait(), 10) == 0
    finally:
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)
