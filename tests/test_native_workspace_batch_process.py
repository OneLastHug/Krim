"""独立进程验证持久批次关闭准入、迟到发布、原子项回执恢复及显式核实。"""

import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy import update

from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    AgentHandleRow,
    NativeWorkspaceBatchRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.persistence.workspace_batches import NativeBatchStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


@asynccontextmanager
async def process(directory, mode):
    script = Path(__file__).parent / "fixtures" / "native_workspace_batch_worker.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
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


async def handshake(child):
    line = await asyncio.wait_for(child.stdout.readline(), 35)
    if not line:
        _, errors = await asyncio.wait_for(child.communicate(), 10)
        pytest.fail(f"批次生产者在握手前退出：{errors.decode()}")
    values = json.loads(line)
    assert values["phase"] == "paused" and values["pid"] == child.pid
    assert values["pid"] != os.getpid() and values["provider_calls"] == 0
    assert [item["path"] for item in values["batch"]["items"]] == ["second.txt", "first.txt"]
    return values


def prepare(directory):
    root = directory / "files"
    root.mkdir()
    for name in ("first.txt", "second.txt"):
        (root / name).write_text(f"old:{name}\n", encoding="utf-8")
    return root


@pytest.mark.parametrize("mode", ["before-reserve", "before-lock"])
@pytest.mark.parametrize("stop", ["expire", "cancel"])
async def test_other_process_closes_batch_before_late_producer_can_reserve_or_publish(
    tmp_path, mode, stop
):
    root = prepare(tmp_path)
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'batch.sqlite'}")
    observer = NativeBatchStore(WorkspaceEditStore(database))
    try:
        async with process(tmp_path, mode) as child:
            ready = await handshake(child)
            scope, batch_id = ready["scope_id"], ready["batch_id"]
            current = await observer.get(scope, batch_id)
            assert current.producer_status == "running"
            operation = current.items[0].operation
            if mode == "before-lock":
                assert operation is not None and operation.status == "running"
            else:
                assert operation is None
            if stop == "expire":
                async with database.sessions.begin() as session:
                    await session.execute(
                        update(NativeWorkspaceBatchRow)
                        .where(NativeWorkspaceBatchRow.batch_id == batch_id)
                        .values(lease_expires_at=time.time() - 1)
                    )
                stopped = await observer.get(scope, batch_id)
                assert stopped.producer_status == "interrupted"
            else:
                stopped = await observer.cancel(scope, batch_id)
                assert stopped.producer_status == "cancelled"
            assert stopped.items[1].status == "skipped" and stopped.items[1].operation is None
            if operation is not None:
                assert stopped.status == "uncertain"
                assert stopped.items[0].operation.operation_id == operation.operation_id
            else:
                assert stopped.items[0].status == "skipped"
            # 只关闭batch资格；Agent及已reserve操作仍有效，隔离验证batch发布fence。
            child.stdin.write(b"release\n")
            await child.stdin.drain()
            child.stdin.close()
            output, errors = await asyncio.wait_for(child.communicate(), 35)
            assert child.returncode == 0, errors.decode()
            result = json.loads(output)
            assert result["phase"] == "finished"
            final = await observer.get(scope, batch_id)
            assert final.producer_status == stopped.producer_status
            assert final.items[1].status == "skipped" and final.items[1].operation is None
            for item in final.items:
                assert (root / item.path).read_text() == f"new:{item.path}\n"
                history = result["histories"][item.proposal_id]
                if operation is not None and item.position == 0:
                    assert item.status == "rejected"
                    assert item.operation.operation_id == operation.operation_id
                    assert item.operation.receipt.error_code == "workspace_batch_fenced"
                    assert len(history) == 2
                else:
                    assert item.status == "skipped" and item.operation is None
                    assert len(history) == 1
            assert result["provider_calls"] == 0
            assert not list(root.glob("._krim-tx-*"))
    finally:
        await database.close()


@pytest.mark.parametrize("mode", ["crash-after-done", "crash-after-write"])
async def test_crashed_batch_recovers_original_operation_and_never_replays_unstarted_item(
    tmp_path, mode
):
    root = prepare(tmp_path)
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'batch.sqlite'}")
    try:
        async with process(tmp_path, mode) as child:
            ready = await handshake(child)
            original = ready["batch"]["items"][0]["operation"]
            assert original["status"] == ("done" if mode == "crash-after-done" else "running")
            assert (root / "second.txt").read_text() == "old:second.txt\n"
            assert (root / "first.txt").read_text() == "new:first.txt\n"
            child.stdin.write(b"crash\n")
            await child.stdin.drain()
            child.stdin.close()
            _, errors = await asyncio.wait_for(child.communicate(), 10)
            assert child.returncode == 23, errors.decode()
        # 已确认进程终止后，仅加速原租约到期；未接管任何活跃Agent。
        async with database.sessions.begin() as session:
            for model, predicate in (
                (AgentHandleRow, AgentHandleRow.agent_id == ready["agent_id"]),
                (NativeWorkspaceBatchRow, NativeWorkspaceBatchRow.batch_id == ready["batch_id"]),
                (
                    WorkspaceEditAttemptRow,
                    WorkspaceEditAttemptRow.operation_id == original["operation_id"],
                ),
            ):
                await session.execute(
                    update(model)
                    .where(predicate, model.lease_expires_at.is_not(None))
                    .values(lease_expires_at=time.time() - 1)
                )
        async with process(tmp_path, "recover") as resumed:
            output, errors = await asyncio.wait_for(resumed.communicate(), 40)
            assert resumed.returncode == 0, errors.decode()
            recovered = json.loads(output)
            assert recovered["phase"] == "recovered"
            assert recovered["writes"] == recovered["provider_calls"] == 0
            assert recovered["batch"]["batch_id"] == ready["batch_id"]
            first, second = recovered["batch"]["items"]
            assert first["operation"]["operation_id"] == original["operation_id"]
            assert first["status"] == "done" and second["status"] == "skipped"
            assert second["operation"] is None
            assert recovered["original_status"] == (
                "interrupted" if mode == "crash-after-done" else "uncertain"
            )
        assert (root / "second.txt").read_text() == "old:second.txt\n"
        assert (root / "first.txt").read_text() == "new:first.txt\n"
        assert not list(root.glob("._krim-tx-*"))
    finally:
        await database.close()
