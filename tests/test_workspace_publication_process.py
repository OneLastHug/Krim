"""真实双进程证明：过期生产者不能越过核实，核实不能越过在途文件发布。"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from threading import Event

import pytest
from sqlalchemy import update

from krim_agent.application.workspace_changes import WorkspaceChangeService
from krim_agent.persistence import Database
from krim_agent.persistence.database import WorkspaceEditAttemptRow
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.tools import ToolContext
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace import Workspace, WorkspaceBinding

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


@pytest.mark.parametrize("mode", ["before-lock", "after-authorization"])
async def test_real_process_publication_fence_orders_producer_and_reconciliation(
    tmp_path, monkeypatch, mode
):
    import fcntl

    root = tmp_path / "files"
    root.mkdir()
    target = root / "file.txt"
    target.write_text("old\n")
    database_path = tmp_path / "publication.sqlite"
    script = Path(__file__).parent / "fixtures" / "workspace_publication_worker.py"
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        str(database_path),
        str(root),
        mode,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{database_path}")
    verifier = WorkspaceChangeService(WorkspaceEditStore(database))
    verifying = None
    try:
        assert child.stdin is not None and child.stdout is not None
        line = await asyncio.wait_for(child.stdout.readline(), 30)
        if not line:
            _, errors = await asyncio.wait_for(child.communicate(), 10)
            pytest.fail(f"生产者在握手前退出：{errors.decode()}")
        ready = json.loads(line)
        assert ready["phase"] == "paused" and ready["mode"] == mode
        assert ready["pid"] == child.pid and ready["pid"] != os.getpid()
        assert ready["owner_id"] != verifier.store.owner_id
        operation_id = ready["operation_id"]
        scope = f"topic:{ready['topic_id']}"
        original = await verifier.store.operation(scope, operation_id)
        assert original.view.status == "running"
        assert original.view.proposal_id == ready["proposal_id"]
        # 独立数据库连接只推进已存在操作的到期时间，不接管活跃会话。
        async with database.sessions.begin() as session:
            changed = await session.execute(
                update(WorkspaceEditAttemptRow)
                .where(WorkspaceEditAttemptRow.operation_id == operation_id)
                .values(lease_expires_at=time.time() - 1)
            )
            assert changed.rowcount == 1

        with Workspace(root, writable=True) as workspace:
            context = ToolContext(
                builtin_registry(),
                topic_id=ready["topic_id"],
                workspace=WorkspaceBinding("files", "v1", workspace, workspace.fingerprint),
                origin="host",
            )
            try:
                if mode == "before-lock":
                    resolved = await asyncio.wait_for(
                        verifier.tool_operation(
                            "reconcile", {"operation_id": operation_id}, context
                        ),
                        10,
                    )
                    assert resolved["status"] == "rejected"
                    assert target.read_text() == "old\n"
                else:
                    blocked = Event()
                    original_flock = fcntl.flock

                    def observed_flock(*args):
                        try:
                            return original_flock(*args)
                        except BlockingIOError:
                            # 观察真实内核锁冲突；单靠sleep无法证明核实已经到达锁边界。
                            blocked.set()
                            raise

                    monkeypatch.setattr(fcntl, "flock", observed_flock)
                    verifying = asyncio.create_task(
                        verifier.tool_operation(
                            "reconcile", {"operation_id": operation_id}, context
                        )
                    )
                    assert await asyncio.to_thread(blocked.wait, 3)
                    assert not verifying.done()
                    assert (
                        await verifier.store.operation(scope, operation_id)
                    ).view.status == "uncertain"
                    assert target.read_text() == "old\n"

                child.stdin.write(b"release\n")
                await child.stdin.drain()
                child.stdin.close()
                if verifying is not None:
                    resolved = await asyncio.wait_for(verifying, 10)
                    assert resolved["status"] == "done"
                output, errors = await asyncio.wait_for(child.communicate(), 30)
                assert child.returncode == 0, errors.decode()
                finished = json.loads(output)
                assert finished["phase"] == "finished"
                assert finished["result"]["operation_id"] == operation_id
                expected = "rejected" if mode == "before-lock" else "done"
                stored = await verifier.store.operation(scope, operation_id)
                assert stored.view.status == expected
                assert target.read_text() == ("old\n" if mode == "before-lock" else "new\n")
                assert len(await verifier.store.history(scope, ready["proposal_id"])) == 1
                cleaned = await verifier.tool_operation(
                    "finalize", {"operation_id": operation_id}, context
                )
                assert not cleaned["cleanup_pending"]
                assert not list(root.glob("._krim-tx-*"))
            finally:
                # 先终止子进程释放内核文件锁，再等待核实线程；避免失败遗留后台写入。
                if child.returncode is None:
                    child.kill()
                    await asyncio.wait_for(child.communicate(), 10)
                if verifying is not None:
                    await asyncio.gather(verifying, return_exceptions=True)
    finally:
        if child.returncode is None:
            child.kill()
            await asyncio.wait_for(child.communicate(), 10)
        await verifier.close()
        await database.close()
