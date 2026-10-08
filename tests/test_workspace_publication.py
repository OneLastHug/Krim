"""发布资格须在文件锁内校验，迟到生产者不能改写已核实文件。"""

import asyncio
import hashlib
import os
import sys
import time
from threading import Event

import pytest
from sqlalchemy import update

from krim_agent.application.workspace_changes import WorkspaceChangeService
from krim_agent.persistence.database import WorkspaceEditAttemptRow
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.tools import ToolContext
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


async def context_for(repository, workspace):
    from krim_agent.tools.builtins import builtin_registry

    topic = await repository.create_topic("发布资格")
    registry = builtin_registry()
    binding = WorkspaceBinding("files", "v1", workspace, workspace.fingerprint)
    return ToolContext(registry, topic_id=topic.id, workspace=binding, origin="host")


async def proposal(backend, context):
    return await backend.tool_operation(
        "preview",
        {
            "spec": {
                "path": "file.txt",
                "operation": "write",
                "expected_sha256": hashlib.sha256(b"old\n").hexdigest(),
                "content": "new\n",
            },
            "request_key": "preview",
        },
        context,
    )


async def expire(database, operation_id):
    async with database.sessions.begin() as session:
        await session.execute(
            update(WorkspaceEditAttemptRow)
            .where(WorkspaceEditAttemptRow.operation_id == operation_id)
            .values(lease_expires_at=time.time() - 1)
        )


async def paused_renew(_context, _operation_id, _abort):
    # 模拟旧生产者暂停，不能借助后台心跳设置abort来通过测试。
    await asyncio.Event().wait()


async def test_late_producer_before_file_lock_cannot_publish_after_reconciliation(
    repository, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    with Workspace(tmp_path, writable=True) as workspace:
        producer = WorkspaceChangeService(WorkspaceEditStore(repository.database))
        verifier = WorkspaceChangeService(WorkspaceEditStore(repository.database))
        context = await context_for(repository, workspace)
        entered, release = Event(), Event()
        original_apply = WorkspaceWriter.apply

        def late(self, *args, **kwargs):
            entered.set()
            assert release.wait(10)
            return original_apply(self, *args, **kwargs)

        monkeypatch.setattr(WorkspaceWriter, "apply", late)
        monkeypatch.setattr(producer, "_renew", paused_renew)
        task = None
        try:
            view = await proposal(producer, context)
            task = asyncio.create_task(
                producer.tool_operation(
                    "change",
                    {
                        "proposal_id": view["proposal_id"],
                        "action": "apply",
                        "expected_revision": 0,
                        "request_key": "apply",
                    },
                    context,
                )
            )
            assert await asyncio.to_thread(entered.wait, 5)
            scope = f"topic:{context.topic_id}"
            attempt = (await producer.store.history(scope, view["proposal_id"]))[0]
            await expire(repository.database, attempt.operation_id)
            result = await verifier.tool_operation(
                "reconcile", {"operation_id": attempt.operation_id}, context
            )
            assert result["status"] == "rejected" and file.read_text() == "old\n"
            release.set()
            await task
            assert file.read_text() == "old\n", "已核实未执行的旧生产者仍发布了文件"
            assert not list(tmp_path.glob("._krim-tx-*"))
            assert (
                await producer.store.operation(scope, attempt.operation_id)
            ).view.status == "rejected"
        finally:
            release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            await producer.close()
            await verifier.close()


async def test_reconciliation_cannot_pass_writer_lock_after_authorization(
    repository, tmp_path, monkeypatch
):
    import krim_agent.workspace_writer as writer_module

    file = tmp_path / "file.txt"
    file.write_text("old\n")
    with Workspace(tmp_path, writable=True) as workspace:
        producer = WorkspaceChangeService(WorkspaceEditStore(repository.database))
        verifier = WorkspaceChangeService(WorkspaceEditStore(repository.database))
        context = await context_for(repository, workspace)
        entered, release = Event(), Event()
        original_rename = writer_module.rename_at

        def stopped_after_check(*args, **kwargs):
            entered.set()
            assert release.wait(10)
            return original_rename(*args, **kwargs)

        monkeypatch.setattr(writer_module, "rename_at", stopped_after_check)
        monkeypatch.setattr(producer, "_renew", paused_renew)
        recovered = asyncio.Event()
        original_recover = verifier.store.recover_interrupted

        async def observe_recovery():
            result = await original_recover()
            recovered.set()
            return result

        monkeypatch.setattr(verifier.store, "recover_interrupted", observe_recovery)
        task = verifying = None
        try:
            view = await proposal(producer, context)
            scope = f"topic:{context.topic_id}"
            task = asyncio.create_task(
                producer.tool_operation(
                    "change",
                    {
                        "proposal_id": view["proposal_id"],
                        "action": "apply",
                        "expected_revision": 0,
                        "request_key": "apply",
                    },
                    context,
                )
            )
            assert await asyncio.to_thread(entered.wait, 5)
            attempt = (await producer.store.history(scope, view["proposal_id"]))[0]
            await expire(repository.database, attempt.operation_id)
            verifying = asyncio.create_task(
                verifier.tool_operation(
                    "reconcile", {"operation_id": attempt.operation_id}, context
                )
            )
            await asyncio.wait_for(recovered.wait(), 5)
            assert (
                await verifier.store.operation(scope, attempt.operation_id)
            ).view.status == "uncertain"
            await asyncio.sleep(0.05)
            assert not verifying.done() and file.read_text() == "old\n"
            release.set()
            result = await verifying
            await task
            assert result["status"] == "done" and file.read_text() == "new\n"
        finally:
            release.set()
            await asyncio.gather(
                *(value for value in (task, verifying) if value is not None), return_exceptions=True
            )
            await producer.close()
            await verifier.close()


async def test_revoked_operation_during_staging_does_not_publish(repository, tmp_path, monkeypatch):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    with Workspace(tmp_path, writable=True) as workspace:
        producer = WorkspaceChangeService(WorkspaceEditStore(repository.database))
        verifier = WorkspaceChangeService(WorkspaceEditStore(repository.database))
        context = await context_for(repository, workspace)
        entered, release = Event(), Event()
        original_sync = os.fsync
        first = True

        def block_staging(fd):
            nonlocal first
            original_sync(fd)
            if first:
                first = False
                entered.set()
                assert release.wait(10)

        monkeypatch.setattr(os, "fsync", block_staging)
        monkeypatch.setattr(producer, "_renew", paused_renew)
        task = None
        try:
            view = await proposal(producer, context)
            scope = f"topic:{context.topic_id}"
            task = asyncio.create_task(
                producer.tool_operation(
                    "change",
                    {
                        "proposal_id": view["proposal_id"],
                        "action": "apply",
                        "expected_revision": 0,
                        "request_key": "apply",
                    },
                    context,
                )
            )
            assert await asyncio.to_thread(entered.wait, 5)
            attempt = (await producer.store.history(scope, view["proposal_id"]))[0]
            await expire(repository.database, attempt.operation_id)
            assert await verifier.store.recover_interrupted() == 1
            release.set()
            await task
            assert file.read_text() == "old\n" and not list(tmp_path.glob("._krim-tx-*"))
            resolved = await verifier.tool_operation(
                "reconcile", {"operation_id": attempt.operation_id}, context
            )
            assert resolved["status"] == "rejected"
        finally:
            release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            await producer.close()
            await verifier.close()


async def test_publication_database_failure_is_rejected_without_file_effect(
    repository, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    with Workspace(tmp_path, writable=True) as workspace:
        producer = WorkspaceChangeService(WorkspaceEditStore(repository.database))
        context = await context_for(repository, workspace)
        checks = 0
        original = producer.store.assert_execution

        async def unavailable(scope, operation_id):
            nonlocal checks
            checks += 1
            if checks == 2:
                raise OSError("publication database read unavailable")
            await original(scope, operation_id)

        monkeypatch.setattr(producer.store, "assert_execution", unavailable)
        try:
            view = await proposal(producer, context)
            result = await producer.tool_operation(
                "change",
                {
                    "proposal_id": view["proposal_id"],
                    "action": "apply",
                    "expected_revision": 0,
                    "request_key": "apply",
                },
                context,
            )
            assert result["status"] == "rejected"
            assert result["receipt"]["error_code"] == "workspace_publication_unavailable"
            assert checks == 2 and file.read_text() == "old\n"
            assert not list(tmp_path.glob("._krim-tx-*"))
        finally:
            await producer.close()


async def test_real_rejected_undo_preserves_previously_kept_proposal(
    repository, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    with Workspace(tmp_path, writable=True) as workspace:
        producer = WorkspaceChangeService(WorkspaceEditStore(repository.database))
        context = await context_for(repository, workspace)
        scope = f"topic:{context.topic_id}"
        try:
            view = await proposal(producer, context)

            async def action(kind):
                current = await producer.store.get(scope, view["proposal_id"])
                return await producer.tool_operation(
                    "change",
                    {
                        "proposal_id": view["proposal_id"],
                        "action": kind,
                        "expected_revision": current.view.revision,
                        "request_key": kind,
                    },
                    context,
                )

            assert (await action("apply"))["status"] == "done"
            assert (await action("keep"))["status"] == "done"
            original_apply = WorkspaceWriter.apply

            def external_change(self, *args, **kwargs):
                file.write_text("external\n")
                return original_apply(self, *args, **kwargs)

            monkeypatch.setattr(WorkspaceWriter, "apply", external_change)
            result = await action("undo")
            assert result["status"] == "rejected"
            assert (await producer.store.get(scope, view["proposal_id"])).view.status == "kept"
            assert file.read_text() == "external\n"
        finally:
            await producer.close()
