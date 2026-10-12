"""原生批次经过真实SDK、同一Registry和文件账本，不以Hook返回值冒充效果。"""

import asyncio
import sys
import time
from threading import Event

import pytest
from sqlalchemy import select, update
from test_workspace_changes import setup, wait_event
from test_workspace_effect_chain import apply_content

from krim_agent.persistence.database import (
    NativeWorkspaceBatchChildRow,
    NativeWorkspaceBatchRow,
    WorkspaceEditAttemptRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession
from krim_agent.tools import ShortCircuit, ToolError
from krim_agent.workspace_change import WorkspaceChangeAction
from krim_agent.workspace_list import NativeBatchRequest, NativeBatchTarget
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


def request(page, kind="keep", key="batch", items=None):
    return NativeBatchRequest(
        action=kind,
        snapshot_revision=page.snapshot_revision,
        request_key=key,
        targets=[
            NativeBatchTarget(proposal_id=item.proposal_id, expected_revision=item.revision)
            for item in (page.items if items is None else items)
        ],
    )


async def run_batch(session, value, *, confirmed=False):
    started = await session.start_file_change_batch(value, confirmed=confirmed)
    assert started.status == "running"
    # 32项真实文件fsync在并行回归时可能超过SDK默认30秒；超时仅结束等待。
    result = await session.wait_file_change_batch(started.batch_id, timeout_seconds=120)
    assert result.producer_status != "running"
    return result


async def assert_ledger(runtime, batch):
    """核对公开逐项结果与持久child→原operation关联一致。"""
    database = runtime.repository.database
    async with database.write_lock, database.sessions() as sql:
        children = list(
            await sql.scalars(
                select(NativeWorkspaceBatchChildRow)
                .where(NativeWorkspaceBatchChildRow.batch_id == batch.batch_id)
                .order_by(NativeWorkspaceBatchChildRow.position)
            )
        )
        assert len(children) == len(batch.items)
        for child, item in zip(children, batch.items, strict=True):
            assert not child.admission_open
            if item.operation is None:
                assert child.operation_id is None
                assert item.status != "done"
            else:
                assert child.operation_id == item.operation.operation_id
                attempt = await sql.get(WorkspaceEditAttemptRow, child.operation_id)
                assert attempt.proposal_id == item.proposal_id
                assert attempt.status == item.status


async def test_chain_keep_and_reverse_undo_sort_targets_and_use_real_ledger(runtime, tmp_path):
    file = tmp_path / "chain.txt"
    file.write_text("A\n")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        first, _ = await apply_content(session, file, "B\n", "first")
        second, _ = await apply_content(session, file, "C\n", "second")
        page = await session.file_change_list()
        assert page.total == 2 and [item.proposal_id for item in page.items] == [
            second.proposal_id,
            first.proposal_id,
        ]
        stamp = file.stat()
        kept = await run_batch(session, request(page, items=list(reversed(page.items))))
        current = file.stat()
        attributes = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_nlink",
            "st_uid",
            "st_gid",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        # 原keep核实会读取内容并推进atime，内容与身份/权限/纳秒修改时间仍须完全保持。
        assert kept.status == "done"
        assert tuple(getattr(current, name) for name in attributes) == tuple(
            getattr(stamp, name) for name in attributes
        )
        assert file.read_bytes() == b"C\n"
        assert [item.proposal_id for item in kept.items] == [second.proposal_id, first.proposal_id]
        assert all(not item.operation.receipt.changed for item in kept.items)
        proof = await session.file_change_proof(kept.items[1].operation.operation_id)
        assert proof.kind == "successor_chain" and proof.tip_proposal_id == second.proposal_id
        await assert_ledger(runtime, kept)
        page = await session.file_change_list(filter="all")
        undone = await run_batch(
            session, request(page, "undo", "undo", items=list(reversed(page.items)))
        )
        assert undone.status == "done" and file.read_text() == "A\n"
        assert [item.proposal_id for item in undone.items] == [
            second.proposal_id,
            first.proposal_id,
        ]
        assert all(item.operation.receipt.changed for item in undone.items)
        await assert_ledger(runtime, undone)
        assert (await session.file_change_list()).total == 0
    finally:
        await service.close()


@pytest.mark.parametrize("successor", ["changed", "noop", "aba", "foreign"])
async def test_undo_cannot_skip_unselected_or_foreign_successor(runtime, tmp_path, successor):
    file = tmp_path / "chain.txt"
    file.write_text("A\n")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        first, _ = await apply_content(session, file, "B\n", "first")
        owner = (
            AgentSession(service, (await service.spawn("default")).agent_id)
            if successor == "foreign"
            else session
        )
        await apply_content(owner, file, "B\n" if successor == "noop" else "C\n", "successor")
        if successor in {"aba", "foreign"}:
            await apply_content(owner, file, "B\n", "return")
        expected = file.read_text()
        page = await session.file_change_list()
        batch = await run_batch(
            session,
            request(
                page,
                "undo",
                items=[item for item in page.items if item.proposal_id == first.proposal_id],
            ),
        )
        assert batch.status == "failed" and file.read_text() == expected
        assert batch.items[0].error_code == "workspace_batch_dependency"
        assert batch.items[0].operation is None
        assert len(await session.file_change_history(first.proposal_id)) == 1
        await assert_ledger(runtime, batch)
    finally:
        await service.close()


async def test_one_file_failure_blocks_earlier_chain_but_other_file_continues(runtime, tmp_path):
    file, other = tmp_path / "chain.txt", tmp_path / "independent.txt"
    file.write_text("A\n")
    other.write_text("one\n")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        first, _ = await apply_content(session, file, "B\n", "first")
        separate, _ = await apply_content(session, other, "two\n", "separate")
        latest, _ = await apply_content(session, file, "C\n", "latest")
        page = await session.file_change_list()
        file.write_text("external\n")
        batch = await run_batch(session, request(page, "undo"))
        assert batch.status == "partial"
        by_id = {item.proposal_id: item for item in batch.items}
        assert by_id[latest.proposal_id].status == "rejected"
        assert by_id[first.proposal_id].status == "skipped"
        assert by_id[first.proposal_id].error_code == "workspace_batch_dependency"
        assert by_id[separate.proposal_id].status == "done"
        assert file.read_text() == "external\n" and other.read_text() == "one\n"
        await assert_ledger(runtime, batch)
    finally:
        await service.close()


async def test_thirty_three_real_changes_are_paginated_and_explicit_pages_processed(
    runtime, tmp_path
):
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposals = []
        for index in range(33):
            proposal, _ = await apply_content(
                session, tmp_path / f"item-{index}.txt", f"value-{index}\n", f"item-{index}"
            )
            proposals.append(proposal)
        first = await session.file_change_list(limit=32)
        assert first.total == 33 and len(first.items) == 32 and first.next_cursor
        second = await session.file_change_list(cursor=first.next_cursor, limit=32)
        assert second.total == 33 and len(second.items) == 1 and second.next_cursor is None
        seen = {item.proposal_id for item in first.items + second.items}
        assert seen == {item.proposal_id for item in proposals}
        # “本页”是显式32项；第二批只处理游标页的一项，不能隐式吞掉第一页。
        result = await run_batch(session, request(second, "undo", "page-2"))
        assert result.status == "done" and len(result.items) == 1
        assert not (tmp_path / second.items[0].path).exists()
        assert (await session.file_change_list()).total == 32
        await assert_ledger(runtime, result)
        # 任何动作改变快照：旧页不能继续误用，重新读取后处理剩余32项。
        with pytest.raises(ServiceError) as stale:
            await session.start_file_change_batch(request(first, "undo", "stale-page"))
        assert stale.value.code == "workspace_list_revision_conflict"
        with pytest.raises(ServiceError) as cursor:
            await session.file_change_list(cursor=first.next_cursor)
        assert cursor.value.code == "workspace_list_stale"
        remaining = await session.file_change_list()
        result = await run_batch(session, request(remaining, "undo", "remaining"))
        assert result.status == "done" and len(result.items) == 32
        assert not list(tmp_path.glob("item-*.txt"))
        assert (await session.file_change_list()).total == 0
        await assert_ledger(runtime, result)
    finally:
        await service.close()


async def test_batch_key_is_idempotent_snapshot_bound_and_scope_isolated(runtime, tmp_path):
    file = tmp_path / "one.txt"
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await apply_content(session, file, "new\n", "one")
        page = await session.file_change_list()
        value = request(page, "keep", "same-key")
        first = await session.start_file_change_batch(value)
        again = await session.start_file_change_batch(value)
        assert again.batch_id == first.batch_id
        with pytest.raises(ServiceError) as conflict:
            await session.start_file_change_batch(
                value.model_copy(update={"request_key": "same-key", "action": "undo"})
            )
        assert conflict.value.code == "workspace_batch_idempotency_conflict"
        finished = await session.wait_file_change_batch(first.batch_id)
        assert finished.status == "done"
        # 重试和读取不能再次触摸已由外部修改的文件，也不能另造operation。
        file.write_text("external\n")
        for observed in (
            await session.start_file_change_batch(value),
            await session.file_change_batch(first.batch_id),
            await session.file_change_batch_by_key("same-key"),
            await session.wait_file_change_batch(first.batch_id),
        ):
            assert observed.items == finished.items and observed.batch_id == first.batch_id
        assert file.read_text() == "external\n"
        assert len(await session.file_change_history(proposal.proposal_id)) == 2
        other = AgentSession(service, (await service.spawn("default")).agent_id)
        assert await other.file_change_batch_by_key("same-key") is None
        for method in (
            other.file_change_batch,
            other.wait_file_change_batch,
            other.cancel_file_change_batch,
        ):
            with pytest.raises(ServiceError) as foreign:
                await method(first.batch_id)
            assert foreign.value.code == "workspace_batch_not_found"
        foreign_page = await other.file_change_list()
        assert foreign_page.total == 0
        with pytest.raises(ServiceError) as target:
            await other.start_file_change_batch(request(foreign_page, items=page.items))
        assert target.value.code == "workspace_proposal_not_found"
        assert (await session.file_change_batch(first.batch_id)).items == finished.items
    finally:
        await service.close()


@pytest.mark.parametrize("mode", ["deny", "ask"])
async def test_batch_uses_same_registry_authorization(runtime, tmp_path, mode):
    file = tmp_path / "one.txt"
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await apply_content(session, file, "new\n", "one")
        page = await session.file_change_list()
        value = request(page, "keep", f"auth-{mode}")

        async def policy(definition, _context):
            return mode if definition.name == "workspace_change" else "allow"

        service._handle(session.agent_id).runtime.tool_policy = policy
        if mode == "deny":
            result = await run_batch(session, value, confirmed=True)
            assert result.status == "failed" and result.items[0].error_code == "permission_denied"
        else:
            result = await run_batch(session, value)
            assert result.status == "failed" and result.items[0].error_code == "approval_required"
        assert file.read_text() == "new\n"
        assert len(await session.file_change_history(proposal.proposal_id)) == 1
        await assert_ledger(runtime, result)
        if mode == "ask":
            confirmed = await run_batch(session, request(page, "keep", "confirmed"), confirmed=True)
            assert confirmed.status == "done"
            await assert_ledger(runtime, confirmed)
    finally:
        await service.close()


async def test_shortcircuit_or_forged_hook_cannot_report_unassociated_done(runtime, tmp_path):
    file = tmp_path / "one.txt"
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    try:
        proposal, applied = await apply_content(session, file, "new\n", "one")
        page = await session.file_change_list()

        async def short(definition, context):
            if definition.name == "workspace_change":
                # 合法done回执也不代表当前子项执行过；它未被reserve关联。
                return ShortCircuit(applied.model_dump(mode="json"))
            return None

        registry.add_global_pre(short)
        result = await run_batch(session, request(page, "keep", "short"), confirmed=True)
        assert result.status == "failed" and result.items[0].status != "done"
        assert result.items[0].operation is None
        assert result.items[0].error_code == "workspace_batch_not_executed"
        assert len(await session.file_change_history(proposal.proposal_id)) == 1
        assert file.read_text() == "new\n"
        await assert_ledger(runtime, result)
    finally:
        registry._global_pre.clear()
        await service.close()


@pytest.mark.parametrize("field", ["proposal_id", "request_key", "action"])
async def test_hook_cannot_change_frozen_child_request(runtime, tmp_path, field):
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    try:
        first, _ = await apply_content(session, tmp_path / "one.txt", "one\n", "one")
        second, _ = await apply_content(session, tmp_path / "two.txt", "two\n", "two")
        page = await session.file_change_list()

        async def change(definition, context):
            if definition.name == "workspace_change":
                context.arguments[field] = {
                    "proposal_id": first.proposal_id,
                    "request_key": "hook-changed",
                    "action": "undo",
                }[field]

        registry.add_global_pre(change)
        batch = await run_batch(
            session,
            request(
                page, items=[item for item in page.items if item.proposal_id == second.proposal_id]
            ),
        )
        assert batch.status == "failed" and batch.items[0].operation is None
        assert batch.items[0].error_code == "workspace_batch_fence_mismatch"
        assert (tmp_path / "one.txt").read_text() == "one\n"
        assert (tmp_path / "two.txt").read_text() == "two\n"
        assert len(await session.file_change_history(first.proposal_id)) == 1
        assert len(await session.file_change_history(second.proposal_id)) == 1
        await assert_ledger(runtime, batch)
    finally:
        registry._global_pre.clear()
        await service.close()


async def test_hook_output_failure_cannot_hide_associated_success(runtime, tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    try:
        proposal, _ = await apply_content(session, file, "new\n", "one")
        page = await session.file_change_list()

        async def invalid(definition, result, _context):
            return {"invalid": True} if definition.name == "workspace_change" else result

        registry.add_global_post(invalid)
        batch = await run_batch(session, request(page, "undo"))
        assert batch.status == "done" and file.read_text() == "old\n"
        assert batch.items[0].operation.receipt.changed
        assert batch.items[0].operation.status == "done"
        assert len(await session.file_change_history(proposal.proposal_id)) == 2
        await assert_ledger(runtime, batch)
    finally:
        registry._global_post.clear()
        await service.close()


async def test_background_hook_cannot_reserve_closed_child_of_still_live_batch(runtime, tmp_path):
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    late_release, second_entered, second_release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    late_task = None
    try:
        later_child, _ = await apply_content(session, tmp_path / "second.txt", "second\n", "second")
        first_child, applied = await apply_content(
            session, tmp_path / "first.txt", "first\n", "first"
        )
        page = await session.file_change_list()

        async def hook(definition, context):
            nonlocal late_task
            if definition.name != "workspace_change":
                return None
            if context.arguments["proposal_id"] == first_child.proposal_id:
                arguments = dict(context.arguments)

                async def late():
                    await late_release.wait()
                    return await service.manager.workspace_changes.tool_operation(
                        "change", arguments, context
                    )

                late_task = asyncio.create_task(late())
                return ShortCircuit(applied.model_dump(mode="json"))
            second_entered.set()
            await second_release.wait()
            return None

        registry.add_global_pre(hook)
        started = await session.start_file_change_batch(request(page))
        await asyncio.wait_for(second_entered.wait(), 5)
        assert (await session.file_change_batch(started.batch_id)).producer_status == "running"
        late_release.set()
        with pytest.raises(ToolError) as late:
            await asyncio.wait_for(late_task, 5)
        assert late.value.code == "workspace_batch_child_closed"
        assert len(await session.file_change_history(first_child.proposal_id)) == 1
        second_release.set()
        batch = await session.wait_file_change_batch(started.batch_id)
        assert batch.status == "partial"
        assert batch.items[0].operation is None
        assert batch.items[1].proposal_id == later_child.proposal_id
        assert batch.items[1].status == "done"
        await assert_ledger(runtime, batch)
    finally:
        late_release.set()
        second_release.set()
        if late_task is not None:
            await asyncio.gather(late_task, return_exceptions=True)
        registry._global_pre.clear()
        await service.close()


@pytest.mark.parametrize("stop", ["cancel", "close", "lease"])
@pytest.mark.parametrize("phase", ["before_lock", "published"])
async def test_stop_before_lock_blocks_write_but_preserves_already_published_undo(
    runtime, tmp_path, monkeypatch, stop, phase
):
    import krim_agent.workspace_writer as writer_module

    file, other = tmp_path / "file.txt", tmp_path / "other.txt"
    file.write_text("old\n")
    other.write_text("first\n")
    service, session, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    stopping = None
    try:
        await apply_content(session, other, "second\n", "other")
        proposal, _ = await apply_content(session, file, "new\n", "one")
        page = await session.file_change_list()
        original = WorkspaceWriter.apply
        rename = writer_module.rename_at

        def before_lock(self, *args, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(self, *args, **kwargs)

        def published(*args, **kwargs):
            value = rename(*args, **kwargs)
            entered.set()
            assert release.wait(10)
            return value

        async def stopped_heartbeat(_execution, _worker):
            # 让SQL到期事实决定资格，不能借心跳取消线程掩盖迟到发布问题。
            await asyncio.Event().wait()

        if phase == "before_lock":
            monkeypatch.setattr(WorkspaceWriter, "apply", before_lock)
        else:
            monkeypatch.setattr(writer_module, "rename_at", published)
        monkeypatch.setattr(service.workspace_batches, "_renew", stopped_heartbeat)
        started = await session.start_file_change_batch(request(page, "undo"))
        await wait_event(entered)
        assert file.read_text() == ("new\n" if phase == "before_lock" else "old\n")
        scope = f"session:{session.session_id}"
        if stop == "lease":
            async with runtime.repository.database.sessions.begin() as sql:
                await sql.execute(
                    update(NativeWorkspaceBatchRow)
                    .where(NativeWorkspaceBatchRow.batch_id == started.batch_id)
                    .values(lease_expires_at=time.time() - 1)
                )
            stale = await session.file_change_batch(started.batch_id)
            assert stale.producer_status == "interrupted"
        else:
            stopping = asyncio.create_task(
                session.cancel_file_change_batch(started.batch_id)
                if stop == "cancel"
                else service.close()
            )

            async def await_closed_fence():
                while True:
                    state = await service.workspace_batch_store.get(scope, started.batch_id)
                    if state.producer_status != "running":
                        return state
                    await asyncio.sleep(0.01)

            stopped = await asyncio.wait_for(await_closed_fence(), 5)
            assert stopped.producer_status == "cancelled"
            assert not stopping.done(), "取消须等待已开始的文件线程收束"
        release.set()
        if stopping is not None:
            await asyncio.wait_for(stopping, 10)
        else:
            entry = service.workspace_batches._tasks.get(started.batch_id)
            if entry is not None:
                await asyncio.wait_for(asyncio.shield(entry[1]), 10)
        batch = await service.workspace_batch_store.get(scope, started.batch_id)
        first, unstarted = batch.items
        assert first.proposal_id == proposal.proposal_id
        assert first.status == ("rejected" if phase == "before_lock" else "done")
        assert first.operation is not None
        assert first.operation.receipt.changed == (phase == "published")
        assert unstarted.status == "skipped" and unstarted.operation is None
        assert file.read_text() == ("new\n" if phase == "before_lock" else "old\n")
        assert other.read_text() == "second\n"
        await assert_ledger(runtime, batch)
    finally:
        release.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        await service.close()


async def test_wait_timeout_and_waiter_cancellation_do_not_stop_batch(runtime, tmp_path):
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    entered, release = asyncio.Event(), asyncio.Event()
    try:
        await apply_content(session, tmp_path / "one.txt", "one\n", "one")

        async def pause(definition, _context):
            if definition.name == "workspace_change":
                entered.set()
                await release.wait()

        registry.add_global_pre(pause)
        started = await session.start_file_change_batch(request(await session.file_change_list()))
        await asyncio.wait_for(entered.wait(), 5)
        still_running = await session.wait_file_change_batch(started.batch_id, timeout_seconds=0.01)
        assert still_running.status == "running" and still_running.items[0].operation is None
        waiter = asyncio.create_task(session.wait_file_change_batch(started.batch_id))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert (await session.file_change_batch(started.batch_id)).producer_status == "running"
        release.set()
        finished = await session.wait_file_change_batch(started.batch_id)
        assert finished.status == "done"
        await assert_ledger(runtime, finished)
    finally:
        release.set()
        registry._global_pre.clear()
        await service.close()


async def test_cancel_after_keep_verification_cannot_commit_new_acceptance(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file.txt"
    file.write_text("old\n")
    service, session, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceWriter.verify
    cancelling = None
    fence_closed = asyncio.Event()
    cancel_store = service.workspace_batch_store.cancel

    async def cancel_committed(*args, **kwargs):
        result = await cancel_store(*args, **kwargs)
        fence_closed.set()
        return result

    monkeypatch.setattr(service.workspace_batch_store, "cancel", cancel_committed)
    try:
        proposal, _ = await apply_content(session, file, "new\n", "one")

        def paused(self, *args, **kwargs):
            result = original(self, *args, **kwargs)
            assert result.receipt.status == "done"
            entered.set()
            assert release.wait(10)
            return result

        monkeypatch.setattr(WorkspaceWriter, "verify", paused)
        page = await session.file_change_list()
        started = await session.start_file_change_batch(request(page, "keep", "keep-cancel"))
        await wait_event(entered)
        cancelling = asyncio.create_task(session.cancel_file_change_batch(started.batch_id))

        await asyncio.wait_for(fence_closed.wait(), 5)
        assert not cancelling.done()
        release.set()
        result = await asyncio.wait_for(cancelling, 5)
        assert result.producer_status == "cancelled" and result.status == "uncertain"
        item = result.items[0]
        assert item.status == "uncertain" and item.operation is not None
        assert item.operation.receipt.changed is False
        assert (await session.file_change(proposal.proposal_id)).status == "uncertain"
        assert file.read_text() == "new\n"
        await assert_ledger(runtime, result)
    finally:
        release.set()
        if cancelling is not None:
            await asyncio.gather(cancelling, return_exceptions=True)
        await service.close()


async def test_public_caller_cannot_reserve_internal_batch_request_key(runtime, tmp_path):
    file = tmp_path / "file.txt"
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await apply_content(session, file, "new\n", "one")
        with pytest.raises(ServiceError) as blocked:
            await session.change_file(
                WorkspaceChangeAction(
                    proposal_id=proposal.proposal_id,
                    action="keep",
                    expected_revision=proposal.revision,
                    request_key="krim-batch:forged:0",
                )
            )
        assert blocked.value.code == "workspace_batch_request_reserved"
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["pre", "post"])
async def test_registry_hook_can_cancel_own_batch_without_waiting_for_itself(
    runtime, tmp_path, phase
):
    file, other = tmp_path / "file.txt", tmp_path / "other.txt"
    file.write_text("old\n")
    other.write_text("first\n")
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    called = asyncio.Event()
    try:
        await apply_content(session, other, "second\n", "other")
        proposal, _ = await apply_content(session, file, "new\n", "one")

        async def pre(definition, context):
            if definition.name == "workspace_change":
                called.set()
                await session.cancel_file_change_batch(context.workspace_batch.batch_id)

        async def post(definition, result, context):
            if definition.name == "workspace_change":
                called.set()
                await session.cancel_file_change_batch(context.workspace_batch.batch_id)
            return result

        if phase == "pre":
            registry.add_global_pre(pre)
        else:
            registry.add_global_post(post)
        started = await session.start_file_change_batch(
            request(await session.file_change_list(), "undo")
        )
        entry = service.workspace_batches._tasks.get(started.batch_id)
        assert entry is not None
        await asyncio.wait_for(called.wait(), 5)
        await asyncio.wait_for(asyncio.shield(entry[1]), 5)
        result = await session.file_change_batch(started.batch_id)
        assert result.producer_status == "cancelled"
        assert result.items[0].proposal_id == proposal.proposal_id
        if phase == "pre":
            assert result.items[0].operation is None
            assert file.read_text() == "new\n"
        else:
            assert result.items[0].status == "done"
            assert result.items[0].operation.receipt.changed
            assert file.read_text() == "old\n"
        assert result.items[1].status == "skipped"
        assert result.items[1].operation is None
        assert other.read_text() == "second\n"
        await assert_ledger(runtime, result)
    finally:
        registry._global_pre.clear()
        registry._global_post.clear()
        await service.close()


async def test_callback_close_is_rejected_before_closing_service(runtime, tmp_path):
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    called = False
    try:
        await apply_content(session, tmp_path / "one.txt", "one\n", "one")

        async def hook(definition, _context):
            nonlocal called
            if definition.name != "workspace_change":
                return None
            called = True
            for close in (service.close, service.workspace_batches.close):
                with pytest.raises(ServiceError) as rejected:
                    await close()
                assert rejected.value.code == "workspace_batch_close_reentrant"
                assert rejected.value.status == 409
            assert not service._closed and not service.workspace_batches._closed
            return None

        registry.add_global_pre(hook)
        result = await run_batch(session, request(await session.file_change_list()))
        assert called and result.status == "done"
        assert (await session.read_file("one.txt")).content == "one\n"
        await assert_ledger(runtime, result)
    finally:
        registry._global_pre.clear()
        await service.close()


async def test_finished_hook_background_task_is_external_for_cancel_and_close(runtime, tmp_path):
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    release = asyncio.Event()
    background = None
    finished_batch = None
    try:
        await apply_content(session, tmp_path / "one.txt", "one\n", "one")

        async def hook(definition, context):
            nonlocal background
            if definition.name != "workspace_change":
                return None
            batch_id = context.workspace_batch.batch_id

            async def after_parent_finished():
                # Task继承回调ContextVar，执行时其原worker已真实退出。
                await release.wait()
                assert batch_id not in service.workspace_batches._tasks
                cancelled = await session.cancel_file_change_batch(batch_id)
                await service.close()
                return cancelled

            background = asyncio.create_task(after_parent_finished())
            return None

        registry.add_global_pre(hook)
        started = await session.start_file_change_batch(request(await session.file_change_list()))
        worker = service.workspace_batches._tasks[started.batch_id][1]
        await asyncio.wait_for(asyncio.shield(worker), 5)
        assert worker.done() and started.batch_id not in service.workspace_batches._tasks
        finished_batch = await session.file_change_batch(started.batch_id)
        assert finished_batch.status == "done"
        release.set()
        result = await asyncio.wait_for(asyncio.shield(background), 5)
        assert result.items == finished_batch.items
        assert service._closed and service.workspace_batches._closed
        assert not service.workspace_batches._tasks
    finally:
        release.set()
        if background is not None:
            await asyncio.gather(background, return_exceptions=True)
        registry._global_pre.clear()
        await service.close()


async def test_two_scopes_hooks_cancel_each_other_without_worker_join_cycle(
    runtime, tmp_path, monkeypatch
):
    service, first, _ = await setup(runtime, tmp_path)
    second = AgentSession(service, (await service.spawn("default")).agent_id)
    sessions = [first, second]
    registries = [service._handle(item.agent_id).runtime.tools for item in sessions]
    hooks_entered = asyncio.Barrier(2)
    stops_entered = asyncio.Barrier(2)
    all_started = asyncio.Event()
    batches = {}
    cancelled_scopes = set()
    original_cancel = service.workspace_batch_store.cancel

    async def simultaneous_cancel(scope, batch_id):
        # 两条stop任务均已启动，再允许任一任务取消对方，稳定复现潜在等待环。
        cancelled_scopes.add(scope)
        await stops_entered.wait()
        return await original_cancel(scope, batch_id)

    monkeypatch.setattr(service.workspace_batch_store, "cancel", simultaneous_cancel)
    try:
        for index, session in enumerate(sessions):
            await apply_content(session, tmp_path / f"{index}.txt", f"value-{index}\n", "one")

        def make_hook(index):
            async def hook(definition, _context):
                if definition.name != "workspace_change":
                    return None
                await all_started.wait()
                await hooks_entered.wait()
                other = 1 - index
                await sessions[other].cancel_file_change_batch(batches[other].batch_id)
                return None

            return hook

        for index, session in enumerate(sessions):
            registries[index].add_global_pre(make_hook(index))
            batches[index] = await session.start_file_change_batch(
                request(await session.file_change_list())
            )
        workers = [
            service.workspace_batches._tasks[value.batch_id][1] for value in batches.values()
        ]
        all_started.set()
        await asyncio.wait_for(
            asyncio.gather(*(asyncio.shield(worker) for worker in workers), return_exceptions=True),
            5,
        )
        assert len(cancelled_scopes) == 2 and all(worker.done() for worker in workers)
        assert not service.workspace_batches._tasks
        for index, session in enumerate(sessions):
            batch = await session.file_change_batch(batches[index].batch_id)
            assert batch.producer_status == "cancelled"
            assert batch.items[0].operation is None
            assert (tmp_path / f"{index}.txt").read_text() == f"value-{index}\n"
            await assert_ledger(runtime, batch)
    finally:
        all_started.set()
        monkeypatch.setattr(service.workspace_batch_store, "cancel", original_cancel)
        for registry in registries:
            registry._global_pre.clear()
        await service.close()


async def test_owned_pythonai_database_survives_rejected_callback_close(tmp_path, monkeypatch):
    from krim_agent.providers import FakeProvider
    from krim_agent.sdk import PythonAI

    ai = await PythonAI.create(
        provider=FakeProvider(delay=0), workspace_root=tmp_path, workspace_writable=True
    )
    service = ai.agents()
    session = await ai.agent()
    service._handle(session.agent_id).runtime.tool_policy = allow_all
    registry = service._handle(session.agent_id).runtime.tools
    database = ai._owned_database
    assert database is not None
    database_close = database.close
    database_close_calls = 0
    callback_ran = False
    callback_evidence = []

    async def observe_database_close():
        nonlocal database_close_calls
        database_close_calls += 1
        await database_close()

    monkeypatch.setattr(database, "close", observe_database_close)
    try:
        await apply_content(session, tmp_path / "one.txt", "one\n", "one")

        async def hook(definition, context):
            nonlocal callback_ran
            if definition.name != "workspace_change":
                return None
            with pytest.raises(ServiceError) as rejected:
                await ai.close()
            callback_evidence.append(
                (rejected.value.code, ai._closed, service._closed, database_close_calls)
            )
            assert rejected.value.code == "workspace_batch_close_reentrant"
            assert not ai._closed and not service._closed
            assert database_close_calls == 0
            callback_ran = True
            return None

        registry.add_global_pre(hook)
        batch = await run_batch(session, request(await session.file_change_list()))
        assert callback_ran, (callback_evidence, batch.items[0].error_code)
        assert batch.status == "done"
        assert (await session.read_file("one.txt")).content == "one\n"
        async with database.write_lock, database.sessions() as sql:
            assert await sql.get(NativeWorkspaceBatchRow, batch.batch_id)
        await assert_ledger(ai.runtime, batch)
        registry._global_pre.clear()
        await asyncio.wait_for(ai.close(), 5)
        assert ai._closed and service._closed and database_close_calls == 1
    finally:
        registry._global_pre.clear()
        await ai.close()


async def allow_all(_definition, _context):
    return "allow"


@pytest.mark.parametrize("fault", ["cancel", "finish", "both"])
async def test_close_drains_local_tasks_when_batch_storage_immediately_fails(
    runtime, tmp_path, monkeypatch, fault
):
    service, session, _ = await setup(runtime, tmp_path)
    registry = service._handle(session.agent_id).runtime.tools
    entered = asyncio.Event()
    faults = set()
    try:
        await apply_content(session, tmp_path / "one.txt", "one\n", "one")

        async def pause(definition, _context):
            if definition.name == "workspace_change":
                entered.set()
                await asyncio.Event().wait()

        async def fail_cancel(*_args, **_kwargs):
            faults.add("cancel")
            raise OSError("注入批次cancel立即存储失败")

        async def fail_finish(*_args, **_kwargs):
            faults.add("finish")
            raise OSError("注入批次finish立即存储失败")

        registry.add_global_pre(pause)
        started = await session.start_file_change_batch(request(await session.file_change_list()))
        worker = service.workspace_batches._tasks[started.batch_id][1]
        await asyncio.wait_for(entered.wait(), 5)
        if fault in {"cancel", "both"}:
            monkeypatch.setattr(service.workspace_batch_store, "cancel", fail_cancel)
        if fault in {"finish", "both"}:
            monkeypatch.setattr(service.workspace_batch_store, "finish", fail_finish)
        await asyncio.wait_for(service.close(), 5)
        assert service._closed and worker.done()
        assert faults == ({"cancel", "finish"} if fault == "both" else {fault})
        assert not service.workspace_batches._tasks and not service._workspace_operations
        assert (tmp_path / "one.txt").read_text() == "one\n"
    finally:
        registry._global_pre.clear()
        await service.close()
