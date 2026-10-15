"""原计划/准备/意图/回执SQL故障、丢回程核实和原期限均不产生第二次效果。"""

import time

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import apply, execute_request, prepared, ready
from test_directory_restore_plans import request, target

from krim_agent.persistence.database import (
    WorkspaceDirectoryCheckpointRow,
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestorePlanRow,
)
from krim_agent.persistence.workspace_directory_restores import WorkspaceDirectoryRestoreStore
from krim_agent.protocol.domain import ServiceError


async def test_reserve_rollback_source_and_metadata_commit_loss_are_distinguished(capture_context):
    captures, scope, binding, _, _ = capture_context
    identifier = await target(capture_context)
    store = WorkspaceDirectoryRestoreStore(captures)

    def fail(_conn, _cursor, statement, _params, _context, _many):
        if statement.startswith("INSERT INTO workspace_directory_restore_plans"):
            raise OSError("原计划SQL写失败")

    event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(OSError):
            await store.reserve(scope, binding, request(identifier), qualify=lambda: None)
    finally:
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
    assert await store.by_key(scope, "preview") is None
    fired = []

    def lost(_session):
        if not fired:
            fired.append(True)
            raise OSError("原提交已成功但回程丢失")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(OSError):
            await store.reserve(scope, binding, request(identifier), qualify=lambda: None)
    finally:
        event.remove(Session, "after_commit", lost)
    saved = await store.by_key(scope, "preview")
    assert saved.phase == "preparing"
    assert await store.reserve(scope, binding, request(identifier), qualify=lambda: None) == (
        saved,
        False,
    )
    assert store.database.engine.sync_engine.pool.checkedout() == 0


async def test_baseline_write_failure_and_successful_commit_lost_reply_never_recaptures(
    capture_context,
):
    captures, scope, binding, snapshot, _ = capture_context
    identifier = await target(capture_context)
    store = WorkspaceDirectoryRestoreStore(captures)
    planned, _ = await store.reserve(scope, binding, request(identifier), qualify=lambda: None)
    fence = await store.preparation(scope, planned.plan_id)

    def fail(_conn, _cursor, statement, _params, _context, _many):
        if (
            statement.startswith("UPDATE workspace_directory_restore_plans")
            and "baseline_material_id" in statement
        ):
            raise OSError("关联完整备份事务失败")

    event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(OSError):
            await store.record_baseline(scope, fence, binding, snapshot, qualify=lambda: None)
    finally:
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
    assert (await store.get(scope, planned.plan_id)).backup is None
    async with store.database.sessions() as sql:
        assert (
            await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryCheckpointRow)) == 1
        )
    fired = []

    def lost(_session):
        if not fired:
            fired.append(True)
            raise OSError("完整备份提交后回程未知")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(OSError):
            await store.record_baseline(scope, fence, binding, snapshot, qualify=lambda: None)
    finally:
        event.remove(Session, "after_commit", lost)
    saved = await store.get(scope, planned.plan_id)
    assert saved.backup.snapshot_fingerprint == snapshot.fingerprint
    assert (
        await store.record_baseline(scope, fence, binding, snapshot, qualify=lambda: None) == saved
    )
    assert store.database.engine.sync_engine.pool.checkedout() == 0


async def test_intent_commit_loss_queries_original_id_without_second_dispatch(capture_context):
    store, scope, binding, _, planned = await ready(capture_context)
    op, fence, _writer, change, _authorize = await prepared(store, scope, binding, planned)
    fired = []

    def lost(_session):
        if not fired:
            fired.append(True)
            raise OSError("派发SQL提交后回程丢失")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(OSError):
            await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    finally:
        event.remove(Session, "after_commit", lost)
    original = await store.operation(scope, op.operation_id)
    assert original.intent_recorded and original.status == "running"
    with pytest.raises(ServiceError):
        await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    assert await store.operation_by_key(scope, "execute") == original
    assert store.database.engine.sync_engine.pool.checkedout() == 0


async def test_receipt_commit_loss_keeps_actual_done_and_original_request_zero_namespace(
    capture_context,
):
    store, scope, binding, _, planned = await ready(capture_context)
    op, fence, writer, change, authorize = await prepared(store, scope, binding, planned)
    effect = await apply(store, scope, binding, fence, writer, change, authorize)
    assert effect.receipt.status == "done"
    fired = []

    def lost(_session):
        if not fired:
            fired.append(True)
            raise OSError("原真实回执提交后丢回程")

    event.listen(Session, "after_commit", lost)
    try:
        with pytest.raises(OSError):
            await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    finally:
        event.remove(Session, "after_commit", lost)
    done = await store.get(scope, planned.plan_id)
    assert done.phase == "finished" and done.operation_id == op.operation_id
    assert (await store.start(scope, binding, execute_request(planned), qualify=lambda: None))[
        1
    ] is False
    assert store.database.engine.sync_engine.pool.checkedout() == 0


@pytest.mark.parametrize("phase", ["ready", "intent", "settle"])
async def test_final_flush_loss_of_original_qualification_rolls_back_phase(
    capture_context, monkeypatch, phase
):
    from sqlalchemy.ext.asyncio import AsyncSession

    store, scope, binding, before, planned = await ready(capture_context)
    if phase == "ready":
        next_plan, _ = await store.reserve(
            scope, binding, request(planned.target.identifier, "another"), qualify=lambda: None
        )
        fence = await store.preparation(scope, next_plan.plan_id)
        await store.record_baseline(scope, fence, binding, before, qualify=lambda: None)
        operation_id = None
    else:
        op, fence, writer, change, authorize = await prepared(store, scope, binding, planned)
        operation_id = op.operation_id
        if phase == "settle":
            effect = await apply(store, scope, binding, fence, writer, change, authorize)
    flush = AsyncSession.flush
    entered = []

    async def revoke(session, *args, **kwargs):
        result = await flush(session, *args, **kwargs)
        if not entered:
            entered.append(True)
            binding.revoked.set()
        return result

    monkeypatch.setattr(AsyncSession, "flush", revoke)
    with pytest.raises(ServiceError):
        if phase == "ready":
            await store.ready(scope, fence, binding, qualify=lambda: None)
        elif phase == "intent":
            await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
        else:
            await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    monkeypatch.setattr(AsyncSession, "flush", flush)
    assert entered
    if phase == "ready":
        assert (await store.get(scope, next_plan.plan_id)).phase == "preparing"
    else:
        state = await store.operation(scope, operation_id)
        assert state.status == "running"
        assert state.intent_recorded is (phase == "settle")
    # 原owner仅能关闭准入，不要求被撤销binding恢复可用。
    closed = await store.fail(scope, fence, "original_binding_revoked")
    assert closed.phase == ("uncertain" if phase == "settle" else "failed")


@pytest.mark.parametrize("kind", ["preparing", "ready"])
async def test_preview_expiry_and_same_key_only_project_original_fact(capture_context, kind):
    store, scope, binding, _, planned = await ready(capture_context)
    if kind == "preparing":
        planned, _ = await store.reserve(
            scope,
            binding,
            request(planned.target.identifier, "preparing-expire"),
            qualify=lambda: None,
        )
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceDirectoryRestorePlanRow, planned.plan_id)
        row.expires_at = time.time() - 1
    state, created = await store.reserve(
        scope,
        binding,
        request(planned.target.identifier, planned.request_key),
        qualify=lambda: None,
    )
    assert state.phase == "expired" and not created
    assert await store.recover_interrupted() == 0


async def test_receipt_view_decode_crossing_original_lease_cannot_commit_done(
    capture_context, monkeypatch
):
    import asyncio

    store, scope, binding, _, planned = await ready(capture_context)
    op, fence, writer, change, authorize = await prepared(store, scope, binding, planned)
    effect = await apply(store, scope, binding, fence, writer, change, authorize)
    assert effect.receipt.status == "done"
    deadline = time.time() + 1
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceDirectoryRestoreAttemptRow, op.operation_id)
        row.lease_expires_at = deadline
    view = store._view
    entered = []

    async def slow(sql, row, attempt):
        if row.phase == "finished":
            entered.append(True)
            await asyncio.sleep(max(0, deadline - time.time()) + 0.02)
        return await view(sql, row, attempt)

    monkeypatch.setattr(store, "_view", slow)
    with pytest.raises(ServiceError) as denied:
        await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert denied.value.code == "workspace_directory_restore_fenced" and entered
    async with store.database.sessions() as sql:
        row = await sql.get(WorkspaceDirectoryRestoreAttemptRow, op.operation_id)
        assert row.status == "running" and row.intent_recorded and row.lease_expires_at == deadline
