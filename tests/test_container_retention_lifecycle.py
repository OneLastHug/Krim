"""取消/租约/冷核实只改变独立回收事实；旧Source与目标不重放。"""

import asyncio
import time
from dataclasses import replace

import pytest
from sqlalchemy import func, select, update
from test_container_retention_store import remove, start
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import bridge
from test_missing_directory_restore_store import missing as missing

import krim_agent.workspace_empty_container_retention as physical
from krim_agent.persistence.database import WorkspaceContainerRetentionLockRow as Lock
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.persistence.database import WorkspaceMissingDirectoryRestorePlanRow as SourcePlan
from krim_agent.persistence.workspace_container_retentions import WorkspaceContainerRetentionStore
from krim_agent.protocol.domain import ServiceError


async def intent(context):
    store, scope, binding, origin, _old, core, _request, _view, fence = context
    observed = core.inspect(origin.stage, authorize=lambda: None)
    await store.record_intent(scope, fence, binding, observed, qualify=lambda: None)


async def inspect(context, fence):
    store, scope, binding, origin, _old, core, *_ = context
    authorize = bridge(
        asyncio.get_running_loop(),
        lambda: store.assert_execution(scope, fence, binding, qualify=lambda: None),
    )
    return await asyncio.to_thread(core.inspect, origin.stage, authorize=authorize)


async def locks(store):
    async with store.database.sessions() as sql:
        return await sql.scalar(select(func.count()).select_from(Lock))


async def test_cancel_before_intent_fences_old_worker_and_allows_fresh_explicit_key(missing):
    context = await start(missing)
    store, scope, binding, _origin, _old, _core, request, view, fence = context
    cancelled = await store.cancel(
        scope, view.operation_id, binding, expected_revision=view.revision, qualify=lambda: None
    )
    assert cancelled.status == "cancelled" and cancelled.cancelled and not cancelled.intent_recorded
    assert await locks(store) == 0
    with pytest.raises(ServiceError):
        await store.inputs(scope, fence, binding, qualify=lambda: None)
    fresh = request.model_copy(update={"request_key": "fresh-reviewed-after-cancel"})
    another, created = await store.reserve(scope, binding, fresh, qualify=lambda: None)
    assert created and another.operation_id != cancelled.operation_id
    assert await store.by_key(scope, request.request_key) == cancelled


async def test_cancel_after_intent_stays_unknown_then_unchanged_empty_is_not_removed(
    missing, monkeypatch
):
    context = await start(missing)
    store, scope, binding, origin, original, _core, request, view, _fence = context
    await intent(context)
    current = await store.get(scope, view.operation_id)
    unknown = await store.cancel(
        scope, view.operation_id, binding, expected_revision=current.revision, qualify=lambda: None
    )
    assert unknown.status == "unknown" and unknown.cancelled and await locks(store) == 3
    monkeypatch.setattr(
        physical.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("只读核实不删除")
    )
    checked, fence = await store.claim_verify(
        scope,
        view.operation_id,
        binding,
        expected_revision=unknown.revision,
        request_key="cancel-readback",
        qualify=lambda: None,
    )
    assert fence is not None and checked.cancelled and checked.status == "running"
    observed = await inspect(context, fence)
    assert observed.status == "empty"
    final = await store.finish_verify(scope, fence, binding, observed, qualify=lambda: None)
    assert (
        final.status == "rejected"
        and final.receipt.error_code == "workspace_container_retention_not_removed"
    )
    assert final.cancelled and not final.receipt.changed and await locks(store) == 0
    assert await store.restores.get(scope, origin.view.source_plan_id) == original
    fresh, created = await store.reserve(
        scope,
        binding,
        request.model_copy(update={"request_key": "fresh-after-proven-no-removal"}),
        qualify=lambda: None,
    )
    assert created and fresh.operation_id != final.operation_id
    assert await store.by_key(scope, request.request_key) == final


async def test_real_rmdir_lost_reply_cold_explicit_absence_verification_never_repeats(
    missing, monkeypatch
):
    context = await start(missing)
    store, scope, binding, origin, original, _core, request, view, original_fence = context
    rmdir = physical.os.rmdir

    def lost(name, *, dir_fd):
        rmdir(name, dir_fd=dir_fd)
        raise OSError("lost reply")

    monkeypatch.setattr(physical.os, "rmdir", lost)
    effect = await remove(context)
    unknown = await store.settle(scope, original_fence, binding, effect, qualify=lambda: None)
    assert unknown.status == "unknown"
    monkeypatch.setattr(
        physical.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("冷核实不能重放删除")
    )
    cold = WorkspaceContainerRetentionStore(store.restores)
    checked, fence = await cold.claim_verify(
        scope,
        view.operation_id,
        binding,
        expected_revision=unknown.revision,
        request_key="cold-observe-original",
        qualify=lambda: None,
    )
    assert fence is not None and checked.verification_count == 1
    current = (cold, *context[1:])
    observed = await inspect(current, fence)
    assert observed.status == "absent"
    final = await cold.finish_verify(scope, fence, binding, observed, qualify=lambda: None)
    assert (
        final.status == "done"
        and final.receipt.outcome == "absence_verified"
        and not final.receipt.changed
    )
    assert await locks(cold) == 0
    assert await cold.claim_verify(
        scope,
        view.operation_id,
        binding,
        expected_revision=unknown.revision,
        request_key="cold-observe-original",
        qualify=lambda: pytest.fail("同核实键只读"),
    ) == (final, None)
    assert await cold.by_key(scope, request.request_key) == final
    async with cold.database.sessions() as sql:
        assert (await sql.get(SourcePlan, origin.view.source_plan_id)).staging_reserved_entries == 0
    assert await cold.restores.get(scope, origin.view.source_plan_id) == original


@pytest.mark.parametrize("recorded", [False, True])
async def test_expired_owner_recovery_only_fences_metadata_and_old_worker(
    recorded, missing, monkeypatch
):
    context = await start(missing)
    store, scope, binding, _origin, _old, _core, request, view, fence = context
    if recorded:
        await intent(context)
    async with store.database.sessions() as sql, sql.begin():
        await sql.execute(
            update(Row)
            .where(Row.operation_id == view.operation_id)
            .values(lease_expires_at=time.time() - 1)
        )
    monkeypatch.setattr(
        physical.os, "rmdir", lambda *_args, **_kwargs: pytest.fail("恢复不自动删除")
    )
    cold = WorkspaceContainerRetentionStore(store.restores)
    assert await cold.recover() == 1
    final = await cold.by_key(scope, request.request_key)
    assert final.status == ("unknown" if recorded else "interrupted")
    assert await locks(cold) == (3 if recorded else 0)
    assert await cold.recover() == 0
    with pytest.raises(ServiceError):
        await store.assert_execution(scope, fence, binding, qualify=lambda: None)


async def test_last_qualifier_revocation_rolls_back_receipt_and_quota(missing):
    context = await start(missing)
    store, scope, binding, origin, _old, _core, _request, view, fence = context
    effect = await remove(context)
    assert effect.status == "removed"
    calls = []

    def revoked():
        calls.append(True)
        if len(calls) == 2:
            binding.revoked.set()

    with pytest.raises(ServiceError):
        await store.settle(scope, fence, binding, effect, qualify=revoked)
    current = await store.get(scope, view.operation_id)
    assert current.status == "running"
    async with store.database.sessions() as sql:
        assert (await sql.get(SourcePlan, origin.view.source_plan_id)).staging_reserved_entries == 1
    unknown = await store.fail(scope, fence, "workspace_container_retention_resource_revoked")
    assert unknown.status == "unknown" and await locks(store) == 3


async def test_old_epoch_cannot_overwrite_new_readonly_check(missing):
    context = await start(missing)
    store, scope, binding, origin, _old, _core, request, view, old_fence = context
    await intent(context)
    unknown = await store.fail(scope, old_fence, "synthetic_interrupted")
    _view, fence = await store.claim_verify(
        scope,
        view.operation_id,
        binding,
        expected_revision=unknown.revision,
        request_key="new-epoch",
        qualify=lambda: None,
    )
    assert fence is not None and fence.epoch == 1
    from krim_agent.workspace_empty_container_retention import EmptyContainerReceipt

    effect = EmptyContainerReceipt(
        origin.stage,
        request.expected_container_fingerprint,
        "rejected",
        False,
        False,
        error_code="stale-thread",
    )
    with pytest.raises(ServiceError):
        await store.settle(scope, old_fence, binding, effect, qualify=lambda: None)
    observed = await inspect(context, fence)
    assert observed.status == "empty"
    await store.finish_verify(scope, fence, binding, replace(observed), qualify=lambda: None)
