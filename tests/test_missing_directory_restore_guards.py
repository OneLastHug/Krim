"""原资格/活观察不可冒领，最后投影跨租约/撤权须整事务回滚。"""

import asyncio
import time
from dataclasses import replace

import pytest
from sqlalchemy import select
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_missing_directory_restore_store import (
    apply,
    counts,
    execute,
    prepared,
    preview,
    ready,
)
from test_missing_directory_restore_store import (
    missing as missing,
)

from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreAttemptRow as Attempt,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestoreLockRow as Lock,
)
from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRestorePlanRow as Plan,
)
from krim_agent.persistence.workspace_missing_directory_restores import (
    WorkspaceMissingDirectoryRestoreStore,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader


@pytest.mark.parametrize(
    "fault", ["scope", "token", "owner", "resource", "early-ready", "foreign-reader"]
)
async def test_original_preparation_cannot_borrow_scope_token_resource_or_historical_absence(
    missing, fault
):
    store, scope, binding, _, identifier, reader = missing
    planned, _ = await store.reserve(scope, binding, preview(identifier), qualify=lambda: None)
    fence = await store.preparation(scope, planned.plan_id)
    before = reader.capture("tree")
    with pytest.raises(ServiceError):
        if fault == "scope":
            await store.get("topic:foreign", planned.plan_id)
        elif fault == "token":
            await store.preparation_view(
                scope, replace(fence, token="f" * 64), binding, qualify=lambda: None
            )
        elif fault == "owner":
            await WorkspaceMissingDirectoryRestoreStore(store.captures).preparation(
                scope, planned.plan_id
            )
        elif fault == "resource":
            await store.preparation_view(
                scope, fence, replace(binding, fingerprint="f" * 64), qualify=lambda: None
            )
        elif fault == "early-ready":
            await store.ready(scope, fence, binding, qualify=lambda: None)
        else:
            with WorkspaceDirectoryAbsenceReader(binding.workspace) as cold:
                await store.record_absence(
                    scope, fence, binding, before, observation_reader=cold, qualify=lambda: None
                )
    assert (await store.get(scope, planned.plan_id)).baseline is None
    assert (await counts(store)) == [1, 0, 0]


async def test_released_then_recaptured_observation_cannot_replace_original_witness(missing):
    store, scope, binding, _, identifier, reader = missing
    planned, _ = await store.reserve(scope, binding, preview(identifier), qualify=lambda: None)
    fence = await store.preparation(scope, planned.plan_id)
    before = reader.capture("tree")
    original = await store.record_absence(
        scope, fence, binding, before, observation_reader=reader, qualify=lambda: None
    )
    assert reader.release(before)
    replacement = reader.capture("tree")
    assert replacement.fingerprint != before.fingerprint
    with pytest.raises(ServiceError):
        await store.record_absence(
            scope, fence, binding, replacement, observation_reader=reader, qualify=lambda: None
        )
    assert await store.get(scope, planned.plan_id) == original


async def test_physical_rejection_after_observer_closes_still_records_actual_original_result(
    missing,
):
    store, scope, binding, *_, reader = missing
    planned, _ = await ready(missing)
    operation, fence, writer, change, authorize = await prepared(missing, planned)
    reader.close()
    effect = await apply(missing, fence, writer, change, authorize)
    assert effect.receipt.status == "rejected"
    result = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert result.phase == "failed" and result.operation_id == operation.operation_id
    assert not (await store.operation(scope, operation.operation_id)).intent_recorded
    assert (await counts(store))[2] == 0


async def test_deleted_namespace_lock_is_never_silently_reacquired(missing):
    store, scope, binding, *_ = missing
    planned, _ = await ready(missing)
    operation, fence, _, change, _ = await prepared(missing, planned)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.scalar(select(Lock).where(Lock.operation_id == operation.operation_id))
        await sql.delete(row)
    with pytest.raises(ServiceError) as denied:
        await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    assert denied.value.code == "workspace_missing_directory_restore_lock_lost"
    assert not (await store.operation(scope, operation.operation_id)).intent_recorded


@pytest.mark.parametrize("phase", ["ready", "start", "settle"])
@pytest.mark.parametrize("fault", ["lease", "qualification"])
async def test_last_projection_cannot_commit_ready_execution_or_done_past_original_fence(
    missing, monkeypatch, phase, fault
):
    store, scope, binding, _, identifier, reader = missing
    if phase == "ready":
        planned, _ = await store.reserve(scope, binding, preview(identifier), qualify=lambda: None)
        fence = await store.preparation(scope, planned.plan_id)
        before = reader.capture("tree")
        await store.record_absence(
            scope, fence, binding, before, observation_reader=reader, qualify=lambda: None
        )
        table, key, before_phase = Plan, planned.plan_id, "preparing"
    else:
        planned, _ = await ready(missing)
        table, key, before_phase = Plan, planned.plan_id, "ready"
        if phase == "settle":
            operation, fence, writer, change, authorize = await prepared(missing, planned)
            effect = await apply(missing, fence, writer, change, authorize)
            assert effect.receipt.status == "done"
            table, key, before_phase = Attempt, operation.operation_id, "executing"
    deadline = time.time() + 0.25
    if fault == "lease":
        if phase == "start":
            store.lease_seconds = 0.2
        else:
            async with store.database.sessions() as sql, sql.begin():
                row = await sql.get(table, key)
                row.lease_expires_at = deadline
    view, projected = store._view, []

    async def delayed(sql, row, attempt):
        result = await view(sql, row, attempt)
        projected.append(True)
        if fault == "lease":
            original_deadline = attempt.lease_expires_at if phase == "start" else deadline
            await asyncio.sleep(max(0, original_deadline - time.time()) + 0.01)
        return result

    def qualify():
        if projected and fault == "qualification":
            raise ServiceError("original_parent_revoked", "原父资格已撤销", 403)

    monkeypatch.setattr(store, "_view", delayed)
    with pytest.raises(ServiceError) as denied:
        if phase == "ready":
            await store.ready(scope, fence, binding, qualify=qualify)
        elif phase == "start":
            await store.start(scope, binding, execute(planned), qualify=qualify)
        else:
            await store.settle(scope, fence, binding, effect, qualify=qualify)
    assert projected == [True]
    assert denied.value.code == (
        "workspace_missing_directory_restore_fenced"
        if fault == "lease"
        else "original_parent_revoked"
    )
    async with store.database.sessions() as sql:
        plan = await sql.get(Plan, planned.plan_id)
        assert plan.phase == before_phase
        if phase == "start":
            assert await sql.scalar(select(Attempt).where(Attempt.plan_id == plan.plan_id)) is None
        if phase == "settle":
            row = await sql.get(Attempt, operation.operation_id)
            assert row.status == "running" and row.intent_recorded and row.effect_metadata is None
