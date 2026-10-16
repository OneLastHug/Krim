"""最后投影、材料精确字节和损坏来源在全部执行边界的拒绝。"""

import asyncio
import time

import pytest
from sqlalchemy import text
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_missing_baseline_rollback_store import preview
from test_missing_baseline_rollback_store import rollback_origin as rollback_origin
from test_missing_directory_restore_store import missing as missing
from test_workspace_tree_move_ledger import action, publish

from krim_agent.persistence.database import (
    WorkspaceMissingDirectoryRollbackRow,
    WorkspaceTreeMoveAttemptRow,
)
from krim_agent.protocol.domain import ServiceError


@pytest.mark.parametrize("phase", ["reserve", "settle"])
async def test_lease_must_still_be_live_after_last_projection(rollback_origin, monkeypatch, phase):
    store, scope, binding, *_ = rollback_origin
    writer, _, change, proposal = await preview(rollback_origin)
    identifier = deadline = effect = None
    if phase == "settle":
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        identifier = attempt.view.operation_id
        effect = await publish(store, writer, scope, attempt)
        assert effect.receipt.status == "done"
        async with store.database.sessions() as sql, sql.begin():
            row = await sql.get(WorkspaceTreeMoveAttemptRow, identifier)
            deadline = time.time() + 0.4
            row.lease_expires_at = deadline
    else:
        store.lease_seconds = 0.4
    original = store._attempt_value

    async def delayed(sql, row, parent):
        nonlocal deadline
        value = await original(sql, row, parent)
        if phase == "reserve" or row.status == "done":
            if phase == "reserve":
                deadline = row.lease_expires_at
            await asyncio.sleep(max(0, deadline - time.time()) + 0.05)
        return value

    monkeypatch.setattr(store, "_attempt_value", delayed)
    with pytest.raises(ServiceError) as denied:
        if phase == "reserve":
            await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        else:
            await store.settle(scope, identifier, effect)
    assert denied.value.code == "workspace_tree_move_fenced"
    async with store.database.sessions() as sql:
        if identifier is not None:
            row = await sql.get(WorkspaceTreeMoveAttemptRow, identifier)
            assert row.status == "running" and row.lease_expires_at == deadline
        else:
            assert not (await sql.execute(text("SELECT * FROM workspace_tree_move_attempts"))).all()


async def material_bytes(sql):
    # 独立读取SQLite实际存储字节；包含NULL JSON的实际四字节、完整回执和Unicode。
    columns = {
        "workspace_tree_move_proposals": (
            "payload",
            "prepared",
            "request",
            "missing_restore_plan_id",
            "missing_restore_hash",
        ),
        "workspace_tree_move_attempts": (
            "payload",
            "prepared",
            "after_payload",
            "after_prepared",
            "receipt",
            "receipt_history",
        ),
        "workspace_missing_directory_rollbacks": (
            "proposal_id",
            "source_plan_id",
            "source_operation_id",
            "scope_id",
            "source_hash",
            "source_metadata",
        ),
    }
    total = 0
    for table, fields in columns.items():
        values = "+".join(f"COALESCE(length(CAST({field} AS BLOB)),0)" for field in fields)
        total += (
            await sql.execute(text(f"SELECT COALESCE(SUM({values}),0) FROM {table}"))
        ).scalar_one()
    return total


@pytest.mark.parametrize("phase", ["preview", "reserve", "settle"])
async def test_quota_counts_all_actual_material_bytes_and_exact_reservation(
    rollback_origin, monkeypatch, phase
):
    store, scope, binding, *_ = rollback_origin
    (binding.workspace.root / "tree/中文材料").write_bytes(b"\0\xffquota")
    original = store._bytes
    reservation = []

    async def audited(sql, added):
        reservation.append((await material_bytes(sql), added))
        await original(sql, added)

    monkeypatch.setattr(store, "_bytes", audited)
    writer, _, change, proposal = await preview(rollback_origin)
    if phase != "preview":
        attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        if phase == "settle":
            result = await publish(store, writer, scope, attempt)
            assert result.receipt.status == "done"
            await store.settle(scope, attempt.view.operation_id, result)
    async with store.database.sessions() as sql:
        total = await material_bytes(sql)
        assert sum(reservation[-1]) == total, "预留必须等于本次实际持久材料增量"
        store.max_snapshot_bytes = total - 1
        with pytest.raises(ServiceError) as denied:
            await original(sql, 0)
        assert denied.value.code == "workspace_tree_move_snapshot_limit"
        store.max_snapshot_bytes = total
        await original(sql, 0)


@pytest.mark.parametrize("phase", ["authorize", "intent", "renew", "settle", "reconcile"])
async def test_lost_source_rejects_every_execution_or_receipt_entry(rollback_origin, phase):
    store, scope, binding, *_ = rollback_origin
    writer, _, change, proposal = await preview(rollback_origin)
    attempt, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
    identifier = attempt.view.operation_id
    effect = (
        await publish(store, writer, scope, attempt) if phase in {"settle", "reconcile"} else None
    )
    if phase == "reconcile":
        await store.mark_uncertain(scope, identifier)
    async with store.database.sessions() as sql, sql.begin():
        await sql.delete(
            await sql.get(WorkspaceMissingDirectoryRollbackRow, proposal.view.proposal_id)
        )
    operation = {
        "authorize": lambda: store.assert_execution(scope, identifier),
        "intent": lambda: store.record_intent(scope, identifier, change),
        "renew": lambda: store.renew(scope, identifier),
        "settle": lambda: store.settle(scope, identifier, effect),
        "reconcile": lambda: store.settle(scope, identifier, effect, resolving=True),
    }[phase]
    with pytest.raises(ServiceError):
        await operation()
