"""同事务投影后仍须核实原租约和父资格；展示耗时不能创造执行资格。"""

import asyncio
import time
from dataclasses import replace

import pytest
from test_directory_restore_execution import prepared
from test_directory_restore_model_sources import model_ready
from test_directory_restore_plans import request
from test_model_directory_checkpoint_store import context as context

from krim_agent.persistence.database import (
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestorePlanRow,
)
from krim_agent.protocol.domain import ServiceError


@pytest.mark.parametrize(
    "kind,fault",
    [(kind, fault) for kind in ("prepare", "execute") for fault in ("lease", "qualification")],
)
async def test_qualified_view_cannot_return_permission_after_projection_crosses_original_fence(
    context, monkeypatch, kind, fault
):
    _, _, binding, scope, source, _ = context
    store, planned = await model_ready(context)
    if kind == "prepare":
        planned, _ = await store.reserve(
            scope,
            binding,
            request(planned.target.identifier, "qualified-view"),
            model_source=replace(source, call_id="qualified-preview"),
            qualify=lambda: None,
        )
        fence = await store.preparation(scope, planned.plan_id)
        table, identifier = WorkspaceDirectoryRestorePlanRow, planned.plan_id
    else:
        operation, fence, _, _, _ = await prepared(
            store,
            scope,
            binding,
            planned,
            model_source=replace(source, call_id="qualified-execute"),
        )
        table, identifier = WorkspaceDirectoryRestoreAttemptRow, operation.operation_id
    deadline = time.time() + 0.5
    if fault == "lease":
        async with store.database.sessions() as sql, sql.begin():
            row = await sql.get(table, identifier)
            row.lease_expires_at = deadline
    view, projected = store._view, []

    async def delayed(*args, **kwargs):
        result = await view(*args, **kwargs)
        projected.append(True)
        if fault == "lease":
            await asyncio.sleep(max(0, deadline - time.time()) + 0.01)
        return result

    def qualify():
        if projected and fault == "qualification":
            raise ServiceError("original_parent_qualification_lost", "原父资格已撤销", 403)

    monkeypatch.setattr(store, "_view", delayed)
    read = store.preparation_view if kind == "prepare" else store.execution_view
    with pytest.raises(ServiceError) as denied:
        await read(scope, fence, binding, qualify=qualify)
    assert projected == [True]
    assert denied.value.code == (
        "workspace_directory_restore_fenced"
        if fault == "lease"
        else "original_parent_qualification_lost"
    )
    async with store.database.sessions() as sql:
        row = await sql.get(table, identifier)
        assert row.owner_id == store.owner_id
        if fault == "lease":
            assert row.lease_expires_at == deadline
        if kind == "execute":
            assert row.status == "running" and not row.intent_recorded
