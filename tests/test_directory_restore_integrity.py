"""实际材料配额、未暂存关闭和持久状态交叉关联的拒绝证据。"""

from dataclasses import replace

import pytest
from sqlalchemy import LargeBinary, Text, cast, func, select
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import apply, execute_request, prepared, ready
from test_directory_restore_plans import request

import krim_agent.workspace_directory_restore_writer as physical
from krim_agent.persistence.database import (
    WorkspaceDirectoryRestoreAttemptRow,
    WorkspaceDirectoryRestorePlanRow,
)
from krim_agent.persistence.directory_restore_codec import MAX_METADATA
from krim_agent.protocol.domain import ServiceError


async def test_unknown_receipt_uses_original_reserved_metadata_capacity(
    capture_context, monkeypatch
):
    store, scope, binding, _, planned = await ready(capture_context)
    op, fence, writer, change, authorize = await prepared(store, scope, binding, planned)
    p = WorkspaceDirectoryRestorePlanRow
    # 按真实数据库JSON编码计费，精确冻结准备已获准时的容量，不扩大原配额。
    size = func.length(p.prepared_payload)
    for column in (p.stage_metadata, p.prepared_metadata, p.target_summary, p.baseline_summary):
        size += func.coalesce(func.length(cast(cast(column, Text), LargeBinary)), 0)
    async with store.database.sessions() as sql:
        original = await sql.scalar(select(size).where(p.plan_id == planned.plan_id))
    store.max_snapshot_bytes = original + MAX_METADATA
    rename = physical.rename_at

    def lost(*args, **kwargs):
        rename(*args, **kwargs)
        raise OSError("原namespace已发布，只有回程丢失")

    monkeypatch.setattr(physical, "rename_at", lost)
    effect = await apply(store, scope, binding, fence, writer, change, authorize)
    assert effect.receipt.status == "uncertain"
    result = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert result.phase == "uncertain" and result.operation_id == op.operation_id
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    async with store._transaction() as sql:
        await store._material_capacity(sql)


@pytest.mark.parametrize("close", ["cancel", "fail"])
async def test_close_before_stage_releases_reservation_without_fake_cleanup(capture_context, close):
    store, scope, binding, _, planned = await ready(capture_context)
    op, _ = await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
    fence = await store.execution(scope, op.operation_id)
    if close == "cancel":
        result = await store.cancel(scope, planned.plan_id)
    else:
        result = await store.fail(scope, fence, "original_failure_before_stage")
    assert not result.cleanup_pending
    assert not list(binding.workspace.root.glob("._krim-tx-*"))
    async with store.database.sessions() as sql:
        row = await sql.get(WorkspaceDirectoryRestorePlanRow, planned.plan_id)
        assert row.staging_reserved_bytes == row.staging_reserved_entries == 0


@pytest.mark.parametrize("fault", ["phase", "revision", "operation_error", "verification_mode"])
async def test_persisted_plan_and_operation_must_describe_same_state(capture_context, fault):
    store, scope, binding, _, planned = await ready(capture_context)
    op, fence, _, change, _ = await prepared(store, scope, binding, planned)
    await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(WorkspaceDirectoryRestorePlanRow, planned.plan_id)
        attempt = await sql.get(WorkspaceDirectoryRestoreAttemptRow, op.operation_id)
        if fault == "phase":
            row.phase = "ready"
        elif fault == "revision":
            row.revision = attempt.expected_revision
        elif fault == "verification_mode":
            attempt.mode = "verify"
        else:
            # 每张表本身均合法，交叉事实却矛盾，不能展示为正在执行。
            attempt.status, attempt.error_code = "uncertain", "unknown_original_intent"
            attempt.owner_id = attempt.lease_expires_at = None
    with pytest.raises(ServiceError) as denied:
        await store.get(scope, planned.plan_id)
    assert denied.value.code == "workspace_directory_restore_corrupt"


@pytest.mark.parametrize("phase", ["prepare", "execute"])
async def test_non_ascii_private_token_is_controlled_rejection(capture_context, phase):
    store, scope, binding, _, planned = await ready(capture_context)
    if phase == "prepare":
        preview, _ = await store.reserve(
            scope, binding, request(planned.target.identifier, "second"), qualify=lambda: None
        )
        fence = await store.preparation(scope, preview.plan_id)
        assertion = store.assert_preparation
    else:
        op, _ = await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
        fence = await store.execution(scope, op.operation_id)
        assertion = store.assert_execution
    with pytest.raises(ServiceError) as denied:
        await assertion(scope, replace(fence, token="汉" * 64), binding, qualify=lambda: None)
    assert denied.value.code == "workspace_directory_restore_fenced"
