"""真实Missing终态来源、冷读取与独立回收协议语义，未连接物理删除。"""

import copy

import pytest
from pydantic import ValidationError
from sqlalchemy import update
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_missing_directory_restore_store import apply, prepared, ready
from test_missing_directory_restore_store import missing as missing

from krim_agent.persistence.container_retention_sources import (
    ContainerRetentionSources,
    decode_origin,
)
from krim_agent.persistence.database import WorkspaceMissingDirectoryRestorePlanRow as Plan
from krim_agent.persistence.directory_restore_codec import encode_stage
from krim_agent.persistence.workspace_edits import digest
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace_container_retention import ContainerRetentionView


async def terminal(context):
    store, scope, binding, *_ = context
    plan, _ = await ready(context)
    operation, fence, writer, change, authorize = await prepared(context, plan)
    effect = await apply(context, fence, writer, change, authorize)
    assert effect.receipt.status == "done"
    view = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    return plan, operation, view


async def source(context, plan_id):
    store, scope, binding, *_ = context
    async with store.database.sessions() as sql:
        return await ContainerRetentionSources(store).read(
            sql,
            scope,
            plan_id,
            workspace_key=binding.key,
            resource_fingerprint=binding.fingerprint,
            workspace_fingerprint=binding.workspace.fingerprint,
        )


async def test_actual_done_origin_preserves_private_stage_and_old_finished_view(missing):
    store, scope, binding, *_ = missing
    plan, operation, original = await terminal(missing)
    value = await source(missing, plan.plan_id)
    assert value.view.source_plan_id == plan.plan_id
    assert value.view.source_operation_id == operation.operation_id
    assert value.view.published_fingerprint == original.receipt.after_fingerprint
    assert value.stage.root_path == "tree"
    assert value.stage.token not in repr(value) + value.view.model_dump_json()
    assert decode_origin(value.metadata, value.view.source_hash).source_plan_id == plan.plan_id
    (binding.workspace.root / "tree/user-added").write_bytes(b"user\0\xff")
    assert await source(missing, plan.plan_id) == value
    # 后续仅回收原槽位计费，不改变旧finished事实或Source hash。
    async with store.database.sessions() as sql, sql.begin():
        await sql.execute(
            update(Plan).where(Plan.plan_id == plan.plan_id).values(staging_reserved_entries=0)
        )
    assert await source(missing, plan.plan_id) == value
    assert await store.get(scope, plan.plan_id) == original


@pytest.mark.parametrize("phase", ["ready", "unknown"])
async def test_unfinished_original_cannot_supply_retention_authority(missing, phase):
    store, scope, binding, *_ = missing
    plan, _ = await ready(missing)
    if phase == "unknown":
        _operation, fence, _writer, change, _authorize = await prepared(missing, plan)
        await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
        await store.fail(scope, fence, "synthetic_uncertain")
    with pytest.raises(ServiceError) as error:
        await source(missing, plan.plan_id)
    assert error.value.code == "workspace_container_retention_incomplete"


@pytest.mark.parametrize("fault", ["scope", "resource", "stage", "payload"])
async def test_other_scope_binding_or_corrupt_original_is_not_cleanup_source(missing, fault):
    store, scope, binding, *_ = missing
    plan, _operation, _view = await terminal(missing)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(Plan, plan.plan_id)
        if fault == "stage":
            value = copy.deepcopy(row.stage_metadata)
            value["stage"]["token"] = "f" * 32
            row.stage_metadata = value
        elif fault == "payload":
            row.prepared_payload = row.prepared_payload + b"wrong"
    async with store.database.sessions() as sql:
        with pytest.raises(ServiceError):
            await ContainerRetentionSources(store).read(
                sql,
                "topic:other" if fault == "scope" else scope,
                plan.plan_id,
                workspace_key=binding.key,
                resource_fingerprint="f" * 64 if fault == "resource" else binding.fingerprint,
                workspace_fingerprint=binding.workspace.fingerprint,
            )


def record(outcome="already_absent", *, intent=False, status="done"):
    now = utc_now()
    return {
        "operation_id": "cleanup-operation",
        "source": {
            "source_plan_id": "restore-plan",
            "source_operation_id": "restore-operation",
            "scope_id": "topic:topic",
            "topic_id": "topic",
            "root_path": "tree",
            "workspace_key": "files",
            "resource_fingerprint": "a" * 64,
            "source_hash": "b" * 64,
            "published_fingerprint": "c" * 64,
        },
        "request": {
            "source_plan_id": "restore-plan",
            "expected_source_hash": "b" * 64,
            "expected_container_fingerprint": "d" * 64,
            "request_key": "original-cleanup",
        },
        "status": status,
        "revision": 1,
        "intent_recorded": intent,
        "cancelled": status == "cancelled",
        "receipt": {
            "outcome": outcome,
            "expected_fingerprint": "d" * 64,
            "observed_fingerprint": "e" * 64,
            "changed": outcome == "removed",
            "recovery_required": False,
            "error_code": None,
        },
        "verification_count": 0,
        "error_code": None if status == "done" else "original_rejected",
        "created_at": now,
        "updated_at": now,
    }


def test_already_absent_without_namespace_intent_remains_truthful_readonly_terminal():
    value = ContainerRetentionView.model_validate(record())
    assert value.status == "done" and not value.intent_recorded and not value.receipt.changed


@pytest.mark.parametrize("status", ["rejected", "cancelled", "interrupted"])
def test_non_success_cannot_embed_removed_receipt(status):
    with pytest.raises(ValidationError):
        ContainerRetentionView.model_validate(record("removed", intent=True, status=status))


def test_public_retention_does_not_accept_private_stage_or_owner():
    for field in ["owner_id", "stage", "token"]:
        value = record(intent=True)
        value[field] = "not-public"
        with pytest.raises(ValidationError):
            ContainerRetentionView.model_validate(value)


async def test_private_codec_keeps_original_unicode_path_and_call_id_bounds(missing):
    from dataclasses import replace

    plan, _operation, _original = await terminal(missing)
    stored = await source(missing, plan.plan_id)
    metadata = copy.deepcopy(stored.metadata)
    root_path = "/".join(["🧪" * 30] * 4)
    assert len(root_path.encode("utf-8")) <= 512
    stage = replace(stored.stage, root_path=root_path)
    metadata["root_path"] = root_path
    metadata["stage_metadata"] = encode_stage(stage, metadata["restore_request_hash"])
    for field in ["preview_source", "execute_source", "verify_source"]:
        metadata[field] = {"run_id": "original-run", "call_id": "🧪" * 128}
    # 仅验证私有codec规范容量；伪造快照仍不能替代Store回查真实原Source。
    assert decode_origin(metadata, digest(metadata)).root_path == root_path
