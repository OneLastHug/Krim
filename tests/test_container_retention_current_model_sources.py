"""本次GC的真实SQL/Run与合成call标记；尚不冒称模型父工具已交付。"""

import asyncio
import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import delete, func, select, update
from test_container_retention_host import controller as controller
from test_container_retention_host import request
from test_container_retention_store import remove

from krim_agent.persistence.database import RunRow
from krim_agent.persistence.database import WorkspaceContainerRetentionModelSourceRow as Source
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Row
from krim_agent.persistence.workspace_container_retentions import WorkspaceContainerRetentionStore
from krim_agent.persistence.workspace_edits import digest
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.tools.core import ToolContext
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention


@pytest.fixture
async def actor(controller, runtime):
    service, agent, binding, original, preview = controller
    handle = service._handle(agent.agent_id)
    # 真实会话/Run创建；不运行Provider，GC call id是本单元明确合成的存储层输入。
    run, _ = await runtime.repository.begin_run(
        handle.topic_id,
        ChatRequest(text="可信GC来源存储验证", idempotency_key="current-gc-source-run"),
        agent_session_id=agent.session_id,
        agent_id=agent.agent_id,
        agent_owner_id=service.manager.owner_id,
    )
    yield (
        service,
        agent,
        binding,
        original,
        preview,
        "session:" + agent.session_id,
        CheckpointModelSource(run.id, "original-gc-remove"),
    )


async def reserved(actor):
    service, _agent, binding, original, preview, scope, source = actor
    store = service.container_retention_store
    wanted = request(preview)
    view, created = await store.reserve(
        scope, binding, wanted, qualify=lambda: None, model_source=source
    )
    assert created and view.status == "running"
    fence = await store.execution(scope, view.operation_id)
    assert fence.model_source == source
    origin = await store.inputs(scope, fence, binding, qualify=lambda: None)
    core = WorkspaceEmptyContainerRetention(binding.workspace)
    return store, scope, binding, origin, original, core, wanted, view, fence


async def test_actual_remove_origin_and_terminal_run_cold_same_key_preserve_old_material(
    actor, monkeypatch
):
    context = await reserved(actor)
    store, scope, binding, origin, original, _, wanted, view, fence = context
    source = actor[-1]
    assert (await store.model_sources(scope, view.operation_id)).remove == source
    for selected in [None, replace(source, call_id="other-call")]:
        with pytest.raises(ServiceError) as denied:
            await store.assert_execution(
                scope, replace(fence, model_source=selected), binding, qualify=lambda: None
            )
        assert denied.value.code == "workspace_container_retention_model_context"
    effect = await remove(context)
    assert effect.status == "removed"
    final = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
    assert final.status == "done" and final.receipt.changed
    async with store.database.sessions() as sql, sql.begin():
        await sql.execute(
            update(RunRow).where(RunRow.id == source.run_id).values(status="completed")
        )
    cold = WorkspaceContainerRetentionStore(store.restores)
    assert await cold.get(scope, view.operation_id) == final
    assert await cold.model_sources(scope, view.operation_id) == await store.model_sources(
        scope, view.operation_id
    )
    assert await cold.reserve(
        scope, binding, wanted, qualify=lambda: pytest.fail("同原键不再准入"), model_source=source
    ) == (final, False)
    assert await cold.reserve(
        scope, binding, wanted, qualify=lambda: pytest.fail("Host历史也不生产")
    ) == (final, False)
    assert await store.restores.get(scope, origin.view.source_plan_id) == original
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"


async def test_foreign_run_admission_and_even_resigned_foreign_origin_are_rejected(actor, runtime):
    service, _, binding, _, preview, scope, source = actor
    store = service.container_retention_store
    other = await runtime.repository.create_topic("其他合成Run作用域")
    run, _ = await runtime.repository.begin_run(
        other.id, ChatRequest(text="其他话题", idempotency_key="other")
    )
    with pytest.raises(ServiceError) as denied:
        await store.reserve(
            scope,
            binding,
            request(preview),
            qualify=lambda: None,
            model_source=replace(source, run_id=run.id),
        )
    assert denied.value.code == "workspace_run_mismatch"
    async with store.database.sessions() as sql:
        assert await sql.scalar(select(func.count()).select_from(Row)) == 0
    _, _, _, _, _, _, _, view, _ = await reserved(actor)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(Row, view.operation_id)
        value = copy.deepcopy(row.model_sources_metadata)
        value["remove"]["run_id"] = run.id
        unsigned = {k: v for k, v in value.items() if k != "material_sha256"}
        value["material_sha256"] = digest(unsigned)
        row.model_sources_metadata = value
        await sql.execute(
            update(Source).where(Source.operation_id == view.operation_id).values(run_id=run.id)
        )
    with pytest.raises(ServiceError) as denied:
        await store.get(scope, view.operation_id)
    assert denied.value.code == "workspace_container_retention_corrupt"


async def test_existing_host_leaf_cannot_relabel_current_model_fence_as_host(actor):
    service, agent, binding, _, _, scope, _ = actor
    store, _, _, _, _, _, _, view, fence = await reserved(actor)
    handle = service._handle(agent.agent_id)
    result = await handle.runtime.tools.invoke(
        "workspace_container_retention_remove",
        {"operation_id": view.operation_id},
        ToolContext(
            handle.runtime.tools,
            topic_id=handle.topic_id,
            agent_id=agent.agent_id,
            session_id=agent.session_id,
            workspace=binding,
            origin="host",
            container_retention=fence,
            workspace_container_retentions=service.container_retentions,
        ),
    )
    assert result.is_error and result.code == "workspace_container_retention_context"
    assert (await store.get(scope, view.operation_id)).status == "running"
    assert list(binding.workspace.root.glob("._krim-tx-*"))
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"


@pytest.mark.parametrize("bad", [False, 0, "", {}, SimpleNamespace(run_id="run", call_id="call")])
async def test_falsey_or_shaped_source_never_admits_host_record(actor, bad):
    service, _, binding, _, preview, scope, _ = actor
    with pytest.raises(ServiceError) as denied:
        await service.container_retention_store.reserve(
            scope, binding, request(preview), qualify=lambda: None, model_source=bad
        )
    assert denied.value.code == "workspace_container_retention_model_context"
    async with service.container_retention_store.database.sessions() as sql:
        assert await sql.scalar(select(func.count()).select_from(Row)) == 0
        assert await sql.scalar(select(func.count()).select_from(Source)) == 0


@pytest.mark.parametrize(
    "fault", ["index", "marker", "hash", "scope", "table-call", "version-bool"]
)
async def test_marker_index_or_bound_metadata_loss_is_corrupt_not_host(actor, fault):
    store, scope, _, _, _, _, _, view, _ = await reserved(actor)
    async with store.database.sessions() as sql, sql.begin():
        row = await sql.get(Row, view.operation_id)
        if fault == "index":
            await sql.execute(delete(Source).where(Source.operation_id == view.operation_id))
        elif fault == "marker":
            row.model_sources_metadata = None
        elif fault == "table-call":
            await sql.execute(
                update(Source)
                .where(Source.operation_id == view.operation_id)
                .values(call_id="other")
            )
        else:
            value = copy.deepcopy(row.model_sources_metadata)
            if fault == "hash":
                value["material_sha256"] = "0" * 64
            else:
                if fault == "version-bool":
                    value["version"] = True
                else:
                    value["scope_id"] = "session:other"
                unsigned = {k: v for k, v in value.items() if k != "material_sha256"}
                value["material_sha256"] = digest(unsigned)
            row.model_sources_metadata = value
    with pytest.raises(ServiceError) as denied:
        await store.get(scope, view.operation_id)
    assert denied.value.code == "workspace_container_retention_corrupt"


async def test_same_key_requires_original_model_call_but_host_history_only_reads(actor):
    store, scope, binding, _, _, _, wanted, view, _ = await reserved(actor)
    source = actor[-1]
    assert await store.reserve(
        scope, binding, wanted, qualify=lambda: None, model_source=source
    ) == (view, False)
    with pytest.raises(ServiceError) as denied:
        await store.reserve(
            scope,
            binding,
            wanted,
            qualify=lambda: None,
            model_source=replace(source, call_id="different-parent-call"),
        )
    assert denied.value.code == "workspace_container_retention_model_context"
    assert await store.reserve(scope, binding, wanted, qualify=lambda: None) == (view, False)


@pytest.mark.parametrize("kind", ["model", "host"])
async def test_original_model_remove_and_new_epoch_verify_keep_separate_origins(actor, kind):
    context = await reserved(actor)
    store, scope, binding, origin, original, core, wanted, view, fence = context
    effect = await remove(context)
    assert effect.status == "removed"
    unknown = await store.fail(scope, fence, "synthetic_receipt_lost_after_real_remove")
    assert unknown.status == "unknown"
    verify_source = replace(actor[-1], call_id="original-gc-verify") if kind == "model" else None
    started, verification = await store.claim_verify(
        scope,
        view.operation_id,
        binding,
        expected_revision=unknown.revision,
        request_key="original-verify",
        qualify=lambda: None,
        model_source=verify_source,
    )
    assert verification.model_source == verify_source and verification.epoch == 1
    origins = await store.model_sources(scope, view.operation_id)
    assert origins.remove == actor[-1] and origins.verify == verify_source
    wrong = None if verify_source is not None else actor[-1]
    with pytest.raises(ServiceError) as denied:
        await store.assert_execution(
            scope, replace(verification, model_source=wrong), binding, qualify=lambda: None
        )
    assert denied.value.code == "workspace_container_retention_model_context"
    observed = await asyncio.to_thread(core.inspect, origin.stage, authorize=lambda: None)
    final = await store.finish_verify(scope, verification, binding, observed, qualify=lambda: None)
    assert final.status == "done" and final.receipt.outcome == "absence_verified"
    assert not final.receipt.changed and final.verification_count == 1
    assert await store.claim_verify(
        scope,
        view.operation_id,
        binding,
        expected_revision=unknown.revision,
        request_key="original-verify",
        qualify=lambda: None,
        model_source=verify_source,
    ) == (final, None)
    if verify_source is not None:
        with pytest.raises(ServiceError) as denied:
            await store.claim_verify(
                scope,
                view.operation_id,
                binding,
                expected_revision=unknown.revision,
                request_key="original-verify",
                qualify=lambda: None,
                model_source=replace(verify_source, call_id="different-verify"),
            )
        assert denied.value.code == "workspace_container_retention_model_context"
    assert await store.restores.get(scope, origin.view.source_plan_id) == original
