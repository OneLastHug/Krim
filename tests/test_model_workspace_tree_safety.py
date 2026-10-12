"""模型树真实拒绝/取消/丢回程与原已接纳Writer的收束边界。"""

import asyncio
import json
from dataclasses import replace
from threading import Event

import pytest
from sqlalchemy import func, select
from test_model_workspace_tree_tools import context, counts, setup
from test_model_workspace_tree_tools import pytestmark as pytestmark
from test_workspace_changes import wait_event

from krim_agent.agents.permissions import profile_policy
from krim_agent.persistence.database import WorkspaceTreeModelSourceRow
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.tools import ShortCircuit
from krim_agent.usage import TokenUsage
from krim_agent.workspace_directory import WorkspaceDirectoryWriter
from krim_agent.workspace_tree import TreePlanView


async def prepare(service, agent, provider):
    ctx = await context(service, agent, "prepare")
    result = await ctx.registry.invoke(
        "workspace_tree_model_prepare", provider.request.model_dump(mode="json"), ctx
    )
    assert not result.is_error, result
    return replace(ctx, call_id="start"), TreePlanView.model_validate_json(json.dumps(result.data))


async def source_count(runtime, kind):
    async with runtime.repository.database.sessions() as sql:
        return await sql.scalar(
            select(func.count())
            .select_from(WorkspaceTreeModelSourceRow)
            .where(WorkspaceTreeModelSourceRow.kind == kind)
        )


@pytest.mark.parametrize("failure", ["renew-and-cancel", "cancel"])
async def test_failed_parent_close_still_settles_original_worker_and_child(
    runtime, tmp_path, monkeypatch, failure
):
    service, agent, provider = await setup(runtime, tmp_path)
    entered, child_closed = asyncio.Event(), asyncio.Event()
    task = None
    try:
        ctx, plan = await prepare(service, agent, provider)
        store = service.manager.tree_models.store
        original = store.cancel
        close_calls = 0

        async def cancel(*args):
            nonlocal close_calls
            close_calls += 1
            if close_calls == 1:
                raise ServiceError("synthetic_storage_unavailable", "测试首个父关闭失败", 503)
            return await original(*args)

        async def renew(*_args):
            await entered.wait()
            raise ServiceError("workspace_tree_fenced", "测试原租约失效", 409)

        async def pause(definition, child):
            if definition.name == "directory_change" and child.tree_fence is not None:
                entered.set()
                try:
                    async with child.human_wait():
                        await asyncio.Event().wait()
                finally:
                    child_closed.set()

        monkeypatch.setattr(store, "cancel", cancel)
        if failure == "renew-and-cancel":
            monkeypatch.setattr(store, "renew", renew)
        ctx.registry.add_global_pre(pause)
        task = asyncio.create_task(
            ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        )
        await asyncio.wait_for(entered.wait(), 10)
        if failure == "cancel":
            task.cancel()
        done, _pending = await asyncio.wait({task}, timeout=4)
        assert task in done, "父关闭失败不能让失效Worker或原child继续等待"
        with pytest.raises(asyncio.CancelledError):
            await task
        assert child_closed.is_set() and close_calls >= 2
        assert not service.manager.tree_models._tasks and not service.manager.tree_models._plans
        assert (await agent.directory_tree(plan.plan_id)).producer_status == "cancelled"
        assert await counts(runtime.repository.database) == [1, 1, 1, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_tool_timeout_closes_original_parent_and_child_before_return(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    entered, closed = asyncio.Event(), asyncio.Event()
    try:
        ctx, plan = await prepare(service, agent, provider)
        ctx = replace(ctx, timeout=1.5)

        async def pause(definition, child):
            if definition.name == "directory_change" and child.tree_fence is not None:
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    closed.set()

        ctx.registry.add_global_pre(pause)
        result = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert result.is_error and result.code == "timeout", result
        assert entered.is_set() and closed.is_set()
        assert (await agent.directory_tree(plan.plan_id)).producer_status == "cancelled"
        assert not service.manager.tree_models._tasks and list(tmp_path.iterdir()) == []
        assert await counts(runtime.repository.database) == [1, 1, 1, 0, 0, 0]
    finally:
        await service.close()


async def test_nested_approval_borrows_original_deadline_and_keeps_run_identity(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    task = None
    try:
        ctx, plan = await prepare(service, agent, provider)
        observations = []

        async def policy(definition, _child):
            return "ask" if definition.name == "directory_change" else "allow"

        async def approve(definition, child):
            observations.append((child.run_id, child.source_call_id, child.depth))
            assert definition.name == "directory_change"
            assert len(child.execution_timeouts) == 2
            assert all(budget.deadline.when() is None for budget in child.execution_timeouts)
            entered.set()
            await release.wait()
            return True

        # 使用原Profile实际工具时限；审批暂停的证明不与完整树正常耗时竞争。
        ctx = replace(
            ctx,
            timeout=service._handle(agent.agent_id).definition.tool_timeout_seconds,
            policy=policy,
            approval_handler=approve,
        )
        task = asyncio.create_task(
            ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        )
        await asyncio.wait_for(entered.wait(), 10)
        await asyncio.sleep(ctx.timeout + 0.2)
        assert not task.done(), "人工审批须暂停原父工具和原child执行deadline"
        release.set()
        result = await task
        assert not result.is_error and result.data["status"] == "done", result
        assert observations == [(ctx.run_id, "start", 2)] * 2
        assert (tmp_path / "tree/sub/data.bin").read_bytes() == b"\0\xff"
    finally:
        release.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("permission", ["deny", "ask", "plan"])
async def test_start_permission_does_not_claim_or_create_original_proposals(
    runtime, tmp_path, permission
):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        ctx, plan = await prepare(service, agent, provider)

        async def policy(definition, _child):
            return permission if definition.name == "workspace_tree_start" else "allow"

        ctx = replace(
            ctx, policy=profile_policy("plan", ctx.policy) if permission == "plan" else policy
        )
        result = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert result.is_error and result.code == (
            "approval_required" if permission == "ask" else "permission_denied"
        )
        assert (await agent.directory_tree(plan.plan_id)).status == "planned"
        assert await source_count(runtime, "start") == 0
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_real_provider_plan_mode_can_prepare_but_cannot_start(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        handle = service._handle(agent.agent_id)
        handle.runtime.tool_policy = profile_policy("plan", handle.runtime.tool_policy)
        provider.error = "permission_denied"
        result = await agent.execute("仅审阅原树", idempotency_key="plan-only")
        assert result.status == "completed" and provider.calls == 3
        plan = await agent.directory_tree_by_key(provider.request.request_key)
        assert plan.status == "planned" and await source_count(runtime, "start") == 0
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_real_provider_depth_limit_rejects_nested_tree_before_proposal(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        service._handle(agent.agent_id).runtime.tools.max_depth = 1
        provider.expected_status = "failed"
        result = await agent.execute("原嵌套深度拒绝", idempotency_key="depth-one")
        assert result.status == "completed" and provider.calls == 4
        page = await agent.directory_tree_steps(provider.plan.plan_id)
        assert page.items[0].error_code == "max_depth"
        assert all(item.operation is None for item in page.items)
        assert provider.plan.done_count == 0 and list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_real_provider_budget_exhaustion_after_prepare_cannot_request_start(
    runtime, tmp_path
):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        handle = service._handle(agent.agent_id)

        async def consume_shared_account(definition, value, child):
            if definition.name != "workspace_tree_model_prepare":
                return value
            store = handle.runtime.budget_store
            assert child.budget_account_ids
            account = await store.account(child.budget_account_ids[-1])
            assert account.spent_tokens == 11 and account.held_tokens == 0
            topic = await runtime.repository.create_topic("合成同账户累计消耗")
            run, _ = await runtime.repository.begin_run(
                topic.id, ChatRequest(text="synthetic charge", idempotency_key="charge")
            )
            remaining = account.config.max_total_tokens - account.spent_tokens
            reserved = await store.reserve(
                run.id, 0, child.budget_account_ids, remaining - 1, 1, "synthetic-cumulative-charge"
            )
            await store.transition(reserved.request_id, "dispatched")
            await store.settle(
                reserved.request_id,
                TokenUsage(input_tokens=remaining - 1, output_tokens=1, total_tokens=remaining),
            )
            return value

        handle.runtime.tools.add_global_post(consume_shared_account)
        result = await agent.execute("累计预算拒绝启动", idempotency_key="budget-exhausted")
        assert result.status == "failed" and result.code == "budget_tokens_exceeded", result
        assert provider.calls == 1
        plan = await agent.directory_tree_by_key(provider.request.request_key)
        assert plan.status == "planned" and await source_count(runtime, "start") == 0
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
        accounts = await handle.runtime.budget_store.accounts()
        assert accounts[0].spent_tokens == 100_000 and accounts[0].held_tokens == 0
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault",
    [
        "pre-plan",
        "pre-call",
        "pre-deadline",
        "pre-child-backend",
        "short",
        "post-value",
        "post-marker",
    ],
)
async def test_start_hooks_preserve_original_call_and_actual_receipts(runtime, tmp_path, fault):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        ctx, plan = await prepare(service, agent, provider)

        async def pre(definition, child):
            if definition.name != "workspace_tree_start":
                return None
            if fault == "pre-plan":
                child.arguments["plan_id"] = "other"
            elif fault == "pre-call":
                child.call_id = "fake"
            elif fault == "pre-deadline":
                child.execution_timeouts = ()
            elif fault == "pre-child-backend":
                child.workspace_changes = object()
            elif fault == "short":
                return ShortCircuit(plan.model_dump(mode="json"))
            return None

        async def post(definition, value, child):
            if definition.name == "workspace_tree_start" and fault.startswith("post"):
                value["request_key"] = "forged"
                if fault == "post-marker":
                    child._tree_model_response = ("start", json.dumps(value, sort_keys=True))
            return value

        ctx.registry.add_global_pre(pre)
        ctx.registry.add_global_post(post)
        result = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert result.is_error and result.code in {
            "workspace_tree_fence_mismatch",
            "workspace_tree_model_unverified",
        }
        original = await agent.directory_tree(plan.plan_id)
        assert original.request_key == plan.request_key
        assert original.status == ("done" if fault.startswith("post") else "planned")
        assert await counts(runtime.repository.database) == (
            [1, 1, 2, 2, 1, 1] if fault.startswith("post") else [1, 1, 0, 0, 0, 0]
        )
        # 手动可信上下文尚未经Runtime.finish；持久审计由实际Provider循环另行验证。
        audits = ctx.registry.audits_for_run(ctx.run_id)
        assert any(
            item.name == "workspace_tree_start" and item.call_id == "start" for item in audits
        )
    finally:
        await service.close()


async def test_bad_child_return_keeps_original_effect_and_closes_descendants(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        ctx, plan = await prepare(service, agent, provider)

        async def corrupt(definition, value, _child):
            if definition.name == "directory_change":
                value["status"] = "rejected"
            return value

        ctx.registry.add_global_post(corrupt)
        result = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert not result.is_error and result.data["status"] == "partial"
        assert result.data["done_count"] == 1 and (tmp_path / "tree").is_dir()
        assert not (tmp_path / "tree/sub").exists()
        assert await counts(runtime.repository.database) == [1, 1, 1, 1, 0, 0]
        retry = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert retry == result
    finally:
        await service.close()


async def test_unknown_root_reconciles_same_operation_without_resuming_model_tree(
    runtime, tmp_path, monkeypatch
):
    import krim_agent.workspace_directory as module

    service, agent, provider = await setup(runtime, tmp_path)
    calls = 0
    original = module.rename_at

    def ambiguous(*args, **kwargs):
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        raise OSError("合成原rename成功后回执丢失")

    try:
        ctx, plan = await prepare(service, agent, provider)
        monkeypatch.setattr(module, "rename_at", ambiguous)
        result = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert not result.is_error and result.data["status"] == "uncertain", result
        page = await agent.directory_tree_steps(plan.plan_id)
        operation = page.items[0].operation.operation_id
        assert all(item.status == "skipped" for item in page.items[1:])
        assert (await agent.reconcile_directory_change(operation)).status == "done"
        cold = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert cold.data["status"] == "partial" and cold.data["done_count"] == 1
        assert calls == 1 and await counts(runtime.repository.database) == [1, 1, 1, 1, 0, 0]
        assert not (tmp_path / "tree/sub").exists()
    finally:
        await service.close()


async def test_real_provider_stop_closes_parent_before_admitted_writer_returns(
    runtime, tmp_path, monkeypatch
):
    service, agent, _provider = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    parent_closed = asyncio.Event()
    original = WorkspaceDirectoryWriter.apply
    executing = stopping = None
    try:
        store = service.manager.tree_models.store
        original_cancel = store.cancel

        async def cancel(*args):
            value = await original_cancel(*args)
            parent_closed.set()
            return value

        def blocked(writer, prepared, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(writer, prepared, **kwargs)

        monkeypatch.setattr(store, "cancel", cancel)
        monkeypatch.setattr(WorkspaceDirectoryWriter, "apply", blocked)
        executing = asyncio.create_task(agent.execute("原Writer前停止", idempotency_key="stop"))
        await wait_event(entered)
        stopping = asyncio.create_task(agent.stop())
        await asyncio.wait_for(parent_closed.wait(), 3)
        assert not stopping.done() and not release.is_set()
        release.set()
        await stopping
        result = await executing
        assert result.status in {"failed", "aborted"}
        plan = await agent.directory_tree_by_key("树/../?confirmed=false#%")
        assert plan.producer_status == "cancelled" and plan.done_count == 0
        assert await counts(runtime.repository.database) == [1, 1, 1, 1, 0, 0]
        assert list(tmp_path.iterdir()) == [] and not service.manager.tree_models._tasks
    finally:
        release.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        if executing is not None:
            await asyncio.gather(executing, return_exceptions=True)
        await service.close()


async def test_child_self_close_is_rejected_before_closing_agent_service(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    observed = []
    try:
        ctx, plan = await prepare(service, agent, provider)

        async def close(definition, _child):
            if definition.name == "directory_source_preview":
                with pytest.raises(ServiceError) as denied:
                    await service.close()
                observed.append(denied.value.code)

        ctx.registry.add_global_pre(close)
        result = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert not result.is_error and result.data["status"] == "done", result
        assert observed == ["workspace_tree_model_reentrant"] * 2
        assert not service.manager.tree_models._closed
    finally:
        await service.close()


async def test_competing_same_call_claim_keeps_only_actual_producer_cancel_index(
    runtime, tmp_path, monkeypatch
):
    service, agent, provider = await setup(runtime, tmp_path)
    claims, entered = asyncio.Event(), asyncio.Event()
    tasks = []
    try:
        ctx, plan = await prepare(service, agent, provider)
        store = service.manager.tree_models.store
        original = store.claim
        arrivals = 0

        async def claim(*args, **kwargs):
            nonlocal arrivals
            arrivals += 1
            if arrivals == 2:
                claims.set()
            await claims.wait()
            return await original(*args, **kwargs)

        async def pause(definition, child):
            if definition.name == "directory_change":
                entered.set()
                async with child.human_wait():
                    await asyncio.Event().wait()

        monkeypatch.setattr(store, "claim", claim)
        ctx.registry.add_global_pre(pause)
        for _ in range(2):
            tasks.append(
                asyncio.create_task(
                    ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
                )
            )
        await asyncio.wait_for(entered.wait(), 10)
        done, pending = await asyncio.wait(tasks, timeout=5, return_when=asyncio.FIRST_COMPLETED)
        assert len(done) == len(pending) == 1 and arrivals == 2
        result = await next(iter(done))
        assert not result.is_error and result.data["producer_status"] == "running"
        worker = service.manager.tree_models._plans[plan.plan_id][1]
        assert not worker.done() and await source_count(runtime, "start") == 1
        assert (await agent.cancel_directory_tree(plan.plan_id)).producer_status == "cancelled"
        with pytest.raises(asyncio.CancelledError):
            await next(iter(pending))
        assert not service.manager.tree_models._tasks and not service.manager.tree_models._plans
        assert await counts(runtime.repository.database) == [1, 1, 1, 0, 0, 0]
        assert list(tmp_path.iterdir()) == []
    finally:
        claims.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await service.close()


async def test_model_prepare_cold_key_rejects_changed_call_or_parameters(
    runtime, tmp_path, monkeypatch
):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        ctx, plan = await prepare(service, agent, provider)
        ctx = replace(ctx, call_id="prepare")

        def forbidden(*_args, **_kwargs):
            raise AssertionError("原模型key冷读不能重新观察/准备根")

        monkeypatch.setattr(WorkspaceDirectoryWriter, "prepare_artifact", forbidden)
        for _ in range(2):
            result = await ctx.registry.invoke(
                "workspace_tree_model_prepare", provider.request.model_dump(mode="json"), ctx
            )
            assert not result.is_error and result.data == plan.model_dump(mode="json")
        changed = provider.request.model_copy(update={"accept_failed": True})
        conflict = await ctx.registry.invoke(
            "workspace_tree_model_prepare", changed.model_dump(mode="json"), ctx
        )
        assert conflict.is_error and conflict.code == "workspace_tree_idempotency_conflict"
        conflict = await ctx.registry.invoke(
            "workspace_tree_model_prepare",
            provider.request.model_dump(mode="json"),
            replace(ctx, call_id="other-prepare"),
        )
        assert conflict.is_error and conflict.code == "workspace_tree_model_source_conflict"
        assert await source_count(runtime, "prepare") == 1
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
    finally:
        await service.close()


async def test_disabled_execution_keeps_prepared_model_tree_unclaimed(runtime, tmp_path):
    service, agent, provider = await setup(runtime, tmp_path)
    try:
        ctx, plan = await prepare(service, agent, provider)
        service.disable_execution("sandbox")
        result = await ctx.registry.invoke("workspace_tree_start", {"plan_id": plan.plan_id}, ctx)
        assert result.is_error and result.code == "execution_unavailable"
        assert (await agent.directory_tree(plan.plan_id)).status == "planned"
        assert await source_count(runtime, "start") == 0
        assert await counts(runtime.repository.database) == [1, 1, 0, 0, 0, 0]
    finally:
        await service.close()
