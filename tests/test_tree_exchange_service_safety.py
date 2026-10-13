"""原双树同调用取消/超时/当前Stop/self-close/unknown零重放。"""

import asyncio
import json
from dataclasses import replace
from threading import Event

import pytest
from sqlalchemy import update
from test_tree_exchange_tools import call, context, counts, preview, setup
from test_workspace_changes import wait_event
from test_workspace_tree_exchange_ledger import action

import krim_agent.workspace_tree_exchange as physical
from krim_agent.persistence.database import RunRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolError
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_exchange_types import TreeExchangePreviewView


@pytest.mark.parametrize("phase", ["before", "after", "timeout"])
async def test_repeated_cancel_or_timeout_waits_original_admitted_writer_and_receipt(
    runtime, tmp_path, monkeypatch, phase
):
    service, agent, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeExchangeWriter.apply
    task = None
    try:
        proposal, _ = await preview(service, agent)
        request = action(proposal)

        def paused(writer, prepared, **kwargs):
            effect = original(writer, prepared, **kwargs) if phase == "after" else None
            entered.set()
            assert release.wait(10)
            return effect if effect is not None else original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", paused)
        ctx = await context(service, agent)
        budgets = []
        if phase == "timeout":

            async def record_budget(_definition, child):
                budgets.append(child.execution_timeouts[-1].deadline)

            ctx.registry.add_global_pre(record_budget)
        task = asyncio.create_task(
            ctx.registry.call_tool("tree_exchange_change", request.model_dump(), ctx)
        )
        await wait_event(entered)
        if phase == "timeout":
            # 明确从真实Writer已接纳后注入预算到期，不靠SQL准入抢跑短时限。
            budgets[0].reschedule(asyncio.get_running_loop().time() + 0.05)
            await asyncio.sleep(0.1)
        else:
            task.cancel()
            await asyncio.sleep(0.02)
            task.cancel()
        assert not task.done(), "已接纳线程和实际回执收束前不能结束取消"
        release.set()
        if phase == "timeout":
            with pytest.raises(ToolError) as refused:
                await task
            assert refused.value.code == "timeout"
        else:
            with pytest.raises(asyncio.CancelledError):
                await task
        store = service.manager.tree_exchanges.store
        saved = await store.operation_by_key("session:" + agent.session_id, request.request_key)
        assert saved.view.status == ("done" if phase == "after" else "rejected")
        assert saved.view.receipt.changed == (phase == "after")
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == (
            b"\x80\x00new" if phase == "after" else b"\0\xff\x80"
        )
        assert await counts(store.database) == [1, 1, 0, 0, 0, 0]
        assert not service.manager.tree_exchanges._active
        assert not service.manager.tree_exchanges._operations
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_public_cancel_closes_original_admission_and_waits_same_active_operation(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeExchangeWriter.apply
    worker = cancelling = None
    try:
        proposal, _ = await preview(service, agent)
        request = action(proposal)

        def paused(writer, prepared, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", paused)
        worker = asyncio.create_task(
            call(service, agent, "tree_exchange_change", request.model_dump())
        )
        await wait_event(entered)
        exchange, scope = service.manager.tree_exchanges, "session:" + agent.session_id
        persisted = asyncio.Event()
        original_cancel = exchange.store.cancel

        async def noted(bound_scope, identifier):
            value = await original_cancel(bound_scope, identifier)
            persisted.set()
            return value

        monkeypatch.setattr(exchange.store, "cancel", noted)
        cancelling = asyncio.create_task(
            service.tree_exchange_cancel(agent.agent_id, proposal.proposal_id)
        )
        async with asyncio.timeout(5):
            await persisted.wait()
        assert (await exchange.store.operation_by_key(scope, request.request_key)).view.cancelled
        assert not cancelling.done()
        release.set()
        result, cancelled = await asyncio.gather(worker, cancelling)
        assert result["status"] == "rejected" and result["cancelled"]
        assert cancelled.status == "cancelled"
        assert await call(service, agent, "tree_exchange_change", request.model_dump()) == result
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, cancelling) if task is not None), return_exceptions=True
        )
        await service.close()


@pytest.mark.parametrize("kind", ["apply", "keep"])
async def test_receipt_save_failure_keeps_original_operation_and_reconciles_zero_replay(
    runtime, tmp_path, monkeypatch, kind
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        exchange, scope = service.manager.tree_exchanges, "session:" + agent.session_id
        if kind == "keep":
            assert (
                await call(service, agent, "tree_exchange_change", action(proposal).model_dump())
            )["status"] == "done"
            proposal = (await exchange.store.proposal(scope, proposal.proposal_id)).view
        original_settle = exchange.store.settle

        async def unavailable(*_args, **_kwargs):
            raise OSError("原SQL回执保存中断")

        monkeypatch.setattr(exchange.store, "settle", unavailable)
        request = action(proposal, kind, kind)
        unknown = await call(service, agent, "tree_exchange_change", request.model_dump())
        assert unknown["status"] == "uncertain"
        saved = await exchange.store.operation(scope, unknown["operation_id"])
        assert (
            saved.prepared == (await exchange.store.proposal(scope, proposal.proposal_id)).prepared
        )
        assert await counts(exchange.store.database) == [1, 2 if kind == "keep" else 1, 10, 0, 0, 0]
        monkeypatch.setattr(exchange.store, "settle", original_settle)

        def forbidden(*_args, **_kwargs):
            pytest.fail("unknown核实不得重发原交换/prepare/Provider")

        monkeypatch.setattr(physical, "rename_at", forbidden)
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", forbidden)
        resolved = await call(
            service, agent, "tree_exchange_reconcile", {"operation_id": saved.view.operation_id}
        )
        assert resolved["status"] == "done" and resolved["operation_id"] == unknown["operation_id"]
        assert resolved["receipt"]["changed"] == (kind == "apply")
        assert await call(service, agent, "tree_exchange_change", request.model_dump()) == resolved
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\x80\x00new"
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["pre", "post"])
@pytest.mark.parametrize("target", ["service", "manager", "runtime", "exchange", "sdk"])
async def test_self_close_in_registry_callbacks_refuses_before_state_or_resources_release(
    runtime, tmp_path, phase, target
):
    service, agent, _ = await setup(runtime, tmp_path)
    ai = PythonAI(runtime)
    ai._agent_service = service
    try:
        ctx = await context(service, agent)
        caught = []

        async def close():
            selected = {
                "service": service,
                "manager": service.manager,
                "runtime": service._handle(agent.agent_id).runtime,
                "exchange": service.manager.tree_exchanges,
                "sdk": ai,
            }[target]
            try:
                await selected.close()
            except ServiceError as error:
                caught.append(error.code)

        async def before(_definition, _child):
            await close()

        async def after(_definition, value, _child):
            await close()
            return value

        if phase == "pre":
            ctx.registry.add_global_pre(before)
        else:
            ctx.registry.add_global_post(after)
        result = await ctx.registry.call_tool("tree_exchange_inspect", {"path": "holder/live"}, ctx)
        assert result["found"] and caught == ["workspace_tree_exchange_self_close"]
        assert not ai._closed and not service._closed and not service.manager._closed
        assert service.resources.workspace("files").available
    finally:
        await service.close()


async def test_original_model_run_stop_before_last_writer_permission_refuses_namespace(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        ctx = await context(service, agent, model=True, call_id="original-model")
        # 原preview也必须属于此Model调用，不能把host提案提升为Model来源。
        left = proposal.left_before_fingerprint
        right = proposal.right_before_fingerprint
        model = await ctx.registry.call_tool(
            "tree_exchange_preview",
            {
                "left_path": "holder/live",
                "right_path": "new-parent/prepared",
                "expected_left": left,
                "expected_right": right,
                "request_key": "model-preview",
            },
            ctx,
        )
        ctx = replace(ctx, call_id="model-apply")
        original = WorkspaceTreeExchangeWriter.apply
        loop = asyncio.get_running_loop()

        async def stopped():
            async with service.workspace_edits.database.sessions.begin() as sql:
                await sql.execute(
                    update(RunRow).where(RunRow.id == ctx.run_id).values(stop_requested=True)
                )

        def stop_then_apply(writer, prepared, **kwargs):
            asyncio.run_coroutine_threadsafe(stopped(), loop).result(timeout=5)
            return original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeExchangeWriter, "apply", stop_then_apply)
        result = await ctx.registry.call_tool(
            "tree_exchange_change",
            action(TreeExchangePreviewView.model_validate_json(json.dumps(model))).model_dump(),
            ctx,
        )
        assert result["status"] == "rejected" and not result["receipt"]["changed"]
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"
    finally:
        await service.close()
