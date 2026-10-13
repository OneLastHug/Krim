"""原树Writer已接纳、取消/超时/自关闭与unknown同operation核实。"""

import asyncio
from dataclasses import replace
from threading import Event
from types import SimpleNamespace

import pytest
from test_tree_move_tools import call, context, preview, setup
from test_workspace_changes import wait_event
from test_workspace_tree_move_ledger import action, counts

from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import PythonAI
from krim_agent.tools import ToolError
from krim_agent.workspace_tree_move import TreeMovePreviewRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


@pytest.mark.parametrize("phase", ["before", "after", "timeout"])
async def test_repeated_cancel_and_timeout_wait_admitted_thread_real_receipt(
    runtime, tmp_path, monkeypatch, phase
):
    service, agent, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeWriter.apply
    task = None
    try:
        proposal, _ = await preview(service, agent)

        def paused(writer, prepared, **kwargs):
            effect = original(writer, prepared, **kwargs) if phase == "after" else None
            entered.set()
            assert release.wait(10)
            return effect if effect is not None else original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", paused)
        ctx = await context(service, agent)
        if phase == "timeout":
            ctx.timeout = 0.15
        request = action(proposal)
        task = asyncio.create_task(
            ctx.registry.call_tool("tree_move_change", request.model_dump(), ctx)
        )
        await wait_event(entered)
        if phase == "timeout":
            await asyncio.sleep(0.2)
        else:
            task.cancel()
            await asyncio.sleep(0.02)
            task.cancel()
        assert not task.done(), "取消/超时返回前必须等待已接纳线程收束"
        release.set()
        if phase == "timeout":
            with pytest.raises(ToolError) as error:
                await task
            assert error.value.code == "timeout"
        else:
            with pytest.raises(asyncio.CancelledError):
                await task
        store = service.manager.tree_moves.store
        saved = await store.operation_by_key("session:" + agent.session_id, request.request_key)
        assert saved.view.status == ("done" if phase == "after" else "rejected")
        assert saved.view.receipt.changed == (phase == "after")
        assert (tmp_path / "dest/tree").exists() == (phase == "after")
        assert await counts(store.database) == [1, 1, 0, 0]
        assert not service.manager.tree_moves._operations and not service.manager.tree_moves._active
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_public_cancel_closes_original_admission_and_waits_original_worker(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeWriter.apply
    worker = cancelling = None
    try:
        proposal, _ = await preview(service, agent)
        request = action(proposal)

        def paused(writer, prepared, **kwargs):
            entered.set()
            assert release.wait(10)
            return original(writer, prepared, **kwargs)

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", paused)
        worker = asyncio.create_task(call(service, agent, "tree_move_change", request.model_dump()))
        await wait_event(entered)
        mover, scope = service.manager.tree_moves, "session:" + agent.session_id
        saved_cancel = asyncio.Event()
        original_cancel = mover.store.cancel

        async def notified_cancel(bound_scope, identifier):
            result = await original_cancel(bound_scope, identifier)
            saved_cancel.set()
            return result

        monkeypatch.setattr(mover.store, "cancel", notified_cancel)
        cancelling = asyncio.create_task(
            service.tree_move_cancel(agent.agent_id, proposal.proposal_id)
        )
        async with asyncio.timeout(5):
            await saved_cancel.wait()
        assert (await mover.store.operation_by_key(scope, request.request_key)).view.cancelled
        assert not cancelling.done()
        release.set()
        result = await worker
        cancelled = await cancelling
        assert result["status"] == "rejected" and result["cancelled"]
        assert cancelled.status == "cancelled"
        assert await call(service, agent, "tree_move_change", request.model_dump()) == result
        assert await counts(mover.store.database) == [1, 1, 0, 0]
        assert (tmp_path / "tree").exists() and not (tmp_path / "dest/tree").exists()
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, cancelling) if task is not None), return_exceptions=True
        )
        await service.close()


@pytest.mark.parametrize("kind", ["apply", "keep"])
async def test_receipt_save_failure_retains_original_material_and_reconciles_zero_replay(
    runtime, tmp_path, monkeypatch, kind
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        mover, scope = service.manager.tree_moves, "session:" + agent.session_id
        if kind == "keep":
            assert (await call(service, agent, "tree_move_change", action(proposal).model_dump()))[
                "status"
            ] == "done"
            proposal = (await mover.store.proposal(scope, proposal.proposal_id)).view
        original_settle = mover.store.settle

        async def unavailable(*_args, **_kwargs):
            raise OSError("合成回执保存中断")

        monkeypatch.setattr(mover.store, "settle", unavailable)
        request = action(proposal, kind, "save-failure")
        unknown = await call(service, agent, "tree_move_change", request.model_dump())
        assert unknown["status"] == "uncertain" and unknown["recovery_required"]
        assert unknown["receipt"] is None
        assert (tmp_path / "dest/tree/sub/data.bin").read_bytes() == b"\0\xff\x80"
        saved = await mover.store.operation(scope, unknown["operation_id"])
        assert saved.prepared.before.root_path == "tree"
        assert saved.intent_recorded == (kind == "apply")
        monkeypatch.setattr(mover.store, "settle", original_settle)

        def forbidden(*_args, **_kwargs):
            pytest.fail("unknown核实重发原树namespace")

        monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        reconciled = await call(
            service, agent, "tree_move_reconcile", {"operation_id": saved.view.operation_id}
        )
        assert (
            reconciled["operation_id"] == unknown["operation_id"] and reconciled["status"] == "done"
        )
        assert reconciled["receipt"]["changed"] == (kind == "apply")
        assert await call(service, agent, "tree_move_change", request.model_dump()) == reconciled
        assert (
            await call(
                service, agent, "tree_move_reconcile", {"operation_id": saved.view.operation_id}
            )
            == reconciled
        )
        assert await counts(mover.store.database) == [1, 2 if kind == "keep" else 1, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["pre", "post"])
@pytest.mark.parametrize("target", ["service", "manager", "runtime", "sdk"])
async def test_self_close_before_sdk_database_release_and_original_hook_can_finish(
    runtime, tmp_path, phase, target
):
    service, agent, _ = await setup(runtime, tmp_path)
    ai = PythonAI(runtime)
    ai._agent_service = service
    caught = []
    try:
        handle = service._handle(agent.agent_id)

        async def close_self():
            closer = {
                "service": service.close,
                "manager": service.manager.close,
                "runtime": handle.runtime.close,
                "sdk": ai.close,
            }[target]
            with pytest.raises(ServiceError) as error:
                await closer()
            caught.append(error.value.code)
            assert not ai._closed and not service._closed and not service.manager._closed
            assert (await agent.state()).status == "open"

        async def pre(_definition, _child):
            if phase == "pre":
                await close_self()

        async def post(_definition, value, _child):
            if phase == "post":
                await close_self()
            return value

        handle.runtime.tools.add_global_pre(pre)
        handle.runtime.tools.add_global_post(post)
        result = await call(service, agent, "tree_move_inspect", {"path": "tree"})
        assert result["found"] and caught == ["workspace_tree_move_self_close"]
    finally:
        await ai.close()


async def test_internal_cancel_in_last_authority_never_waits_itself_or_publishes(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        mover, scope = service.manager.tree_moves, "session:" + agent.session_id
        original = mover.store.assert_execution
        called = 0

        async def cancel_from_authority(bound_scope, identifier):
            nonlocal called
            called += 1
            await mover.cancel(bound_scope, proposal.proposal_id)
            await original(bound_scope, identifier)

        monkeypatch.setattr(mover.store, "assert_execution", cancel_from_authority)
        result = await call(service, agent, "tree_move_change", action(proposal).model_dump())
        assert called == 1 and result["status"] == "rejected" and result["cancelled"]
        assert (await mover.store.proposal(scope, proposal.proposal_id)).view.status == "cancelled"
        assert (tmp_path / "tree").exists() and not (tmp_path / "dest/tree").exists()
    finally:
        await service.close()


async def test_query_cold_closed_session_and_disabled_resource_but_model_run_is_required(
    runtime, tmp_path
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        ctx = await context(service, agent, model=True, call_id="live-query")
        session = await agent.state()
        await agent.close(expected_revision=session.revision)
        service.disable_workspace("files")
        query = {"kind": "proposal", "identifier": proposal.proposal_id}
        assert (await call(service, agent, "tree_move_query", query))["proposal"][
            "proposal_id"
        ] == proposal.proposal_id
        with pytest.raises(ToolError) as error:
            await ctx.registry.call_tool("tree_move_query", query, ctx)
        assert error.value.code in {"agent_session_closed", "workspace_unavailable"}
        assert await counts(service.manager.tree_moves.store.database) == [1, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize(
    "field",
    [
        "tree_fence",
        "mixed_batch",
        "workspace_batch",
        "checkpoint_capture",
        "checkpoint_restore",
        "command_capture",
        "source_call_id",
    ],
)
async def test_other_transaction_context_cannot_open_another_tree(runtime, tmp_path, field):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        value = (
            SimpleNamespace(source="file", phase="change")
            if field in {"tree_fence", "mixed_batch"}
            else object()
        )
        ctx = replace(await context(service, agent), **{field: value})
        with pytest.raises(ToolError) as error:
            await ctx.registry.call_tool("tree_move_inspect", {"path": "tree"}, ctx)
        assert error.value.code in {
            "workspace_tree_action",
            "workspace_mixed_batch_action",
            "workspace_tree_move_context",
        }
        assert await counts(service.manager.tree_moves.store.database) == [0, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["deny", "ask", "stopped_run", "disabled", "depth"])
async def test_original_authority_rejection_never_creates_new_attempt(runtime, tmp_path, fault):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        ctx = await context(service, agent, model=True, call_id="original-apply")
        if fault in {"deny", "ask"}:

            async def policy(_definition, _context):
                return fault

            ctx.policy = policy
        elif fault == "stopped_run":
            await runtime.repository.request_stop(ctx.run_id)
        elif fault == "disabled":
            service.disable_workspace("files")
        else:
            ctx.depth = ctx.registry.max_depth
        with pytest.raises(ToolError) as error:
            await ctx.registry.call_tool("tree_move_change", action(proposal).model_dump(), ctx)
        assert (
            error.value.code
            == {
                "deny": "permission_denied",
                "ask": "approval_required",
                "stopped_run": "workspace_run_closed",
                "disabled": "workspace_unavailable",
                "depth": "max_depth",
            }[fault]
        )
        assert await counts(service.manager.tree_moves.store.database) == [1, 0, 0, 0]
        assert (tmp_path / "tree").exists() and not (tmp_path / "dest/tree").exists()
    finally:
        await service.close()


async def test_true_model_cold_source_retry_rejects_new_call_before_scan(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        ctx = await context(service, agent, model=True, call_id="original-preview")
        inspected = await ctx.registry.call_tool("tree_move_inspect", {"path": "tree"}, ctx)
        request = TreeMovePreviewRequest(
            source_path="tree",
            target_path="dest/tree",
            expected_fingerprint=inspected["fingerprint"],
            request_key="model-key",
        )
        proposal = await ctx.registry.call_tool("tree_move_preview", request.model_dump(), ctx)

        def forbidden(*_args, **_kwargs):
            pytest.fail("模型原键再次扫树")

        monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
        assert (
            await ctx.registry.call_tool("tree_move_preview", request.model_dump(), ctx) == proposal
        )
        other_call = replace(ctx, call_id="other-preview")
        with pytest.raises(ToolError) as error:
            await ctx.registry.call_tool("tree_move_preview", request.model_dump(), other_call)
        assert error.value.code == "workspace_tree_move_source_conflict"
        assert await counts(service.manager.tree_moves.store.database) == [1, 0, 0, 1]
    finally:
        await service.close()


async def test_list_and_history_project_expiry_and_reject_foreign_proposal_cursor(
    runtime, tmp_path
):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        proposal, _ = await preview(service, agent)
        done = await call(service, agent, "tree_move_change", action(proposal).model_dump())
        inspected = await call(service, agent, "tree_move_inspect", {"path": "dest/tree"})
        second_request = TreeMovePreviewRequest(
            source_path="dest/tree",
            target_path="back",
            expected_fingerprint=inspected["fingerprint"],
            request_key="second-preview",
        )
        second = await service.tree_move_preview(agent.agent_id, second_request)
        mover, scope = service.manager.tree_moves, "session:" + agent.session_id
        mover.store.lease_seconds = 0.2
        material = (await mover.store.proposal(scope, second.proposal_id)).prepared
        attempt, created = await mover.store.reserve(
            scope, action(second, key="expiring"), binding.fingerprint, material
        )
        assert created
        await asyncio.sleep(0.22)
        page = await call(service, agent, "tree_move_query", {"kind": "proposals"})
        assert (
            next(
                entry
                for entry in page["proposals"]["items"]
                if entry["proposal_id"] == second.proposal_id
            )["status"]
            == "uncertain"
        )
        key = await call(
            service,
            agent,
            "tree_move_query",
            {"kind": "operation_by_key", "request_key": "expiring"},
        )
        assert (
            key["operation"]["status"] == "uncertain"
            and key["operation"]["operation_id"] == attempt.view.operation_id
        )
        with pytest.raises(ServiceError) as error:
            await call(
                service,
                agent,
                "tree_move_query",
                {
                    "kind": "history",
                    "identifier": second.proposal_id,
                    "after": done["operation_id"],
                },
            )
        assert error.value.code == "workspace_tree_move_page"
        assert (await counts(mover.store.database))[1] == 2
        assert (tmp_path / "dest/tree").exists() and not (tmp_path / "back").exists()
    finally:
        await service.close()
