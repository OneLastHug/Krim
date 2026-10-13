"""当前只读/深度/原身份/事务/资源和外部close边界实际拒绝。"""

import asyncio
from threading import Event

import pytest
from test_tree_exchange_tools import call, context, counts, preview, setup
from test_workspace_changes import wait_event
from test_workspace_tree_exchange_ledger import action

from krim_agent.tools import ToolError
from krim_agent.workspace_mixed_batch import MixedBatchFence
from krim_agent.workspace_tree import TreePlanFence
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter


async def test_readonly_inspection_never_grants_direct_backend_preview(runtime, tmp_path):
    service, agent, _ = await setup(runtime, tmp_path, writable=False)
    try:
        left = await call(service, agent, "tree_exchange_inspect", {"path": "holder/live"})
        right = await call(service, agent, "tree_exchange_inspect", {"path": "new-parent/prepared"})
        ctx = await context(service, agent)
        with pytest.raises(ToolError) as refused:
            await ctx.tree_exchanges.tool_operation(
                "preview",
                {
                    "left_path": left["path"],
                    "right_path": right["path"],
                    "expected_left": left["fingerprint"],
                    "expected_right": right["fingerprint"],
                    "request_key": "forbidden",
                },
                ctx,
            )
        assert refused.value.code == "workspace_read_only"
        assert await counts(service.workspace_edits.database) == [0, 0, 0, 0, 0, 0]
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault", ["source_call", "tree_fence", "mixed", "agent", "origin", "depth", "disabled"]
)
async def test_current_origin_scope_transactions_depth_and_resource_fail_closed(
    runtime, tmp_path, fault
):
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        ctx = await context(service, agent)
        if fault == "source_call":
            ctx.source_call_id = "other"
        elif fault == "tree_fence":
            ctx.tree_fence = TreePlanFence("plan", 0, "file", "apply", "token")
        elif fault == "mixed":
            ctx.mixed_batch = MixedBatchFence("batch", 0, "token", "file")
        elif fault == "agent":
            ctx.agent_id = "other"
        elif fault == "origin":
            ctx.origin = "model"
        elif fault == "depth":
            ctx.depth = ctx.registry.max_depth
        else:
            service.disable_workspace("files")
        with pytest.raises(ToolError):
            await ctx.registry.call_tool("tree_exchange_inspect", {"path": "holder/live"}, ctx)
        assert await counts(service.workspace_edits.database) == [0, 0, 0, 0, 0, 0]
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"
    finally:
        await service.close()


async def test_external_service_close_waits_admitted_writer_original_receipt(
    runtime, tmp_path, monkeypatch
):
    service, agent, _ = await setup(runtime, tmp_path)
    entered, release = Event(), Event()
    original = WorkspaceTreeExchangeWriter.apply
    worker = closing = None
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
        exchange = service.manager.tree_exchanges
        closing = asyncio.create_task(exchange.close())
        await asyncio.sleep(0.05)
        assert not closing.done() and exchange._closed
        release.set()
        result = await worker
        await closing
        assert result["status"] == "rejected" and not result["receipt"]["changed"]
        assert not exchange._active and not exchange._operations
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\0\xff\x80"
    finally:
        release.set()
        await asyncio.gather(
            *(task for task in (worker, closing) if task is not None), return_exceptions=True
        )
        await service.close()
