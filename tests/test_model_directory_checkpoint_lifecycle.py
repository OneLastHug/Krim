"""原模型控制pre/post自关闭、Run stop、独立总预算与物理线程收束。"""

import asyncio
from threading import Event

import pytest
from test_directory_checkpoint_capture import make_tree
from test_model_directory_checkpoints import (
    CANCEL,
    CREATE,
    LEAF,
    OneShotProvider,
    setup,
    source_count,
)

from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import DirectoryCheckpointCreate
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


@pytest.mark.parametrize(
    "scope,phase,target",
    [
        pytest.param(scope, phase, target, id=f"{scope}_{phase}_{target}")
        for scope in ("root", "leaf")
        for phase in ("pre", "post")
        for target in ("service", "runtime", "directory")
    ],
)
async def test_control_callbacks_cannot_close_original_services(
    runtime, tmp_path, scope, phase, target
):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    handle = service._handle(agent.agent_id)
    called = []

    async def close():
        selected = (
            service
            if target == "service"
            else handle.runtime
            if target == "runtime"
            else service.directory_checkpoints
        )
        with pytest.raises(ServiceError) as rejected:
            await selected.close()
        assert rejected.value.code == "workspace_checkpoint_model_reentrant"
        called.append(target)

    async def pre(definition, _context):
        if phase == "pre" and definition.name == (CREATE if scope == "root" else LEAF):
            await close()

    async def post(definition, value, _context):
        if phase == "post" and definition.name == (CREATE if scope == "root" else LEAF):
            await close()
        return value

    handle.runtime.tools.add_global_pre(pre)
    handle.runtime.tools.add_global_post(post)
    try:
        result = await agent.execute("目录自关闭边界", idempotency_key="self-close")
        final = await agent.directory_checkpoint_by_key("one-directory")
        assert result.status == "completed" and final is not None and final.status == "available", (
            result,
            final,
        )
        assert called == [target]
        assert (
            not service._closed
            and not handle.runtime._closed
            and not service.directory_checkpoints._closed
        )
        assert (await agent.state()).status == "open"
    finally:
        await service.close()


async def test_stop_closes_model_capture_and_drains_original_worker(runtime, tmp_path):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    entered = asyncio.Event()

    async def hold(definition, _context):
        if definition.name == LEAF:
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    task = asyncio.create_task(agent.execute("停止原目录捕获", idempotency_key="stop"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await agent.stop()
        result = await asyncio.wait_for(task, 5)
        final = await agent.directory_checkpoint_by_key("one-directory")
        assert result.status == "aborted" and final.status == "cancelled", (result, final)
        assert final.snapshot is None and await source_count(runtime) == 1
        assert not service.directory_checkpoints._tasks and not service.model_checkpoints._calls
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_agent_whole_round_deadline_is_independent_of_tool_budget(runtime, tmp_path):
    make_tree(tmp_path)
    service, agent = await setup(
        runtime, tmp_path, OneShotProvider(), tool_timeout=10, round_timeout=2
    )
    entered = []

    async def hold(definition, context):
        if definition.name == LEAF:
            entered.append(context.timeout)
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    try:
        result = await asyncio.wait_for(
            agent.execute("总轮次预算独立", idempotency_key="whole-round"), 6
        )
        final = await agent.directory_checkpoint_by_key("one-directory")
        assert entered == [10] and result.status == "aborted" and result.code == "timeout", result
        assert final.status == "cancelled" and final.snapshot is None
        assert not service.directory_checkpoints._tasks and not service.model_checkpoints._calls
    finally:
        await service.close()


async def test_original_tool_deadline_cancels_and_drains_physical_thread(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider(), tool_timeout=1)
    entered, exited = Event(), Event()

    def wait_for_cancel(_self, _root, *, authorize, cancelled):
        entered.set()
        try:
            while not cancelled():
                authorize()
        finally:
            exited.set()
        raise ServiceError("workspace_tree_snapshot_cancelled", "测试协作收束", 409)

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", wait_for_cancel)
    try:
        result = await asyncio.wait_for(
            agent.execute("原工具截止线程收束", idempotency_key="thread-timeout"), 6
        )
        final = await agent.directory_checkpoint_by_key("one-directory")
        assert entered.is_set() and exited.is_set()
        assert result.status == "completed" and final.status == "cancelled", (result, final)
        assert final.snapshot is None and not service.directory_checkpoints._tasks
    finally:
        await service.close()


async def test_model_cancel_drains_original_host_capture_without_new_source(runtime, tmp_path):
    make_tree(tmp_path)
    provider = OneShotProvider(CANCEL, {"request_key": "host-pending"})
    service, agent = await setup(runtime, tmp_path, provider)
    entered = asyncio.Event()

    async def hold(definition, context):
        if definition.name == LEAF and context.origin == "host":
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    try:
        original = await agent.create_directory_checkpoint(
            DirectoryCheckpointCreate(root_path="tree", request_key="host-pending")
        )
        await asyncio.wait_for(entered.wait(), 5)
        result = await agent.execute("取消原宿主目录捕获", idempotency_key="cancel-host")
        final = await agent.directory_checkpoint(original.checkpoint_id)
        assert result.status == "completed" and final.status == "cancelled", (result, final)
        assert await source_count(runtime) == 0 and not service.directory_checkpoints._tasks
    finally:
        await service.close()


async def test_late_host_cancel_of_available_capture_does_not_cancel_model_run(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    entered, release = asyncio.Event(), asyncio.Event()

    async def renewal(scope, fence, worker, invocation=None, agent_id=None):
        # 先经真实Store续原租约，再在受控取消清理窗口挂起，精确暴露finally竞争。
        await service.directory_checkpoint_store.renew(scope, fence, authorize=invocation.qualify)
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            entered.set()
            await release.wait()
            raise

    monkeypatch.setattr(service.directory_checkpoints, "_renew", renewal)
    task = asyncio.create_task(agent.execute("已完成目录的迟到取消", idempotency_key="late-cancel"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        original = await agent.directory_checkpoint_by_key("one-directory")
        assert original.status == "available"
        final = await asyncio.wait_for(agent.cancel_directory_checkpoint(original.checkpoint_id), 5)
        assert final == original
        result = await asyncio.wait_for(task, 5)
        assert result.status == "completed", result
        assert (await agent.directory_checkpoint(original.checkpoint_id)) == original
        assert not service.directory_checkpoints._tasks and not service.model_checkpoints._calls
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_completed_capture_can_finish_receipt_after_next_renewal_tick(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    original = service.directory_checkpoint_store.finish
    receipt = []

    async def delayed(*args, **kwargs):
        await original(*args, **kwargs)
        await asyncio.sleep(0.3)
        receipt.append(True)

    monkeypatch.setattr(service.directory_checkpoint_store, "finish", delayed)
    try:
        result = await agent.execute("已完成捕获的原回执", idempotency_key="completed-receipt")
        final = await agent.directory_checkpoint_by_key("one-directory")
        assert result.status == "completed" and final.status == "available", (result, final)
        assert receipt == [True]
        audits = await runtime.repository.audits(result.run_id)
        assert len(audits) == 2 and all(audit.ok for audit in audits), audits
    finally:
        await service.close()
