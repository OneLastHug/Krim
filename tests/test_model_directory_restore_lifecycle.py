"""原模型停止/超时/自关闭、准入丢回程与unknown核实沿同一持久操作。"""

import asyncio
import json
from threading import Event

import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session
from test_model_directory_restores import CANCEL, PREVIEW, START, VERIFY, phase_setup

import krim_agent.workspace_directory_restore_writer as physical
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_restore import DirectoryRestorePreview
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


@pytest.mark.parametrize(
    "target,phase",
    [(target, phase) for target in ("root", "leaf") for phase in ("policy", "pre", "post")],
)
async def test_all_outer_callbacks_refuse_own_close_before_lifecycle_changes(
    runtime, tmp_path, target, phase
):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, "start")
    handle = service._handle(agent.agent_id)
    selected = START if target == "root" else DIRECTORY_RESTORE_STEP_TOOLS["execute"]
    observed = []

    async def reject_close():
        for name, close in (
            ("service", service.close),
            ("runtime", handle.runtime.close),
            ("directory", service.directory_restores.close),
            ("stop", agent.stop),
            ("session", agent.close),
            ("destroy", agent.destroy),
        ):
            with pytest.raises(ServiceError) as denied:
                await close()
            assert denied.value.code == "workspace_checkpoint_model_reentrant"
            observed.append(name)

    async def policy(definition, _context):
        if phase == "policy" and definition.name == selected:
            await reject_close()
        return "allow"

    async def pre(definition, _context):
        if phase == "pre" and definition.name == selected:
            await reject_close()

    async def post(definition, value, _context):
        if phase == "post" and definition.name == selected:
            await reject_close()
        return value

    handle.runtime.tool_policy = policy
    handle.runtime.tools.add_global_pre(pre)
    handle.runtime.tools.add_global_post(post)
    try:
        result = await agent.execute("目录恢复自关闭拒绝", idempotency_key="self-close")
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "completed" and final.phase == "finished", (
            result,
            final,
            provider.reply,
        )
        assert observed == ["service", "runtime", "directory", "stop", "session", "destroy"]
        assert (
            not service._closed
            and not handle.runtime._closed
            and not service.directory_restores._closed
        )
        assert not handle.destroyed and (await agent.state()).status == "open"
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_original_run_stop_closes_own_stage_and_drains_workers(runtime, tmp_path, action):
    service, agent, binding, _, _ = await phase_setup(runtime, tmp_path, action)
    entered = asyncio.Event()

    async def hold(definition, _context):
        if (
            definition.name
            == DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]
        ):
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    task = asyncio.create_task(agent.execute("原Run停止", idempotency_key="stop"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await agent.stop()
        result = await asyncio.wait_for(task, 5)
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "aborted" and final.phase == "cancelled", (result, final)
        assert not final.cleanup_pending
        assert not service.directory_restores._tasks and not service.directory_restores._fences
        assert not service.directory_restores._pending and not service.model_checkpoints._calls
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_whole_round_deadline_cancels_without_renewing_original_tool_budget(
    runtime, tmp_path, action
):
    service, agent, binding, _, _ = await phase_setup(runtime, tmp_path, action, round_timeout=2)
    entered = []

    async def hold(definition, context):
        if (
            definition.name
            == DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]
        ):
            entered.append(context.timeout)
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    try:
        result = await asyncio.wait_for(
            agent.execute("原整轮含排队预算", idempotency_key="round-timeout"), 6
        )
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "aborted" and result.code == "timeout" and entered == [10], result
        assert final.phase == "cancelled" and not service.directory_restores._tasks
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_tool_timeout_waits_for_real_thread_after_repeated_cancellation(
    runtime, tmp_path, monkeypatch, action
):
    service, agent, binding, _, _ = await phase_setup(runtime, tmp_path, action, tool_timeout=1)
    entered, release, exited = Event(), Event(), Event()
    cls = WorkspaceTreeSnapshotReader if action == "preview" else WorkspaceDirectoryRestoreWriter
    method = "capture" if action == "preview" else "prepare"
    original = getattr(cls, method)

    def hold(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(10), "测试主动释放原IO线程"
            return original(*args, **kwargs)
        finally:
            exited.set()

    monkeypatch.setattr(cls, method, hold)
    task = asyncio.create_task(agent.execute("原工具截止线程收束", idempotency_key="tool-timeout"))
    try:
        assert await asyncio.to_thread(entered.wait, 4)
        await asyncio.sleep(1.15)
        assert not task.done() and not exited.is_set(), "原物理线程退出前不得返回"
        workers = list(service.directory_restores._tasks.values())
        for _, worker in workers:
            worker.cancel()
            worker.cancel()
        release.set()
        result = await asyncio.wait_for(task, 6)
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert exited.is_set() and result.status == "completed", result
        assert final.phase in {"cancelled", "failed"}, final
        assert not service.directory_restores._tasks and not service.directory_restores._callbacks
        assert (
            not service.directory_restores._invocations and not service.directory_restores._pending
        )
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("action,point", [("preview", "reserve"), ("start", "start")])
async def test_committed_admission_lost_reply_closes_only_original_request(
    runtime, tmp_path, monkeypatch, action, point
):
    service, agent, binding, provider, _ = await phase_setup(runtime, tmp_path, action)
    original = getattr(service.directory_restore_store, point)
    admitted = []

    async def lost(*args, **kwargs):
        def after_commit(_session):
            admitted.append(True)
            raise OSError("原SQL提交已成功，准入回程丢失")

        event.listen(Session, "after_commit", after_commit)
        try:
            return await original(*args, **kwargs)
        finally:
            event.remove(Session, "after_commit", after_commit)

    monkeypatch.setattr(service.directory_restore_store, point, lost)
    try:
        result = await agent.execute("准入丢回程", idempotency_key="admission-lost")
        assert result.status == "completed" and len(admitted) == 1, (result, provider.reply)
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert final.phase == "cancelled" and not final.cleanup_pending, final
        assert not service.directory_restores._tasks and not service.directory_restores._invocations
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    finally:
        await service.close()


async def unknown(service, agent, provider, monkeypatch):
    rename, calls = physical.rename_at, []

    def lost(*args, **kwargs):
        rename(*args, **kwargs)
        calls.append(True)
        raise OSError("原namespace真实发布后丢回程")

    monkeypatch.setattr(physical, "rename_at", lost)
    result = await agent.execute("原模型发布未知", idempotency_key="unknown-run")
    final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
    assert result.status == "completed" and final.phase == "uncertain" and calls == [True], (
        result,
        final,
        provider.reply,
    )
    assert await service.directory_restore_store.source(
        final.scope_id, final.plan_id, "execute"
    ) == CheckpointModelSource(result.run_id, provider.call_id)
    return result, final


async def test_new_run_readonly_verifies_original_unknown_operation_without_replay(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, provider, _ = await phase_setup(runtime, tmp_path, "start")
    try:
        original_run, original = await unknown(service, agent, provider, monkeypatch)

        def forbidden(*_args, **_kwargs):
            pytest.fail("核实只读原双侧，不得重新捕获或发布")

        monkeypatch.setattr(physical, "rename_at", forbidden)
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
        provider.calls, provider.name_value = 0, VERIFY
        provider.arguments = {"request_key": "boundary-execute"}
        result = await agent.execute("新Run核实原操作", idempotency_key="verify-run")
        final = await agent.directory_checkpoint_restore(original.plan_id)
        assert result.status == "completed" and result.run_id != original_run.run_id
        assert final.phase == "finished" and final.operation_id == original.operation_id, (
            final,
            provider.reply,
        )
        store = service.directory_restore_store
        assert await store.source(
            final.scope_id, final.plan_id, "execute"
        ) == CheckpointModelSource(original_run.run_id, provider.call_id)
        assert await store.source(final.scope_id, final.plan_id, "verify") == CheckpointModelSource(
            result.run_id, provider.call_id
        )
        assert json.loads(provider.reply) == final.model_dump(mode="json")
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
        assert not service.directory_restores._tasks
    finally:
        await service.close()


async def test_db_unavailable_after_own_worker_spawn_still_aborts_and_drains_exact_invocation(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, _, _ = await phase_setup(runtime, tmp_path)
    original = service.directory_restores.preview_model
    entered, exited = asyncio.Event(), asyncio.Event()

    async def hold(definition, _context):
        if definition.name == DIRECTORY_RESTORE_STEP_TOOLS["prepare"]:
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                exited.set()

    async def lost(*args, **kwargs):
        await original(*args, **kwargs)
        await entered.wait()
        raise OSError("本次worker已生成但准入回程失联")

    async def offline(*_args, **_kwargs):
        raise OSError("准入核实DB暂不可用")

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    monkeypatch.setattr(service.directory_restores, "preview_model", lost)
    monkeypatch.setattr(service.directory_restore_store, "cancel_model_admission", offline)
    try:
        result = await agent.execute("原准入失联仍收束本地worker", idempotency_key="own-worker")
        assert result.status == "completed" and entered.is_set() and exited.is_set(), result
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert (
            final.phase == "failed"
            and final.error_code == "workspace_directory_restore_cancelled"
            and final.backup is None
        )
        assert not service.directory_restores._tasks and not service.directory_restores._invocations
        assert not service.directory_restores._callbacks and not service.model_checkpoints._calls
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    finally:
        await service.close()


async def test_model_cancel_original_host_worker_and_repeated_cancel_preserve_exact_fact(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, provider, _ = await phase_setup(runtime, tmp_path)
    entered = asyncio.Event()

    async def hold(definition, context):
        if definition.name == DIRECTORY_RESTORE_STEP_TOOLS["prepare"] and context.origin == "host":
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    try:
        original = await agent.preview_directory_checkpoint_restore(
            DirectoryRestorePreview.model_validate(provider.arguments)
        )
        await asyncio.wait_for(entered.wait(), 5)
        monkeypatch.setattr(
            WorkspaceTreeSnapshotReader,
            "capture",
            lambda *_args, **_kwargs: pytest.fail("取消原Host不得重读目录"),
        )
        provider.name_value, provider.arguments = CANCEL, {"request_key": "boundary-preview"}
        first = await agent.execute("模型取消原Host准备", idempotency_key="cancel-host")
        final = await agent.directory_checkpoint_restore(original.plan_id)
        assert first.status == "completed" and final.phase == "cancelled", (
            first,
            final,
            provider.reply,
        )
        assert json.loads(provider.reply) == final.model_dump(mode="json")
        provider.calls = 0
        second = await agent.execute("再次取消保留原事实", idempotency_key="repeat-cancel")
        assert second.status == "completed" and json.loads(provider.reply) == final.model_dump(
            mode="json"
        )
        assert not service.directory_restores._tasks and not service.directory_restores._pending
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
    finally:
        await service.close()


async def test_repeated_parent_cancel_after_admission_closed_still_drains_original_worker(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, _, _ = await phase_setup(runtime, tmp_path)
    leaf_entered, cleanup_entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    roots = []
    original = service.directory_restore_store.cancel_model_admission

    async def hold(definition, _context):
        if definition.name == PREVIEW:
            roots.append(asyncio.current_task())
        elif definition.name == DIRECTORY_RESTORE_STEP_TOOLS["prepare"]:
            leaf_entered.set()
            await asyncio.Event().wait()

    async def paused(*args, **kwargs):
        result = await original(*args, **kwargs)
        cleanup_entered.set()
        await release.wait()
        return result

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    monkeypatch.setattr(service.directory_restore_store, "cancel_model_admission", paused)
    producing = asyncio.create_task(
        agent.execute("重复取消原父收尾", idempotency_key="repeated-parent")
    )
    stopping = None
    try:
        await asyncio.wait_for(leaf_entered.wait(), 5)
        stopping = asyncio.create_task(agent.stop())
        await asyncio.wait_for(cleanup_entered.wait(), 5)
        assert (
            await agent.directory_checkpoint_restore_by_key("boundary-preview")
        ).phase == "cancelled"
        roots[0].cancel()
        await asyncio.sleep(0)
        roots[0].cancel()
        release.set()
        await asyncio.wait_for(stopping, 5)
        result = await asyncio.wait_for(producing, 5)
        assert result.status == "aborted"
        assert not service.directory_restores._tasks and not service.directory_restores._callbacks
        assert (
            not service.directory_restores._fences and not service.directory_restores._invocations
        )
        assert not service.directory_restores._pending and not service.model_checkpoints._calls
    finally:
        release.set()
        for task in (producing, stopping):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *[task for task in (producing, stopping) if task is not None], return_exceptions=True
        )
        await service.close()
