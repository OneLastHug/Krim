"""原缺失模型父停止/超时/准入丢回程与新Run只读核实，收束同worker。"""

import asyncio
import json
from threading import Event

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session
from test_model_missing_directory_restores import CANCEL, PREVIEW, START, VERIFY, phase_setup

import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.persistence.database import WorkspaceMissingDirectoryRestoreModelSourceRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestorePreview
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter


def drained(service):
    controller = service.missing_directory_restores
    assert not controller._tasks and not controller._fences
    assert not controller._invocations and not controller._pending and not controller._callbacks
    assert not service.model_checkpoints._calls


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_run_stop_closes_only_original_missing_stage_and_drains(runtime, tmp_path, action):
    service, agent, _, _, _ = await phase_setup(runtime, tmp_path, action)
    entered = asyncio.Event()
    selected = MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]

    async def hold(definition, context):
        if definition.name == selected:
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    task = asyncio.create_task(agent.execute("停止原缺失模型阶段", idempotency_key="stop-stage"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await agent.stop()
        result = await asyncio.wait_for(task, 5)
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "aborted" and final.phase == "cancelled", (result, final)
        assert not final.cleanup_pending and not (tmp_path / "tree").exists()
        drained(service)
        assert not service.missing_directory_restores._observation_slots
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_original_round_budget_cancels_missing_stage_without_new_tool_budget(
    runtime, tmp_path, action
):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, action, round_timeout=2)
    observed = []
    selected = MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare" if action == "preview" else "execute"]

    async def hold(definition, context):
        if definition.name == selected:
            observed.append(context.timeout)
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    try:
        result = await asyncio.wait_for(
            agent.execute("原整轮两秒预算", idempotency_key="round-limit"), 6
        )
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "aborted" and result.code == "timeout" and observed == [10], (
            result,
            provider.reply,
        )
        assert final.phase == "cancelled" and not final.cleanup_pending
        assert not (tmp_path / "tree").exists()
        drained(service)
    finally:
        await service.close()


@pytest.mark.parametrize("action,method", [("preview", "reserve"), ("start", "start")])
async def test_committed_missing_admission_reply_loss_reads_exact_source_and_drains(
    runtime, tmp_path, monkeypatch, action, method
):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, action)
    store = service.missing_directory_restore_store
    original = getattr(store, method)
    committed = []

    async def lost(*args, **kwargs):
        def fail(_session):
            committed.append(True)
            raise OSError("synthetic committed admission reply loss")

        event.listen(Session, "after_commit", fail)
        try:
            return await original(*args, **kwargs)
        finally:
            event.remove(Session, "after_commit", fail)

    monkeypatch.setattr(store, method, lost)
    try:
        result = await agent.execute("只关闭原已提交准入", idempotency_key="committed-reply-loss")
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed" and committed == [True], (result, provider.reply)
        assert final.phase == "cancelled" and not final.cleanup_pending
        assert not (tmp_path / "tree").exists()
        drained(service)
        assert store.database.engine.sync_engine.pool.checkedout() == 0
    finally:
        await service.close()


async def test_new_model_run_verifies_same_missing_unknown_without_republication(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, "start")
    original = physical.rename_at
    calls = []

    def lost(*args, **kwargs):
        original(*args, **kwargs)
        calls.append(True)
        raise OSError("原缺失namespace发布后丢回程")

    monkeypatch.setattr(physical, "rename_at", lost)
    try:
        first = await agent.execute("缺失发布原unknown", idempotency_key="original-unknown")
        unknown = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert first.status == "completed" and unknown.phase == "uncertain" and calls == [True], (
            first,
            unknown,
            provider.reply,
        )
        assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"

        def forbidden(*_args, **_kwargs):
            pytest.fail("新Run核实不能重发准备或namespace")

        monkeypatch.setattr(physical, "rename_at", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
        provider.calls, provider.name_value = 0, VERIFY
        provider.arguments = {"operation_id": unknown.operation_id}
        provider.call_id = "new-missing-verify"
        second = await agent.execute("新Run仅核实原操作", idempotency_key="verify-original")
        final = await service.workspace_missing_directory_restore(agent.agent_id, unknown.plan_id)
        assert second.status == "completed" and second.run_id != first.run_id
        assert final.phase == "finished" and final.operation_id == unknown.operation_id, (
            final,
            provider.reply,
        )
        assert json.loads(provider.reply) == final.model_dump(mode="json")
        async with service.missing_directory_restore_store.database.sessions() as sql:
            sources = list(
                await sql.scalars(select(WorkspaceMissingDirectoryRestoreModelSourceRow))
            )
        actual = {(row.phase, row.run_id, row.call_id) for row in sources}
        assert ("execute", first.run_id, "original-missing-start") in actual
        assert ("verify", second.run_id, "new-missing-verify") in actual
        assert calls == [True]
        drained(service)
    finally:
        await service.close()


@pytest.mark.parametrize("level", ["root", "leaf"])
@pytest.mark.parametrize("phase", ["policy", "pre", "post"])
async def test_missing_own_callbacks_refuse_close_without_lifecycle_change(
    runtime, tmp_path, level, phase
):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path, "start")
    handle = service._handle(agent.agent_id)
    selected = START if level == "root" else MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]
    observed = []

    async def reject_close():
        for name, close in (
            ("service", service.close),
            ("runtime", handle.runtime.close),
            ("missing", service.missing_directory_restores.close),
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
        result = await agent.execute("原缺失模型自关闭拒绝", idempotency_key="own-close")
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed" and final.phase == "finished", (result, provider.reply)
        assert observed == ["service", "runtime", "missing", "stop", "session", "destroy"]
        assert not service._closed and not handle.runtime._closed
        assert not service.missing_directory_restores._closed and not handle.destroyed
        assert (await agent.state()).status == "open"
        drained(service)
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["preview", "start"])
async def test_missing_tool_timeout_and_repeated_cancel_drain_original_thread(
    runtime, tmp_path, monkeypatch, action
):
    service, agent, _, _, _ = await phase_setup(runtime, tmp_path, action, tool_timeout=1)
    cls = (
        WorkspaceDirectoryAbsenceReader
        if action == "preview"
        else WorkspaceMissingDirectoryRestoreWriter
    )
    method = "capture" if action == "preview" else "prepare"
    original = getattr(cls, method)
    entered, release, exited = Event(), Event(), Event()

    def hold(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(8), "测试主动释放原物理线程"
            return original(*args, **kwargs)
        finally:
            exited.set()

    monkeypatch.setattr(cls, method, hold)
    task = asyncio.create_task(agent.execute("原线程必须收束", idempotency_key="thread-timeout"))
    try:
        assert await asyncio.to_thread(entered.wait, 4)
        await asyncio.sleep(1.15)
        assert not task.done() and not exited.is_set()
        for _, worker in list(service.missing_directory_restores._tasks.values()):
            worker.cancel()
            worker.cancel()
        release.set()
        result = await asyncio.wait_for(task, 6)
        final = await service.workspace_missing_directory_restore_by_key(
            agent.agent_id, "boundary-preview"
        )
        assert result.status == "completed" and exited.is_set(), result
        assert final.phase in {"cancelled", "failed"} and not (tmp_path / "tree").exists()
        drained(service)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_missing_model_cancel_drains_original_host_worker_and_repeat_is_cold(
    runtime, tmp_path
):
    service, agent, _, provider, _ = await phase_setup(runtime, tmp_path)
    entered = asyncio.Event()

    async def hold(definition, _context):
        if definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare"]:
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    try:
        original = await service.workspace_preview_missing_directory_restore(
            agent.agent_id, MissingDirectoryRestorePreview.model_validate(provider.arguments)
        )
        await asyncio.wait_for(entered.wait(), 5)
        provider.name_value, provider.arguments = CANCEL, {"plan_id": original.plan_id}
        result = await agent.execute("模型收束原Host预览", idempotency_key="cancel-host")
        final = await service.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
        assert result.status == "completed" and final.phase == "cancelled", (result, provider.reply)
        assert json.loads(provider.reply) == final.model_dump(mode="json")
        drained(service)
        provider.calls = 0
        second = await agent.execute("原取消重复冷读", idempotency_key="repeat-cancel-host")
        assert second.status == "completed" and json.loads(provider.reply) == final.model_dump(
            mode="json"
        )
        assert not (tmp_path / "tree").exists()
    finally:
        await service.close()


async def test_missing_repeated_parent_cancel_cannot_skip_original_admission_drain(
    runtime, tmp_path, monkeypatch
):
    service, agent, _, _, _ = await phase_setup(runtime, tmp_path)
    leaf_entered, cleanup_entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    parents = []
    original = service.missing_directory_restore_store.cancel_model_admission

    async def hold(definition, _context):
        if definition.name == PREVIEW:
            parents.append(asyncio.current_task())
        elif definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare"]:
            leaf_entered.set()
            await asyncio.Event().wait()

    async def paused(*args, **kwargs):
        result = await original(*args, **kwargs)
        cleanup_entered.set()
        await release.wait()
        return result

    service._handle(agent.agent_id).runtime.tools.add_global_pre(hold)
    monkeypatch.setattr(service.missing_directory_restore_store, "cancel_model_admission", paused)
    producing = asyncio.create_task(
        agent.execute("原缺失父重复取消", idempotency_key="repeat-parent")
    )
    stopping = None
    try:
        await asyncio.wait_for(leaf_entered.wait(), 5)
        stopping = asyncio.create_task(agent.stop())
        await asyncio.wait_for(cleanup_entered.wait(), 5)
        assert (
            await service.workspace_missing_directory_restore_by_key(
                agent.agent_id, "boundary-preview"
            )
        ).phase == "cancelled"
        parents[0].cancel()
        await asyncio.sleep(0)
        parents[0].cancel()
        release.set()
        await asyncio.wait_for(stopping, 5)
        assert (await asyncio.wait_for(producing, 5)).status == "aborted"
        drained(service)
    finally:
        release.set()
        tasks = [task for task in (producing, stopping) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await service.close()
