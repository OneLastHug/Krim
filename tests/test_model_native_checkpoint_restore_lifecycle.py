"""模型恢复跨Run、原线程超时/取消收束及关闭边界。"""

import asyncio
import json
from threading import Event

import pytest
from test_model_native_checkpoint_restores import phase_fixture
from test_model_native_checkpoints import saved_audits
from test_workspace_changes import wait_event

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_restore import CheckpointRestoreExecute, CheckpointRestorePreview
from krim_agent.workspace_writer import WorkspaceWriter


async def test_preview_start_and_history_in_distinct_model_runs_with_complete_baseline_rollback(
    runtime, tmp_path, monkeypatch
):
    service, agent, provider, target, _ = await phase_fixture(runtime, tmp_path, "preview")
    try:
        first = await agent.execute("模型预览", idempotency_key="preview-run")
        plan = await agent.file_checkpoint_restore_by_key("model-preview")
        assert first.status == "completed" and plan.phase == "ready", (first, provider.reply)
        provider.calls = 0
        provider.name_to_call = "workspace_checkpoint_restore_start"
        provider.arguments = {
            "plan_id": plan.plan_id,
            "expected_revision": plan.revision,
            "request_key": "new-run-start",
        }
        second = await agent.execute("另一模型Run执行", idempotency_key="start-run")
        done = await agent.file_checkpoint_restore(plan.plan_id)
        assert second.status == "completed" and second.run_id != first.run_id
        assert done.status == "done" and (tmp_path / "plain").read_bytes() == b"target\n", (
            done,
            provider.reply,
        )
        store = service.workspace_restore_store
        assert await store.source(plan.scope_id, plan.plan_id, "preview") == CheckpointModelSource(
            first.run_id, "one-root-call"
        )
        assert await store.source(plan.scope_id, plan.plan_id, "execute") == CheckpointModelSource(
            second.run_id, "one-root-call"
        )
        backup = await agent.file_checkpoint(plan.baseline_checkpoint_id)
        accepted = await agent.preview_file_checkpoint_restore(
            CheckpointRestorePreview(
                checkpoint_id=backup.checkpoint_id,
                request_key="host-rollback",
            )
        )
        rollback = await agent.wait_file_checkpoint_restore(accepted.plan_id)
        assert rollback.phase == "ready", rollback
        await agent.execute_file_checkpoint_restore(
            CheckpointRestoreExecute(
                plan_id=rollback.plan_id,
                expected_revision=rollback.revision,
                request_key="host-rollback-start",
            )
        )
        restored = await agent.wait_file_checkpoint_restore(rollback.plan_id)
        assert restored.status == "done" and (tmp_path / "plain").read_bytes() == b"current\n"
        assert await agent.file_checkpoint_restore(plan.plan_id) == done

        service.disable_workspace("files")
        service.resources.workspaces["files"].binding.workspace.close()

        def no_io(*_args, **_kwargs):
            pytest.fail("只读历史不能重放模型基线或文件事务")

        monkeypatch.setattr(WorkspaceWriter, "capture_file", no_io)
        monkeypatch.setattr(WorkspaceWriter, "apply", no_io)
        provider.calls = 0
        provider.name_to_call = "workspace_checkpoint_restore_query"
        provider.arguments = {"kind": "key", "request_key": "model-preview"}
        third = await agent.execute("关闭根后原历史", idempotency_key="history-run")
        assert third.status == "completed" and third.run_id not in {first.run_id, second.run_id}
        assert json.loads(provider.reply)["plan"] == done.model_dump(mode="json")
    finally:
        await service.close()


@pytest.mark.parametrize(
    "phase,level,action",
    [
        ("preview", "root", "service"),
        ("preview", "leaf", "runtime"),
        ("preview", "leaf", "session"),
        ("start", "root", "destroy"),
        ("start", "leaf", "stop"),
        ("start", "leaf", "service"),
    ],
)
async def test_restore_callbacks_refuse_self_close_before_state_changes(
    runtime, tmp_path, phase, level, action
):
    service, agent, provider, _, _ = await phase_fixture(runtime, tmp_path, phase)
    handle = service._handle(agent.agent_id)
    selected = (
        "workspace_checkpoint_restore_" + phase
        if level == "root"
        else ("workspace_checkpoint_capture" if phase == "preview" else "workspace_change")
    )
    observed = []

    async def pre(definition, _context):
        if definition.name != selected:
            return None
        with pytest.raises(ServiceError) as refused:
            if action == "service":
                await service.close()
            elif action == "runtime":
                await handle.runtime.close()
            elif action == "session":
                await agent.close()
            elif action == "destroy":
                await agent.destroy()
            else:
                await agent.stop()
        assert refused.value.code == "workspace_checkpoint_model_reentrant"
        observed.append(action)

    handle.runtime.tools.add_global_pre(pre)
    try:
        result = await agent.execute("恢复回调关闭拒绝", idempotency_key="self-close")
        plan = await agent.file_checkpoint_restore_by_key(
            "model-preview" if phase == "preview" else "host-preview"
        )
        assert result.status == "completed" and plan.phase == (
            "ready" if phase == "preview" else "finished"
        ), (result, provider.reply)
        assert observed == [action]
        assert not service._closed and not handle.runtime._closed and not handle.destroyed
        assert (await agent.state()).status == "open"
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["preview", "start"])
async def test_natural_model_restore_timeout_drains_real_file_thread_and_blocks_late_effect(
    runtime, tmp_path, monkeypatch, phase
):
    service, agent, provider, _, original_plan = await phase_fixture(
        runtime, tmp_path, phase, tool_timeout=1
    )
    entered, release = Event(), Event()
    method = "capture_file" if phase == "preview" else "apply"
    original = getattr(WorkspaceWriter, method)

    def delayed(self, *args, **kwargs):
        entered.set()
        assert release.wait(20), "测试释放事件超时"
        return original(self, *args, **kwargs)

    monkeypatch.setattr(WorkspaceWriter, method, delayed)
    task = asyncio.create_task(agent.execute("自然原1秒恢复期限", idempotency_key="timeout"))
    try:
        await wait_event(entered)
        await asyncio.sleep(1.15)
        assert not task.done(), "必须等待真实文件线程收束"
        release.set()
        result = await asyncio.wait_for(task, 20)
        plan = await agent.file_checkpoint_restore_by_key(
            "model-preview" if phase == "preview" else "host-preview"
        )
        assert result.status == "completed" and plan.phase in {"failed", "cancelled"}, (
            result,
            plan,
        )
        assert (tmp_path / "plain").read_bytes() == b"current\n"
        assert not service.workspace_restores._tasks and not service.workspace_checkpoints._tasks
        assert (
            not service.manager.workspace_changes._operations
            and not service.model_checkpoints._calls
        )
        if phase == "preview":
            backup = await agent.file_checkpoint(plan.baseline_checkpoint_id)
            assert all(not item.captured for item in backup.files)
        else:
            assert plan.items[0].operation is not None
            assert plan.items[0].operation.status == "rejected", plan
        audits = await saved_audits(runtime, result.run_id)
        assert any(
            item["name"] == "workspace_checkpoint_restore_" + phase and item["code"] == "timeout"
            for item in audits
        )
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_model_cancel_drains_original_host_restore_baseline_and_keeps_id(runtime, tmp_path):
    service, agent, provider, target, _ = await phase_fixture(runtime, tmp_path, "preview")
    entered = asyncio.Event()

    async def pre(definition, context):
        if definition.name == "workspace_checkpoint_capture" and context.origin == "host":
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(pre)
    try:
        accepted = await agent.preview_file_checkpoint_restore(
            CheckpointRestorePreview(
                checkpoint_id=target.checkpoint_id,
                request_key="host-running",
            )
        )
        await asyncio.wait_for(entered.wait(), 10)
        provider.name_to_call = "workspace_checkpoint_restore_cancel"
        provider.arguments = {"plan_id": accepted.plan_id}
        result = await agent.execute("模型取消原Host计划", idempotency_key="cancel-host")
        plan = await agent.file_checkpoint_restore(accepted.plan_id)
        assert result.status == "completed" and plan.phase == "cancelled", (result, plan)
        assert json.loads(provider.reply) == plan.model_dump(mode="json")
        assert plan.baseline_checkpoint_id is not None
        backup = await agent.file_checkpoint(plan.baseline_checkpoint_id)
        assert backup.status == "cancelled" and all(not item.captured for item in backup.files)
        assert not service.workspace_restores._tasks and not service.workspace_checkpoints._tasks
        assert (tmp_path / "plain").read_bytes() == b"current\n"
    finally:
        await service.close()


async def test_external_model_stop_closes_restore_and_baseline_admission(runtime, tmp_path):
    service, agent, _, _, _ = await phase_fixture(runtime, tmp_path, "preview")
    entered = asyncio.Event()

    async def pre(definition, context):
        if definition.name == "workspace_checkpoint_capture" and context.origin == "model":
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(pre)
    task = asyncio.create_task(agent.execute("停止原模型恢复", idempotency_key="stop"))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        await agent.stop()
        result = await asyncio.wait_for(task, 10)
        plan = await agent.file_checkpoint_restore_by_key("model-preview")
        assert result.status == "aborted" and plan.phase == "cancelled", (result, plan)
        assert not service.workspace_restores._tasks and not service.workspace_checkpoints._tasks
        backup = await agent.file_checkpoint(plan.baseline_checkpoint_id)
        assert all(not item.captured for item in backup.files)
        assert (tmp_path / "plain").read_bytes() == b"current\n"
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()
