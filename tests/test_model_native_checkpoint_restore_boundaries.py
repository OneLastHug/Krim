"""恢复冻结能力、跨主体、只读资源及原审批暂停边界。"""

import asyncio
import json

import pytest
from sqlalchemy import func, select
from test_model_native_checkpoint_restores import NAMES, phase_fixture, setup, target_fixture
from test_model_native_checkpoints import OneShotProvider, saved_audits

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import WorkspaceRestorePlanRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession
from krim_agent.workspace_restore import CheckpointRestoreExecute
from krim_agent.workspace_writer import WorkspaceWriter


@pytest.mark.parametrize("operation", ["preview", "start", "cancel"])
async def test_readonly_resource_refuses_new_restore_write_profiles(runtime, tmp_path, operation):
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", tmp_path, writable=False, version="1")
    service.register_checkpoint_restore_controls()
    try:
        with pytest.raises(ServiceError) as refused:
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="readonly-restore",
                        description="只读恢复",
                        workspace_key="files",
                        tool_names=["workspace_read", "workspace_checkpoint_restore_" + operation],
                    ),
                )
            )
        assert refused.value.code == "workspace_read_only"
        async with runtime.repository.database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(WorkspaceRestorePlanRow)) == 0
    finally:
        await service.close()


@pytest.mark.parametrize("operation", ["query", "cancel"])
async def test_other_agent_cannot_query_or_cancel_known_restore_id(runtime, tmp_path, operation):
    service, original, provider, _, plan = await phase_fixture(runtime, tmp_path, "start")
    try:
        second = AgentSession(service, (await service.spawn("model-restore")).agent_id)
        provider.name_to_call = "workspace_checkpoint_restore_" + operation
        provider.arguments = {
            "plan_id": plan.plan_id,
            **({"kind": "id"} if operation == "query" else {}),
        }
        result = await second.execute("跨主体已知原ID拒绝", idempotency_key="scope")
        assert result.status == "completed", result
        audits = await saved_audits(runtime, result.run_id)
        assert any(item["code"] == "workspace_restore_not_found" for item in audits), provider.reply
        assert await original.file_checkpoint_restore(plan.plan_id) == plan
        assert (tmp_path / "plain").read_bytes() == b"current\n"
    finally:
        await service.close()


async def test_register_restore_controls_preserves_preexisting_frozen_agent_tools(
    runtime, tmp_path
):
    provider = OneShotProvider()
    service, agent = await setup(runtime, tmp_path, provider, names=["workspace_read"])
    handle = service._handle(agent.agent_id)
    before = [item.schema().model_dump(mode="json") for item in handle.runtime.tools.definitions]
    try:
        service.register_checkpoint_restore_controls()
        assert [
            item.schema().model_dump(mode="json") for item in handle.runtime.tools.definitions
        ] == before
        provider.name_to_call = "workspace_checkpoint_restore_query"
        provider.arguments = {"kind": "history"}
        result = await agent.execute("旧冻结能力不扩张", idempotency_key="frozen")
        assert result.status == "completed"
        audits = await saved_audits(runtime, result.run_id)
        assert any(item["name"] == provider.name_to_call and not item["ok"] for item in audits)
    finally:
        await service.close()


async def test_missing_frozen_bytes_tool_refuses_model_preview_before_plan_admission(
    runtime, tmp_path, monkeypatch
):
    (tmp_path / "plain").write_text("target\n")
    provider = OneShotProvider("workspace_checkpoint_restore_preview")
    service, agent = await setup(
        runtime,
        tmp_path,
        provider,
        names=[name for name in NAMES if name != "workspace_checkpoint_capture"],
    )
    target = await target_fixture(service, agent, tmp_path)
    provider.arguments = {"checkpoint_id": target.checkpoint_id, "request_key": "missing-bytes"}

    def no_capture(*_args, **_kwargs):
        pytest.fail("缺少原冻结字节能力不得读取文件")

    monkeypatch.setattr(WorkspaceWriter, "capture_file", no_capture)
    try:
        result = await agent.execute("缺字节冻结能力", idempotency_key="missing")
        assert result.status == "completed"
        assert await agent.file_checkpoint_restore_by_key("missing-bytes") is None
        audits = await saved_audits(runtime, result.run_id)
        assert any(item["code"] == "workspace_checkpoint_bytes_unavailable" for item in audits), (
            provider.reply
        )
    finally:
        await service.close()


async def test_original_model_leaf_ask_pauses_and_resumes_parent_budget_during_renewal(
    runtime, tmp_path
):
    async def policy(definition, _context):
        return "ask" if definition.name == "workspace_change" else "allow"

    service, agent, provider, _, _ = await phase_fixture(runtime, tmp_path, "start", policy=policy)
    paused = []

    async def approve(definition, context):
        assert definition.name == "workspace_change" and context.depth == 2
        paused.append(context.execution_timeouts[0])
        assert paused[-1].deadline.when() is None
        await asyncio.sleep(0.25)
        return True

    service._handle(agent.agent_id).runtime.approval_handler = approve
    try:
        result = await agent.execute("原恢复叶工具真实审批", idempotency_key="leaf-ask")
        plan = await agent.file_checkpoint_restore_by_key("host-preview")
        assert result.status == "completed" and plan.status == "done", (
            result,
            plan,
            provider.reply,
        )
        assert len(paused) == 1 and paused[0].pause_extension >= 0.25
    finally:
        await service.close()


async def test_host_can_execute_model_preview_with_its_own_separate_stage(runtime, tmp_path):
    service, agent, provider, _, _ = await phase_fixture(runtime, tmp_path, "preview")
    try:
        result = await agent.execute("原模型预览后宿主执行", idempotency_key="preview")
        plan = await agent.file_checkpoint_restore_by_key("model-preview")
        assert result.status == "completed" and plan.phase == "ready", (result, provider.reply)
        await agent.execute_file_checkpoint_restore(
            CheckpointRestoreExecute(
                plan_id=plan.plan_id, expected_revision=plan.revision, request_key="host-start"
            )
        )
        done = await agent.wait_file_checkpoint_restore(plan.plan_id)
        assert done.status == "done" and (tmp_path / "plain").read_bytes() == b"target\n", done
        assert (
            await service.workspace_restore_store.source(plan.scope_id, plan.plan_id, "preview")
            is not None
        )
        assert (
            await service.workspace_restore_store.source(plan.scope_id, plan.plan_id, "execute")
            is None
        )
    finally:
        await service.close()


async def test_model_history_query_and_cancel_by_original_preview_key_do_not_recapture(
    runtime, tmp_path, monkeypatch
):
    service, agent, provider, _, plan = await phase_fixture(runtime, tmp_path, "start")

    def no_io(*_args, **_kwargs):
        pytest.fail("只读历史与ready取消不应重新捕获")

    monkeypatch.setattr(WorkspaceWriter, "capture_file", no_io)
    try:
        provider.name_to_call = "workspace_checkpoint_restore_query"
        provider.arguments = {"kind": "history", "offset": 0, "limit": 1}
        result = await agent.execute("有界原恢复历史", idempotency_key="history")
        assert result.status == "completed" and json.loads(provider.reply)["entries"] == [
            plan.model_dump(mode="json")
        ]
        provider.calls = 0
        provider.name_to_call = "workspace_checkpoint_restore_cancel"
        provider.arguments = {"request_key": "host-preview"}
        second = await agent.execute("按原键关闭ready计划", idempotency_key="cancel")
        cancelled = await agent.file_checkpoint_restore(plan.plan_id)
        assert second.status == "completed" and cancelled.phase == "cancelled"
        assert json.loads(provider.reply) == cancelled.model_dump(mode="json")
    finally:
        await service.close()
