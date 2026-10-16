"""最后SQL仍受原父资格约束，跨Run恢复完整备份保持原字节与元数据。"""

import json

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from test_model_directory_restores import PREVIEW, START, phase_setup

from krim_agent.workspace_checkpoint import CheckpointModelSource


@pytest.mark.parametrize("phase", ["ready", "record_intent", "settle"])
async def test_last_sql_flush_rechecks_original_parent_backend_and_rolls_back_success(
    runtime, tmp_path, monkeypatch, phase
):
    action = "preview" if phase == "ready" else "start"
    service, agent, binding, provider, _ = await phase_setup(runtime, tmp_path, action)
    parent, fired, active, checked = [], [], [], []
    store = service.directory_restore_store
    original, flush = getattr(store, phase), AsyncSession.flush

    async def observe(definition, context):
        if definition.name == (PREVIEW if action == "preview" else START):
            parent.append(context)

    async def scoped(*args, **kwargs):
        original_qualify = kwargs["qualify"]

        def qualify():
            if not fired:
                return original_qualify()
            # 只在flush后的真实SQL资格回调中撤销原父Backend并立即恢复。
            # 不跨await暴露给heartbeat，避免以另一路取消代替最后资格核对。
            parent[0].workspace_directory_restores = None
            checked.append(True)
            try:
                return original_qualify()
            finally:
                parent[0].workspace_directory_restores = service.directory_restores

        kwargs["qualify"] = qualify
        active.append(True)
        try:
            return await original(*args, **kwargs)
        finally:
            active.clear()
            parent[0].workspace_directory_restores = service.directory_restores

    async def revoke(sql, *args, **kwargs):
        value = await flush(sql, *args, **kwargs)
        if active and not fired:
            fired.append(True)
        return value

    service._handle(agent.agent_id).runtime.tools.add_global_pre(observe)
    monkeypatch.setattr(store, phase, scoped)
    monkeypatch.setattr(AsyncSession, "flush", revoke)
    try:
        result = await agent.execute("最后SQL原父资格", idempotency_key="final-sql")
        final = await agent.directory_checkpoint_restore_by_key("boundary-preview")
        assert result.status == "completed" and fired == [True] and checked == [True], (
            result,
            provider.reply,
        )
        assert final.phase == ("uncertain" if phase == "settle" else "failed"), final
        assert final.error_code == "workspace_directory_restore_model_context", final
        if phase == "record_intent":
            assert final.receipt.status == "rejected" and not final.receipt.changed
            operation = await agent.directory_checkpoint_restore_operation(final.operation_id)
            assert not operation.intent_recorded
        else:
            assert final.receipt is None
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == (
            b"\0\xff" if phase == "settle" else b"current\x80\0"
        )
        assert not service.directory_restores._tasks and not service.directory_restores._pending
    finally:
        await service.close()


async def test_distinct_model_runs_restore_complete_baseline_bytes_metadata_and_original_sources(
    runtime, tmp_path
):
    service, agent, binding, provider, planned = await phase_setup(runtime, tmp_path, "start")
    file = binding.workspace.root / "tree/nested/nul"
    before_directory_mode = (binding.workspace.root / "tree/nested").stat().st_mode & 0o777
    try:
        first = await agent.execute("原目标发布", idempotency_key="first-start")
        finished = await agent.directory_checkpoint_restore(planned.plan_id)
        assert first.status == "completed" and finished.phase == "finished", (
            first,
            finished,
            provider.reply,
        )
        provider.calls, provider.name_value = 0, PREVIEW
        provider.arguments = {
            "target": {"kind": "baseline", "identifier": finished.plan_id},
            "request_key": "model-rollback",
        }
        second = await agent.execute("模型读取完整旧备份", idempotency_key="rollback-preview")
        rollback = await agent.directory_checkpoint_restore_by_key("model-rollback")
        assert second.status == "completed" and rollback.phase == "ready", (
            second,
            rollback,
            provider.reply,
        )
        assert rollback.target_snapshot == finished.backup
        provider.calls, provider.name_value = 0, START
        provider.arguments = {
            "plan_id": rollback.plan_id,
            "expected_revision": rollback.revision,
            "request_key": "model-rollback-execute",
        }
        third = await agent.execute("另一模型Run批准备份回退", idempotency_key="rollback-start")
        restored = await agent.directory_checkpoint_restore(rollback.plan_id)
        assert third.status == "completed" and restored.phase == "finished", (
            third,
            restored,
            provider.reply,
        )
        assert json.loads(provider.reply) == restored.model_dump(mode="json")
        assert len({first.run_id, second.run_id, third.run_id}) == 3
        assert file.read_bytes() == b"current\x80\0" and file.stat().st_mode & 0o777 == 0o640
        assert (
            binding.workspace.root / "tree/nested"
        ).stat().st_mode & 0o777 == before_directory_mode
        assert await agent.directory_checkpoint_restore(finished.plan_id) == finished
        store = service.directory_restore_store
        assert await store.source(
            restored.scope_id, restored.plan_id, "preview"
        ) == CheckpointModelSource(second.run_id, provider.call_id)
        assert await store.source(
            restored.scope_id, restored.plan_id, "execute"
        ) == CheckpointModelSource(third.run_id, provider.call_id)
    finally:
        await service.close()
