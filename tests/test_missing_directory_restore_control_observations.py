"""原活观察有界保留、自然到期释放、同实例证明与冷unknown生命周期。"""

import asyncio
import time

import pytest
from test_missing_directory_restore_control import (
    checkpoint,
    drain,
    execute_request,
    preview_request,
    ready,
    setup,
)

import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.application.agents import AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter


def forbidden(*_args, **_kwargs):
    pytest.fail("原事实核实不能重新捕获、构树或发布")


async def test_observation_quota_reserves_before_worker_but_does_not_block_original_execute(
    runtime, tmp_path
):
    service, agent, binding = await setup(runtime, tmp_path)
    service.missing_directory_restores.max_observations = 1
    try:
        target, planned = await ready(service, agent, binding)
        controller = service.missing_directory_restores
        reader = controller._readers[planned.plan_id]
        witness = reader._observations["tree"].watch
        assert len(controller._observation_slots) == 1 and witness.descriptor >= 0
        with pytest.raises(ServiceError) as denied:
            await service.workspace_preview_missing_directory_restore(
                agent.agent_id, preview_request(target.checkpoint_id, "overflow")
            )
        assert denied.value.code == "workspace_missing_directory_restore_busy"
        assert (
            await service.workspace_missing_directory_restore_by_key(agent.agent_id, "overflow")
            is None
        )
        # 容量已满仍必须允许收束原计划，不能把原执行/核实也锁死。
        await service.workspace_start_missing_directory_restore(
            agent.agent_id, execute_request(planned)
        )
        await drain(service)
        assert (
            await service.workspace_missing_directory_restore(agent.agent_id, planned.plan_id)
        ).phase == "finished"
        assert reader._closed and witness.descriptor == -1
        assert (
            not controller._readers
            and not controller._writers
            and not controller._observation_slots
        )
        (binding.workspace.root / "tree").rename(binding.workspace.root / "retained-restored")
        next_plan = await service.workspace_preview_missing_directory_restore(
            agent.agent_id, preview_request(target.checkpoint_id, "after-release")
        )
        next_plan = await service.workspace_missing_directory_restore_wait(
            agent.agent_id, next_plan.plan_id
        )
        await drain(service)
        assert next_plan.phase == "ready" and len(controller._readers) == 1
        next_reader = controller._readers[next_plan.plan_id]
    finally:
        await service.close()
    assert next_reader._closed and not controller._observation_slots and not controller._readers


async def test_expired_ready_naturally_releases_old_observer_without_recapture(runtime, tmp_path):
    service, agent, binding = await setup(runtime, tmp_path)
    service.missing_directory_restores.max_observations = 1
    try:
        target = await checkpoint(agent)
        (binding.workspace.root / "tree").rename(binding.workspace.root / "retained-original")
        request = preview_request(target.checkpoint_id).model_copy(update={"ttl_seconds": 2.0})
        planned = await service.workspace_preview_missing_directory_restore(agent.agent_id, request)
        planned = await service.workspace_missing_directory_restore_wait(
            agent.agent_id, planned.plan_id
        )
        await drain(service)
        assert planned.phase == "ready"
        old_reader = service.missing_directory_restores._readers[planned.plan_id]
        await asyncio.sleep(max(0, planned.expires_at - time.time()) + 0.02)
        another = await service.workspace_preview_missing_directory_restore(
            agent.agent_id, preview_request(target.checkpoint_id, "fresh-explicit-preview")
        )
        another = await service.workspace_missing_directory_restore_wait(
            agent.agent_id, another.plan_id
        )
        await drain(service)
        assert another.phase == "ready" and old_reader._closed
        original = await service.workspace_missing_directory_restore(
            agent.agent_id, planned.plan_id
        )
        assert original.phase == "expired" and original.baseline == planned.baseline
        assert len(service.missing_directory_restores._readers) == 1
    finally:
        await service.close()


async def unknown_after_real_effect(service, agent, planned):
    async def lost(definition, value, _context):
        if definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]:
            raise RuntimeError("原真实effect已经发生，模拟后置Hook丢回程")
        return value

    service._handle(agent.agent_id).runtime.tools.add_global_post(lost)
    await service.workspace_start_missing_directory_restore(
        agent.agent_id, execute_request(planned)
    )
    await drain(service)
    result = await service.workspace_missing_directory_restore(agent.agent_id, planned.plan_id)
    assert result.phase == "uncertain"
    return result


async def test_same_instance_unknown_keeps_original_writer_evidence_then_releases_it(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        _, planned = await ready(service, agent, binding)
        original = await unknown_after_real_effect(service, agent, planned)
        controller = service.missing_directory_restores
        retained = controller._writers[original.operation_id][1]
        assert retained._namespace_known and retained._result.receipt.status == "done"
        reader = controller._readers[planned.plan_id]
        monkeypatch.setattr(physical, "rename_at", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
        await service.workspace_verify_missing_directory_restore(
            agent.agent_id, original.operation_id
        )
        await drain(service)
        verified = await service.workspace_missing_directory_restore(
            agent.agent_id, planned.plan_id
        )
        assert verified.phase == "finished" and verified.operation_id == original.operation_id
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
        assert reader._closed and not controller._writers and not controller._readers
    finally:
        await service.close()


async def test_cold_host_unknown_never_reconstructs_original_writer_authority(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path, persistent=True)
    cold = None
    try:
        _, planned = await ready(service, agent, binding)
        original = await unknown_after_real_effect(service, agent, planned)
        inode = (tmp_path / "tree").stat().st_ino
        await service.close()
        cold = AgentService(runtime, runtime.settings)
        cold.register_workspace("files", tmp_path, version="1", writable=True, default=True)
        cold.register_directory_checkpoints()
        cold.register_missing_directory_restores()
        await cold.restore(agent.agent_id)
        monkeypatch.setattr(physical, "rename_at", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "verify", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
        await cold.workspace_verify_missing_directory_restore(agent.agent_id, original.operation_id)
        await drain(cold)
        current = await cold.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
        assert current.phase == "uncertain" and current.operation_id == original.operation_id
        assert current.receipt.error_code == "workspace_missing_directory_restore_unverifiable"
        assert (tmp_path / "tree").stat().st_ino == inode
        assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
        assert (
            not cold.missing_directory_restores._readers
            and not cold.missing_directory_restores._writers
        )
    finally:
        if cold is not None:
            await cold.close()
        await service.close()
