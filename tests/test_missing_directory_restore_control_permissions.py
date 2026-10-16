"""read预览、write发布与原Ask确认分开，冻结能力和停用资源不得提升。"""

import pytest
from test_missing_directory_restore_control import (
    checkpoint,
    drain,
    execute_request,
    preview_request,
    setup,
)

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


@pytest.mark.parametrize("decision", ["deny", "ask", "confirmed"])
async def test_read_preview_and_write_permission_or_original_confirmation(
    runtime, tmp_path, decision
):
    async def policy(definition, _context):
        if definition.risk == "read":
            return "allow"
        return "deny" if decision == "deny" else "ask"

    service, agent, binding = await setup(runtime, tmp_path, policy=policy)
    try:
        target = await checkpoint(agent)
        (binding.workspace.root / "tree").rename(binding.workspace.root / "retained-original")
        started = await service.workspace_preview_missing_directory_restore(
            agent.agent_id, preview_request(target.checkpoint_id)
        )
        planned = await service.workspace_missing_directory_restore_wait(
            agent.agent_id, started.plan_id, timeout_seconds=10
        )
        assert planned.phase == "ready" and not list(binding.workspace.root.glob("._krim-tx-*"))
        await drain(service)
        started = await service.workspace_start_missing_directory_restore(
            agent.agent_id, execute_request(planned), confirmed=decision == "confirmed"
        )
        await drain(service)
        result = await service.workspace_missing_directory_restore(agent.agent_id, started.plan_id)
        assert result.phase == ("finished" if decision == "confirmed" else "failed"), result
        if decision != "confirmed":
            assert result.error_code == (
                "permission_denied" if decision == "deny" else "approval_required"
            )
            assert not result.cleanup_pending and not list(
                binding.workspace.root.glob("._krim-tx-*")
            )
        if decision == "confirmed":
            assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
        else:
            assert not (binding.workspace.root / "tree").exists()
    finally:
        await service.close()


async def test_original_frozen_agent_without_restore_leaf_cannot_admit_plan(runtime, tmp_path):
    service, agent, _ = await setup(
        runtime, tmp_path, names=["workspace_directory_checkpoint_capture"]
    )
    try:
        target = await checkpoint(agent)
        with pytest.raises(ServiceError) as denied:
            await service.workspace_preview_missing_directory_restore(
                agent.agent_id, preview_request(target.checkpoint_id)
            )
        assert denied.value.code == "workspace_missing_directory_restore_unavailable"
        assert await service.workspace_missing_directory_restore_list(agent.agent_id) == []
    finally:
        await service.close()


async def test_revocation_stops_new_execution_but_retains_original_read_only_history(
    runtime, tmp_path, monkeypatch
):
    from test_missing_directory_restore_control import ready

    service, agent, binding = await setup(runtime, tmp_path)
    try:
        _, planned = await ready(service, agent, binding)
        binding.revoked.set()

        def forbidden(*_args, **_kwargs):
            pytest.fail("停用后不得启动物理生产者")

        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        with pytest.raises(ServiceError) as denied:
            await service.workspace_start_missing_directory_restore(
                agent.agent_id, execute_request(planned)
            )
        assert denied.value.code == "workspace_unavailable"
        assert (
            await service.workspace_missing_directory_restore(agent.agent_id, planned.plan_id)
            == planned
        )
        differences = await service.workspace_missing_directory_restore_diff(
            agent.agent_id, planned.plan_id
        )
        assert len(differences) == planned.target_snapshot.entry_count
        assert all(item.operation == "create" and item.before is None for item in differences)
    finally:
        await service.close()


async def test_current_tree_changed_after_review_is_rejected_without_recapture_or_publication(
    runtime, tmp_path
):
    from test_missing_directory_restore_control import ready

    service, agent, binding = await setup(runtime, tmp_path)
    try:
        _, planned = await ready(service, agent, binding)
        (binding.workspace.root / "tree/nested").mkdir(parents=True)
        (binding.workspace.root / "tree/nested/nul").write_bytes(b"later\0change")
        await service.workspace_start_missing_directory_restore(
            agent.agent_id, execute_request(planned)
        )
        await drain(service)
        result = await service.workspace_missing_directory_restore(agent.agent_id, planned.plan_id)
        assert result.phase == "failed" and not result.cleanup_pending
        assert not (
            await service.workspace_missing_directory_restore_operation(
                agent.agent_id, result.operation_id
            )
        ).intent_recorded
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"later\0change"
        assert not list(binding.workspace.root.glob("._krim-tx-*"))
    finally:
        await service.close()


async def test_other_agent_cannot_read_plan_or_borrow_original_available_target(runtime, tmp_path):
    from test_missing_directory_restore_control import ready

    service, agent, _ = await setup(runtime, tmp_path)
    try:
        target, planned = await ready(
            service, agent, service._handle(agent.agent_id).definition.workspace
        )
        other = await service.spawn("missing-directory-restore")
        with pytest.raises(ServiceError) as denied:
            await service.workspace_missing_directory_restore(other.agent_id, planned.plan_id)
        assert denied.value.status == 404
        with pytest.raises(ServiceError):
            await service.workspace_preview_missing_directory_restore(
                other.agent_id, preview_request(target.checkpoint_id)
            )
        assert await service.workspace_missing_directory_restore_list(other.agent_id) == []
    finally:
        await service.close()


async def test_plan_mode_read_preview_can_never_grant_write_namespace(runtime, tmp_path):
    from test_missing_directory_restore_control import ready

    service, agent, binding = await setup(runtime, tmp_path, mode="plan")
    try:
        _, planned = await ready(service, agent, binding)
        await service.workspace_start_missing_directory_restore(
            agent.agent_id, execute_request(planned), confirmed=True
        )
        await drain(service)
        result = await service.workspace_missing_directory_restore(agent.agent_id, planned.plan_id)
        assert result.phase == "failed" and result.error_code == "permission_denied"
        assert not (binding.workspace.root / "tree").exists() and not result.cleanup_pending
        assert not (
            await service.workspace_missing_directory_restore_operation(
                agent.agent_id, result.operation_id
            )
        ).intent_recorded
        assert not service.missing_directory_restores._readers
    finally:
        await service.close()


async def test_read_only_binding_rejects_before_target_or_observation_admission(runtime, tmp_path):
    service, agent, binding = await setup(runtime, tmp_path, writable=False)
    try:
        with pytest.raises(ServiceError) as denied:
            await service.workspace_preview_missing_directory_restore(
                agent.agent_id, preview_request("unavailable-target")
            )
        assert denied.value.code == "workspace_read_only"
        assert await service.workspace_missing_directory_restore_list(agent.agent_id) == []
        assert not service.missing_directory_restores._readers
        assert not service.missing_directory_restores._observation_slots
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
    finally:
        await service.close()
