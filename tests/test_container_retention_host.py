"""真实Host Missing完成后沿同Registry回收；旧来源和已发布目录保持。"""

import asyncio

import pytest
from test_directory_checkpoint_capture import make_tree
from test_missing_directory_restore_control import execute_request, ready

import krim_agent.workspace_empty_container_retention as physical
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession
from krim_agent.workspace_container_retention import ContainerRetentionRequest
from krim_agent.workspace_container_retention_control import CONTAINER_RETENTION_STEP_TOOLS
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)


async def setup(
    runtime, root, *, names=None, persistent=False, mode="agent", tool_timeout=10, agent_lease=None
):
    make_tree(root)

    async def allow(_definition, _context):
        return "allow"

    runtime.tool_policy = allow
    service = AgentService(runtime, runtime.settings)
    if agent_lease is not None:
        service.manager.store.lease_seconds = agent_lease
    binding = service.register_workspace("files", root, version="1", writable=True, default=True)
    service.register_directory_checkpoints()
    service.register_missing_directory_restores()
    service.register_container_retentions()
    service.register_container_retentions()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="container-retention",
                description="原空事务容器受控回收",
                workspace_key="files",
                persistent=persistent,
                mode=mode,
                tool_timeout_seconds=tool_timeout,
                tool_names=names
                if names is not None
                else [
                    "workspace_directory_checkpoint_capture",
                    *MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values(),
                    *CONTAINER_RETENTION_STEP_TOOLS.values(),
                ],
            ),
        )
    )
    view = await service.spawn("container-retention")
    return service, AgentSession(service, view.agent_id), binding


async def drain(service):
    await asyncio.gather(*[task for _, task in list(service.container_retentions._tasks.values())])


async def completed(service, agent, binding):
    _, plan = await ready(service, agent, binding)
    await service.workspace_start_missing_directory_restore(agent.agent_id, execute_request(plan))
    await asyncio.gather(
        *[task for _, task in list(service.missing_directory_restores._tasks.values())]
    )
    final = await service.workspace_missing_directory_restore(agent.agent_id, plan.plan_id)
    assert final.phase == "finished" and final.status == "done"
    return final


def request(preview, key="original-retention"):
    return ContainerRetentionRequest(
        source_plan_id=preview.source.source_plan_id,
        expected_source_hash=preview.source.source_hash,
        expected_container_fingerprint=preview.fingerprint,
        request_key=key,
    )


@pytest.fixture
async def controller(runtime, tmp_path):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        original = await completed(service, agent, binding)
        preview = await service.workspace_preview_container_retention(
            agent.agent_id, original.plan_id
        )
        assert preview.status == "empty" and preview.existing_operation_id is None
        yield service, agent, binding, original, preview
    finally:
        await service.close()


async def test_host_inspect_remove_registry_audit_and_original_keys_zero_replay(
    controller, monkeypatch
):
    service, agent, binding, original, preview = controller
    tree = binding.workspace.root / "tree"
    inode = tree.stat().st_ino
    (tree / "user-added").write_bytes(b"user\0\xff")
    actual_rmdir, calls = physical.os.rmdir, []

    def remove(name, *, dir_fd):
        calls.append(name)
        return actual_rmdir(name, dir_fd=dir_fd)

    monkeypatch.setattr(physical.os, "rmdir", remove)
    wanted = request(preview)
    started = await service.workspace_start_container_retention(agent.agent_id, wanted)
    await drain(service)
    final = await service.workspace_container_retention_wait(agent.agent_id, started.operation_id)
    assert final.status == "done" and final.receipt.outcome == "removed", final
    assert final.intent_recorded and final.receipt.changed and len(calls) == 1
    assert not list(binding.workspace.root.glob("._krim-tx-*"))
    assert tree.stat().st_ino == inode and (tree / "nested/nul").read_bytes() == b"\0\xff"
    assert (tree / "user-added").read_bytes() == b"user\0\xff"
    assert (
        await service.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
        == original
    )
    registry = service._handle(agent.agent_id).runtime.tools
    audits = [a for a in registry.audit if a.name in CONTAINER_RETENTION_STEP_TOOLS.values()]
    assert [(a.name, a.ok, a.run_id) for a in audits] == [
        (CONTAINER_RETENTION_STEP_TOOLS["inspect"], True, "local"),
        (CONTAINER_RETENTION_STEP_TOOLS["remove"], True, "local"),
    ]
    info = await service.workspace_info(agent.agent_id)
    assert set(CONTAINER_RETENTION_STEP_TOOLS.values()) <= set(info.tool_names)

    def forbidden(*_args, **_kwargs):
        pytest.fail("原键和历史不能重放物理inspect/remove")

    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "remove", forbidden)
    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "inspect", forbidden)
    assert await service.workspace_container_retention(agent.agent_id, final.operation_id) == final
    assert (
        await service.workspace_container_retention_by_key(agent.agent_id, wanted.request_key)
        == final
    )
    assert await service.workspace_start_container_retention(agent.agent_id, wanted) == final
    binding.revoked.set()
    assert await service.workspace_start_container_retention(agent.agent_id, wanted) == final
    assert not service.container_retentions._pending and not service.container_retentions._jobs
    assert service.container_retention_store.database.engine.sync_engine.pool.checkedout() == 0


async def test_changed_empty_container_is_rejected_before_intent_and_user_content_survives(
    controller,
):
    service, agent, binding, original, preview = controller
    container = next(binding.workspace.root.glob("._krim-tx-*"))
    (container / "user").write_bytes(b"keep\0\xff")
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert final.status == "rejected" and not final.intent_recorded
    assert final.receipt.error_code == "workspace_empty_container_not_empty"
    assert (container / "user").read_bytes() == b"keep\0\xff"
    assert (
        await service.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
        == original
    )


async def test_after_real_rmdir_lost_reply_only_explicit_read_verify_same_operation(
    controller, monkeypatch
):
    service, agent, binding, original, preview = controller
    actual_rmdir, calls = physical.os.rmdir, []

    def lost(name, *, dir_fd):
        actual_rmdir(name, dir_fd=dir_fd)
        calls.append(name)
        raise OSError("真实rmdir已完成，回程丢失")

    monkeypatch.setattr(physical.os, "rmdir", lost)
    wanted = request(preview)
    started = await service.workspace_start_container_retention(agent.agent_id, wanted)
    await drain(service)
    unknown = await service.workspace_container_retention(agent.agent_id, started.operation_id)
    assert unknown.status == "unknown" and unknown.intent_recorded and len(calls) == 1

    def forbidden(*_args, **_kwargs):
        pytest.fail("核实只能inspect，不重删")

    monkeypatch.setattr(WorkspaceEmptyContainerRetention, "remove", forbidden)
    assert await service.workspace_start_container_retention(agent.agent_id, wanted) == unknown
    await service.workspace_verify_container_retention(
        agent.agent_id,
        unknown.operation_id,
        expected_revision=unknown.revision,
        request_key="explicit-cold-verify",
    )
    await drain(service)
    final = await service.workspace_container_retention(agent.agent_id, unknown.operation_id)
    assert final.status == "done" and final.receipt.outcome == "absence_verified"
    assert not final.receipt.changed and final.verification_count == 1
    assert (
        await service.workspace_verify_container_retention(
            agent.agent_id,
            unknown.operation_id,
            expected_revision=unknown.revision,
            request_key="explicit-cold-verify",
        )
        == final
    )
    audit = list(service._handle(agent.agent_id).runtime.tools.audit)[-1]
    assert audit.name == CONTAINER_RETENTION_STEP_TOOLS["verify"] and audit.ok
    assert (
        await service.workspace_missing_directory_restore(agent.agent_id, original.plan_id)
        == original
    )
    assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"


async def test_original_key_parameter_conflict_and_other_agent_scope_are_rejected(controller):
    service, agent, _, _, preview = controller
    started = await service.workspace_start_container_retention(agent.agent_id, request(preview))
    await drain(service)
    with pytest.raises(ServiceError) as denied:
        await service.workspace_start_container_retention(
            agent.agent_id, request(preview).model_copy(update={"expected_source_hash": "0" * 64})
        )
    assert denied.value.code == "workspace_container_retention_key_conflict"
    other = await service.spawn("container-retention")
    with pytest.raises(ServiceError) as denied:
        await service.workspace_container_retention(other.agent_id, started.operation_id)
    assert denied.value.status == 404
    assert (
        await service.workspace_container_retention_by_key(other.agent_id, "original-retention")
        is None
    )
