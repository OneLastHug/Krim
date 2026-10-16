"""真实缺失Host准备/一次发布经原Registry，原Reader/watch与账本生命周期。"""

import asyncio

import pytest
from test_directory_checkpoint_capture import make_tree

import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.sdk import AgentSession, DirectoryCheckpointCreate
from krim_agent.workspace_missing_directory_restore import (
    DirectoryRestoreTarget,
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestorePreview,
)
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def setup(
    runtime,
    root,
    *,
    policy=None,
    names=None,
    tool_timeout=10,
    persistent=False,
    mode="agent",
    writable=True,
):
    make_tree(root)

    async def allow(_definition, _context):
        return "allow"

    runtime.tool_policy = policy or allow
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace(
        "files", root, version="1", writable=writable, default=True
    )
    service.register_directory_checkpoints()
    service.register_missing_directory_restores()
    service.register_missing_directory_restores()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="missing-directory-restore",
                description="完整目录受控恢复",
                workspace_key="files",
                mode=mode,
                tool_names=names
                if names is not None
                else [
                    "workspace_directory_checkpoint_capture",
                    *MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values(),
                ],
                tool_timeout_seconds=tool_timeout,
                persistent=persistent,
            ),
        )
    )
    agent = await service.spawn("missing-directory-restore")
    return service, AgentSession(service, agent.agent_id), binding


async def checkpoint(agent):
    started = await agent.create_directory_checkpoint(
        DirectoryCheckpointCreate(root_path="tree", request_key="original-target")
    )
    result = await agent.wait_directory_checkpoint(started.checkpoint_id, timeout_seconds=10)
    assert result.status == "available", result
    return result


def preview_request(identifier, key="preview"):
    return MissingDirectoryRestorePreview(
        target=DirectoryRestoreTarget(identifier=identifier), request_key=key
    )


def execute_request(view, key="execute"):
    return MissingDirectoryRestoreExecute(
        plan_id=view.plan_id, expected_revision=view.revision, request_key=key
    )


async def drain(service):
    await asyncio.gather(
        *[task for _, task in list(service.missing_directory_restores._tasks.values())]
    )


async def ready(service, agent, binding):
    target = await checkpoint(agent)
    (binding.workspace.root / "tree").rename(binding.workspace.root / "retained-original")
    started = await service.workspace_preview_missing_directory_restore(
        agent.agent_id, preview_request(target.checkpoint_id)
    )
    planned = await service.workspace_missing_directory_restore_wait(
        agent.agent_id, started.plan_id, timeout_seconds=10
    )
    assert planned.phase == "ready", planned
    assert not list(binding.workspace.root.glob("._krim-tx-*"))
    await drain(service)
    return target, planned


async def test_real_controlled_missing_restore_and_same_keys_cold_queries_zero_replay(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        target, planned = await ready(service, agent, binding)
        before = planned.baseline
        assert before.kind == "absent" and not (binding.workspace.root / "tree").exists()
        rename, calls = physical.rename_at, []

        def count(*args, **kwargs):
            calls.append(True)
            return rename(*args, **kwargs)

        monkeypatch.setattr(physical, "rename_at", count)
        wanted = execute_request(planned)
        started = await service.workspace_start_missing_directory_restore(agent.agent_id, wanted)
        finished = await service.workspace_missing_directory_restore_wait(
            agent.agent_id, started.plan_id, timeout_seconds=10
        )
        assert finished.status == "done", finished
        assert len(calls) == 1 and finished.baseline.fingerprint == before.fingerprint
        assert finished.receipt.target_fingerprint == target.snapshot.snapshot_fingerprint
        assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
        await drain(service)
        assert (
            not service.missing_directory_restores._pending
            and not service.missing_directory_restores._callbacks
        )
        handle = service._handle(agent.agent_id)
        audits = [
            item
            for item in handle.runtime.tools.audit
            if item.name in MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values()
        ]
        assert [(item.name, item.ok) for item in audits] == [
            (MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare"], True),
            (MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"], True),
        ]

        def forbidden(*_args, **_kwargs):
            pytest.fail("历史/原键不能重放目录生产者")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
        assert (
            await service.workspace_start_missing_directory_restore(agent.agent_id, wanted)
            == finished
        )
        assert (
            await service.workspace_preview_missing_directory_restore(
                agent.agent_id, preview_request(target.checkpoint_id)
            )
            == finished
        )
        assert (
            await service.workspace_missing_directory_restore_by_key(agent.agent_id, "preview")
            == finished
        )
        assert await service.workspace_missing_directory_restore_list(agent.agent_id) == [finished]
        assert (
            await service.workspace_missing_directory_restore_operation_by_key(
                agent.agent_id, "execute"
            )
        ).operation_id == finished.operation_id
        assert (
            await service.workspace_missing_directory_restore_cancel(
                agent.agent_id, finished.plan_id
            )
            == finished
        )
        assert (
            await service.workspace_verify_missing_directory_restore(
                agent.agent_id, finished.operation_id
            )
            == finished
        )
        assert (
            service.missing_directory_restore_store.database.engine.sync_engine.pool.checkedout()
            == 0
        )
    finally:
        await service.close()
