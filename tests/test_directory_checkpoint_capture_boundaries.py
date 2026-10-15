"""原目录捕获的真实物理截止/只读边界/跨调用与冷请求完整性。"""

import asyncio
import os
import time
from threading import Event

import pytest
from sqlalchemy import select
from test_directory_checkpoint_capture import capture, counts, make_tree, setup

from krim_agent.persistence.database import WorkspaceDirectoryCaptureRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import DirectoryCheckpointCreate
from krim_agent.tools import ToolContext
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointCaptureFence
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def test_concurrent_public_same_key_starts_only_one_actual_reader(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path)
    original = WorkspaceTreeSnapshotReader.capture
    reads = []

    def observed(self, path, **kwargs):
        reads.append(path)
        return original(self, path, **kwargs)

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", observed)
    try:
        request = DirectoryCheckpointCreate(root_path="tree", request_key="same")
        first, second = await asyncio.gather(
            agent.create_directory_checkpoint(request), agent.create_directory_checkpoint(request)
        )
        assert first.checkpoint_id == second.checkpoint_id
        final = await agent.wait_directory_checkpoint(first.checkpoint_id)
        assert final.status == "available" and reads == ["tree"], final
        assert await counts(service) == (1, 1)
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault,code",
    [
        ("symlink", "workspace_link"),
        ("hardlink", "workspace_link"),
        ("sensitive", "workspace_sensitive"),
        ("file-size", "workspace_tree_snapshot_bytes"),
    ],
)
async def test_actual_reader_preserves_original_link_sensitive_and_file_limits(
    runtime, tmp_path, fault, code
):
    tree = make_tree(tmp_path)
    if fault == "symlink":
        (tree / "outside").symlink_to(tmp_path)
    elif fault == "hardlink":
        os.link(tree / "a.txt", tree / "linked")
    elif fault == "sensitive":
        (tree / ".env").write_text("synthetic-only")
    else:
        (tree / "oversized").write_bytes(b"x" * (1048576 + 1))
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        _, final = await capture(agent)
        assert final.status == "failed" and final.error_code == code, final
        assert final.snapshot is None and await counts(service) == (1, 0)
    finally:
        await service.close()


async def test_actual_five_second_physical_budget_stops_blocked_qualification_and_drains_thread(
    runtime, tmp_path, monkeypatch
):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path)
    original = WorkspaceTreeSnapshotReader.capture
    entered, gate, exited = Event(), Event(), Event()
    elapsed = []

    def observe(self, path, **kwargs):
        began = time.monotonic()
        entered.set()
        try:
            assert gate.wait(1)
            return original(self, path, **kwargs)
        finally:
            elapsed.append(time.monotonic() - began)
            exited.set()

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", observe)
    try:
        view = await agent.create_directory_checkpoint(
            DirectoryCheckpointCreate(root_path="tree", request_key="physical-five")
        )
        assert await asyncio.to_thread(entered.wait, 5)
        async with runtime.repository.database.write_lock:
            gate.set()
            assert await asyncio.to_thread(exited.wait, 6)
        final = await agent.wait_directory_checkpoint(view.checkpoint_id, timeout_seconds=5)
        assert (
            final.status == "failed" and final.error_code == "workspace_directory_capture_stopped"
        ), final
        assert 4.5 <= elapsed[0] <= 5.5 and exited.is_set(), elapsed
        assert await counts(service) == (1, 0)
    finally:
        gate.set()
        await service.close()


async def test_directory_fence_cannot_be_reused_for_file_read_or_other_tool(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(
        runtime, tmp_path, names=["workspace_read", "workspace_directory_checkpoint_capture"]
    )
    registry = service._handle(agent.agent_id).runtime.tools

    def no_read(*_args, **_kwargs):
        pytest.fail("目录围栏不可给其它叶工具授权")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", no_read)
    try:
        context = ToolContext(
            registry,
            origin="host",
            topic_id=service._handle(agent.agent_id).topic_id,
            agent_id=agent.agent_id,
            session_id=agent.session_id,
            workspace=binding,
            workspace_directory_checkpoints=service.directory_checkpoints,
            directory_checkpoint_capture=DirectoryCheckpointCaptureFence("fake", "0" * 64),
        )
        result = await registry.invoke("workspace_read", {"path": "other"}, context)
        assert result.is_error and result.code == "workspace_directory_capture_context"
        assert len(registry.audit) == 1 and not registry.audit[0].ok
        assert await counts(service) == (0, 0)
    finally:
        await service.close()


async def test_repeated_pending_key_rejects_corrupt_frozen_request(runtime, tmp_path):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        scope = "session:" + agent.session_id
        from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointMaterialCreate

        request = DirectoryCheckpointMaterialCreate(root_path="tree", request_key="original")
        view, _ = await service.directory_checkpoint_store.reserve(scope, binding, request)
        async with runtime.repository.database.sessions() as sql, sql.begin():
            row = await sql.get(WorkspaceDirectoryCaptureRow, view.checkpoint_id)
            row.root_path = "tampered"
        with pytest.raises(ServiceError) as corrupt:
            await service.directory_checkpoint_store.reserve(scope, binding, request)
        assert corrupt.value.code == "workspace_directory_capture_corrupt"
        async with runtime.repository.database.sessions() as sql:
            assert (await sql.scalar(select(WorkspaceDirectoryCaptureRow))).material_id is None
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["cancel", "timeout"])
async def test_private_material_awaiting_post_hook_is_never_published_after_stop(
    runtime, tmp_path, action
):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path, tool_timeout=1)
    entered, release = asyncio.Event(), asyncio.Event()

    async def hold(_definition, value, _context):
        entered.set()
        await release.wait()
        return value

    service._handle(agent.agent_id).runtime.tools.add_global_post(hold)
    try:
        view = await agent.create_directory_checkpoint(
            DirectoryCheckpointCreate(root_path="tree", request_key=action)
        )
        await asyncio.wait_for(entered.wait(), 5)
        in_progress = await agent.directory_checkpoint(view.checkpoint_id)
        assert in_progress.status == "capturing" and in_progress.snapshot is not None
        if action == "cancel":
            final = await agent.cancel_directory_checkpoint(view.checkpoint_id)
            assert final.status == "cancelled"
        else:
            final = await agent.wait_directory_checkpoint(view.checkpoint_id, timeout_seconds=5)
            assert final.status == "failed" and final.error_code == "timeout", final
        assert final.snapshot == in_progress.snapshot
        assert await counts(service) == (1, 1)
    finally:
        release.set()
        await service.close()
