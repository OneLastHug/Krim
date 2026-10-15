"""完整目录生产捕获、原Registry/权限/私有资格和不可重放的持久任务。"""

import asyncio
import copy
import json
from dataclasses import replace
from threading import Event

import pytest
from sqlalchemy import func, select

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import (
    AgentSessionRow,
    WorkspaceDirectoryCaptureRow,
    WorkspaceDirectoryCheckpointRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession, DirectoryCheckpointCreate
from krim_agent.tools import ShortCircuit, ToolContext
from krim_agent.workspace_directory_checkpoint import DirectoryCheckpointCaptureFence
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

NAME = "workspace_directory_checkpoint_capture"


def make_tree(root):
    tree = root / "tree"
    tree.mkdir()
    (tree / "nested").mkdir()
    (tree / "empty-dir").mkdir()
    (tree / "a.txt").write_bytes(b"private synthetic text\n")
    (tree / "nested/nul").write_bytes(b"\0\xff")
    (tree / "empty").write_bytes(b"")
    (tree / "nested/nul").chmod(0o640)
    return tree


async def setup(
    runtime, root, *, policy=None, names=None, writable=True, tool_timeout=10, persistent=False
):
    async def allow(_definition, _context):
        return "allow"

    runtime.tool_policy = policy or allow
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace(
        "files", root, version="1", writable=writable, default=True
    )
    service.register_directory_checkpoints()
    service.register_directory_checkpoints()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="directory-capture",
                description="完整目录捕获",
                workspace_key="files",
                tool_names=[NAME] if names is None else names,
                tool_timeout_seconds=tool_timeout,
                persistent=persistent,
            ),
        )
    )
    view = await service.spawn("directory-capture")
    return service, AgentSession(service, view.agent_id), binding


async def capture(agent, key="capture", *, confirmed=False):
    request = DirectoryCheckpointCreate(root_path="tree", request_key=key)
    started = await agent.create_directory_checkpoint(request, confirmed=confirmed)
    final = await agent.wait_directory_checkpoint(started.checkpoint_id, timeout_seconds=10)
    return request, final


async def counts(service):
    async with service.directory_checkpoint_store.database.sessions() as sql:
        return tuple(
            [
                await sql.scalar(select(func.count()).select_from(table))
                for table in (
                    WorkspaceDirectoryCaptureRow,
                    WorkspaceDirectoryCheckpointRow,
                )
            ]
        )


async def test_actual_full_directory_material_source_metadata_and_cold_keys(
    runtime, tmp_path, monkeypatch
):
    tree = make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        request, final = await capture(agent)
        assert final.status == "available", final
        assert final.source == "workspace_directory" and final.snapshot.entry_count == 6
        assert (final.snapshot.file_count, final.snapshot.directory_count) == (3, 3)
        assert final.snapshot.byte_size == len(b"private synthetic text\n\0\xff")
        assert await counts(service) == (1, 1)
        async with runtime.repository.database.sessions() as sql:
            row = await sql.get(WorkspaceDirectoryCaptureRow, final.checkpoint_id)
            stored = await service.directory_checkpoint_store._material(sql, row)
            assert row.material_id != final.checkpoint_id and row.owner_id is None
            assert stored.snapshot.root_path == "tree"
            files = {entry.path: entry.file for entry in stored.snapshot.entries if entry.file}
            assert files["tree/nested/nul"].data == b"\0\xff"
            assert files["tree/nested/nul"].mode == 0o640
        text = final.model_dump_json()
        assert all(
            value not in text
            for value in (str(tmp_path), "private synthetic", "material_id", "token", row.token)
        )
        registry = service._handle(agent.agent_id).runtime.tools
        assert len(registry.audit) == 1 and registry.audit[0].ok and registry.audit[0].name == NAME

        def no_read(*_args, **_kwargs):
            pytest.fail("同原键、冷历史和取消终态不能重新读取工作区")

        monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", no_read)
        (tree / "nested/nul").unlink()
        assert await agent.create_directory_checkpoint(request) == final
        assert await agent.directory_checkpoint_by_key(request.request_key) == final
        assert await agent.directory_checkpoint(final.checkpoint_id) == final
        assert await agent.directory_checkpoints(limit=1) == [final]
        assert await agent.cancel_directory_checkpoint(final.checkpoint_id) == final
        with pytest.raises(ServiceError) as conflict:
            await agent.create_directory_checkpoint(request.model_copy(update={"title": "变参"}))
        assert conflict.value.code == "workspace_directory_capture_key_conflict"
        other = await service.spawn("directory-capture")
        with pytest.raises(ServiceError) as foreign:
            await service.workspace_directory_checkpoint(other.agent_id, final.checkpoint_id)
        assert foreign.value.code == "workspace_directory_capture_not_found"
        service.disable_workspace("files")
        current = await service.session(agent.agent_id)
        await service.close_session(agent.agent_id, current.revision)
        assert await agent.directory_checkpoint(final.checkpoint_id) == final
        assert await agent.directory_checkpoint_by_key(request.request_key) == final
        assert await counts(service) == (1, 1)
    finally:
        await service.close()


@pytest.mark.parametrize(
    "decision,confirmed,code",
    [("deny", True, "permission_denied"), ("ask", False, "approval_required"), ("ask", True, None)],
)
async def test_capture_obeys_policy_and_confirmation(runtime, tmp_path, decision, confirmed, code):
    make_tree(tmp_path)

    async def policy(_definition, _context):
        return decision

    service, agent, _ = await setup(runtime, tmp_path, policy=policy)
    try:
        _, final = await capture(agent, confirmed=confirmed)
        assert final.status == ("available" if code is None else "failed"), final
        assert final.error_code == code
        assert await counts(service) == (1, 1 if code is None else 0)
    finally:
        await service.close()


@pytest.mark.parametrize(
    "kind,code",
    [("frozen", "workspace_directory_capture_unavailable"), ("readonly", "workspace_read_only")],
)
async def test_no_frozen_capability_upgrade_or_readonly_rows(
    runtime, tmp_path, monkeypatch, kind, code
):
    service, agent, _ = await setup(
        runtime,
        tmp_path,
        names=["workspace_read"] if kind == "frozen" else None,
        writable=kind != "readonly",
    )

    def no_read(*_args, **_kwargs):
        pytest.fail("原能力未授予时不得读取")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", no_read)
    try:
        with pytest.raises(ServiceError) as denied:
            await capture(agent)
        assert denied.value.code == code
        assert await counts(service) == (0, 0)
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault",
    [
        "path",
        "session",
        "backend",
        "fence",
        "bad-fence",
        "timeout",
        "deadline",
        "short",
        "post",
        "marker",
    ],
)
async def test_original_context_and_complete_material_cannot_be_forged_by_hooks(
    runtime, tmp_path, fault
):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools
    seen_status = []

    async def pre(_definition, context):
        if fault == "path":
            context.arguments["root_path"] = "other"
        elif fault == "session":
            context.session_id = "other"
        elif fault == "backend":
            context.workspace_directory_checkpoints = None
        elif fault == "fence":
            context.directory_checkpoint_capture = replace(
                context.directory_checkpoint_capture, token="0" * 64
            )
        elif fault == "bad-fence":
            context.directory_checkpoint_capture = object()
        elif fault == "timeout":
            context.timeout = 100
        elif fault == "deadline":
            context.execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 100
            )
        elif fault == "short":
            return ShortCircuit({"captured": True})

    async def post(_definition, value, context):
        view = await agent.directory_checkpoint(context.directory_checkpoint_capture.checkpoint_id)
        seen_status.append(view.status)
        if fault in {"post", "marker"}:
            value = copy.deepcopy(value)
            value["snapshot_fingerprint"] = "0" * 64
            if fault == "marker":
                context._directory_checkpoint_capture_response = json.dumps(value, sort_keys=True)
        return value

    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        _, final = await capture(agent)
        assert final.status == "failed", final
        assert final.error_code in {
            "workspace_directory_capture_context",
            "workspace_directory_capture_unverified",
        }, final
        assert seen_status == (["capturing"] if fault in {"post", "marker"} else [])
        assert await counts(service) == (1, 1 if fault in {"post", "marker"} else 0)
        assert len(registry.audit) == 1 and not registry.audit[0].ok
    finally:
        await service.close()


class FalseFence:
    def __bool__(self):
        return False


@pytest.mark.parametrize(
    "fence", [None, object(), FalseFence(), DirectoryCheckpointCaptureFence("bad", "0" * 64)]
)
async def test_unbound_model_and_bad_fences_fail_closed_with_audit(
    runtime, tmp_path, monkeypatch, fence
):
    service, agent, binding = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools

    def no_read(*_args, **_kwargs):
        pytest.fail("模型或坏Fence不得进入Reader")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", no_read)
    try:
        for origin in ("host", "model"):
            context = ToolContext(
                registry,
                topic_id=service._handle(agent.agent_id).topic_id,
                origin=origin,
                run_id="local",
                agent_id=agent.agent_id,
                session_id=agent.session_id,
                workspace=binding,
                workspace_directory_checkpoints=service.directory_checkpoints,
                directory_checkpoint_capture=fence,
            )
            result = await registry.invoke(NAME, {"root_path": "tree"}, context)
            assert result.is_error and result.code in {
                "workspace_directory_capture_context",
                "workspace_directory_capture_not_found",
            }, result
        assert len(registry.audit) == 2 and all(not item.ok for item in registry.audit)
        assert await counts(service) == (0, 0)
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["cancel", "timeout", "disable", "self-cancel"])
async def test_waiting_hook_cancel_timeout_revocation_and_self_close(
    runtime, tmp_path, monkeypatch, action
):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path, tool_timeout=1)
    registry = service._handle(agent.agent_id).runtime.tools
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait(_definition, context):
        entered.set()
        if action == "self-cancel":
            for close in (service.close, service.directory_checkpoints.close):
                with pytest.raises(ServiceError) as rejected:
                    await close()
                assert rejected.value.code == "workspace_directory_capture_reentrant"
            await agent.cancel_directory_checkpoint(
                context.directory_checkpoint_capture.checkpoint_id
            )
        else:
            await release.wait()

    def no_read(*_args, **_kwargs):
        pytest.fail("原捕获已取消、超时或撤权，不得开始IO")

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", no_read)
    registry.add_global_pre(wait)
    try:
        view = await agent.create_directory_checkpoint(
            DirectoryCheckpointCreate(root_path="tree", request_key=action)
        )
        await asyncio.wait_for(entered.wait(), 5)
        if action == "cancel":
            final = await agent.cancel_directory_checkpoint(view.checkpoint_id)
            assert final.status == "cancelled"
        else:
            if action == "disable":
                service.disable_workspace("files")
                release.set()
            final = await agent.wait_directory_checkpoint(view.checkpoint_id, timeout_seconds=5)
            assert final.status == ("cancelled" if action == "self-cancel" else "failed"), final
        assert (
            final.error_code
            == {
                "cancel": "workspace_directory_capture_cancelled",
                "self-cancel": "workspace_directory_capture_cancelled",
                "timeout": "timeout",
                "disable": "workspace_unavailable",
            }[action]
        )
        assert final.snapshot is None and await counts(service) == (1, 0)
        assert await agent.directory_checkpoint_by_key(action) == final
    finally:
        release.set()
        await service.close()


async def test_cancel_drains_actual_io_thread_before_return(runtime, tmp_path, monkeypatch):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path)
    entered, exited = Event(), Event()

    def capture_wait(_self, _path, *, authorize, cancelled):
        entered.set()
        try:
            assert entered.wait(1)
            while not cancelled():
                authorize()
        finally:
            exited.set()
        raise ServiceError("workspace_tree_snapshot_cancelled", "测试协作取消", 409)

    monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", capture_wait)
    try:
        view = await agent.create_directory_checkpoint(
            DirectoryCheckpointCreate(root_path="tree", request_key="thread")
        )
        assert await asyncio.to_thread(entered.wait, 5)
        final = await asyncio.wait_for(agent.cancel_directory_checkpoint(view.checkpoint_id), 5)
        assert final.status == "cancelled" and exited.is_set()
        assert await counts(service) == (1, 0)
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["resource", "owner", "closed"])
async def test_revocation_after_private_material_never_becomes_available_and_worker_settles(
    runtime, tmp_path, fault
):
    make_tree(tmp_path)
    service, agent, _ = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools

    async def revoke(_definition, value, _context):
        if fault == "closed":
            session = await service.session(agent.agent_id)
            await service.close_session(agent.agent_id, session.revision)
        else:
            async with (
                runtime.repository.database.write_lock,
                runtime.repository.database.sessions() as sql,
                sql.begin(),
            ):
                row = await sql.get(AgentSessionRow, agent.session_id)
                if fault == "owner":
                    row.owner_id = "other-owner"
                else:
                    permissions = copy.deepcopy(row.permissions)
                    permissions["workspace"]["fingerprint"] = "0" * 64
                    row.permissions = permissions
        return value

    registry.add_global_post(revoke)
    try:
        started = await agent.create_directory_checkpoint(
            DirectoryCheckpointCreate(root_path="tree", request_key=fault)
        )
        entry = service.directory_checkpoints._tasks.get(started.checkpoint_id)
        if entry is not None:
            await asyncio.wait_for(asyncio.gather(entry[1], return_exceptions=True), 10)
        async with runtime.repository.database.sessions() as sql:
            row = await sql.get(WorkspaceDirectoryCaptureRow, started.checkpoint_id)
            assert row.status == "failed" and row.material_id is not None
        assert await counts(service) == (1, 1)
    finally:
        if fault == "owner":
            async with runtime.repository.database.sessions() as sql, sql.begin():
                row = await sql.get(AgentSessionRow, agent.session_id)
                row.owner_id = service.manager.owner_id
        await service.close()
