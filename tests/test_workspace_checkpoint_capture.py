"""原生检查点捕获的真实文件、权限、竞争及取消边界。"""

import asyncio
import hashlib
import json
import os
import sys
import time
from threading import Event

import pytest
from sqlalchemy import select, update
from test_workspace_changes import setup, wait_event
from test_workspace_effect_chain import apply_content

from krim_agent.persistence.database import NativeWorkspaceCheckpointRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import PythonAI, WorkspaceCheckpointCreate
from krim_agent.tools.core import ShortCircuit
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件协作锁")


async def captured(session, paths, key="capture", *, confirmed=False):
    started = await session.create_file_checkpoint(
        WorkspaceCheckpointCreate(paths=paths, request_key=key), confirmed=confirmed
    )
    return await session.wait_file_checkpoint(started.checkpoint_id, timeout_seconds=10)


def file_identity(path):
    value = path.stat()
    # 读取可以更新atime；核对内容/身份/权限和修改时间，不把访问时间当写副作用。
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


async def test_capture_complete_large_empty_and_absent_files_without_changing_them(
    runtime, tmp_path
):
    content = "完整快照\n" * 8000
    (tmp_path / "large.txt").write_text(content)
    (tmp_path / "large.txt").chmod(0o640)
    (tmp_path / "empty.txt").write_bytes(b"")
    before = {path.name: file_identity(path) for path in tmp_path.iterdir()}
    service, session, _ = await setup(runtime, tmp_path)
    try:
        result = await captured(session, ["large.txt", "empty.txt", "missing.txt"])
        assert result.status == "available", result
        assert [(item.captured, item.exists, item.byte_size) for item in result.files] == [
            (True, True, len(content.encode())),
            (True, True, 0),
            (True, False, 0),
        ]
        assert result.files[0].mode == 0o640
        assert result.files[1].sha256 == hashlib.sha256(b"").hexdigest()
        assert result.files[2].sha256 is None
        assert (
            content not in result.model_dump_json()
            and str(tmp_path) not in result.model_dump_json()
        )
        snapshots = await service.workspace_checkpoint_store.captures(
            result.scope_id, result.checkpoint_id
        )
        assert snapshots[0].snapshot.data.decode() == content
        assert snapshots[2].snapshot is None
        assert {path.name: file_identity(path) for path in tmp_path.iterdir()} == before
        assert not list(tmp_path.glob("._krim-tx-*"))
        assert (await session.file_checkpoint_by_key("capture")) == result
        assert (await session.file_checkpoints())[0] == result
        (tmp_path / "large.txt").write_text("later")
        repeated = await captured(session, ["large.txt", "empty.txt", "missing.txt"])
        assert repeated == result
        assert (
            await service.workspace_checkpoint_store.captures(result.scope_id, result.checkpoint_id)
        )[0].snapshot.data.decode() == content
        with pytest.raises(ServiceError) as conflict:
            await captured(session, ["empty.txt"])
        assert conflict.value.code == "workspace_checkpoint_key_conflict"
    finally:
        await service.close()


@pytest.mark.parametrize(
    "kind,code",
    [
        ("binary", "workspace_binary"),
        ("encoding", "workspace_encoding"),
        ("symlink", "workspace_access"),
        ("hardlink", "workspace_link"),
    ],
)
async def test_capture_rejects_unsupported_files(runtime, tmp_path, kind, code):
    file = tmp_path / "target.txt"
    if kind == "binary":
        file.write_bytes(b"abc\0xyz")
    elif kind == "encoding":
        file.write_bytes(b"\xff")
    else:
        original = tmp_path / "original.txt"
        original.write_text("original")
        if kind == "symlink":
            file.symlink_to(original)
        else:
            os.link(original, file)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        result = await captured(session, ["target.txt"])
        assert result.status == "failed" and result.error_code == code
        assert not result.files[0].captured and result.files[0].exists is None
    finally:
        await service.close()


@pytest.mark.parametrize(
    "path", ["../outside", "/tmp/outside", ".env", ".", "x/../file", "._krim-tx-hidden"]
)
def test_capture_request_rejects_unsafe_paths(path):
    with pytest.raises(ValueError):
        WorkspaceCheckpointCreate(paths=[path], request_key="request")


@pytest.mark.parametrize(
    "decision,confirmed,expected",
    [("deny", True, "permission_denied"), ("ask", False, "approval_required"), ("ask", True, None)],
)
async def test_capture_obeys_the_same_registry_policy(
    runtime, tmp_path, decision, confirmed, expected
):
    (tmp_path / "file.txt").write_text("original")

    async def policy(_definition, _context):
        return decision

    service, session, _ = await setup(runtime, tmp_path, policy=policy)
    try:
        result = await captured(session, ["file.txt"], confirmed=confirmed)
        assert result.error_code == expected
        assert result.status == ("available" if expected is None else "failed")
        assert result.files[0].captured == (expected is None)
        audit = service._handle(session.agent_id).runtime.tools.audit
        final_decision = "allow" if expected is None else decision
        assert any(
            item.name == "workspace_read" and item.decision == final_decision for item in audit
        )
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["shortcircuit", "path", "range"])
async def test_hook_cannot_forge_capture_or_change_frozen_target(runtime, tmp_path, fault):
    (tmp_path / "file.txt").write_text("original")
    (tmp_path / "other.txt").write_text("other")
    service, session, _ = await setup(runtime, tmp_path)

    async def hook(_definition, context):
        if context.checkpoint_capture:
            if fault == "shortcircuit":
                return ShortCircuit(
                    {
                        "path": "file.txt",
                        "sha256": hashlib.sha256(b"original").hexdigest(),
                        "byte_size": 8,
                        "start_line": 1,
                        "lines": 1,
                        "content": "original",
                        "truncated": False,
                    }
                )
            context.arguments = {
                **context.arguments,
                **({"path": "other.txt"} if fault == "path" else {"start_line": 2}),
            }

    service._handle(session.agent_id).runtime.tools.add_global_pre(hook)
    try:
        result = await captured(session, ["file.txt"])
        assert result.status == "failed"
        assert result.error_code == (
            "workspace_checkpoint_not_captured"
            if fault == "shortcircuit"
            else "workspace_checkpoint_target"
        )
        assert not result.files[0].captured
    finally:
        await service.close()


async def test_changes_between_reads_cannot_become_available_checkpoint(runtime, tmp_path):
    first = tmp_path / "first.txt"
    first.write_text("A")
    (tmp_path / "second.txt").write_text("B")
    service, session, _ = await setup(runtime, tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()

    async def pause(_definition, context):
        if context.checkpoint_capture and context.arguments["path"] == "second.txt":
            entered.set()
            await release.wait()

    service._handle(session.agent_id).runtime.tools.add_global_pre(pause)
    try:
        started = await session.create_file_checkpoint(
            WorkspaceCheckpointCreate(paths=["first.txt", "second.txt"], request_key="capture")
        )
        await asyncio.wait_for(entered.wait(), 5)
        await apply_content(session, first, "changed", "change")
        release.set()
        result = await session.wait_file_checkpoint(started.checkpoint_id)
        assert result.status == "failed" and result.error_code == "workspace_checkpoint_changed"
        assert first.read_text() == "changed"
    finally:
        release.set()
        await service.close()


async def test_capture_context_cannot_invoke_nested_file_modification(runtime, tmp_path):
    from test_workspace_changes import action, preview

    file = tmp_path / "file.txt"
    file.write_text("original")
    service, session, _ = await setup(runtime, tmp_path)
    try:
        proposal = await preview(session, file, content="changed")

        async def hook(_definition, context):
            if context.checkpoint_capture and _definition.name == "workspace_read":
                await context.call_tool("workspace_change", action(proposal).model_dump())

        service._handle(session.agent_id).runtime.tools.add_global_pre(hook)
        result = await captured(session, ["file.txt"])
        assert result.status == "failed" and result.error_code == "workspace_checkpoint_action"
        assert file.read_text() == "original"
        assert (await session.file_change(proposal.proposal_id)).status == "proposed"
    finally:
        await service.close()


@pytest.mark.parametrize("stop", ["cancel", "expire"])
async def test_late_capture_before_file_lock_cannot_append_after_parent_is_stopped(
    runtime, tmp_path, monkeypatch, stop
):
    (tmp_path / "file.txt").write_text("original")
    service, session, _ = await setup(runtime, tmp_path)
    original = WorkspaceWriter.capture_file
    entered, release = Event(), Event()

    def paused(self, *args, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(WorkspaceWriter, "capture_file", paused)
    cancellation = None
    try:
        started = await session.create_file_checkpoint(
            WorkspaceCheckpointCreate(paths=["file.txt"], request_key="capture")
        )
        await wait_event(entered)
        if stop == "cancel":
            await service.workspace_checkpoint_store.cancel(
                f"session:{session.session_id}", started.checkpoint_id
            )
            cancellation = asyncio.create_task(
                session.cancel_file_checkpoint(started.checkpoint_id)
            )
        else:
            async with (
                service.workspace_edits.database.write_lock,
                service.workspace_edits.database.sessions.begin() as sql,
            ):
                await sql.execute(
                    update(NativeWorkspaceCheckpointRow)
                    .where(NativeWorkspaceCheckpointRow.checkpoint_id == started.checkpoint_id)
                    .values(lease_expires_at=time.time() - 1)
                )
            assert (await session.file_checkpoint(started.checkpoint_id)).status == "interrupted"
        release.set()
        if cancellation:
            await cancellation
        await asyncio.gather(
            *(value[1] for value in service.workspace_checkpoints._tasks.values()),
            return_exceptions=True,
        )
        result = await session.file_checkpoint(started.checkpoint_id)
        assert result.status == ("cancelled" if stop == "cancel" else "interrupted")
        assert not result.files[0].captured
        assert (tmp_path / "file.txt").read_text() == "original"
    finally:
        release.set()
        if cancellation:
            await asyncio.gather(cancellation, return_exceptions=True)
        await service.close()


async def test_capture_returned_view_cannot_mutate_worker_and_shared_close_guard(tmp_path):
    (tmp_path / "file.txt").write_text("original")
    async with await PythonAI.create(workspace_root=tmp_path, workspace_writable=True) as ai:
        session = await ai.agent()
        service = session.service
        entered, release = asyncio.Event(), asyncio.Event()

        async def hook(_definition, context):
            if context.checkpoint_capture:
                entered.set()
                await release.wait()
                with pytest.raises(ServiceError) as guarded:
                    await ai.close()
                assert guarded.value.code == "workspace_batch_close_reentrant"
                assert not ai._closed and not service._closed

        service._handle(session.agent_id).runtime.tools.add_global_pre(hook)
        started = await session.create_file_checkpoint(
            WorkspaceCheckpointCreate(paths=["file.txt"], request_key="capture")
        )
        identifier = started.checkpoint_id
        await asyncio.wait_for(entered.wait(), 5)
        started.checkpoint_id = "caller-mutated"
        started.files[0].path = "other.txt"
        release.set()
        result = await session.wait_file_checkpoint(identifier)
        assert result.status == "available", result
        assert result.files[0].path == "file.txt"
        await asyncio.gather(*(value[1] for value in service.workspace_checkpoints._tasks.values()))
        assert not service.workspace_checkpoints._tasks


async def test_capture_callback_can_cancel_itself_without_deadlock(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("original")
    service, session, _ = await setup(runtime, tmp_path)

    async def hook(_definition, context):
        if context.checkpoint_capture:
            await session.cancel_file_checkpoint(context.checkpoint_capture.checkpoint_id)

    service._handle(session.agent_id).runtime.tools.add_global_pre(hook)
    try:
        result = await asyncio.wait_for(captured(session, ["file.txt"]), 5)
        assert result.status == "cancelled"
        assert not result.files[0].captured
    finally:
        await service.close()


async def test_checkpoint_closed_history_scope_and_resource_limits(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("x" * 2048)
    service, session, _ = await setup(runtime, tmp_path)
    try:
        service.workspace_checkpoint_store.max_snapshot_bytes = 1024
        denied = await captured(session, ["file.txt"], "large")
        assert denied.status == "failed" and denied.error_code == "workspace_checkpoint_bytes"
        service.workspace_checkpoint_store.max_snapshot_bytes = 4096
        saved = await captured(session, ["file.txt"], "small")
        assert saved.status == "available"
        view = await service.session(session.agent_id)
        await service.close_session(session.agent_id, view.revision)
        assert (await session.file_checkpoint(saved.checkpoint_id)) == saved
        assert len(await session.file_checkpoints()) == 2
        from krim_agent.sdk import AgentSession

        other = AgentSession(service, (await service.spawn("default")).agent_id)
        with pytest.raises(ServiceError) as hidden:
            await other.file_checkpoint(saved.checkpoint_id)
        assert hidden.value.code == "workspace_checkpoint_not_found"
        async with (
            runtime.repository.database.write_lock,
            runtime.repository.database.sessions() as sql,
        ):
            rows = list(await sql.scalars(select(NativeWorkspaceCheckpointRow)))
            assert len(rows) == 2 and all(row.owner_id is None for row in rows)
        assert "content" not in json.loads(saved.model_dump_json())
    finally:
        await service.close()
