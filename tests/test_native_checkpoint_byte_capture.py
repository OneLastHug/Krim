"""原生bytes实际捕获、冻结能力/Registry/Schema/Hook/资源资格；不是空文本。"""

import asyncio
import copy
import hashlib
import json
import os
from dataclasses import replace

import pytest
from sqlalchemy import func, select
from test_workspace_changes import setup as old_setup

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import AgentSessionRow, NativeWorkspaceCheckpointRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession, WorkspaceCheckpointCreate
from krim_agent.tools import ShortCircuit, ToolContext
from krim_agent.tools.workspace import workspace_tools

NAME = "workspace_checkpoint_capture"
DATA = {"utf": b"hello\n", "nul": b"\0\xff", "invalid": b"\xff", "empty": b"", "absent": None}


async def setup(
    runtime, root, *, policy=None, writable=True, names=None, persistent=False, tool_timeout=10
):
    async def allow(_definition, _context):
        return "allow"

    runtime.tool_policy = policy or allow
    service = AgentService(runtime, runtime.settings)
    binding = service.register_workspace(
        "files", root, version="1", writable=writable, default=True
    )
    service.register_checkpoint_bytes()
    service.register_checkpoint_bytes()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="byte-capture",
                description="原生字节捕获",
                workspace_key="files",
                tool_names=names if names is not None else ["workspace_read", NAME],
                persistent=persistent,
                tool_timeout_seconds=tool_timeout,
            ),
        )
    )
    view = await service.spawn("byte-capture")
    return service, AgentSession(service, view.agent_id), binding


async def capture(agent, paths, key="capture-bytes", confirmed=False):
    request = WorkspaceCheckpointCreate(paths=paths, request_key=key, capture_format="bytes")
    start = await agent.create_file_checkpoint(request, confirmed=confirmed)
    final = await agent.wait_file_checkpoint(start.checkpoint_id, timeout_seconds=10)
    return request, final


async def test_real_mixed_bytes_metadata_schema_audit_key_and_no_text_or_content_leak(
    runtime, tmp_path, monkeypatch
):
    for path, data in DATA.items():
        if data is not None:
            (tmp_path / path).write_bytes(data)
    (tmp_path / "nul").chmod(0o640)
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        request, final = await capture(agent, list(DATA))
        assert final.status == "available", final
        assert final.capture_format == "bytes" and final.files[1].mode == 0o640
        assert [item.content_kind for item in final.files] == [
            "text",
            "binary",
            "binary",
            "text",
            "absent",
        ]
        values = await service.workspace_checkpoint_store.captures(
            final.scope_id, final.checkpoint_id
        )
        assert [item.snapshot.data if item.snapshot else None for item in values] == list(
            DATA.values()
        )
        audit = service._handle(agent.agent_id).runtime.tools.audit
        assert len(audit) == 5 and all(
            item.name == NAME and item.ok and item.risk == "read" for item in audit
        )
        assert (
            "hello" not in final.model_dump_json() and str(tmp_path) not in final.model_dump_json()
        )
        schemas = {tool.name: tool for tool in workspace_tools()}
        assert set(schemas) == {"workspace_read", "workspace_list", "workspace_search"}
        assert (
            schemas["workspace_read"].schema().output_schema["properties"]["content"]["type"]
            == "string"
        )
        from krim_agent.workspace_writer import WorkspaceWriter

        def no_replay(*_args, **_kwargs):
            pytest.fail("原键/冷历史不得再捕获或核实文件")

        monkeypatch.setattr(WorkspaceWriter, "capture_file", no_replay)
        monkeypatch.setattr(WorkspaceWriter, "verify_captured", no_replay)
        (tmp_path / "nul").write_bytes(b"changed")
        assert await agent.create_file_checkpoint(request) == final
        assert await agent.file_checkpoint_by_key(request.request_key) == final
        assert await agent.file_checkpoint(final.checkpoint_id) == final
        assert (await agent.file_checkpoints())[0] == final
        with pytest.raises(ServiceError) as conflict:
            await agent.create_file_checkpoint(
                WorkspaceCheckpointCreate(paths=list(DATA), request_key=request.request_key)
            )
        assert conflict.value.code == "workspace_checkpoint_key_conflict"
    finally:
        await service.close()


async def test_explicit_registration_never_upgrades_old_frozen_agent_and_no_rows_when_missing(
    runtime, tmp_path
):
    (tmp_path / "nul").write_bytes(b"\0\xff")
    service, agent, _ = await old_setup(runtime, tmp_path)
    try:
        service.register_checkpoint_bytes()
        assert NAME not in {
            item.name for item in service._handle(agent.agent_id).runtime.tools.definitions
        }
        with pytest.raises(ServiceError) as denied:
            await capture(agent, ["nul"])
        assert denied.value.code == "workspace_checkpoint_bytes_unavailable"
        async with runtime.repository.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(NativeWorkspaceCheckpointRow))
                == 0
            )
    finally:
        await service.close()


@pytest.mark.parametrize(
    "decision,confirmed,code",
    [
        ("deny", True, "permission_denied"),
        ("ask", False, "approval_required"),
        ("ask", True, None),
    ],
)
async def test_binary_capture_obeys_original_policy_and_confirmation(
    runtime, tmp_path, decision, confirmed, code
):
    (tmp_path / "nul").write_bytes(b"\0\xff")

    async def policy(_definition, _context):
        return decision

    service, agent, _ = await setup(runtime, tmp_path, policy=policy)
    try:
        _, final = await capture(agent, ["nul"], confirmed=confirmed)
        assert final.error_code == code and final.status == (
            "available" if code is None else "failed"
        )
        assert final.files[0].captured == (code is None)
    finally:
        await service.close()


@pytest.mark.parametrize(
    "kind,code",
    [
        ("symlink", "workspace_access"),
        ("hardlink", "workspace_link"),
        ("special", "workspace_checkpoint_mode"),
        ("directory", "workspace_file_type"),
    ],
)
async def test_bytes_does_not_relax_link_mode_or_regular_file_checks(runtime, tmp_path, kind, code):
    original = tmp_path / "original"
    original.write_bytes(b"\0\xff")
    target = tmp_path / "target"
    if kind == "symlink":
        target.symlink_to(original)
    elif kind == "hardlink":
        os.link(original, target)
    elif kind == "special":
        target.write_bytes(b"\0\xff")
        target.chmod(0o4640)
    else:
        target.mkdir()
    service, agent, _ = await setup(runtime, tmp_path)
    try:
        _, final = await capture(agent, ["target"])
        assert final.status == "failed" and final.error_code == code, final
        assert not final.files[0].captured
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault",
    ["path", "session", "workspace", "fence", "backend", "timeout", "short", "post", "marker"],
)
async def test_registry_rejects_pre_post_context_budget_and_response_forgery(
    runtime, tmp_path, fault
):
    (tmp_path / "nul").write_bytes(b"\0\xff")
    (tmp_path / "other").write_bytes(b"other")
    service, agent, _ = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools

    async def pre(definition, context):
        if definition.name != NAME:
            return
        if fault == "path":
            context.arguments["path"] = "other"
        elif fault == "session":
            context.session_id = "other"
        elif fault == "workspace":
            context.workspace = None
        elif fault == "fence":
            context.checkpoint_capture = replace(context.checkpoint_capture, position=1)
        elif fault == "backend":
            context.workspace_checkpoints = None
        elif fault == "timeout":
            context.timeout = 100
        elif fault == "short":
            return ShortCircuit(
                {
                    "captured": True,
                    "exists": True,
                    "path": "nul",
                    "sha256": hashlib.sha256(b"\0\xff").hexdigest(),
                    "byte_size": 2,
                    "mode": 0o644,
                    "content_kind": "binary",
                }
            )

    async def post(definition, value, context):
        if definition.name == NAME and fault in {"post", "marker"}:
            value = copy.deepcopy(value)
            value["sha256"] = "0" * 64
            if fault == "marker":
                context._checkpoint_capture_response = json.dumps(value, sort_keys=True)
        return value

    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        _, final = await capture(agent, ["nul"])
        assert final.status == "failed" and final.error_code in {
            "workspace_checkpoint_context",
            "workspace_checkpoint_unverified",
        }, final
        assert final.files[0].captured == (fault in {"post", "marker"})
        assert (tmp_path / "nul").read_bytes() == b"\0\xff"
    finally:
        await service.close()


async def test_unbound_tool_and_foreign_format_fail_before_file_access(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools
    from krim_agent.workspace_checkpoint import CheckpointCaptureFence
    from krim_agent.workspace_writer import WorkspaceWriter

    def no_access(*_args, **_kwargs):
        pytest.fail("没有原Fence不能访问文件")

    monkeypatch.setattr(WorkspaceWriter, "capture_file", no_access)
    try:
        for context in [
            ToolContext(registry, origin="model"),
            ToolContext(registry, origin="host"),
            ToolContext(
                registry,
                origin="host",
                agent_id=agent.agent_id,
                session_id=agent.session_id,
                workspace=binding,
                workspace_checkpoints=service.workspace_checkpoints,
                checkpoint_capture=CheckpointCaptureFence("unknown", 0, "token", "text"),
            ),
        ]:
            result = await registry.invoke(NAME, {"path": "nul"}, context)
            assert result.is_error and result.code == "workspace_checkpoint_context"
    finally:
        await service.close()


async def test_capture_cancel_and_reentrant_close_still_follow_original_worker(runtime, tmp_path):
    (tmp_path / "nul").write_bytes(b"\0\xff")
    service, agent, _ = await setup(runtime, tmp_path)
    registry = service._handle(agent.agent_id).runtime.tools
    seen = []

    async def stop(definition, context):
        if definition.name == NAME:
            with pytest.raises(ServiceError) as denied:
                await service.workspace_checkpoints.close()
            assert denied.value.code == "workspace_checkpoint_close_reentrant"
            seen.append(context.checkpoint_capture.checkpoint_id)
            await agent.cancel_file_checkpoint(seen[-1])

    registry.add_global_pre(stop)
    try:
        _, final = await capture(agent, ["nul"])
        assert final.status == "cancelled" and seen == [final.checkpoint_id]
        assert not final.files[0].captured
    finally:
        await service.close()


async def test_byte_capture_readonly_admission_has_no_row_or_file_access(
    runtime, tmp_path, monkeypatch
):
    from krim_agent.workspace_writer import WorkspaceWriter

    service, agent, _ = await setup(runtime, tmp_path, writable=False)

    def no_access(*_args, **_kwargs):
        pytest.fail("readonly捕获不得访问文件")

    monkeypatch.setattr(WorkspaceWriter, "capture_file", no_access)
    try:
        with pytest.raises(ServiceError) as denied:
            await capture(agent, ["nul"])
        assert denied.value.code == "workspace_read_only"
        async with runtime.repository.database.sessions() as sql:
            assert (
                await sql.scalar(select(func.count()).select_from(NativeWorkspaceCheckpointRow))
                == 0
            )
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["disabled", "version"])
async def test_byte_capture_resource_change_after_admission_prevents_io(
    runtime, tmp_path, monkeypatch, fault
):
    from krim_agent.workspace_writer import WorkspaceWriter

    service, agent, _ = await setup(runtime, tmp_path)
    failures = []

    def no_access(*_args, **_kwargs):
        pytest.fail("原资源资格失效后不得访问文件")

    async def revoke(definition, _context):
        if definition.name != NAME:
            return
        if fault == "disabled":
            service.resources.disable_workspace("files")
        else:
            try:
                database = runtime.repository.database
                async with database.write_lock, database.sessions() as sql:
                    row = await sql.get(AgentSessionRow, agent.session_id)
                    assert row is not None
                    permissions = copy.deepcopy(row.permissions)
                    permissions["workspace"]["version"] = "2"
                    row.permissions = permissions
                    await sql.commit()
            except Exception as error:
                failures.append(repr(error))
                raise

    monkeypatch.setattr(WorkspaceWriter, "capture_file", no_access)
    service._handle(agent.agent_id).runtime.tools.add_global_pre(revoke)
    try:
        _, final = await capture(agent, ["nul"])
        assert final.status == "failed" and final.error_code == (
            "workspace_unavailable" if fault == "disabled" else "workspace_binding_changed"
        ), (final, failures)
        assert not final.files[0].captured
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["cancel", "timeout"])
async def test_byte_capture_external_cancel_or_frozen_timeout_closes_waiting_hook(
    runtime, tmp_path, monkeypatch, action
):
    from krim_agent.workspace_writer import WorkspaceWriter

    service, agent, _ = await setup(runtime, tmp_path, tool_timeout=1)
    entered, release = asyncio.Event(), asyncio.Event()

    def no_access(*_args, **_kwargs):
        pytest.fail("等待中取消或超时后不得捕获")

    async def wait(definition, _context):
        if definition.name == NAME:
            entered.set()
            await release.wait()

    monkeypatch.setattr(WorkspaceWriter, "capture_file", no_access)
    registry = service._handle(agent.agent_id).runtime.tools
    registry.add_global_pre(wait)
    try:
        started = await agent.create_file_checkpoint(
            WorkspaceCheckpointCreate(paths=["nul"], request_key=action, capture_format="bytes")
        )
        await asyncio.wait_for(entered.wait(), 5)
        if action == "cancel":
            final = await agent.cancel_file_checkpoint(started.checkpoint_id)
            assert final.status == "cancelled", final
        else:
            final = await agent.wait_file_checkpoint(started.checkpoint_id, timeout_seconds=5)
            assert final.status == "failed" and final.error_code == "timeout", final
        assert not final.files[0].captured
        assert await agent.file_checkpoint_by_key(action) == final
        assert len(registry.audit) == 1 and not registry.audit[0].ok
    finally:
        release.set()
        await service.close()
