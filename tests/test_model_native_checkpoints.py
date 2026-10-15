"""原模型Run/call同调用真实捕获与旧SDK/HTTP持久消费，不把Fence拒绝算成功。"""

import asyncio
import copy
import json
from threading import Event

import httpx
import pytest
from sqlalchemy import func, select

import krim_agent.api.server as server_module
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import (
    NativeCheckpointModelSourceRow,
    NativeWorkspaceCheckpointRow,
    ToolAuditRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import AgentSession
from krim_agent.tools import ShortCircuit
from krim_agent.usage import TokenUsage
from krim_agent.workspace_checkpoint import (
    CheckpointModelSource,
    WorkspaceCheckpointCreate,
    WorkspaceCheckpointView,
)
from krim_agent.workspace_writer import WorkspaceWriter

NAMES = [
    "workspace_read",
    "workspace_checkpoint_capture",
    "workspace_checkpoint_create",
    "workspace_checkpoint_query",
    "workspace_checkpoint_cancel",
]


class Provider:
    name = "offline-model-native-checkpoint"

    def __init__(self, capture_format="bytes"):
        self.calls = 0
        self.capture_format = capture_format
        self.created = None
        self.queried = None

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(
                id="original-checkpoint-create",
                name="workspace_checkpoint_create",
                arguments={
                    "paths": ["plain.txt", "missing.bin"]
                    if self.capture_format == "text"
                    else ["plain.txt", "binary.bin", "missing.bin"],
                    "request_key": "model-native-snapshot",
                    "capture_format": self.capture_format,
                },
            )
        elif self.calls == 2:
            self.created = WorkspaceCheckpointView.model_validate_json(messages[-1].content)
            yield ModelToolCall(
                id="original-checkpoint-query",
                name="workspace_checkpoint_query",
                arguments={
                    "kind": "id",
                    "checkpoint_id": self.created.checkpoint_id,
                },
            )
        else:
            self.queried = json.loads(messages[-1].content)["checkpoint"]
            yield ModelText(text="原模型检查点已完成")
            yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=1, total_tokens=6))


async def setup(
    runtime,
    root,
    provider,
    *,
    mode="agent",
    policy=None,
    writable=True,
    names=None,
    persistent=False,
    tool_timeout=30,
):
    async def allow(*_args):
        return "allow"

    runtime.provider = provider
    runtime.tool_policy = policy or allow
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, writable=writable, version="1", default=True)
    service.register_checkpoint_bytes()
    service.register_checkpoint_controls()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="model-checkpoint",
                description="模型原检查点",
                workspace_key="files",
                tool_names=names if names is not None else NAMES,
                mode=mode,
                persistent=persistent,
                tool_timeout_seconds=tool_timeout,
            ),
        )
    )
    return service, AgentSession(service, (await service.spawn("model-checkpoint")).agent_id)


class OneShotProvider:
    name = "offline-checkpoint-control-boundary"

    def __init__(self, name="workspace_checkpoint_create", arguments=None):
        self.name_to_call = name
        self.arguments = arguments or {
            "paths": ["plain.txt"],
            "request_key": "one-root",
            "capture_format": "bytes",
        }
        self.calls = 0
        self.reply = None

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(
                id="one-root-call", name=self.name_to_call, arguments=self.arguments
            )
        else:
            self.reply = messages[-1].content
            yield ModelText(text="原控制结果已读取")


async def source_count(runtime):
    async with runtime.repository.database.sessions() as sql:
        return await sql.scalar(select(func.count()).select_from(NativeCheckpointModelSourceRow))


async def saved_audits(runtime, run_id):
    async with runtime.repository.database.sessions() as sql:
        return list(
            await sql.scalars(select(ToolAuditRow.payload).where(ToolAuditRow.run_id == run_id))
        )


@pytest.mark.parametrize(
    "mode,decision,approve,created",
    [
        ("agent", "deny", True, False),
        ("agent", "ask", False, False),
        ("plan", "allow", True, False),
        ("agent", "ask", True, True),
    ],
)
async def test_model_permission_gate_precedes_source_and_capture(
    runtime, tmp_path, monkeypatch, mode, decision, approve, created
):
    (tmp_path / "plain.txt").write_text("permission")

    async def policy(definition, _context):
        return decision if definition.name == "workspace_checkpoint_create" else "allow"

    service, agent = await setup(runtime, tmp_path, OneShotProvider(), mode=mode, policy=policy)

    async def approval(_definition, _context):
        return approve

    service._handle(agent.agent_id).runtime.approval_handler = approval
    try:
        if not created:

            def forbidden(*_args, **_kwargs):
                pytest.fail("未授权模型控制不得捕获文件")

            monkeypatch.setattr(WorkspaceWriter, "capture_file", forbidden)
        result = await agent.execute("权限边界", idempotency_key="permission")
        assert result.status == "completed", result
        assert await source_count(runtime) == int(created)
        saved = await agent.file_checkpoint_by_key("one-root")
        assert (saved is not None) == created
        if created:
            assert saved.status == "available"
        else:
            audits = await saved_audits(runtime, result.run_id)
            assert any(item["code"] == "permission_denied" for item in audits)
    finally:
        await service.close()


async def test_nested_real_ask_pauses_original_budget_without_forgery_rejection(runtime, tmp_path):
    (tmp_path / "plain.txt").write_text("approval")

    async def policy(definition, _context):
        return "ask" if definition.name == "workspace_checkpoint_capture" else "allow"

    service, agent = await setup(runtime, tmp_path, OneShotProvider(), policy=policy)
    paused = []

    async def approval(definition, context):
        assert definition.name == "workspace_checkpoint_capture" and context.depth == 2
        paused.append(context.execution_timeouts[0])
        assert context.execution_timeouts[0].deadline.when() is None
        # 覆盖原0.2秒续租检查发生在真实审批暂停中的窗口。
        await asyncio.sleep(0.25)
        return True

    service._handle(agent.agent_id).runtime.approval_handler = approval
    try:
        result = await agent.execute("内部读取需批准", idempotency_key="approval")
        saved = await agent.file_checkpoint_by_key("one-root")
        assert result.status == "completed" and saved.status == "available", (result, saved)
        assert len(paused) == 1 and paused[0].pause_extension >= 0.25
        assert await source_count(runtime) == 1
    finally:
        await service.close()


@pytest.mark.parametrize(
    "target,phase,fault",
    [
        ("root", "pre", "arguments"),
        ("root", "pre", "origin"),
        ("root", "pre", "run"),
        ("root", "pre", "call"),
        ("root", "pre", "session"),
        ("root", "pre", "backend"),
        ("root", "pre", "budget"),
        ("root", "pre", "short"),
        ("root", "post", "data"),
        ("root", "post", "marker"),
        ("root", "post", "budget"),
        ("leaf", "pre", "arguments"),
        ("leaf", "pre", "origin"),
        ("leaf", "pre", "source_call"),
        ("leaf", "pre", "budget"),
        ("leaf", "post", "data"),
    ],
)
async def test_root_and_internal_capture_hooks_cannot_forge_source_budget_or_facts(
    runtime, tmp_path, target, phase, fault
):
    (tmp_path / "plain.txt").write_text("actual")
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    registry = service._handle(agent.agent_id).runtime.tools
    selected = "workspace_checkpoint_create" if target == "root" else "workspace_checkpoint_capture"

    def corrupt(context):
        if fault == "arguments":
            if target == "root":
                context.arguments["paths"] = ["other.txt"]
            else:
                context.arguments["path"] = "other.txt"
        elif fault == "origin":
            context.origin = "host"
        elif fault == "run":
            context.run_id = "another-run"
        elif fault == "call":
            context.call_id = "another-call"
        elif fault == "session":
            context.session_id = "another-session"
        elif fault == "source_call":
            context.source_call_id = "another-parent-call"
        elif fault == "backend":
            context.workspace_checkpoint_controls = object()
        elif fault == "budget":
            context.execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 60
            )

    async def pre(definition, context):
        if definition.name == selected and phase == "pre":
            if fault == "short":
                return ShortCircuit({})
            corrupt(context)

    async def post(definition, value, context):
        if definition.name == selected and phase == "post":
            if fault == "budget":
                corrupt(context)
            else:
                value = copy.deepcopy(value)
                value["request_key" if target == "root" else "sha256"] = "f" * 64
                if fault == "marker":
                    context._checkpoint_control_response = (
                        "create",
                        json.dumps(value, sort_keys=True),
                    )
        return value

    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        result = await agent.execute("真实Hook边界", idempotency_key="hook")
        assert result.status == "completed", result
        saved = await agent.file_checkpoint_by_key("one-root")
        if target == "root" and phase == "pre":
            assert saved is None and await source_count(runtime) == 0
        elif target == "leaf":
            assert saved.status == "failed" and await source_count(runtime) == 1
            assert saved.error_code in {
                "workspace_checkpoint_model_context",
                "workspace_checkpoint_model_unverified",
            }
        else:
            assert saved.status == "available" and saved.request_key == "one-root"
            assert await source_count(runtime) == 1
        audits = await saved_audits(runtime, result.run_id)
        assert all(item["run_id"] == result.run_id for item in audits)
        if target == "root":
            assert any(
                item["name"] == selected and item["call_id"] == "one-root-call" for item in audits
            )
        assert any(
            not item["ok"]
            and item["code"]
            in {"workspace_checkpoint_model_context", "workspace_checkpoint_model_unverified"}
            for item in audits
        )
    finally:
        await service.close()


async def test_model_direct_byte_metadata_still_requires_private_fence(runtime, tmp_path):
    (tmp_path / "plain.txt").write_text("private")
    service, agent = await setup(
        runtime, tmp_path, OneShotProvider("workspace_checkpoint_capture", {"path": "plain.txt"})
    )
    try:
        result = await agent.execute("无Fence不得捕获", idempotency_key="no-fence")
        assert result.status == "completed" and await source_count(runtime) == 0
        audits = await saved_audits(runtime, result.run_id)
        assert any(
            item["code"] == "workspace_checkpoint_context" and not item["ok"] for item in audits
        )
    finally:
        await service.close()


async def test_model_stop_closes_capture_admission_and_drains_original_worker(runtime, tmp_path):
    (tmp_path / "plain.txt").write_text("cancel")
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    entered = asyncio.Event()

    async def pre(definition, _context):
        if definition.name == "workspace_checkpoint_capture":
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(pre)
    task = asyncio.create_task(agent.execute("停止原捕获", idempotency_key="cancel"))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        await agent.stop()
        result = await asyncio.wait_for(task, 10)
        saved = await agent.file_checkpoint_by_key("one-root")
        assert result.status == "aborted" and saved.status == "cancelled", (result, saved)
        assert not service.workspace_checkpoints._tasks and not service.model_checkpoints._calls
        assert all(not item.captured for item in saved.files)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("phase", ["root", "leaf"])
@pytest.mark.parametrize("action", ["service", "runtime", "session", "destroy", "stop"])
async def test_self_close_callbacks_refuse_before_original_session_or_runtime_changes(
    runtime, tmp_path, phase, action
):
    (tmp_path / "plain.txt").write_text("close-scope")
    service, agent = await setup(runtime, tmp_path, OneShotProvider())
    called = []
    handle = service._handle(agent.agent_id)

    async def close_original():
        if action == "service":
            await service.close()
        elif action == "runtime":
            await handle.runtime.close()
        elif action == "session":
            await agent.close()
        elif action == "destroy":
            await agent.destroy()
        else:
            await agent.stop()

    async def pre(definition, _context):
        selected = (
            "workspace_checkpoint_create" if phase == "root" else "workspace_checkpoint_capture"
        )
        if definition.name == selected:
            with pytest.raises(ServiceError) as rejected:
                await close_original()
            assert rejected.value.code == "workspace_checkpoint_model_reentrant"
            called.append(action)

    handle.runtime.tools.add_global_pre(pre)
    try:
        result = await agent.execute("回调关闭边界", idempotency_key="self-close")
        saved = await agent.file_checkpoint_by_key("one-root")
        assert result.status == "completed" and saved.status == "available", (result, saved)
        assert called == [action]
        assert not service._closed and not handle.runtime._closed and not handle.destroyed
        assert (await agent.state()).status == "open"
    finally:
        await service.close()


async def test_actual_model_cancel_drains_original_host_capture_and_keeps_id(runtime, tmp_path):
    (tmp_path / "plain.txt").write_text("cancel-existing")
    provider = OneShotProvider("workspace_checkpoint_cancel")
    service, agent = await setup(runtime, tmp_path, provider)
    entered = asyncio.Event()

    async def pre(definition, context):
        if definition.name == "workspace_checkpoint_capture" and context.origin == "host":
            entered.set()
            await asyncio.Event().wait()

    service._handle(agent.agent_id).runtime.tools.add_global_pre(pre)
    try:
        start = await agent.create_file_checkpoint(
            WorkspaceCheckpointCreate(
                paths=["plain.txt"], request_key="host-active", capture_format="bytes"
            )
        )
        await asyncio.wait_for(entered.wait(), 10)
        provider.arguments = {"checkpoint_id": start.checkpoint_id}
        result = await agent.execute("取消原检查点", idempotency_key="cancel-existing")
        view = await agent.file_checkpoint(start.checkpoint_id)
        assert result.status == "completed" and view.status == "cancelled", (result, view)
        assert json.loads(provider.reply) == view.model_dump(mode="json")
        assert not service.workspace_checkpoints._tasks and await source_count(runtime) == 0
    finally:
        await service.close()


async def test_natural_tool_timeout_waits_for_physical_capture_thread_and_closes_admission(
    runtime, tmp_path, monkeypatch
):
    (tmp_path / "plain.txt").write_text("original-timeout")
    service, agent = await setup(runtime, tmp_path, OneShotProvider(), tool_timeout=1)
    entered, release = Event(), Event()
    original = WorkspaceWriter.capture_file

    def delayed(writer, *args, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(writer, *args, **kwargs)

    monkeypatch.setattr(WorkspaceWriter, "capture_file", delayed)
    task = asyncio.create_task(agent.execute("真实线程超时", idempotency_key="physical-timeout"))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        await asyncio.sleep(1.2)
        assert not task.done(), "原调用必须等待自己的真实文件线程收束"
        view = await agent.file_checkpoint_by_key("one-root")
        assert view.status != "available" and all(not item.captured for item in view.files)
        release.set()
        result = await asyncio.wait_for(task, 10)
        final = await agent.file_checkpoint(view.checkpoint_id)
        assert result.status == "completed" and final.status in {"cancelled", "failed"}, (
            result,
            final,
        )
        assert all(not item.captured for item in final.files)
        assert not service.workspace_checkpoints._tasks and not service.model_checkpoints._calls
        audits = await saved_audits(runtime, result.run_id)
        assert any(
            item["name"] == "workspace_checkpoint_create" and item["code"] == "timeout"
            for item in audits
        )
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await service.close()


async def test_new_model_run_queries_old_checkpoint_after_workspace_root_closed_without_capture(
    runtime, tmp_path, monkeypatch
):
    (tmp_path / "plain.txt").write_text("frozen-history")
    provider = OneShotProvider()
    service, agent = await setup(runtime, tmp_path, provider)
    try:
        first = await agent.execute("原模型捕获", idempotency_key="history-create")
        original = await agent.file_checkpoint_by_key("one-root")
        assert first.status == "completed" and original.status == "available"
        service.disable_workspace("files")
        service.resources.workspaces["files"].binding.workspace.close()

        def no_capture(*_args, **_kwargs):
            pytest.fail("只读原历史不应依赖原根FD或重新捕获")

        monkeypatch.setattr(WorkspaceWriter, "capture_file", no_capture)
        provider.calls = 0
        provider.name_to_call = "workspace_checkpoint_query"
        provider.arguments = {"kind": "key", "request_key": "one-root"}
        second = await agent.execute("仅查询原检查点", idempotency_key="history-query")
        assert second.status == "completed" and second.run_id != first.run_id
        audits = await saved_audits(runtime, second.run_id)
        assert any(
            item["name"] == "workspace_checkpoint_query" and item["ok"] for item in audits
        ), provider.reply
        assert json.loads(provider.reply)["checkpoint"] == original.model_dump(mode="json")
        assert await source_count(runtime) == 1
    finally:
        await service.close()


async def test_readonly_profile_refuses_model_create_before_source_or_file_access(
    runtime, tmp_path
):
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", tmp_path, writable=False, version="1")
    service.register_checkpoint_controls()
    try:
        with pytest.raises(ServiceError) as readonly:
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="readonly-model-checkpoint",
                        description="只读资源不能创建",
                        workspace_key="files",
                        tool_names=["workspace_read", "workspace_checkpoint_create"],
                    ),
                )
            )
        assert readonly.value.code == "workspace_read_only"
        assert await source_count(runtime) == 0
    finally:
        await service.close()


async def test_frozen_agent_tool_schemas_do_not_expand_when_model_controls_register(
    runtime, tmp_path
):
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", tmp_path, writable=True, version="1")
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="old-checkpoint-reader",
                description="旧冻结能力",
                workspace_key="files",
                tool_names=["workspace_read"],
            ),
        )
    )
    old = await service.spawn("old-checkpoint-reader")
    registry = service._handle(old.agent_id).runtime.tools
    before = [item.schema().model_dump(mode="json") for item in registry.definitions]
    try:
        service.register_checkpoint_bytes()
        service.register_checkpoint_controls()
        assert [item.schema().model_dump(mode="json") for item in registry.definitions] == before
        assert "workspace_checkpoint_create" in {item.name for item in runtime.tools.definitions}
        assert "workspace_checkpoint_create" not in {item.name for item in registry.definitions}
    finally:
        await service.close()


async def test_cross_agent_model_query_cannot_read_known_original_checkpoint(runtime, tmp_path):
    (tmp_path / "plain.txt").write_text("private-to-scope")
    provider = OneShotProvider()
    service, first = await setup(runtime, tmp_path, provider)
    try:
        created = await first.execute("创建原作用域检查点", idempotency_key="scope-create")
        original = await first.file_checkpoint_by_key("one-root")
        assert created.status == "completed" and original.status == "available"
        second = AgentSession(service, (await service.spawn("model-checkpoint")).agent_id)
        provider.calls = 0
        provider.name_to_call = "workspace_checkpoint_query"
        provider.arguments = {"kind": "id", "checkpoint_id": original.checkpoint_id}
        result = await second.execute("另一作用域查询必须拒绝", idempotency_key="scope-query")
        assert result.status == "completed"
        audits = await saved_audits(runtime, result.run_id)
        assert any(
            item["code"] == "workspace_checkpoint_not_found" and not item["ok"] for item in audits
        )
        assert (
            await source_count(runtime) == 1
            and await first.file_checkpoint(original.checkpoint_id) == original
        )
    finally:
        await service.close()


@pytest.mark.parametrize("capture_format", ["text", "bytes"])
async def test_actual_model_create_query_then_original_sdk_http_facts_and_source_without_replay(
    runtime, tmp_path, monkeypatch, capture_format
):
    (tmp_path / "plain.txt").write_text("原模型文本\n")
    (tmp_path / "binary.bin").write_bytes(b"\0\xff")
    provider = Provider(capture_format)
    service, agent = await setup(runtime, tmp_path, provider)
    monkeypatch.setattr(server_module, "AgentService", lambda *_args, **_kwargs: service)
    app = server_module.create_app(runtime=runtime)
    try:
        report = await agent.execute("创建并查询原模型检查点", idempotency_key="model-checkpoint")
        assert report.status == "completed", report
        view = provider.created
        assert view.status == "available" and provider.calls == 3, view
        assert view.model_dump(mode="json") == provider.queried
        original = await agent.file_checkpoint(view.checkpoint_id)
        assert original == view
        assert await service.workspace_checkpoint_store.source(
            view.scope_id, view.checkpoint_id
        ) == CheckpointModelSource(report.run_id, "original-checkpoint-create")
        async with runtime.repository.database.sessions() as sql:
            source = await sql.scalar(select(NativeCheckpointModelSourceRow))
            saved = await sql.get(NativeWorkspaceCheckpointRow, view.checkpoint_id)
            assert source.run_id == report.run_id and saved.model_bound
        stored = await service.workspace_checkpoint_store.captures(
            view.scope_id, view.checkpoint_id
        )
        assert stored[0].snapshot.data == "原模型文本\n".encode()
        assert stored[-1].snapshot is None
        if capture_format == "bytes":
            assert stored[1].snapshot.data == b"\0\xff" and view.files[1].content_kind == "binary"

        def forbidden(*_args, **_kwargs):
            pytest.fail("历史查询不能重新捕获或调用模型")

        monkeypatch.setattr(provider, "stream", forbidden)
        monkeypatch.setattr(service.workspace_checkpoints, "read", forbidden)
        assert await agent.file_checkpoint_by_key("model-native-snapshot") == original
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.get(
                f"/v1/agents/{agent.agent_id}/workspace/native-checkpoints/{view.checkpoint_id}"
            )
            assert response.status_code == 200 and response.json() == original.model_dump(
                mode="json"
            )
    finally:
        await service.close()
