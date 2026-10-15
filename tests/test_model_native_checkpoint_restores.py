"""模型原生恢复使用原阶段来源与真实基线/逐文件事务。"""

import asyncio
import copy
import json

import httpx
import pytest
from sqlalchemy import func, select
from test_model_native_checkpoints import OneShotProvider, saved_audits

import krim_agent.api.server as server_module
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.database import (
    NativeCheckpointModelSourceRow,
    WorkspaceEditAttemptRow,
    WorkspaceRestoreModelSourceRow,
)
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession
from krim_agent.tools import ShortCircuit
from krim_agent.workspace_checkpoint import (
    CheckpointModelSource,
    WorkspaceCheckpointCreate,
    WorkspaceCheckpointView,
)
from krim_agent.workspace_restore import (
    CheckpointRestorePreview,
    CheckpointRestoreView,
)
from krim_agent.workspace_writer import WorkspaceWriter

NAMES = [
    "workspace_read",
    "workspace_checkpoint_capture",
    "workspace_change",
    "workspace_reconcile",
    "workspace_checkpoint_create",
    "workspace_checkpoint_query",
    "workspace_checkpoint_cancel",
    "workspace_checkpoint_restore_preview",
    "workspace_checkpoint_restore_start",
    "workspace_checkpoint_restore_query",
    "workspace_checkpoint_restore_cancel",
]


async def setup(
    runtime,
    root,
    provider,
    *,
    policy=None,
    mode="agent",
    writable=True,
    names=None,
    tool_timeout=30,
    persistent=False,
):
    async def allow(*_args):
        return "allow"

    runtime.provider = provider
    runtime.tool_policy = policy or allow
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, writable=writable, version="1", default=True)
    service.register_checkpoint_bytes()
    service.register_checkpoint_controls()
    service.register_checkpoint_restore_controls()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="model-restore",
                description="原模型恢复",
                workspace_key="files",
                tool_names=NAMES if names is None else names,
                mode=mode,
                tool_timeout_seconds=tool_timeout,
                persistent=persistent,
            ),
        )
    )
    return service, AgentSession(service, (await service.spawn("model-restore")).agent_id)


class RestoreProvider:
    name = "offline-model-checkpoint-restore"

    def __init__(self, root, capture_format):
        self.root, self.capture_format = root, capture_format
        self.calls = 0
        self.target = self.preview = self.done = self.query = None

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(
                id="model-create-target",
                name="workspace_checkpoint_create",
                arguments={
                    "paths": ["plain", "absent"]
                    if self.capture_format == "text"
                    else ["plain", "binary", "absent"],
                    "request_key": "target",
                    "capture_format": self.capture_format,
                },
            )
        elif self.calls == 2:
            self.target = WorkspaceCheckpointView.model_validate_json(messages[-1].content)
            (self.root / "plain").write_text("current\n")
            (self.root / "plain").chmod(0o600)
            (self.root / "absent").write_text("new\n")
            if self.capture_format == "bytes":
                (self.root / "binary").write_bytes(b"changed\x00\xff")
            yield ModelToolCall(
                id="model-preview-restore",
                name="workspace_checkpoint_restore_preview",
                arguments={"checkpoint_id": self.target.checkpoint_id, "request_key": "preview"},
            )
        elif self.calls == 3:
            self.preview = CheckpointRestoreView.model_validate_json(messages[-1].content)
            yield ModelToolCall(
                id="model-start-restore",
                name="workspace_checkpoint_restore_start",
                arguments={
                    "plan_id": self.preview.plan_id,
                    "request_key": "start",
                    "expected_revision": self.preview.revision,
                },
            )
        elif self.calls == 4:
            self.done = CheckpointRestoreView.model_validate_json(messages[-1].content)
            yield ModelToolCall(
                id="model-query-restore",
                name="workspace_checkpoint_restore_query",
                arguments={"kind": "id", "plan_id": self.done.plan_id},
            )
        else:
            self.query = json.loads(messages[-1].content)
            yield ModelText(text="原模型恢复完成")


@pytest.mark.parametrize("capture_format", ["text", "bytes"])
async def test_actual_model_restore_baseline_source_and_original_consumers(
    runtime, tmp_path, monkeypatch, capture_format
):
    (tmp_path / "plain").write_text("target\n")
    (tmp_path / "plain").chmod(0o640)
    if capture_format == "bytes":
        (tmp_path / "binary").write_bytes(b"\x00\xff")
    provider = RestoreProvider(tmp_path, capture_format)
    service, agent = await setup(runtime, tmp_path, provider)
    try:
        result = await agent.execute("原模型检查点与恢复", idempotency_key="model-restore")
        assert result.status == "completed", result
        assert provider.preview.phase == "ready", provider.preview
        assert provider.done.status == "done", provider.done
        assert provider.query["plan"] == provider.done.model_dump(mode="json")
        assert (tmp_path / "plain").read_bytes() == b"target\n"
        assert (tmp_path / "plain").stat().st_mode & 0o777 == 0o640
        assert not (tmp_path / "absent").exists()
        if capture_format == "bytes":
            assert (tmp_path / "binary").read_bytes() == b"\x00\xff"
        plan = await agent.file_checkpoint_restore_by_key("preview")
        assert plan == provider.done
        assert plan.baseline_capture_format == capture_format
        preview_source = CheckpointModelSource(result.run_id, "model-preview-restore")
        execute_source = CheckpointModelSource(result.run_id, "model-start-restore")
        assert (
            await service.workspace_restore_store.source(plan.scope_id, plan.plan_id, "preview")
            == preview_source
        )
        assert (
            await service.workspace_restore_store.source(plan.scope_id, plan.plan_id, "execute")
            == execute_source
        )
        assert await service.workspace_checkpoint_store.source(
            plan.scope_id, plan.baseline_checkpoint_id
        ) == (CheckpointModelSource(result.run_id, "model-preview-restore", plan.plan_id))
        baseline = await service.workspace_checkpoint_store.captures(
            plan.scope_id, plan.baseline_checkpoint_id
        )
        assert baseline[0].snapshot.data == b"current\n"
        assert baseline[0].snapshot.mode == 0o600
        assert baseline[-1].snapshot.data == b"new\n"
        async with runtime.repository.database.sessions() as sql:
            attempts = list(await sql.scalars(select(WorkspaceEditAttemptRow)))
            assert len(attempts) == len(plan.items)
            assert all(
                (item.run_id, item.call_id) == (result.run_id, "model-start-restore")
                for item in attempts
            )
            sources = list(await sql.scalars(select(NativeCheckpointModelSourceRow)))
            assert len(sources) == 2
        audits = await saved_audits(runtime, result.run_id)
        assert any(
            item["name"] == "workspace_change" and item["depth"] == 2 and item["ok"]
            for item in audits
        )
        assert all(item["run_id"] == result.run_id for item in audits)

        def no_capture(*_args, **_kwargs):
            pytest.fail("历史消费不能再次捕获或发布")

        monkeypatch.setattr(service.workspace_checkpoints, "create", no_capture)
        monkeypatch.setattr(service.workspace_checkpoints, "create_model", no_capture)
        monkeypatch.setattr(server_module, "AgentService", lambda *_a, **_kw: service)
        app = server_module.create_app(runtime=runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            response = await client.get(
                f"/v1/agents/{agent.agent_id}/workspace/native-restores/{plan.plan_id}"
            )
            assert response.status_code == 200, response.text
            assert response.json() == plan.model_dump(mode="json")
        assert provider.calls == 5
    finally:
        await service.close()


async def target_fixture(service, agent, root):
    store = service.workspace_checkpoint_store
    binding = service.resources.workspace("files")
    target, _ = await store.reserve(
        "session:" + agent.session_id,
        WorkspaceCheckpointCreate(
            paths=["plain"], request_key="trusted-target", capture_format="bytes"
        ),
        binding.key,
        binding.fingerprint,
        binding.workspace.fingerprint,
    )
    fence = (await store.execution(target.scope_id, target.checkpoint_id))[0]
    capture = await asyncio.to_thread(
        WorkspaceWriter(binding.workspace).capture_file, "plain", allow_binary=True
    )
    await store.record(target.scope_id, fence, capture)
    target = await store.finish(target.scope_id, fence)
    (root / "plain").write_text("current\n")
    return target


async def phase_fixture(runtime, root, phase, *, policy=None, mode="agent", tool_timeout=30):
    (root / "plain").write_text("target\n")
    provider = OneShotProvider()
    service, agent = await setup(
        runtime, root, provider, policy=policy, mode=mode, tool_timeout=tool_timeout
    )
    target = await target_fixture(service, agent, root)
    if phase == "preview":
        plan = None
        arguments = {"checkpoint_id": target.checkpoint_id, "request_key": "model-preview"}
    else:
        accepted = await agent.preview_file_checkpoint_restore(
            CheckpointRestorePreview(
                checkpoint_id=target.checkpoint_id,
                request_key="host-preview",
            )
        )
        plan = await agent.wait_file_checkpoint_restore(accepted.plan_id)
        assert plan.phase == "ready", plan
        arguments = {
            "plan_id": plan.plan_id,
            "expected_revision": plan.revision,
            "request_key": "model-start",
        }
    provider.name_to_call = "workspace_checkpoint_restore_" + phase
    provider.arguments = arguments
    return service, agent, provider, target, plan


@pytest.mark.parametrize("phase", ["preview", "start"])
@pytest.mark.parametrize(
    "mode,decision,approval,accepted",
    [
        ("agent", "deny", True, False),
        ("agent", "ask", False, False),
        ("agent", "ask", True, True),
        ("plan", "allow", True, False),
    ],
)
async def test_restore_original_permission_before_stage_admission(
    runtime, tmp_path, monkeypatch, phase, mode, decision, approval, accepted
):
    name = "workspace_checkpoint_restore_" + phase

    async def policy(definition, _context):
        return decision if definition.name == name else "allow"

    service, agent, provider, target, plan = await phase_fixture(
        runtime, tmp_path, phase, policy=policy, mode=mode
    )

    async def approve(_definition, _context):
        return approval

    service._handle(agent.agent_id).runtime.approval_handler = approve
    if not accepted:

        def no_capture(*_args, **_kwargs):
            pytest.fail("拒绝后不得捕获或写入")

        monkeypatch.setattr(WorkspaceWriter, "capture_file", no_capture)
        monkeypatch.setattr(WorkspaceWriter, "apply", no_capture)
    try:
        result = await agent.execute("恢复阶段准入", idempotency_key="permission")
        assert result.status == "completed", result
        async with runtime.repository.database.sessions() as sql:
            count = await sql.scalar(
                select(func.count()).select_from(WorkspaceRestoreModelSourceRow)
            )
        assert count == int(accepted)
        current = await agent.file_checkpoint_restore_by_key(
            "model-preview" if phase == "preview" else "host-preview"
        )
        if accepted:
            assert current.phase == ("ready" if phase == "preview" else "finished"), current
        elif phase == "preview":
            assert current is None
        else:
            assert current == plan
        if not accepted:
            assert (tmp_path / "plain").read_bytes() == b"current\n"
            audits = await saved_audits(runtime, result.run_id)
            assert any(
                item["name"] == name and item["code"] == "permission_denied" for item in audits
            ), provider.reply
    finally:
        await service.close()


@pytest.mark.parametrize("phase", ["preview", "start"])
@pytest.mark.parametrize(
    "target,hook_phase,fault",
    [
        ("root", "pre", "arguments"),
        ("root", "pre", "run"),
        ("root", "pre", "call"),
        ("root", "pre", "session"),
        ("root", "pre", "budget"),
        ("root", "pre", "short"),
        ("root", "post", "data"),
        ("root", "post", "marker"),
        ("leaf", "pre", "arguments"),
        ("leaf", "pre", "origin"),
        ("leaf", "pre", "source_call"),
        ("leaf", "pre", "budget"),
        ("leaf", "post", "data"),
    ],
)
async def test_restore_root_and_real_leaf_hooks_keep_original_source_and_fact(
    runtime, tmp_path, phase, target, hook_phase, fault
):
    service, agent, provider, checkpoint, original = await phase_fixture(runtime, tmp_path, phase)
    selected = (
        "workspace_checkpoint_restore_" + phase
        if target == "root"
        else ("workspace_checkpoint_capture" if phase == "preview" else "workspace_change")
    )
    registry = service._handle(agent.agent_id).runtime.tools

    def corrupt(context):
        if fault == "arguments":
            field = "request_key" if target == "root" or phase == "start" else "path"
            context.arguments[field] = "forged"
        elif fault == "run":
            context.run_id = "forged-run"
        elif fault == "call":
            context.call_id = "forged-call"
        elif fault == "session":
            context.session_id = "forged-session"
        elif fault == "origin":
            context.origin = "host"
        elif fault == "source_call":
            context.source_call_id = "forged-parent"
        elif fault == "budget":
            context.execution_timeouts[-1].deadline.reschedule(
                asyncio.get_running_loop().time() + 60
            )

    async def pre(definition, context):
        if definition.name == selected and hook_phase == "pre":
            if fault == "short":
                return ShortCircuit({})
            corrupt(context)

    async def post(definition, value, context):
        if definition.name == selected and hook_phase == "post":
            value = copy.deepcopy(value)
            value["request_key" if target == "root" or phase == "start" else "sha256"] = "forged"
            if fault == "marker":
                context._checkpoint_control_response = (
                    "restore_" + phase,
                    json.dumps(value, sort_keys=True),
                )
        return value

    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        result = await agent.execute("原恢复Hook拒绝", idempotency_key="hook")
        assert result.status == "completed", result
        audits = await saved_audits(runtime, result.run_id)
        assert all(item["run_id"] == result.run_id for item in audits)
        assert any(
            item["name"] == selected
            and not item["ok"]
            and item["code"]
            in {
                "workspace_checkpoint_model_context",
                "workspace_checkpoint_model_unverified",
            }
            for item in audits
        ), (provider.reply, audits)
        if target == "root":
            assert any(
                item["name"] == selected and item["call_id"] == "one-root-call" for item in audits
            )
        current = await agent.file_checkpoint_restore_by_key(
            "model-preview" if phase == "preview" else "host-preview"
        )
        if target == "root" and hook_phase == "pre":
            assert current is None if phase == "preview" else current == original
        if hook_phase == "pre":
            assert (tmp_path / "plain").read_bytes() == b"current\n"
        elif phase == "start":
            assert current.status == "done" and (tmp_path / "plain").read_bytes() == b"target\n"
    finally:
        await service.close()
