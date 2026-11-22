"""One-shot process provider tests (plan §5.1 / 阶段6 providers/process.py).

Three layers, no host subprocess and no sandbox deployment:

- argv pre-validation against the real SandboxCommand capability;
- ExecutorSandboxPort over a real ToolExecutor with sandbox tool specs and a
  stateful stub backend: ask/deny decisions, durable approvals, generation
  tracking and the read limits of the existing tool models;
- ProcessAgentLoop driven directly: registered-run events, argv vs controlled
  file delivery, durable park/resume on approvals, result vs cleanup-failure
  separation, and real process cancellation on the way out.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
from types import SimpleNamespace
from typing import Any

import pytest

from krim_agent.config import Settings
from krim_agent.contracts import (
    RunRequest,
    RunView,
    ServiceError,
    StrictModel,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.runtime.contracts import ExecutionBudget, RunSnapshot
from krim_agent.runtime.recovery import sdk_resume
from krim_agent.runtime.sdk_execution import Parked
from krim_agent.runtime.service import RunBinding
from krim_agent.sandbox.command import SandboxCommand
from krim_agent.sandbox.file_models import (
    DirectoryCreate,
    FileList,
    FileRead,
    FileRemove,
    FileWrite,
)
from krim_agent.sandbox.operations import SessionCommandScope
from krim_agent.sandbox.tasks import TaskLog, TaskStart, TaskWait
from krim_agent.subagents.errors import SubagentError
from krim_agent.subagents.providers.base import UnsupportedCapability
from krim_agent.subagents.providers.process import (
    TASK_INPUT_ROOT,
    ExecutorSandboxPort,
    ProcessAgentLoop,
    ProcessBackend,
    ProcessLaunch,
    ProcessProvider,
    SandboxApprovalRequired,
    build_task_document,
    parse_task_document,
    validate_process_launch,
    validate_task_document,
)
from krim_agent.subagents.types import StartTask
from krim_agent.subagents.workspace import WorkspaceAssignment
from krim_agent.tools.core import Invocation, ToolSpec
from krim_agent.tools.executor import Admission, ToolContext, ToolExecutor
from krim_agent.tools.registry import ToolRegistry

HEX = "0" * 64
PROJECT_ID = "11111111-1111-4111-8111-111111111111"
OPERATION_ID = "22222222-2222-4222-8222-222222222222"
# RunBinding.freeze 的完整绑定档案形态（测试用子集；provider 核对 key 与无 context）
BINDING_DESCRIPTOR = {
    "schema_version": 2,
    "key": "subagent:worker",
    "version": "agent-v1",
    "provider": {"id": "krim_agent.subagents.providers.process.ProcessBackend"},
    "sandbox": {"version": "sandbox/1"},
}


class _FakeOperations:
    """Scope operations stand-in; the executor's failure path observes calls."""

    async def observe(self, _invocation):
        return None


# ---------------------------------------------------------------------------
# launch validation
# ---------------------------------------------------------------------------


def test_launch_accepts_python_and_node_payloads_within_capability():
    launch = validate_process_launch(("python", "scripts/worker.py", "--fast"))
    assert launch.program == "python"
    assert launch.payload == ("scripts/worker.py", "--fast")
    assert launch.wall_seconds == 20
    node = validate_process_launch(("node", "-e", "script.js"), wall_seconds=60)
    assert node.program == "node"
    assert node.wall_seconds == 60
    validate_process_launch(("node", "worker.js"))


def test_launch_rejects_out_of_capability_configuration():
    for argv in (
        (),  # empty
        ("bash", "-c", "echo hi"),  # program outside SandboxCommand
        ("python",),  # no payload slot left for the task document
        ("python", "x", "\0"),  # NUL byte
        ("python", *(("a",) * 31)),  # leaves no argv room for the task
        ("node", "-e"),  # eval without script
        ("node", "/abs/path.js"),  # absolute script path
        ("node", "../escape.js"),  # parent traversal
        ("python", "x" * 4096, "y" * 4096),  # no byte room for the document
    ):
        try:
            validate_process_launch(argv)
        except ValueError:
            continue
        raise AssertionError(f"argv should have been rejected: {argv[:2]}")
    try:
        validate_process_launch(("python", "worker.py"), wall_seconds=61)
    except ValueError:
        pass
    else:
        raise AssertionError("wall beyond the sandbox watchdog must be rejected")


def test_task_document_parsing_and_mode_validation():
    document, assignment = parse_task_document(encoded({"task": "do it"}))
    assert document["task"] == "do it"
    assert assignment is None
    for bad in ("not json", encoded(["list"]), encoded({"task": 5})):
        try:
            parse_task_document(bad)
        except ServiceError as error:
            assert error.code == "invalid_input"
        else:
            raise AssertionError(f"document should have been rejected: {bad}")
    broken_workspace = encoded({"task": "t", "workspace": {"mode": "nonsense"}})
    try:
        parse_task_document(broken_workspace)
    except ServiceError as error:
        assert error.code == "invalid_input"
    else:
        raise AssertionError("broken workspace claim must be rejected")

    shared = encoded({"task": "t"})
    assert validate_task_document(shared, workspace_mode="shared_read_only")[0] == {"task": "t"}
    try:
        validate_task_document(shared, workspace_mode="isolated_write")
    except ServiceError as error:
        assert error.code == "invalid_input"
    else:
        raise AssertionError("isolated profile requires a workspace assignment")


def _assignment(mode: str, agent_id: str = "a1") -> WorkspaceAssignment:
    return WorkspaceAssignment(
        mode=mode,  # type: ignore[arg-type]
        agent_id=agent_id,
        source_project_id=PROJECT_ID,
        workspace_project_id=PROJECT_ID,
        directory=(
            None if mode == "shared_read_only"
            else f".krim/subagent-workspaces/{agent_id}"
        ),
        baseline_fingerprint=HEX,
        input_version=1,
    )


def _document(task: str, agent_id: str = "a1") -> str:
    """Domain-provided wrapped document carrying an isolated workspace claim."""
    return build_task_document(task, _assignment("isolated_write", agent_id))


# ---------------------------------------------------------------------------
# provider contract
# ---------------------------------------------------------------------------


class FakeRuntime:
    def __init__(self) -> None:
        self.delegated: list[StartTask] = []
        self.cancelled: list[str] = []

    async def start_delegate(self, specification: StartTask):
        self.delegated.append(specification)
        return RunView(
            id="child-run",
            request_key=specification.request.request_key,
            status="running",
            trace_id="t",
            root_span_id="s",
            created_at=0.0,
        )

    async def cancel_delegate(self, run_id: str):
        self.cancelled.append(run_id)
        return {"run_id": run_id}


def _specification(text: str, target: dict | None = None) -> StartTask:
    return StartTask(
        agent_id="agent-1",
        task_id="task-1",
        activation_id="act-1",
        request=RunRequest(text=text, request_key="ac-child.t1"),
        target=target if target is not None else dict(BINDING_DESCRIPTOR),
    )


async def test_provider_registers_child_run_through_the_run_service():
    runtime = FakeRuntime()
    provider = ProcessProvider(
        runtime,
        dict(BINDING_DESCRIPTOR),
        launch=validate_process_launch(("python", "worker.py")),
        workspace_mode="isolated_write",
    )
    wrapped = build_task_document("t", _assignment("isolated_write"))
    view = await provider.start(_specification(wrapped))
    assert view.id == "child-run"
    assert len(runtime.delegated) == 1
    assert provider.capabilities.continuable is False
    assert provider.capabilities.fork is False
    assert provider.capabilities.depth_policy == "leaf"
    assert provider.capabilities.workspace_isolation is True
    assert provider.descriptor["key"] == "subagent:worker"
    assert provider.version == fingerprint(BINDING_DESCRIPTOR)
    # descriptor 是冻结的完整绑定档案，不是摘要；冻结后不可变
    assert provider.descriptor == BINDING_DESCRIPTOR


async def test_provider_refuses_shared_read_only_before_any_registration():
    """无只读挂载：shared_read_only 的 process 在登记 run 前即拒绝。"""
    runtime = FakeRuntime()
    shared = ProcessProvider(
        runtime,
        dict(BINDING_DESCRIPTOR),
        launch=validate_process_launch(("python", "worker.py")),
    )
    try:
        await shared.start(_specification(encoded({"task": "t"})))
    except Exception as error:
        assert getattr(error, "code", None) == "subagent_capability"
        assert getattr(error, "accepted", None) is False
    else:
        raise AssertionError("shared_read_only process must be refused up front")
    assert runtime.delegated == []


async def test_provider_validates_target_and_workspace_mode_before_registering():
    runtime = FakeRuntime()
    provider = ProcessProvider(
        runtime,
        dict(BINDING_DESCRIPTOR),
        launch=validate_process_launch(("python", "worker.py")),
        workspace_mode="isolated_write",
    )
    try:
        await provider.start(_specification(encoded({"task": "t"}), target={"key": "x"}))
    except ServiceError as error:
        assert error.code == "target_binding_changed"
    else:
        raise AssertionError("foreign target must be rejected")
    try:
        await provider.start(_specification(encoded({"task": "t"})))
    except ServiceError as error:
        assert error.code == "invalid_input"
    else:
        raise AssertionError("isolated profile without assignment must be rejected")

    isolated_claim = encoded({
        "task": "t",
        "workspace": _assignment("isolated_write").model_dump(mode="json"),
    })
    shared_claim = encoded({
        "task": "t",
        "workspace": _assignment("shared_read_only").model_dump(mode="json"),
    })
    try:
        await provider.start(_specification(shared_claim))
    except ServiceError as error:
        assert error.code == "invalid_input"
    else:
        raise AssertionError("isolated profile must not accept another mode's claim")
    assert runtime.delegated == []
    # 已包装的 isolated 文档：透传并登记
    view = await provider.start(_specification(isolated_claim))
    assert view.id == "child-run"
    assert runtime.delegated[-1].request.text == isolated_claim


async def test_provider_before_attach_refuses_and_runtime_injection_unblocks():
    provider = ProcessProvider(
        None,
        dict(BINDING_DESCRIPTOR),
        launch=validate_process_launch(("python", "worker.py")),
        workspace_mode="isolated_write",
    )
    wrapped = build_task_document("t", _assignment("isolated_write"))
    try:
        await provider.start(_specification(wrapped))
    except ServiceError as error:
        assert error.code == "closed"
    else:
        raise AssertionError("unattached provider must refuse to start runs")
    runtime = FakeRuntime()
    provider.runtime = runtime  # PreparedSubagents.attach 后注入
    view = await provider.start(_specification(wrapped))
    assert view.id == "child-run"


def test_task_document_builder_wraps_natural_language_payloads():
    assert build_task_document("fix the bug") == encoded({"task": "fix the bug"})
    with_assignment = build_task_document("fix the bug", _assignment("isolated_write"))
    document, assignment = parse_task_document(with_assignment)
    assert document["task"] == "fix the bug"
    assert assignment is not None and assignment.mode == "isolated_write"
    broken = _assignment("isolated_write").model_copy(update={"directory": None})
    try:
        build_task_document("t", broken)
    except ServiceError as error:
        assert error.code == "invalid_input"
    else:
        raise AssertionError("isolated assignment without a directory must be rejected")
    try:
        build_task_document("t", _assignment("shared_read_only").model_copy(
            update={"directory": ".krim/subagent-workspaces/x"}
        ))
    except ServiceError as error:
        assert error.code == "invalid_input"
    else:
        raise AssertionError("shared assignment must not carry a directory")


def test_one_shot_refusals_and_capabilities():
    provider = ProcessProvider(
        FakeRuntime(),
        dict(BINDING_DESCRIPTOR),
        launch=validate_process_launch(("python", "worker.py")),
    )
    assert provider.refusal("send").code == "subagent_not_continuable"
    assert provider.refusal("fork").code == "subagent_capability"
    assert provider.refusal("resume").code == "subagent_not_recoverable"
    assert isinstance(provider.refusal("send"), SubagentError)
    try:
        provider.capabilities.require(continuable=True)
    except UnsupportedCapability as error:
        assert error.capability == "continuable"
    else:
        raise AssertionError("one-shot provider must not claim continuity")


# ---------------------------------------------------------------------------
# ExecutorSandboxPort over the real tool pipeline
# ---------------------------------------------------------------------------


class SandboxStub:
    """Narrow state machine for the sandbox tools this adapter drives."""

    def __init__(self) -> None:
        self.generation = 0
        self.files: dict[str, bytes] = {}
        self.calls: list[tuple[str, dict]] = []
        self.binding: dict = {
            "project_id": PROJECT_ID,
            "revision": 1,
            "project": {"project_id": PROJECT_ID, "generation": 0},
        }
        self.task_state: dict = {"status": "finished", "exit_code": 0}
        self.command_stdout: bytes = b"done\n"

    async def project(self, _invocation, _values, _context):
        self.calls.append(("project", {}))
        return {**self.binding, "project": dict(self.binding["project"])}

    async def write(self, _invocation, values, _context):
        self.calls.append(("write", values.model_dump(mode="json")))
        if values.expected_generation != self.generation:
            raise ServiceError("input_revision_conflict")
        self.files[values.path] = values.content.encode("utf-8")
        self.generation += 1
        self.binding["project"]["generation"] = self.generation
        return {"path": values.path, "generation": self.generation}

    async def read(self, _invocation, values, _context):
        self.calls.append(("read", {"path": values.path, "limit": values.limit}))
        data = self.files.get(values.path)
        if data is None:
            raise ServiceError("not_found", 404)
        chunk = data[values.offset:values.offset + values.limit]
        return {
            "path": values.path,
            "offset": values.offset,
            "size": len(data),
            "eof": values.offset + len(chunk) >= len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "data": base64.b64encode(chunk).decode(),
        }

    async def task_start(self, _invocation, values, _context):
        self.calls.append(("task_start", values.model_dump(mode="json")))
        return {"operation_id": OPERATION_ID, "status": "accepted"}

    async def task_wait(self, _invocation, values, _context):
        self.calls.append(("task_wait", values.model_dump(mode="json")))
        return {
            "operation_id": values.operation_id,
            "status": self.task_state.get("status", "finished"),
            "exit_code": self.task_state.get("exit_code", 0),
            "code": self.task_state.get("code"),
            "output_complete": True,
            "verification": "not_evaluated",
            "generation": self.generation,
        }

    async def task_log(self, _invocation, values, _context):
        self.calls.append(("task_log", values.model_dump(mode="json")))
        return {
            "stdout": base64.b64encode(self.command_stdout).decode(),
            "stderr": base64.b64encode(b"").decode(),
        }


def build_executor(stub: SandboxStub, decisions: dict[str, str] | None = None) -> ToolExecutor:
    """Real ToolExecutor over sandbox tool specs backed by the stub."""

    async def unused(_values):
        raise AssertionError("unscoped executor must not run for sandbox tools")

    def spec(name, parameters, executor, capability, risk):
        return ToolSpec(
            name, parameters, unused, {"type": "object"}, "1", HEX, risk, executor, capability,
        )

    registry = ToolRegistry((
        spec("sandbox_project", StrictModel, stub.project, "read", "read"),
        spec("sandbox_read", FileRead, stub.read, "read", "read"),
        spec("sandbox_list", FileList, stub.read, "read", "read"),
        spec("sandbox_write", FileWrite, stub.write, "write", "write"),
        spec("sandbox_mkdir", DirectoryCreate, stub.write, "write", "write"),
        spec("sandbox_remove", FileRemove, stub.write, "write", "write"),
        spec("sandbox_task_start", TaskStart, stub.task_start, "command", "exec"),
        spec("sandbox_task_wait", TaskWait, stub.task_wait, "read", "read"),
        spec("sandbox_task_log", TaskLog, stub.task_log, "read", "read"),
    ))

    async def policy(invocation: Invocation):
        return (decisions or {}).get(invocation.name, "allow")

    return ToolExecutor(registry, policy)


def make_context(store_events: list, scope: SessionCommandScope) -> ToolContext:
    async def emit(kind: str, data: dict):
        store_events.append((kind, data))
        return data

    async def reject(_invocation, _timeout):
        raise ServiceError("approval_unavailable")

    return ToolContext(emit, reject, None, lambda: None, scope, None)


def make_scope(project_id: str = PROJECT_ID) -> SessionCommandScope:
    return SessionCommandScope(
        actor=object(),
        workspace_id="workspace",
        session_id="session",
        operations=_FakeOperations(),
        check=lambda: None,
        initial_deadline=None,
        project_id=project_id,
        binding_revision=1,
    )


async def test_adapter_reads_project_state_and_tracks_generation_through_cas():
    stub = SandboxStub()
    events: list = []
    port = ExecutorSandboxPort(build_executor(stub), make_context(events, make_scope()), "run-a")
    assert await port.project_generation() == 0
    await port.write(PROJECT_ID, "notes/a.txt", "one")
    await port.write(
        PROJECT_ID, "notes/a.txt", "two",
        expected_sha256=hashlib.sha256(b"one").hexdigest(),
    )
    assert stub.files["notes/a.txt"] == b"two"
    assert [name for name, _call in stub.calls] == ["project", "write", "write"]
    # 并发方把项目推进了一代：下一次写入按旧 CAS 前置条件被拒
    stub.generation += 1
    stub.binding["project"]["generation"] = stub.generation
    try:
        await port.write(PROJECT_ID, "notes/a.txt", "three")
    except ServiceError as error:
        assert error.code == "input_revision_conflict"
    else:
        raise AssertionError("stale generation must be rejected by CAS")
    # 刷新后恢复
    port._generation.clear()  # type: ignore[attr-defined]
    await port.write(PROJECT_ID, "notes/a.txt", "three")
    assert stub.files["notes/a.txt"] == b"three"


async def test_adapter_read_limit_follows_the_existing_tool_model():
    stub = SandboxStub()
    stub.files["big.txt"] = b"x" * 100
    events: list = []
    port = ExecutorSandboxPort(build_executor(stub), make_context(events, make_scope()), "run-b")
    page = await port.read(PROJECT_ID, "big.txt", limit=65536)
    assert page["size"] == 100
    assert [call["limit"] for name, call in stub.calls if name == "read"] == [16384]
    try:
        await port.read(PROJECT_ID, "missing.txt")
    except ServiceError as error:
        assert error.code == "not_found"
    assert await port.git_head(PROJECT_ID) is None  # .git is blocked by the sandbox surface


async def test_adapter_surfaces_ask_decisions_and_durable_approvals():
    stub = SandboxStub()
    events: list = []
    scope = make_scope()
    context = make_context(events, scope)
    executor = build_executor(stub, decisions={"sandbox_write": "ask"})
    port = ExecutorSandboxPort(executor, context, "run-c")
    try:
        await port.write(PROJECT_ID, "draft.txt", "hello")
    except SandboxApprovalRequired as pending:
        call_id = pending.invocation.call_id
        assert pending.invocation.name == "sandbox_write"
        assert pending.admission.decision == "ask"
        assert all(name != "write" for name, _call in stub.calls)  # 挂起中的写入没有执行
    else:
        raise AssertionError("write under ask policy must park before executing")

    approved = ExecutorSandboxPort(executor, context, "run-c", approved={call_id: True})
    receipt = await approved.write(PROJECT_ID, "draft.txt", "hello")
    assert receipt["generation"] == 1
    denied = ExecutorSandboxPort(executor, context, "run-c", approved={call_id: False})
    try:
        await denied.write(PROJECT_ID, "draft.txt", "hello")
    except ServiceError as error:
        assert error.code == "approval_denied"
    else:
        raise AssertionError("denied reply must not execute")

    deny_executor = build_executor(stub, decisions={"sandbox_write": "deny"})
    denying = ExecutorSandboxPort(deny_executor, make_context(events, scope), "run-c")
    try:
        await denying.write(PROJECT_ID, "draft.txt", "hello")
    except ServiceError as error:
        assert error.code == "permission_denied"
    else:
        raise AssertionError("deny must surface as permission_denied")


async def test_adapter_prepare_workspace_runs_one_approved_bootstrap():
    stub = SandboxStub()
    stub.command_stdout = encoded({
        "mode": "worktree", "head": "c" * 40, "copied": 7, "skipped": 1, "truncated": False,
    }).encode()
    events: list = []
    port = ExecutorSandboxPort(build_executor(stub), make_context(events, make_scope()), "run-d")
    result = await port.prepare_workspace(
        PROJECT_ID, ".krim/subagent-workspaces/a1",
        limits={"max_files": 400, "max_file_bytes": 1_048_576},
    )
    assert result["mode"] == "worktree"
    assert result["head"] == "c" * 40
    assert result["copied"] == 7
    started = [call for name, call in stub.calls if name == "task_start"]
    assert len(started) == 1
    assert started[0]["program"] == "python"
    assert started[0]["argv"][0] == "-c"
    assert started[0]["argv"][2] == ".krim/subagent-workspaces/a1"
    assert "max_files" in started[0]["argv"][3]

    # bootstrap 以非零退出结束：如实失败，不伪装成已准备
    failed_stub = SandboxStub()
    failed_stub.task_state = {"status": "finished", "exit_code": 3}
    failing = ExecutorSandboxPort(
        build_executor(failed_stub), make_context([], make_scope()), "run-e",
    )
    try:
        await failing.prepare_workspace(
            PROJECT_ID, ".krim/subagent-workspaces/a1", limits={"max_files": 1},
        )
    except ServiceError as error:
        assert error.code == "execution_error"
    else:
        raise AssertionError("failed bootstrap must not be reported as prepared")

    # 清理失败（unknown）：单独报错，结果不可信
    unknown_stub = SandboxStub()
    unknown_stub.task_state = {"status": "unknown", "exit_code": None}
    unknown_port = ExecutorSandboxPort(
        build_executor(unknown_stub), make_context([], make_scope()), "run-f",
    )
    try:
        await unknown_port.prepare_workspace(
            PROJECT_ID, ".krim/subagent-workspaces/a1", limits={"max_files": 1},
        )
    except ServiceError as error:
        assert error.code == "sandbox_cleanup_required"
    else:
        raise AssertionError("unknown cleanup must surface as sandbox_cleanup_required")


# ---------------------------------------------------------------------------
# ProcessAgentLoop driven directly
# ---------------------------------------------------------------------------


class MemoryStore:
    def __init__(self, text: str) -> None:
        self.text = text
        self.events: list[tuple[str, dict]] = []
        self.checkpoints: list[tuple[dict, str]] = []

    async def snapshot(self, run_id: str) -> RunSnapshot:
        return RunSnapshot(
            run=RunView(
                id=run_id,
                request_key="ac-child.t",
                status="running",
                trace_id="t",
                root_span_id="s",
                created_at=0.0,
            ),
            request=RunRequest(text=self.text, request_key="ac-child.t"),
            target={"key": "subagent:worker"},
            events=(),
            session_context=None,
        )

    async def append(self, run_id: str, kind: str, data: dict, *, event_id=None):
        self.events.append((kind, data))
        return data

    async def write_checkpoint(self, run_id: str, checkpoint: dict, fingerprint_value: str):
        self.checkpoints.append((checkpoint, fingerprint_value))


class _FakeSandboxConfiguration:
    command_image = "krim-sandbox:test"

    def model_dump_json(self, *, exclude_unset: bool = False) -> str:
        return "{}"


class FakeSandbox:
    def __init__(self, project_id: str = PROJECT_ID, store: Any | None = None) -> None:
        self.project_id = project_id
        # RunBinding.describe/tool_tokens 读取 sandbox.service 的配置与句柄；
        # RunService._drive 收尾经 sandbox.tasks.cancel_run_tasks 触达
        # service.store——真实SQLiteStore时对本run无任务即干净no-op。
        self.service = SimpleNamespace(
            configuration=_FakeSandboxConfiguration(),
            broker=object(),
            identities=object(),
            store=store,
        )

    async def scope(self, run_id, check, budget):
        return SessionCommandScope(
            actor=object(),
            workspace_id="workspace",
            session_id="session",
            operations=_FakeOperations(),
            check=lambda: None,
            initial_deadline=None,
            project_id=self.project_id,
            binding_revision=1,
        )

    async def execute(self, *args: Any, **kwargs: Any):
        raise AssertionError("sandbox execute is not used in this test")


def _ask(run_id: str, call_id: str) -> SandboxApprovalRequired:
    invocation = Invocation(
        run_id=run_id,
        call_id=call_id,
        span_id="s",
        step_index=0,
        name="sandbox_task_start",
        arguments_json="{}",
        tool_hash=HEX,
    )
    return SandboxApprovalRequired(invocation, Admission(None, None, "ask"))


class FakePort:
    """Duck-typed SandboxExecutionPort with scripted waits and ask decisions."""

    def __init__(
        self,
        *,
        generation: int = 0,
        waits: list[str] | None = None,
        ask_on: set[str] | None = None,
        exit_code: int = 0,
        prepare_truncated: bool = False,
        generation_bump_on_prepare: bool = False,
        cancel_status: str = "finished",
    ) -> None:
        self.calls: list[tuple] = []
        self.generation_value = generation
        self.waits = list(waits or ["finished"])
        self.ask_on = ask_on or set()
        self.exit_code = exit_code
        self.prepare_truncated = prepare_truncated
        self.generation_bump_on_prepare = generation_bump_on_prepare
        self.cancelled: tuple | None = None
        self.cancel_raises = False
        self.cancel_status = cancel_status
        self.run_id: str | None = None
        self.approved: dict[str, bool] = {}
        # 补丁收集测试用的工作区内容（read/entries 读取此字典）
        self.files: dict[str, bytes] = {}

    def bind(self, run_id: str, approved) -> FakePort:
        self.run_id = run_id
        self.approved = dict(approved) if approved else {}
        return self

    def _decision(self, label: str) -> bool | None:
        if label not in self.ask_on:
            return True
        return self.approved.get(f"process-{self.run_id}-{label}")

    def tool_hash(self, tool: str) -> str:
        return HEX

    async def project_state(self):
        return {"project": {"project_id": PROJECT_ID, "generation": self.generation_value}}

    async def project_generation(self):
        return self.generation_value

    async def prepare_workspace(
        self, project_id: str, directory: str, *, limits: dict[str, int]
    ):
        decision = self._decision("workspace-prepare")
        if decision is not True:
            if decision is False:
                raise ServiceError("approval_denied")
            raise _ask(self.run_id, f"process-{self.run_id}-workspace-prepare")
        self.calls.append(("prepare", directory, dict(limits)))
        if self.generation_bump_on_prepare:
            self.generation_value += 1  # bootstrap 命令推进项目代
        return {
            "mode": "copy",
            "head": None,
            "copied": 3,
            "skipped": 0,
            "truncated": self.prepare_truncated,
            "operation_id": "ws-op-1",
        }

    async def start(self, command: SandboxCommand, *, label: str = "task-start"):
        decision = self._decision(label)
        if decision is not True:
            if decision is False:
                raise ServiceError("approval_denied")
            raise _ask(self.run_id, f"process-{self.run_id}-{label}")
        self.calls.append(("start", command.model_dump(mode="json")))
        return {"operation_id": OPERATION_ID, "status": "accepted"}

    async def wait(self, project_id: str, operation_id: str, *, seconds: int = 5):
        state = self.waits.pop(0) if self.waits else "finished"
        self.calls.append(("wait", state))
        if state == "cancel-run":
            raise asyncio.CancelledError
        return {
            "status": state,
            "exit_code": self.exit_code if state == "finished" else None,
            "code": None,
            "output_complete": state == "finished",
        }

    async def cancel(self, project_id: str, operation_id: str, *, expected_generation: int):
        self.calls.append(("cancel", operation_id))
        if self.cancel_raises:
            raise ServiceError("sandbox_cleanup_required")
        self.cancelled = (project_id, operation_id, expected_generation)
        return {"status": self.cancel_status}

    async def logs(self, project_id: str, operation_id: str, *, max_bytes: int = 8192):
        return {
            "stdout": base64.b64encode(b"done\n").decode(),
            "stderr": base64.b64encode(b"").decode(),
        }

    async def ensure_directory(self, project_id: str, path: str, *, label: str | None = None):
        decision = self._decision(label or "mkdir")
        if decision is not True:
            raise _ask(self.run_id, f"process-{self.run_id}-{label}")
        self.calls.append(("mkdir", path, label))
        return {"path": path, "generation": 1}

    async def write(
        self,
        project_id: str,
        path: str,
        content: str,
        *,
        expected_sha256: str | None = None,
        label: str | None = None,
    ):
        decision = self._decision(label or "write")
        if decision is not True:
            raise _ask(self.run_id, f"process-{self.run_id}-{label}")
        self.calls.append(("write", path, label))
        self.files[path] = content.encode("utf-8")
        return {"path": path, "generation": 2}

    async def read(self, project_id: str, path: str, *, offset: int = 0, limit: int = 16384):
        data = self.files.get(path)
        if data is None:
            raise ServiceError("not_found", 404)
        chunk = data[offset:offset + limit]
        return {
            "path": path,
            "offset": offset,
            "size": len(data),
            "eof": offset + len(chunk) >= len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "data": base64.b64encode(chunk).decode(),
        }

    async def entries(self, project_id: str, path: str, *, offset: int = 0):
        prefix = "" if path == "." else path.rstrip("/") + "/"
        names: dict[str, str] = {}
        for candidate in sorted(self.files):
            if candidate.startswith(prefix):
                rest = candidate[len(prefix):]
                if "/" in rest:
                    names.setdefault(rest.split("/", 1)[0], "directory")
                else:
                    names[rest] = "file"
        ordered = [
            {"name": name, "type": kind, "size": 0} for name, kind in sorted(names.items())
        ]
        following = offset + 100
        return {
            "entries": ordered[offset:following],
            "offset": offset,
            "next_offset": following if following < len(ordered) else None,
            "total": len(ordered),
        }

    async def remove(
        self, project_id: str, path: str, *, expected_sha256: str, label: str | None = None
    ):
        data = self.files.get(path)
        if data is None:
            raise ServiceError("not_found", 404)
        if hashlib.sha256(data).hexdigest() != expected_sha256:
            raise ServiceError("input_revision_conflict")
        del self.files[path]
        self.calls.append(("remove", path, label))
        return {"path": path, "generation": 3}

    async def git_head(self, project_id: str):
        return None


def make_loop(
    store: MemoryStore,
    port: FakePort,
    *,
    launch: ProcessLaunch | None = None,
    mode: str = "isolated_write",
) -> ProcessAgentLoop:
    launch = launch or validate_process_launch(("python", "worker.py"))

    def factory(_executor, _context, run_id, approved):
        return port.bind(run_id, approved)

    return ProcessAgentLoop(
        store,
        ProcessBackend(),
        ToolExecutor(ToolRegistry(()), None),
        Settings(),
        sandbox=FakeSandbox(),
        launch=launch,
        workspace_mode=mode,  # type: ignore[arg-type]
        port=factory,
    )


async def test_loop_success_keeps_raw_facts_and_returns_normally():
    store = MemoryStore(_document("do the thing"))
    port = FakePort(waits=["running", "finished"], exit_code=0)
    loop = make_loop(store, port)
    await loop.run("run-1", lambda i, r: None)

    start = next(call for call in port.calls if call[0] == "start")
    argv = start[1]["argv"]
    assert argv[0] == "-c"  # cwd 约束包装
    assert "worker.py" in argv
    wrapped = parse_json(argv[-1])  # 单独参数，绝不shell插值
    assert wrapped["task"] == "do the thing"
    assert wrapped["workspace"]["directory"] == ".krim/subagent-workspaces/a1"
    kinds = [kind for kind, _ in store.events]
    assert kinds == ["workspace_prepared", "process_started", "process_finished"]
    finished = store.events[-1][1]
    assert finished["exit_code"] == 0
    assert finished["stdout"] == base64.b64encode(b"done\n").decode()
    assert port.cancelled is None  # 正常结束不取消


async def test_loop_nonzero_exit_fails_the_run_but_keeps_process_facts():
    store = MemoryStore(_document("t"))
    port = FakePort(exit_code=3)
    loop = make_loop(store, port)
    try:
        await loop.run("run-1b", lambda i, r: None)
    except ServiceError as error:
        assert error.code == "execution_error"
    else:
        raise AssertionError("non-zero exit must fail the run, not complete it")
    finished = [data for kind, data in store.events if kind == "process_finished"]
    assert finished and finished[0]["exit_code"] == 3  # 原始事实已固定
    assert finished[0]["stdout"] == base64.b64encode(b"done\n").decode()
    assert port.cancelled is None  # 已结束：不再取消


async def test_loop_unknown_execution_raises_execution_unknown_not_success():
    store = MemoryStore(_document("t"))
    port = FakePort(waits=["unknown"])
    loop = make_loop(store, port)
    try:
        await loop.run("run-8", lambda i, r: None)
    except ServiceError as error:
        assert error.code == "execution_unknown"
    else:
        raise AssertionError("unconfirmed execution must not be reported as success")
    kinds = [kind for kind, _ in store.events]
    assert kinds == ["workspace_prepared", "process_started", "process_finished"]
    assert port.cancelled is None


async def test_loop_delivers_oversized_documents_as_controlled_file():
    store = MemoryStore(encoded({
        "task": "t",
        "extra": "z" * 200,
        "workspace": _assignment("isolated_write").model_dump(mode="json"),
    }))
    port = FakePort()
    loop = make_loop(
        store, port,
        launch=validate_process_launch(("python", "x" * 3900, "y" * 3900)),
        mode="isolated_write",
    )
    await loop.run("run-2", lambda i, r: None)
    prepared = [call for call in port.calls if call[0] == "prepare"]
    assert prepared == [(
        "prepare", ".krim/subagent-workspaces/a1",
        {"max_files": 400, "max_file_bytes": 1_048_576},
    )]
    prepared_events = [data for kind, data in store.events if kind == "workspace_prepared"]
    assert prepared_events and prepared_events[0]["mode"] == "copy"
    writes = [call for call in port.calls if call[0] == "write"]
    assert len(writes) == 1
    assert writes[0][1] == f"{TASK_INPUT_ROOT}/a1/task-input.json"
    assert writes[0][2] == "prepare-input"
    start = next(call for call in port.calls if call[0] == "start")
    argv = start[1]["argv"]
    assert argv[-1] == writes[0][1]  # argv 只携带受控文件路径
    assert argv[0] == "-c" and ".krim/subagent-workspaces/a1" in argv  # cwd 约束
    assert "x" * 3900 in argv


async def test_loop_refreshes_generation_before_task_start_after_bootstrap():
    store = MemoryStore(encoded({
        "task": "t",
        "workspace": _assignment("isolated_write").model_dump(mode="json"),
    }))
    port = FakePort(generation=2, generation_bump_on_prepare=True)
    loop = make_loop(store, port, mode="isolated_write")
    await loop.run("run-2c", lambda i, r: None)
    start = next(call for call in port.calls if call[0] == "start")
    # bootstrap 把项目推进了一代：TaskStart 必须按刷新后的 generation 提交
    assert start[1]["expected_generation"] == 3


async def test_loop_refuses_truncated_copy_before_starting_the_worker():
    store = MemoryStore(encoded({
        "task": "t",
        "workspace": _assignment("isolated_write").model_dump(mode="json"),
    }))
    port = FakePort(prepare_truncated=True)
    loop = make_loop(store, port, mode="isolated_write")
    try:
        await loop.run("run-2d", lambda i, r: None)
    except ServiceError as error:
        assert error.code == "subagent_capability"
    else:
        raise AssertionError("a truncated copy must not be reported as isolated success")
    kinds = [kind for kind, _ in store.events]
    assert "workspace_prepared" not in kinds
    assert not any(call[0] == "start" for call in port.calls)


async def test_loop_holds_process_slot_only_around_the_running_task():
    import contextlib

    store = MemoryStore(_document("t"))
    port = FakePort(waits=["running", "finished"])
    held: list[tuple[str, int]] = []

    @contextlib.asynccontextmanager
    async def slot(run_id: str):
        held.append((run_id, 1))
        try:
            yield
        finally:
            held.append((run_id, 0))

    loop = make_loop(store, port)
    loop._process_slot = slot  # type: ignore[attr-defined]
    await loop.run("run-slot", lambda i, r: None)
    # 实际 TaskStart→结束 之间恰持有一次；结束已释放
    assert held == [("run-slot", 1), ("run-slot", 0)]

    # 审批停靠不持有槽：park 在槽内抛出后随即释放，且不保留清理债
    park_port = FakePort(ask_on={"task-start"})
    park_loop = make_loop(store, park_port)
    park_held: list[str] = []
    park_debt: list[str] = []

    class ParkLease:
        async def retain_debt(self, run_id: str, operation_id: str) -> None:
            park_debt.append("retained")

        async def release(self) -> None:
            park_debt.append("released")

    @contextlib.asynccontextmanager
    async def parking_slot(run_id: str):
        park_held.append("acquire")
        try:
            yield ParkLease()
        finally:
            park_held.append("release")

    park_loop._process_slot = parking_slot  # type: ignore[attr-defined]

    async def approval(invocation, remaining):
        return {"call_id": invocation.call_id}

    with pytest.raises(Parked):
        await park_loop.run("run-slot-2", approval)
    assert park_held == ["acquire", "release"]
    assert park_debt == []  # ask 没有真实进程，不保留清理债


async def test_loop_scopes_worker_cwd_to_the_isolated_directory():
    store = MemoryStore(encoded({
        "task": "t",
        "workspace": _assignment("isolated_write", agent_id="a2").model_dump(mode="json"),
    }))
    port = FakePort()
    loop = make_loop(store, port, mode="isolated_write")
    await loop.run("run-2b", lambda i, r: None)
    start = next(call for call in port.calls if call[0] == "start")
    argv = start[1]["argv"]
    assert argv[0] == "-c"
    assert ".krim/subagent-workspaces/a2" in argv  # wrapper chdir 的目标
    assert "worker.py" in argv
    document = parse_json(argv[-1])
    assert document["task"] == "t"
    assert document["workspace"]["directory"] == ".krim/subagent-workspaces/a2"
    kinds = [kind for kind, _ in store.events]
    assert "workspace_prepared" in kinds


async def test_loop_rejects_runs_without_an_isolated_assignment():
    """isolated profile 收不到 assignment（域准入缺口）时运行前拒绝。"""
    store = MemoryStore(encoded({"task": "t", "extra": "z" * 200}))
    port = FakePort()
    loop = make_loop(
        store, port, launch=validate_process_launch(("python", "x" * 4000, "y" * 4000))
    )
    try:
        await loop.run("run-3", lambda i, r: None)
    except ServiceError as error:
        assert error.code == "subagent_capability"
    else:
        raise AssertionError("no workspace assignment: oversized document must be refused")
    assert port.calls == []


async def test_loop_refuses_shared_read_only_mode():
    """无只读挂载：shared_read_only 的 process 运行前拒绝，不碰端口。"""
    store = MemoryStore(encoded({"task": "t"}))
    port = FakePort()
    loop = make_loop(store, port, mode="shared_read_only")
    try:
        await loop.run("run-3b", lambda i, r: None)
    except ServiceError as error:
        assert error.code == "subagent_capability"
    else:
        raise AssertionError("shared_read_only process run must be refused")
    assert port.calls == []


async def test_loop_parks_on_ask_with_durable_checkpoint_and_resume_completes():
    store = MemoryStore(_document("t"))
    port = FakePort(ask_on={"task-start"})
    loop = make_loop(store, port)

    async def approval(invocation, remaining):
        return {"call_id": invocation.call_id, "budget_remaining": remaining}

    with pytest.raises(Parked):
        await loop.run("run-4", approval)
    requests = [data for kind, data in store.events if kind == "approval_request"]
    assert len(requests) == 1
    checkpoint, stored = store.checkpoints[-1]
    call_id = checkpoint["process"]["awaiting"]
    assert requests[0]["call_id"] == call_id
    assert checkpoint["format"] == "sdk-host-v2"
    assert fingerprint(checkpoint) == stored
    assert checkpoint["process"]["completed"] == ["workspace-prepare"]

    resume = sdk_resume(checkpoint, stored, {call_id: True})
    resumed_port = FakePort()
    resumed_loop = make_loop(store, resumed_port)
    await resumed_loop.run("run-4", approval, resume=resume)
    assert any(call[0] == "start" for call in resumed_port.calls)
    finished = [data for kind, data in store.events if kind == "process_finished"]
    assert len(finished) == 1


async def test_resume_with_denied_reply_fails_without_executing():
    store = MemoryStore(_document("t"))
    port = FakePort(ask_on={"task-start"})
    loop = make_loop(store, port)

    async def approval(invocation, remaining):
        return {"call_id": invocation.call_id}

    with pytest.raises(Parked):
        await loop.run("run-5", approval)
    checkpoint, stored = store.checkpoints[-1]
    resume = sdk_resume(checkpoint, stored, {checkpoint["process"]["awaiting"]: False})
    resumed_port = FakePort(ask_on={"task-start"})
    resumed_loop = make_loop(store, resumed_port)
    try:
        await resumed_loop.run("run-5", approval, resume=resume)
    except ServiceError as error:
        assert error.code == "approval_denied"
    else:
        raise AssertionError("denied reply must not execute the process")
    assert resumed_port.calls == []


async def test_resume_with_foreign_checkpoint_is_refused():
    store = MemoryStore(_document("t"))
    port = FakePort()
    loop = make_loop(store, port)
    forged = {"format": "sdk-host-v2", "run_id": "run-6"}
    resume = sdk_resume(forged, fingerprint(forged), {})
    try:
        await loop.run("run-6", lambda i, r: None, resume=resume)
    except ServiceError as error:
        assert error.code == "facts_corrupt"
    else:
        raise AssertionError("a checkpoint this loop did not write must be refused")


async def test_loop_cancels_the_real_process_on_run_cancellation():
    store = MemoryStore(_document("t"))
    port = FakePort(waits=["running", "cancel-run"])
    loop = make_loop(store, port)
    with pytest.raises(asyncio.CancelledError):
        await loop.run("run-7", lambda i, r: None)
    assert port.cancelled == (PROJECT_ID, OPERATION_ID, 0)
    kinds = [kind for kind, _ in store.events]
    assert kinds == ["workspace_prepared", "process_started"]


async def test_loop_executes_through_the_real_tool_surface_end_to_end():
    stub = SandboxStub()
    stub.command_stdout = encoded({
        "mode": "copy", "head": None, "copied": 1, "skipped": 0, "truncated": False,
    }).encode()
    executor = build_executor(stub, {"sandbox_task_start": "ask"})
    store = MemoryStore(_document("t"))
    loop = ProcessAgentLoop(
        store,
        ProcessBackend(),
        executor,
        Settings(),
        sandbox=FakeSandbox(),
        launch=validate_process_launch(("python", "worker.py")),
        workspace_mode="isolated_write",
    )

    async def approval(invocation, remaining):
        return {"call_id": invocation.call_id, "budget_remaining": remaining}

    replies: dict[str, bool] = {}
    for _ in range(4):
        try:
            await loop.run("run-9", approval, resume=(
                sdk_resume(*store.checkpoints[-1], replies)  # type: ignore[misc]
                if store.checkpoints and replies else None
            ))
            break
        except Parked:
            checkpoint, stored = store.checkpoints[-1]
            replies[checkpoint["process"]["awaiting"]] = True
    else:
        raise AssertionError("process run never completed through approvals")
    started = [call for name, call in stub.calls if name == "task_start"]
    assert len(started) == 2  # workspace bootstrap + worker TaskStart
    assert started[-1]["program"] == "python"
    worker_document = parse_json(started[-1]["argv"][-1])
    assert worker_document["task"] == "t"
    assert worker_document["workspace"]["directory"] == ".krim/subagent-workspaces/a1"
    finished = [data for kind, data in store.events if kind == "process_finished"]
    assert finished and finished[0]["exit_code"] == 0
    prepared_events = [data for kind, data in store.events if kind == "workspace_prepared"]
    assert prepared_events and prepared_events[0]["mode"] == "copy"


async def test_loop_uses_a_real_execution_budget_when_none_is_given():
    store = MemoryStore(_document("t"))
    port = FakePort()
    loop = make_loop(store, port)
    seen_budgets: list[ExecutionBudget] = []
    original_scope = FakeSandbox.scope

    async def scope_with_capture(self, run_id, check, budget):
        seen_budgets.append(budget)
        return await original_scope(self, run_id, check, budget)

    FakeSandbox.scope = scope_with_capture  # type: ignore[method-assign]
    try:
        await loop.run("run-10", lambda i, r: None)
    finally:
        FakeSandbox.scope = original_scope  # type: ignore[method-assign]
    assert len(seen_budgets) == 1
    assert seen_budgets[0].deadline is None  # Settings() 默认无 run_timeout


# ---------------------------------------------------------------------------
# 真实收尾：child run 经 child_run_owner 归属核对的进程清理（stub broker）
# ---------------------------------------------------------------------------


async def test_child_run_cleanup_proves_owner_via_child_run_owner_mapping(tmp_path):
    """child run 无 turns 行：cancel_run_tasks 经 child_run_owner 合法映射
    核对 owner 会话归属后真实取消并结算 stub broker 任务。"""
    from sqlalchemy import insert, select, update

    from krim_agent.sandbox.tasks import cancel_run_tasks
    from krim_agent.storage.account_models import accounts
    from krim_agent.storage.sandbox_models import (
        sandbox_environments,
        sandbox_operations,
        sandbox_projects,
    )
    from krim_agent.storage.sessions import child_run_owner, sessions
    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.storage.subagent_models import subagent_child_runs
    from krim_agent.subagents.storage import SubagentStore
    from krim_agent.subagents.types import AcceptSpawn, ReserveAgent, TreeScope

    store = await SQLiteStore.open(tmp_path / "cleanup.sqlite")
    now = 1234.0
    principal = "owner-it"
    session = "session-it"
    environment = "env-it"
    project_id = "33333333-3333-4333-8333-333333333333"
    operation_id = OPERATION_ID
    # 外键链按引用顺序落真实行：accounts→environments→projects/sessions→trees
    # →members/tasks→child run 映射；child run 本身是真实的 runs 行。
    async with store.engine.begin() as conn:
        await conn.execute(insert(accounts).values(
            principal_id=principal, display_name="Owner", status="active",
            platform_role="member", authorization_generation=0, revision=1,
            legacy=0, created_at=now, updated_at=now,
        ))
        await conn.execute(insert(sandbox_environments).values(
            environment_id=environment, principal_id=principal,
            profile_name=None, quota_override={}, revision=1,
            status="unprovisioned", created_at=now, updated_at=now,
        ))
        await conn.execute(insert(sessions).values(
            id=session, workspace_id="ws-it", owner_id=principal,
            create_key="k-session-it", request_hash="hash", title="宿主会话",
            target_key="target", target={}, context_policy={}, revision=1,
            status="open", dispatch_paused=False, created_at=now,
        ))
        await conn.execute(insert(sandbox_projects).values(
            project_id=project_id, environment_id=environment,
            principal_id=principal, workspace_id="ws-it", title="p",
            generation=0, status="running", lease_id=operation_id,
            lease_expires_at=now + 60, created_at=now, updated_at=now,
        ))
    root, _ = await store.begin(
        RunRequest(text="root", request_key="ac-root.cleanup"), {"key": "target-http"},
    )
    await store.finish(root.id, "completed")
    child, _ = await store.begin(
        RunRequest(text="child", request_key="ac-child.cleanup"), {"key": "target-http"},
    )
    run_id = child.id
    subagents = SubagentStore(store)
    tree = await subagents.ensure_tree(TreeScope(
        session_id=session, run_id=root.id, workspace_id="ws-it",
        owner_id=principal, project_id=project_id, request_key="tree-cleanup",
    ))
    lead = await subagents.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"], parent_id=None, path="lead", role="root",
        provider="sdk_session", profile="default",
        provider_version="provider-v", profile_version="profile-v",
        request_key="spawn:lead",
    ))
    worker = await subagents.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"], parent_id=lead["agent"]["agent_id"], path="worker",
        role="agent", provider="process", profile="p",
        provider_version="provider-v", profile_version="profile-v",
        request_key="spawn:worker",
    ))
    accepted = await subagents.accept_spawn(AcceptSpawn(
        agent_id=worker["agent"]["agent_id"],
        activation_id=worker["activation"]["activation_id"],
        input={"text": "t"},
        parent_run_id=root.id,
    ))
    # child run 合法映射（owner session/project/workspace 归属）
    async with store.engine.begin() as conn:
        await conn.execute(insert(subagent_child_runs).values(
            run_id=run_id, task_id=accepted["task_id"],
            agent_id=worker["agent"]["agent_id"],
            activation_id=worker["activation"]["activation_id"],
            tree_id=tree["tree_id"], owner_session_id=session,
            owner_turn_id=None, source_version=1, created_at=now,
        ))
        await conn.execute(insert(sandbox_operations).values(
            operation_id=operation_id, principal_id=principal,
            request_key="command:it", request_hash="0" * 64,
            project_id=project_id, kind="command", status="intent",
            result={
                "run_id": run_id, "session_id": session,
                "project_id": project_id, "environment_id": environment,
                "background": True,
            },
            created_at=now, updated_at=now,
        ))
    owner = await store.work_read(lambda conn: child_run_owner(conn, run_id))
    assert owner is not None and owner["workspace_id"] == "ws-it"

    cancelled: list[str] = []

    class StubBroker:
        async def call(self, action: str, **arguments: Any):
            assert action == "command_status"
            assert arguments["cancel"] is True
            cancelled.append(str(arguments["operation_id"]))
            return {
                "status": "finished",
                "generation": 1,
                "receipt": {
                    "executor_started": True, "exit_code": 0, "stdout": "",
                    "stderr": "", "output_complete": True,
                    "cleanup": "clean", "code": None,
                },
            }

    service = SimpleNamespace(store=store, broker=StubBroker())
    await cancel_run_tasks(service, run_id)
    assert cancelled == [operation_id]
    async def read_project(conn):
        return (await conn.execute(
            select(sandbox_projects).where(sandbox_projects.c.project_id == project_id)
        )).mappings().one()

    project = await store.work_read(read_project)
    assert project["status"] == "ready" and project["lease_id"] is None

    async def read_operation(conn):
        return (await conn.execute(
            select(sandbox_operations).where(
                sandbox_operations.c.operation_id == operation_id,
            )
        )).mappings().one()

    operation = await store.work_read(read_operation)
    assert operation["status"] == "completed"

    # 归属不符（owner session 不一致）：facts_corrupt，不取消
    await store.work_read(lambda conn: conn.execute(
        update(sandbox_operations).values(
            status="intent", result={
                "run_id": run_id, "session_id": "session-other",
                "project_id": project_id, "environment_id": environment,
                "background": True,
            },
        ).where(sandbox_operations.c.operation_id == operation_id)
    ))
    await store.work_read(lambda conn: conn.execute(
        update(sandbox_projects).values(
            status="running", lease_id=operation_id,
        ).where(sandbox_projects.c.project_id == project_id)
    ))
    try:
        await cancel_run_tasks(service, run_id)
    except ServiceError as error:
        assert error.code == "facts_corrupt"
    else:
        raise AssertionError("mismatched owner session must be refused")


async def test_unknown_execution_retains_process_debt_until_cleanup(tmp_path):
    """unknown：槽按清理债保留、占住并发额度；domain reconcile 确认干净
    （lease.release）后归还。取消失败同样保留。"""
    import contextlib

    store = MemoryStore(_document("t"))
    port = FakePort(waits=["unknown"])

    retained: list[tuple[str, str]] = []
    capacity = {"active": 0}
    acquisitions: list[str] = []

    class FakeLease:
        def __init__(self) -> None:
            self.debt: tuple[str, str] | None = None
            self.released = False

        async def retain_debt(self, run_id: str, operation_id: str) -> None:
            self.debt = (run_id, operation_id)
            retained.append(self.debt)

        async def release(self) -> None:
            self.released = True
            capacity["active"] -= 1
            acquisitions.append("released")

    lease_box: list[FakeLease] = []

    @contextlib.asynccontextmanager
    async def slot(run_id: str):
        if capacity["active"] >= 1:
            raise ServiceError("capacity", 429)
        capacity["active"] += 1
        acquisitions.append("acquired")
        lease = FakeLease()
        lease_box.append(lease)
        try:
            yield lease
        finally:
            if not lease.released:
                capacity["active"] -= 1  # 兜底：测试里不应走到（debt 保留时不释放）

    loop = make_loop(store, port)
    loop._process_slot = slot  # type: ignore[attr-defined]
    try:
        await loop.run("run-debt", lambda i, r: None)
    except ServiceError as error:
        assert error.code == "execution_unknown"
    else:
        raise AssertionError("unknown must fail the run")
    assert lease_box and lease_box[0].debt == ("run-debt", OPERATION_ID)
    assert capacity["active"] == 1  # 清理债占住额度

    # 清理债占住额度期间，新启动被拒（并发额度 1）
    second = slot("run-next")
    try:
        await second.__aenter__()
    except ServiceError as error:
        assert error.code == "capacity"
    else:
        raise AssertionError("retained debt must hold process capacity")

    # domain reconcile 确认干净 → release → 额度归还
    await lease_box[0].release()
    assert capacity["active"] == 0
    third = slot("run-next")
    await third.__aenter__()
    await third.__aexit__(None, None, None)
    assert acquisitions.count("acquired") == 2


async def test_cancel_failure_retains_process_debt(tmp_path=None):
    import contextlib

    store = MemoryStore(_document("t"))
    port = FakePort(waits=["running", "cancel-run"])
    port.cancel_raises = True
    retained: list[str] = []

    class FakeLease:
        async def retain_debt(self, run_id: str, operation_id: str) -> None:
            retained.append(operation_id)

        async def release(self) -> None:
            retained.append("released")

    lease_holder: list[FakeLease] = []

    @contextlib.asynccontextmanager
    async def slot(run_id: str):
        lease = FakeLease()
        lease_holder.append(lease)
        yield lease

    loop = make_loop(store, port)
    loop._process_slot = slot  # type: ignore[attr-defined]
    with pytest.raises(asyncio.CancelledError):
        await loop.run("run-debt-cancel", lambda i, r: None)
    assert retained == [OPERATION_ID]  # 取消失败：清理债保留，未释放


async def test_cancel_returning_unknown_retains_process_debt():
    """cancel 返回 unknown（未确认 finished）：真实进程可能仍在跑，债务保留。"""
    import contextlib

    store = MemoryStore(_document("t"))
    port = FakePort(waits=["running", "cancel-run"], cancel_status="unknown")
    retained: list[str] = []

    class FakeLease:
        async def retain_debt(self, run_id: str, operation_id: str) -> None:
            retained.append(operation_id)

        async def release(self) -> None:
            retained.append("released")

    @contextlib.asynccontextmanager
    async def slot(run_id: str):
        yield FakeLease()

    loop = make_loop(store, port)
    loop._process_slot = slot  # type: ignore[attr-defined]
    with pytest.raises(asyncio.CancelledError):
        await loop.run("run-debt-unknown", lambda i, r: None)
    assert retained == [OPERATION_ID]  # cancel 未确认 finished：债务保留，未释放


# ---------------------------------------------------------------------------
# 真实 RunService 集成：start_delegate → ask 停靠 → reply_approval → 恢复
# ---------------------------------------------------------------------------


async def test_real_runservice_delegate_park_reply_resume(tmp_path):
    """真实 start_delegate/停靠/批复/恢复链路：fake 只有沙箱端口。

    runtime、SQLiteStore、SubagentStore、child_run 归属映射、approval 请求/
    回执事件与 reply_approval 恢复全部走真实实现；profile isolated_write，
    初始 assignment 在文档里，worker 脚本输入保持原样。
    """
    from krim_agent.runtime.service import RunService
    from krim_agent.storage.sessions import sessions
    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.subagents.storage import SubagentStore
    from krim_agent.subagents.types import AcceptSpawn, ReserveAgent, TreeScope

    store = await SQLiteStore.open(tmp_path / "process-it.sqlite")
    subagents_store = SubagentStore(store)
    # 真实 FK：subagent_trees.session_id → sessions.id（宿主会话先行落库）
    from sqlalchemy import insert

    async with store.engine.begin() as conn:
        await conn.execute(insert(sessions).values(
            id="session-it", workspace_id="ws-it", owner_id="owner-it",
            create_key="k-session-it", request_hash="hash", title="宿主会话",
            target_key="target", target={}, context_policy={}, revision=1,
            status="open", dispatch_paused=False, created_at=1.0,
        ))

    # 根 run 锚点（completed → 预算池释放，子 run 按自身期限推进）
    root, _ = await store.begin(
        RunRequest(text="root", request_key="ac-root.it"), {"key": "target-http"},
    )
    await store.finish(root.id, "completed")

    tree = await subagents_store.ensure_tree(TreeScope(
        session_id="session-it",
        run_id=root.id,
        workspace_id="ws-it",
        owner_id="owner-it",
        request_key="tree-it",
    ))
    lead = await subagents_store.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"], parent_id=None, path="lead", role="root",
        provider="sdk_session", profile="default",
        provider_version="provider-v", profile_version="profile-v",
        request_key="spawn:lead",
    ))
    worker = await subagents_store.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"], parent_id=lead["agent"]["agent_id"], path="worker",
        role="agent", provider="process", profile="p",
        provider_version="provider-v", profile_version="profile-v",
        request_key="spawn:worker",
    ))
    accepted = await subagents_store.accept_spawn(AcceptSpawn(
        agent_id=worker["agent"]["agent_id"],
        activation_id=worker["activation"]["activation_id"],
        input={"text": "t", "workspace": _assignment("isolated_write").model_dump(mode="json")},
        parent_run_id=root.id,
    ))

    port = FakePort(ask_on={"task-start"})

    def factory(_executor, _context, run_id, approved):
        return port.bind(run_id, approved)

    loop = ProcessAgentLoop(
        store,
        ProcessBackend(),
        ToolExecutor(ToolRegistry(()), None),
        Settings(),
        sandbox=FakeSandbox(store=store),
        launch=validate_process_launch(("python", "worker.py")),
        workspace_mode="isolated_write",
        port=factory,
    )
    binding = RunBinding.freeze("subagent:test:p", loop, Settings())
    descriptor = parse_json(binding.descriptor_json)
    service = RunService(
        store,
        loop,
        Settings(),
        {"key": "subagent:test:p"},
        bindings={"target-http": binding},
        delegates={"subagent:test:p": binding},
        subagents_store=subagents_store,
    )

    provider = ProcessProvider(
        service,
        descriptor,
        launch=validate_process_launch(("python", "worker.py")),
        workspace_mode="isolated_write",
    )
    spec = StartTask(
        agent_id=worker["agent"]["agent_id"],
        task_id=accepted["task_id"],
        activation_id=worker["activation"]["activation_id"],
        request=RunRequest(
            text=build_task_document("t", _assignment("isolated_write")),
            request_key="ac-child.t1",
        ),
        target=dict(descriptor),
        owner_turn_id=None,
    )
    view = await provider.start(spec)
    assert view.status in {"queued", "running"}

    async def wait_status(*statuses: str) -> RunView:
        deadline = asyncio.get_running_loop().time() + 45
        while asyncio.get_running_loop().time() < deadline:
            current = (await store.snapshot(view.id)).run
            if current.status in statuses:
                return current
            await asyncio.sleep(0.05)
        raise AssertionError(f"run stuck at {current.status}, wanted {statuses}")

    parked = await wait_status("awaiting")
    assert parked.stop_requested is False
    snapshot = await store.snapshot(view.id)
    request = next(
        event.data for event in snapshot.events if event.kind == "approval_request"
    )
    # 停靠点：本 loop 的 process 检查点已落库且可被 RunService 恢复链接受
    stored = await store.read_checkpoint(view.id)
    assert stored is not None
    checkpoint, stored_fp = stored
    assert checkpoint["format"] == "sdk-host-v2"
    assert fingerprint(checkpoint) == stored_fp

    from krim_agent.contracts import ApprovalReply

    reply = ApprovalReply(
        request_key=snapshot.request.request_key,
        call_id=str(request["call_id"]),
        arguments_hash=str(request["arguments_hash"]),
        tool_hash=str(request["tool_hash"]),
        allow=True,
    )
    await service.reply_approval(view.id, str(request["request_id"]), reply)

    finished = await wait_status("completed")
    assert finished.code is None
    # 真实SQLiteStore没有MemoryStore.events列表：loop emitted三类kind从
    # 持久事件流过滤（run_started/approval_request/approval_reply/terminal
    # 为RunService机制事件，不在本断言内）。
    snapshot = await store.snapshot(view.id)
    kinds = [
        event.kind for event in snapshot.events
        if event.kind in {"workspace_prepared", "process_started", "process_finished"}
    ]
    assert kinds == [
        "workspace_prepared", "process_started", "process_finished",
    ]
    # worker 脚本输入保持原样：argv 尾部是投递文档，cwd 指向隔离目录
    start_call = next(call for call in port.calls if call[0] == "start")
    argv = start_call[1]["argv"]
    assert "worker.py" in argv and argv[0] == "-c"
    assert parse_json(argv[-1]) == {
        "task": "t",
        "workspace": parse_json(build_task_document("t", _assignment("isolated_write")))[
            "workspace"
        ],
    }
    assert port.cancelled is None
    await service.close()
