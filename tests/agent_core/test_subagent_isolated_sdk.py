"""SDK child relocation wiring: assignment gate and approval projection wrapper."""

from __future__ import annotations

import hashlib
import math
from types import SimpleNamespace
from typing import Any

import pytest

from krim_agent.config import Settings
from krim_agent.context.types import ChildRunContext
from krim_agent.contracts import ServiceError, ToolObservation, encoded, fingerprint, parse_json
from krim_agent.development.contracts import (
    EnvironmentHandle,
    OperationAdmission,
    OperationIdentity,
    OperationReceipt,
    RunOwnership,
)
from krim_agent.development.service import OutputInput, ProcessInput, ShellInput
from krim_agent.runtime.recovery import BootstrapResume
from krim_agent.runtime.sdk_execution import Parked
from krim_agent.runtime.sdk_loop import DevelopmentSDKLoop
from krim_agent.subagents.isolated_bootstrap import bootstrap_command, prepare_isolated_workspace
from krim_agent.subagents.isolated_tools import isolated_executor
from krim_agent.subagents.workspace import WorkspaceAssignment, workspace_directory
from krim_agent.tools.core import Invocation, ToolSpec
from krim_agent.tools.executor import Admission
from krim_agent.tools.registry import ToolRegistry

DIRECTORY = workspace_directory("a1")
ENVIRONMENT = EnvironmentHandle(
    owner_id="owner", project_id="proj-1", environment_id="dev-1",
    generation=1, state="ready",
)
OWNERSHIP = RunOwnership(owner_id="owner", session_id="session:1", run_id="run-1")
MANIFEST = (
    '{"mode": "worktree", "head": "' + "0" * 40
    + '", "copied": 3, "skipped": 0, "truncated": false}'
)
GOOD_OUTPUT = "prep\n" + MANIFEST + "\n"


def _child(task_id: str = "task-1") -> ChildRunContext:
    return ChildRunContext(
        workspace_id="ws", session_id="session", owner_id="owner", turn_id=None,
        tree_id="tree", agent_id="a1", activation_id="act-1", task_id=task_id,
    )


def _assignment() -> dict:
    return {
        "mode": "isolated_write", "agent_id": "a1",
        "source_project_id": "proj-1", "workspace_project_id": "proj-1",
        "directory": DIRECTORY, "baseline_fingerprint": "0" * 64,
        "input_version": 1,
    }


class _StubRepository:
    def __init__(self, tasks: dict[str, dict]) -> None:
        self._tasks = tasks

    async def task(self, task_id: str) -> dict:
        return self._tasks[task_id]


class _StubSubagents:
    def __init__(self, tasks: dict[str, dict]) -> None:
        self.repository = _StubRepository(tasks)


class _StubStore:
    def __init__(self, session_context: Any = None, events: list | None = None) -> None:
        self._context = session_context
        self.events = events if events is not None else []
        self.appended: list = []
        self.checkpoints: list = []

    async def snapshot(self, run_id: str) -> Any:
        return SimpleNamespace(session_context=self._context, events=self.events)

    async def append(self, run_id: str, kind: str, data: dict) -> None:
        self.appended.append((kind, data))

    async def write_checkpoint(self, run_id: str, checkpoint: dict, stored: str) -> None:
        self.checkpoints.append((checkpoint, stored))


class _ExplodingStore:
    async def snapshot(self, run_id: str) -> Any:
        raise ServiceError("storage_unavailable")


def _loop(
    store: Any,
    subagents: Any = None,
    projection: Any = None,
    session_context: Any = None,
) -> DevelopmentSDKLoop:
    if session_context is not None:
        store = _StubStore(session_context)
    loop = DevelopmentSDKLoop(
        store=store, provider=object(), executor=object(), settings=Settings(),
        resolve_scope=None, repositories=object(), cleanup_run=None, descriptor={},
        approval_projection=projection, media_for_run=None,
    )
    loop.subagents = subagents
    return loop


async def test_assignment_gate_reads_isolated_children() -> None:
    tasks = {
        "isolated": {"input": {"workspace": _assignment()}},
        "shared": {"input": {}},
        "malformed": {"input": {"workspace": {"mode": 42}}},
    }
    loop = _loop(None, _StubSubagents(tasks))

    assignment = await loop._isolated_assignment(_child("isolated"))
    assert assignment is not None
    assert assignment.directory == DIRECTORY
    assert await loop._isolated_assignment(_child("shared")) is None
    with pytest.raises(ServiceError) as corrupt:
        await loop._isolated_assignment(_child("malformed"))
    assert corrupt.value.code == "facts_corrupt"

    # Without the subagent domain service there is nothing to relocate.
    orphan = _loop(None, None)
    assert await orphan._isolated_assignment(_child("isolated")) is None


async def test_projection_wrapper_relocates_only_isolated_children() -> None:
    captured: list = []

    async def projection(invocation: Any, data: dict) -> dict:
        captured.append((invocation, data))
        return data

    subagents = _StubSubagents({"task-1": {"input": {"workspace": _assignment()}}})
    child_loop = _loop(None, subagents, projection, session_context=_child())
    wrapper = child_loop.development_approval_projection
    assert wrapper is not projection

    invocation = Invocation(
        run_id="run-1", call_id="call-1", span_id="span-1", step_index=0,
        name="write", arguments_json=encoded({"path": "src/x.py"}), tool_hash="0" * 64,
    )
    data: dict = {"name": "write", "arguments": {"path": "src/x.py"}}
    result = await wrapper(invocation, dict(data))

    seen_invocation, seen_data = captured[0]
    assert seen_invocation is not invocation
    assert seen_invocation.call_id == "call-1"
    assert parse_json(seen_invocation.arguments_json)["path"] == f"{DIRECTORY}/src/x.py"
    assert seen_data["arguments"]["path"] == f"{DIRECTORY}/src/x.py"
    assert seen_data["workspace"] == {"mode": "isolated_write", "directory": DIRECTORY}
    assert result is seen_data

    # A non-child run keeps the original call and payload untouched.
    plain_loop = _loop(_StubStore(None), subagents, projection)
    await plain_loop.development_approval_projection(invocation, dict(data))
    assert captured[1][0] is invocation
    assert "workspace" not in captured[1][1]

    # A shared-workspace child has no relocation either.
    shared_loop = _loop(
        None, _StubSubagents({"task-1": {"input": {}}}), projection,
        session_context=_child(),
    )
    await shared_loop.development_approval_projection(invocation, dict(data))
    assert captured[2][0] is invocation
    assert "workspace" not in captured[2][1]


async def test_projection_wrapper_falls_back_when_state_is_unreadable() -> None:
    captured: list = []

    async def projection(invocation: Any, data: dict) -> dict:
        captured.append((invocation, data))
        return data

    loop = _loop(_ExplodingStore(), None, projection)
    invocation = Invocation(
        run_id="run-1", call_id="call-1", span_id="span-1", step_index=0,
        name="shell", arguments_json=encoded({"command": "pytest"}), tool_hash="0" * 64,
    )
    await loop.development_approval_projection(invocation, {"name": "shell"})
    # Display-only state resolution must never break the approval flow.
    assert captured[0][0] is invocation


def test_bootstrap_command_quotes_the_fixed_script() -> None:
    command = bootstrap_command(DIRECTORY, {"max_files": 400})
    assert command.startswith("exec /usr/bin/python3 -I -c ")
    assert f" {DIRECTORY} " in command
    with pytest.raises(ServiceError):
        bootstrap_command("", {})


async def _legacy(parameters: Any) -> Any:
    raise ServiceError("workspace_contract_required")


def _specs(captured_scoped: list) -> dict[str, ToolSpec]:
    async def scoped(invocation: Any, parsed: Any, context: Any) -> dict:
        captured_scoped.append((invocation, parsed))
        return {"ok": True}

    return {
        name: ToolSpec(
            name=name, parameters=parameters, executor=_legacy,
            output_schema={"type": "object"}, version="development/1",
            implementation_digest=hashlib.sha256(name.encode()).hexdigest(),
            risk="exec" if name == "shell" else "read",
            scoped_executor=scoped,
            capability="command" if name == "shell" else "read",
            description=name,
        )
        for name, parameters in (
            ("shell", ShellInput),
            ("process_status", ProcessInput),
            ("read_output", OutputInput),
        )
    }


def _observation(name: str, value: dict) -> Any:
    return ToolObservation(
        call_id=f"obs-{name}", name=name, step_index=0, span_id="span-1",
        arguments={}, status="ok", decision="allow", executor_started=True,
        result_available=True, value=value,
    )


class _StubOperations:
    def __init__(self, receipt: Any = None, *, identity: Any = None) -> None:
        self._receipt = receipt
        self._identity = identity
        self.looked: list = []
        self.admitted: list = []
        self.saved: list = []

    async def lookup(self, identity: Any) -> Any:
        self.looked.append(identity)
        return self._receipt

    async def admit(self, identity: Any) -> Any:
        self.admitted.append(identity)
        receipt = self._receipt or OperationReceipt(identity=identity, state="accepted")
        return OperationAdmission(receipt=receipt, execute=self._receipt is None)

    async def save_receipt(self, receipt: Any) -> None:
        self.saved.append(receipt)


class _StubExecutor:
    """Real admission objects over scripted per-tool observations."""

    def __init__(
        self,
        specs: dict[str, ToolSpec],
        observations: dict[str, list[Any]],
        *,
        decision: str = "allow",
    ) -> None:
        self.registry = SimpleNamespace(get=lambda name: specs[name])
        self._observations = observations
        self._decision = decision
        self.admitted: list = []
        self.executed: list = []

    async def admit(self, invocation: Invocation, context: Any, *, timeout_seconds: Any) -> Any:
        self.admitted.append(invocation)
        return Admission(
            spec=self.registry.get(invocation.name),
            parsed=self.registry.get(invocation.name).parameters.model_validate(
                parse_json(invocation.arguments_json),
            ),
            decision=self._decision,
        )

    async def execute(self, invocation: Invocation, context: Any, admission: Any) -> Any:
        self.executed.append((invocation, admission))
        return self._observations[invocation.name].pop(0)


class _Budget:
    remaining: float | None = 30.0
    deadline: float | None = None


async def _run_bootstrap(
    executor: Any,
    store: Any,
    operations: Any,
    *,
    resume: Any = None,
    budget: Any | None = None,
    approval_request: Any = None,
) -> list:
    emitted: list = []

    async def emit(kind: str, data: dict) -> None:
        emitted.append((kind, data))

    if approval_request is None:
        async def approval_request(invocation: Any, remaining: Any) -> dict:
            return {"request_id": "req-1", "call_id": invocation.call_id}

    await prepare_isolated_workspace(
        run_id="run-1",
        store=store,
        operations=operations,
        executor=executor,
        context=object(),
        environment=ENVIRONMENT,
        ownership=OWNERSHIP,
        emit=emit,
        approval_request=approval_request,
        resume=resume,
        budget=budget if budget is not None else _Budget(),
        assignment=WorkspaceAssignment.model_validate(_assignment()),
        task_id="task-1",
    )
    return emitted


def _good_shell_observation() -> Any:
    return _observation("shell", {
        "state": "exited", "exit_code": 0,
        "output": {"text": GOOD_OUTPUT, "eof": True, "truncated": False},
    })


async def test_bootstrap_runs_once_marks_event_and_receipt() -> None:
    captured: list = []
    executor = _StubExecutor(
        _specs(captured), {"shell": [_good_shell_observation()]},
    )
    store, operations = _StubStore(), _StubOperations()

    emitted = await _run_bootstrap(executor, store, operations)

    assert operations.admitted and operations.admitted[0].operation_id == "op-bootstrap-run-1"
    assert len(executor.executed) == 1
    assert executor.executed[0][0].call_id == "bootstrap-run-1"
    assert operations.saved[0].state == "completed"
    assert operations.saved[0].result_ref == "bootstrap-run-1"
    assert len(emitted) == 1
    payload = emitted[0][1]
    assert payload["directory"] == DIRECTORY
    assert payload["agent_id"] == "a1"
    assert payload["task_id"] == "task-1"
    assert payload["baseline_fingerprint"] == "0" * 64
    assert payload["mode"] == "worktree"
    assert payload["truncated"] is False
    assert payload["replayed"] is False


async def test_bootstrap_parks_with_durable_checkpoint_then_replays() -> None:
    captured: list = []
    executor = _StubExecutor(
        _specs(captured), {"shell": [_good_shell_observation()]}, decision="ask",
    )
    store, operations = _StubStore(), _StubOperations()

    with pytest.raises(Parked):
        await _run_bootstrap(executor, store, operations)

    assert executor.executed == []
    assert len(store.appended) == 1
    assert store.appended[0][0] == "approval_request"
    assert len(store.checkpoints) == 1
    checkpoint, stored = store.checkpoints[0]
    assert checkpoint["format"] == "isolated-bootstrap/1"
    assert checkpoint["run_id"] == "run-1"
    assert checkpoint["ownership"] == OWNERSHIP.model_dump(mode="json")
    assert checkpoint["budget"] == {"remaining": 30.0}
    assert checkpoint["pending_invocations"][0]["call_id"] == "bootstrap-run-1"
    assert fingerprint(checkpoint) == stored
    # 真正 JSON 落库语义：序列化往返后 runtime 恢复解析器必须接受。
    from krim_agent.runtime.recovery import bootstrap_resume

    roundtrip = parse_json(encoded(checkpoint))
    envelope = bootstrap_resume(roundtrip, stored, {"bootstrap-run-1": True})
    assert envelope.identity == checkpoint["identity"]
    assert envelope.budget_remaining == 30.0
    assert envelope.pending[0]["call_id"] == "bootstrap-run-1"

    # 批复唤醒：RunService prepare_resume 分发 BootstrapResume，只重放同一
    # 条未开始的 host 调用。
    resume = BootstrapResume(
        checkpoint=checkpoint,
        fingerprint=stored,
        replies={"bootstrap-run-1": True},
        ownership=checkpoint["ownership"],
        identity=checkpoint["identity"],
        budget_remaining=30.0,
        pending=tuple(checkpoint["pending_invocations"]),
    )
    emitted = await _run_bootstrap(executor, store, operations, resume=resume)
    assert len(executor.executed) == 1
    assert executor.executed[0][1].approved is True
    assert operations.saved[0].state == "completed"
    assert emitted[0][1]["replayed"] is False


async def test_bootstrap_rejects_identity_drift_on_replay() -> None:
    executor = _StubExecutor(
        _specs([]), {"shell": [_good_shell_observation()]}, decision="ask",
    )
    store, operations = _StubStore(), _StubOperations()
    drifted = OperationIdentity(
        operation_id="op-other", call_id="other", environment=ENVIRONMENT,
        ownership=OWNERSHIP, request_digest="0" * 64,
    )
    resume = BootstrapResume(
        checkpoint={"format": "isolated-bootstrap/1"},
        fingerprint="stored",
        replies={"bootstrap-run-1": True},
        ownership={},
        identity=drifted.model_dump(mode="json"),
        budget_remaining=30.0,
        pending=(),
    )
    with pytest.raises(ServiceError) as corrupt:
        await _run_bootstrap(executor, store, operations, resume=resume)
    assert corrupt.value.code == "facts_corrupt"


async def test_bootstrap_skips_when_the_event_already_exists() -> None:
    executor = _StubExecutor(_specs([]), {"shell": [_good_shell_observation()]})
    store = _StubStore(events=[SimpleNamespace(kind="workspace_prepared")])

    emitted = await _run_bootstrap(executor, store, _StubOperations())

    assert executor.admitted == [] and executor.executed == []
    assert emitted == []


async def test_bootstrap_completed_receipt_is_verified_from_persisted_end() -> None:
    good = {
        "state": "exited", "exit_code": 0,
        "output": {"text": GOOD_OUTPUT, "eof": True, "truncated": False},
    }
    event_data = ToolObservation(
        call_id="bootstrap-run-1", name="shell", step_index=0, span_id="span-1",
        arguments={}, status="ok", decision="allow", executor_started=True,
        result_available=True, value=good,
    ).model_dump(mode="json")
    store = _StubStore(events=[SimpleNamespace(kind="tool_end", data=event_data)])
    completed = OperationReceipt(
        identity=OperationIdentity(
            operation_id="op-bootstrap-run-1", call_id="bootstrap-run-1",
            environment=ENVIRONMENT, ownership=OWNERSHIP, request_digest="1" * 64,
        ),
        state="completed", result_ref="bootstrap-run-1",
    )
    executor = _StubExecutor(_specs([]), {"shell": []})

    emitted = await _run_bootstrap(executor, store, _StubOperations(receipt=completed))

    assert executor.admitted == [] and executor.executed == []
    assert emitted[0][1]["replayed"] is True
    assert emitted[0][1]["exit_code"] == 0

    # 持久终局不是 exit 0/未截断就不算准备完成。
    bad = ToolObservation(
        call_id="bootstrap-run-1", name="shell", step_index=0, span_id="span-1",
        arguments={}, status="ok", decision="allow", executor_started=True,
        result_available=True,
        value={"state": "exited", "exit_code": 1,
               "output": {"text": GOOD_OUTPUT, "eof": True, "truncated": False}},
    ).model_dump(mode="json")
    bad_store = _StubStore(events=[SimpleNamespace(kind="tool_end", data=bad)])
    with pytest.raises(ServiceError) as failed:
        await _run_bootstrap(
            executor, bad_store, _StubOperations(receipt=completed),
        )
    assert failed.value.code == "subagent_capability"


async def test_bootstrap_existing_unconfirmed_operation_is_never_reexecuted() -> None:
    accepted = OperationReceipt(
        identity=OperationIdentity(
            operation_id="op-bootstrap-run-1", call_id="bootstrap-run-1",
            environment=ENVIRONMENT, ownership=OWNERSHIP, request_digest="1" * 64,
        ),
        state="accepted",
    )
    executor = _StubExecutor(_specs([]), {"shell": [_good_shell_observation()]})
    with pytest.raises(ServiceError) as unconfirmed:
        await _run_bootstrap(executor, _StubStore(), _StubOperations(receipt=accepted))
    assert unconfirmed.value.code == "execution_unknown"
    assert executor.executed == []


async def test_bootstrap_waits_for_the_real_handle_before_verifying() -> None:
    captured: list = []
    specs = _specs(captured)
    running = _observation("shell", {
        "state": "running", "exit_code": None, "process_id": "proc-1",
        "output": None,
    })
    status_running = _observation("process_status", {
        "process_id": "proc-1", "state": "running", "exit_code": None,
    })
    status_exited = _observation("process_status", {
        "process_id": "proc-1", "state": "exited", "exit_code": 0,
    })
    page = _observation("read_output", {
        "process_id": "proc-1", "cursor": 0, "next_cursor": 0,
        "text": GOOD_OUTPUT, "eof": True, "truncated": False,
    })
    executor = _StubExecutor(
        specs,
        {"shell": [running], "process_status": [status_running, status_exited],
         "read_output": [page]},
    )
    store, operations = _StubStore(), _StubOperations()

    emitted = await _run_bootstrap(executor, store, operations)

    names = [invocation.name for invocation, _ in executor.executed]
    assert names == ["shell", "process_status", "process_status", "read_output"]
    assert operations.saved[0].state == "completed"
    assert emitted[0][1]["mode"] == "worktree"
    assert math.isfinite(1.0)


async def test_bootstrap_budget_deadline_stops_the_poll_as_unknown() -> None:
    executor = _StubExecutor(
        _specs([]),
        {"shell": [_observation("shell", {
            "state": "running", "exit_code": None, "process_id": "proc-1",
            "output": None,
        })],
         "process_status": [_observation("process_status", {
             "process_id": "proc-1", "state": "running", "exit_code": None,
         })],
         "read_output": []},
    )

    class _Expired:
        remaining: float | None = 0.0
        deadline: float | None = 0.0

    operations = _StubOperations()
    with pytest.raises(ServiceError) as unknown:
        await _run_bootstrap(executor, _StubStore(), operations, budget=_Expired())
    assert unknown.value.code == "execution_unknown"
    assert operations.saved[0].state == "unknown"


async def test_bootstrap_requires_a_development_shell_surface() -> None:
    fake_spec = SimpleNamespace(scoped_executor=None, capability="read", fingerprint="0" * 64)
    executor = SimpleNamespace(registry=SimpleNamespace(get=lambda name: fake_spec))
    with pytest.raises(ServiceError) as unavailable:
        await _run_bootstrap(executor, _StubStore(), _StubOperations())
    assert unavailable.value.code == "workspace_unavailable"


def test_model_facing_schemas_carry_the_derived_semantics() -> None:
    """The schemas the model sees come from the derived registry, so its
    tool_hash computation and the admission fence share one specification."""
    from krim_agent.agent_sdk.model import agent_tools
    from krim_agent.tools.executor import ToolExecutor

    captured: list = []

    async def allow(invocation: Any) -> str:
        return "allow"

    specs = _specs(captured)
    base = ToolExecutor(ToolRegistry(tuple(specs.values())), allow)
    derived = isolated_executor(base, DIRECTORY)

    tools = agent_tools(derived.registry)
    assert {tool.tool.name for tool in tools} >= set(specs)
    base_digests = {
        spec.name: spec.implementation_digest for spec, _ in base.registry._frozen
    }
    for name in specs:
        derived_spec = derived.registry.get(name)
        if name in {
            "shell", "read", "list", "find", "search", "write", "edit",
        }:
            assert derived_spec.implementation_digest != base_digests[name]
        else:
            # Non-path tools keep the original specification untouched.
            assert derived_spec is base.registry.get(name)
            assert derived_spec.implementation_digest == base_digests[name]
    descriptors = {
        descriptor["name"]: descriptor["implementation_digest"]
        for descriptor in derived.registry.schemas()
    }
    for name in specs:
        assert descriptors[name] == derived.registry.get(name).implementation_digest


async def test_bootstrap_failed_shell_saves_unknown_receipt() -> None:
    executor = _StubExecutor(
        _specs([]),
        {"shell": [ToolObservation(
            call_id="obs-shell", name="shell", step_index=0, span_id="span-1",
            arguments={}, status="denied", decision="deny", executor_started=False,
            result_available=False, code="permission_denied",
        )]},
    )
    operations = _StubOperations()
    with pytest.raises(ServiceError) as denied:
        await _run_bootstrap(executor, _StubStore(), operations)
    assert denied.value.code == "permission_denied"
    assert operations.saved[0].state == "unknown"
