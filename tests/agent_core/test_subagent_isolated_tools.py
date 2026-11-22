"""Isolated-write relocation: real path/cwd redirection, not prompts."""

from __future__ import annotations

import hashlib
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from krim_agent.contracts import ServiceError, encoded, parse_json
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    RunOwnership,
)
from krim_agent.development.file_tools import (
    EditInput,
    FileInput,
    FindInput,
    SearchInput,
    WriteInput,
)
from krim_agent.development.service import ListInput, ReadInput, ShellInput
from krim_agent.sandbox.filesystem import _parts
from krim_agent.subagents.isolated_tools import (
    ISOLATED_TOOL_NAMES,
    ISOLATED_VERSION,
    isolate_arguments,
    isolate_invocation,
    isolated_executor,
    isolated_registry,
    validate_isolated_directory,
)
from krim_agent.subagents.tool_models import SpawnAgent
from krim_agent.subagents.workspace import workspace_directory
from krim_agent.tools.bundles import compose_executor
from krim_agent.tools.core import Invocation, ToolSpec
from krim_agent.tools.executor import ToolContext, ToolExecutor
from krim_agent.tools.pipeline import CURRENT, Scope
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import ToolBundle

DIRECTORY = workspace_directory("a1")


def _digest(name: str) -> str:
    return hashlib.sha256(name.encode()).hexdigest()


def _invocation(name: str, arguments: dict, tool_hash: str = "0" * 64) -> Invocation:
    return _raw_invocation(name, encoded(arguments), tool_hash)


def _raw_invocation(name: str, arguments_json: str, tool_hash: str = "0" * 64) -> Invocation:
    return Invocation(
        run_id="run-1", call_id="call-1", span_id="span-1", step_index=0,
        name=name, arguments_json=arguments_json, tool_hash=tool_hash,
    )


async def _unavailable(parameters: object) -> object:
    raise ServiceError("workspace_contract_required")


def _capturing(captured: list, name: str):
    async def scoped(invocation: Invocation, parsed: object, context: object) -> dict:
        captured.append((name, invocation, parsed, context))
        return {
            "ok": True,
            "path": getattr(parsed, "path", None),
            "cwd": getattr(parsed, "cwd", None),
        }

    return scoped


def _base_registry(captured: list) -> ToolRegistry:
    declarations = [
        ("shell", ShellInput, "command", "exec"),
        ("read", ReadInput, "read", "read"),
        ("list", ListInput, "read", "read"),
        ("find", FindInput, "read", "read"),
        ("search", SearchInput, "read", "read"),
        ("write", WriteInput, "write", "write"),
        ("edit", EditInput, "write", "write"),
        ("browser_action", None, "write", "write"),
    ]
    specs = []
    for name, parameters, capability, risk in declarations:
        assert name in ISOLATED_TOOL_NAMES or name == "browser_action"
        specs.append(ToolSpec(
            name=name,
            parameters=parameters or EditInput,
            executor=_unavailable,
            output_schema={"type": "object"},
            version="development/1",
            implementation_digest=_digest(name),
            risk=risk,
            scoped_executor=_capturing(captured, name),
            capability=capability,
            description=f"{name} tool",
        ))
    return ToolRegistry(tuple(specs))


def test_isolated_namespace_passes_the_real_path_grammar() -> None:
    # The coordinator question, answered by execution: _parts does not reject
    # the managed .krim namespace, so relocated paths reach the existing tools.
    assert _parts(f"{DIRECTORY}/src/file.py") == (
        ".krim", "subagent-workspaces", "a1", "src", "file.py",
    )
    assert FileInput(path=f"{DIRECTORY}/src/file.py").path == f"{DIRECTORY}/src/file.py"
    with pytest.raises(ServiceError) as relative:
        _parts("a/../b.py")
    assert relative.value.code == "invalid_input"
    with pytest.raises(ServiceError) as absolute:
        _parts("/abs/path.py")
    assert absolute.value.code == "invalid_input"
    # Sensitive names stay rejected inside the workspace too.
    with pytest.raises(ServiceError) as env:
        _parts(f"{DIRECTORY}/.env")
    assert env.value.code == "permission_denied"
    with pytest.raises(ServiceError) as git:
        _parts(f"{DIRECTORY}/.git/config")
    assert git.value.code == "permission_denied"


def test_isolate_arguments_relocates_every_isolated_surface() -> None:
    write = isolate_arguments(
        WriteInput,
        WriteInput(path="src/x.py", content="body", expected_sha256=None),
        DIRECTORY,
    )
    assert write.path == f"{DIRECTORY}/src/x.py"
    assert write.content == "body" and write.expected_sha256 is None

    edit = isolate_arguments(
        EditInput,
        EditInput(path="src/x.py", old_string="a", new_string="b", expected_sha256="0" * 64),
        DIRECTORY,
    )
    assert edit.path == f"{DIRECTORY}/src/x.py"
    assert (edit.old_string, edit.new_string, edit.expected_sha256) == ("a", "b", "0" * 64)

    assert isolate_arguments(ReadInput, ReadInput(path="."), DIRECTORY).path == DIRECTORY
    assert isolate_arguments(ListInput, ListInput(), DIRECTORY).path == DIRECTORY

    find = isolate_arguments(FindInput, FindInput(include_glob="src/**/*.py"), DIRECTORY)
    assert find.path == DIRECTORY
    assert find.include_glob == "src/**/*.py"  # globs stay untouched

    search = isolate_arguments(
        SearchInput, SearchInput(query="needle", path="docs"), DIRECTORY,
    )
    assert search.path == f"{DIRECTORY}/docs"
    assert search.query == "needle"

    shell = isolate_arguments(ShellInput, ShellInput(command="pytest"), DIRECTORY)
    assert shell.cwd == DIRECTORY
    assert shell.command == "pytest"
    nested = isolate_arguments(
        ShellInput, ShellInput(command="make", cwd="tools"), DIRECTORY,
    )
    assert nested.cwd == f"{DIRECTORY}/tools"


def test_isolate_arguments_is_idempotent() -> None:
    already = f"{DIRECTORY}/src/x.py"
    write = isolate_arguments(WriteInput, WriteInput(path=already, content=""), DIRECTORY)
    assert write.path == already
    shell = isolate_arguments(ShellInput, ShellInput(command="x", cwd=DIRECTORY), DIRECTORY)
    assert shell.cwd == DIRECTORY


def test_isolate_arguments_rejects_grammar_violations_after_relocation() -> None:
    # A parse-valid short path whose relocated form exceeds the sandbox path
    # grammar fails at the transform instead of surfacing deep in the worker.
    long = "a" * 400 + "/" + "b" * 100
    assert len(long.encode()) <= 512
    with pytest.raises(ServiceError) as too_long:
        isolate_arguments(WriteInput, WriteInput(path=long, content=""), DIRECTORY)
    assert too_long.value.code == "invalid_input"


def test_isolate_invocation_keeps_one_consistent_arguments_json() -> None:
    shell = isolate_invocation(_invocation("shell", {"command": "pytest"}), DIRECTORY)
    assert parse_json(shell.arguments_json) == {"command": "pytest", "cwd": DIRECTORY}
    parsed_shell = ShellInput.model_validate(parse_json(shell.arguments_json))
    assert parsed_shell == isolate_arguments(
        ShellInput, ShellInput.model_validate({"command": "pytest"}), DIRECTORY,
    )

    write = isolate_invocation(
        _invocation("write", {"path": "src/x.py", "content": "b"}), DIRECTORY,
    )
    parsed_write = WriteInput.model_validate(parse_json(write.arguments_json))
    assert parsed_write.path == f"{DIRECTORY}/src/x.py"
    assert parsed_write == isolate_arguments(
        WriteInput, WriteInput.model_validate({"path": "src/x.py", "content": "b"}), DIRECTORY,
    )

    find = isolate_invocation(_invocation("find", {"include_glob": "**/*.py"}), DIRECTORY)
    values = parse_json(find.arguments_json)
    assert values == {"include_glob": "**/*.py", "path": DIRECTORY}

    # Escape attempts keep their original rejection at the unchanged
    # validation layer; relocation never rewrites them into valid paths.
    absolute = isolate_invocation(_invocation("write", {"path": "/abs.py"}), DIRECTORY)
    assert parse_json(absolute.arguments_json) == {"path": "/abs.py"}
    with pytest.raises(ServiceError):
        WriteInput.model_validate(parse_json(absolute.arguments_json))

    browser = _invocation("browser_action", {"action": "goto", "path": "/x"})
    assert isolate_invocation(browser, DIRECTORY) is browser

    scalar = _raw_invocation("write", encoded("not-an-object"))
    with pytest.raises(ServiceError) as invalid:
        isolate_invocation(scalar, DIRECTORY)
    assert invalid.value.code == "invalid_input"


def test_isolated_registry_changes_semantics_not_surface() -> None:
    base = _base_registry([])
    derived = isolated_registry(base, DIRECTORY)

    assert {spec.name for spec, _ in derived._frozen} == {
        spec.name for spec, _ in base._frozen
    }
    for name in ISOLATED_TOOL_NAMES:
        original, derived_spec = base.get(name), derived.get(name)
        assert derived_spec.fingerprint != original.fingerprint
        assert derived_spec.version == ISOLATED_VERSION
        assert derived_spec.capability == original.capability
        assert derived_spec.risk == original.risk
        assert derived_spec.concurrency == original.concurrency
        assert derived_spec.parameters is original.parameters
        assert derived_spec.output_schema == original.output_schema
        assert DIRECTORY in derived_spec.description
    # Untouched tools keep the original object, not a copy.
    assert derived.get("browser_action") is base.get("browser_action")

    again = isolated_registry(base, DIRECTORY)
    assert again.get("write").fingerprint == derived.get("write").fingerprint
    other = isolated_registry(base, workspace_directory("a2"))
    assert other.get("write").fingerprint != derived.get("write").fingerprint


def test_validate_isolated_directory_accepts_only_the_managed_namespace() -> None:
    assert validate_isolated_directory(DIRECTORY) == DIRECTORY
    for rejected in ("src", "/abs", ".krim/other", "", f"{DIRECTORY}/../b", "a\\b"):
        with pytest.raises(ServiceError) as error:
            validate_isolated_directory(rejected)
        assert error.value.code == "invalid_input"


async def test_isolated_executor_redirects_the_original_scoped_executor() -> None:
    captured: list = []
    base = _base_registry(captured)
    derived = isolated_registry(base, DIRECTORY)

    invocation = _invocation("write", {"path": "src/x.py"}, derived.get("write").fingerprint)
    context = object()
    result = await derived.get("write").scoped_executor(
        invocation, WriteInput(path="src/x.py", content="b"), context,
    )
    assert len(captured) == 1
    name, seen_invocation, seen_parsed, seen_context = captured[0]
    assert name == "write"
    # The original executor sees the relocated call identity: same ids, one
    # relocated arguments JSON — the model-facing record stays untouched.
    assert seen_invocation.call_id == invocation.call_id
    assert seen_invocation.name == invocation.name
    assert parse_json(seen_invocation.arguments_json)["path"] == f"{DIRECTORY}/src/x.py"
    assert seen_invocation is not invocation
    assert seen_context is context
    assert seen_parsed.path == f"{DIRECTORY}/src/x.py"
    assert result == {"ok": True, "path": f"{DIRECTORY}/src/x.py", "cwd": None}

    await derived.get("shell").scoped_executor(
        _invocation("shell", {"command": "pytest"}), ShellInput(command="pytest"), context,
    )
    assert captured[1][2].cwd == DIRECTORY

    # Host-side pre-relocation (isolate_invocation wiring) stays single-prefix.
    pre = isolate_invocation(
        _invocation("write", {"path": "src/x.py", "content": "b"}), DIRECTORY,
    )
    pre_parsed = WriteInput.model_validate(parse_json(pre.arguments_json))
    await derived.get("write").scoped_executor(pre, pre_parsed, context)
    assert captured[2][2].path == f"{DIRECTORY}/src/x.py"


async def test_isolated_executor_projects_policy_and_current_scope() -> None:
    seen: list = []

    async def policy(invocation: Invocation) -> str:
        seen.append((invocation, CURRENT.get()))
        return "allow"

    async def legacy(parameters: object) -> object:
        raise ServiceError("workspace_contract_required")

    async def scoped(invocation: Invocation, parsed: object, context: object) -> dict:
        return {"ok": True, "scope_invocation": CURRENT.get().invocation}

    spec = ToolSpec(
        name="write", parameters=WriteInput, executor=legacy,
        output_schema={"type": "object"}, version="development/1",
        implementation_digest=hashlib.sha256(b"write").hexdigest(), risk="write",
        scoped_executor=scoped, capability="write", description="write",
    )
    base = ToolExecutor(ToolRegistry((spec,)), policy)
    derived = isolated_executor(base, DIRECTORY)

    assert derived.policy is not base.policy
    assert derived.registry.get("write").fingerprint != base.registry.get("write").fingerprint

    original = _invocation(
        "write", {"path": "src/x.py", "content": ""}, derived.registry.get("write").fingerprint,
    )
    frame = object()
    scope = Scope(
        frame=frame, access=frame, operations=frame, check=lambda: None,
        initial_deadline=0.0, invocation=original,
    )
    context = ToolContext(
        _unavailable, _unavailable, 5.0, lambda: None, scope, 5.0,
    )
    admission = await derived.admit(original, context, timeout_seconds=None)
    assert admission.decision == "allow"
    # Admission parses the model-facing arguments; relocation applies in the
    # policy and execution views only, so checkpoints keep the original JSON.
    assert admission.parsed.path == "src/x.py"

    # Policy saw the relocated call inside a CURRENT scope bound to it.
    policy_invocation, policy_scope = seen[0]
    assert policy_invocation is not original
    assert parse_json(policy_invocation.arguments_json)["path"] == f"{DIRECTORY}/src/x.py"
    assert policy_scope is not None
    assert policy_scope.invocation is policy_invocation

    # Admission keeps the derived fingerprint: the original one is stale now.
    stale = replace(original, tool_hash=base.registry.get("write").fingerprint)
    with pytest.raises(ServiceError) as stale_error:
        await derived.admit(stale, context, timeout_seconds=None)
    assert stale_error.value.code == "tool_binding_changed"

    # Execution: the original scoped executor runs with the projected call and
    # a CURRENT scope whose invocation is exactly that projected call. The
    # surrounding CURRENT is what executor.execute provides in production.
    token = CURRENT.set(scope)
    try:
        result = await derived.registry.get("write").scoped_executor(
            original, WriteInput(path="src/x.py", content=""), context,
        )
    finally:
        CURRENT.reset(token)
    executed_scope_invocation = result["scope_invocation"]
    assert executed_scope_invocation is not original
    assert parse_json(executed_scope_invocation.arguments_json)["path"] == (
        f"{DIRECTORY}/src/x.py"
    )


def test_command_request_and_terminal_worker_accept_the_relocated_cwd(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    isolated = project / DIRECTORY
    isolated.mkdir(parents=True)
    common = dict(
        operation_id="op:1",
        environment=EnvironmentHandle(
            owner_id="owner", project_id="proj-1", environment_id="dev-1",
            generation=1, state="ready",
        ),
        ownership=RunOwnership(owner_id="owner", session_id="session:1", run_id="run:1"),
        authorization=AuthorizationRef(
            actor_id="owner", actor_generation=1, project_id="proj-1",
            arguments_digest="0" * 64,
        ),
        command="pytest -q",
    )
    # The relocated cwd survives the durable command contract unchanged.
    request = CommandRequest.model_validate({**common, "cwd": DIRECTORY})
    assert request.cwd == DIRECTORY
    for rejected in ("/abs", "a/../b"):
        with pytest.raises(Exception) as cwd_error:
            CommandRequest.model_validate({**common, "cwd": rejected})
        assert not isinstance(cwd_error.value, ServiceError)

    # The trusted worker pins the shell's real working directory to the
    # relocated cwd and rejects symlink escapes (Linux only).
    if os.name != "posix":
        pytest.skip("terminal worker cwd pinning requires a posix host")
    from krim_agent.dev_terminal.worker import open_cwd

    fd = open_cwd(project, DIRECTORY)
    try:
        assert Path(os.readlink(f"/proc/self/fd/{fd}")) == isolated
    finally:
        os.close(fd)
    outside = tmp_path / "outside"
    outside.mkdir()
    (project / "link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError):
        open_cwd(project, "link")
    with pytest.raises(ValueError):
        open_cwd(project, f"{DIRECTORY}/..")


async def test_composed_owner_policy_resolves_derived_calls() -> None:
    """Real compose_executor owners key on original fingerprints: the derived
    executor must present relocated arguments under the original hash, or
    every isolated read/write/shell call is denied by the owner map."""
    seen: list = []

    async def base_policy(invocation: Invocation) -> str:
        seen.append((invocation, CURRENT.get()))
        return "allow"

    async def scoped_write(invocation: Any, parsed: Any, context: Any) -> dict:
        return {"ok": True, "path": parsed.path}

    write = ToolSpec(
        name="write", parameters=WriteInput, executor=_unavailable,
        output_schema={"type": "object"}, version="development/1",
        implementation_digest=hashlib.sha256(b"write").hexdigest(),
        risk="write", scoped_executor=scoped_write, capability="write",
        description="write",
    )
    spawn = ToolSpec(
        name="spawn_agent", parameters=SpawnAgent, executor=_unavailable,
        output_schema={"type": "object"}, version="subagents/1",
        implementation_digest=hashlib.sha256(b"spawn").hexdigest(),
        risk="read", scoped_executor=_unavailable, capability="session_read",
        concurrency="parallel", description="spawn",
    )
    base = ToolExecutor(ToolRegistry((write,)), base_policy)
    composed = compose_executor(base, (ToolBundle(specs=(spawn,), policy=base_policy),))
    derived = isolated_executor(composed, DIRECTORY)

    original = _invocation(
        "write", {"path": "src/x.py", "content": ""},
        derived.registry.get("write").fingerprint,
    )
    frame = object()
    scope = Scope(
        frame=frame, access=frame, operations=frame, check=lambda: None,
        initial_deadline=0.0, invocation=original,
    )
    context = ToolContext(
        _unavailable, _unavailable, 5.0, lambda: None, scope, 5.0,
    )
    admission = await derived.admit(original, context, timeout_seconds=None)
    # Without the owner-view hash mapping every isolated call was denied here.
    assert admission.decision == "allow"
    assert len(seen) == 1
    # Owner view: original fingerprint for the owners map, relocated arguments
    # for the permission decision, bound as the CURRENT scope invocation.
    owner_invocation, owner_scope = seen[0]
    assert owner_invocation.tool_hash == composed.registry.get("write").fingerprint
    assert parse_json(owner_invocation.arguments_json)["path"] == f"{DIRECTORY}/src/x.py"
    assert owner_scope is not None
    assert owner_scope.invocation is owner_invocation
