"""Isolated-write relocation for the development tool surface (plan §9.2).

The host fixes a child's ``assignment.directory`` as its file root: file and
search arguments and the shell working directory are relocated into that
directory inside the same project. The relocation is a convention with real
enforcement at the existing layers — path grammar (``_parts``), command cwd
validation and the terminal worker's ``open_cwd`` (which resolves symlinks and
rejects escapes) — but it is not a hard security boundary by itself: a shell
command string can still name outside paths. The project mount, command
admission and the approval pipeline stay the actual boundary.

Derived specifications keep the original name, capability, risk, concurrency
and parameter model, so the strict name-to-capability policy mapping and every
admission gate keep working unchanged. Version and implementation digest
cover the original specification fingerprint plus the directory policy: path
remapping is new execution semantics, and approvals, checkpoints and recovery
must bind to the remapped specification, never to the original fingerprint.

``isolate_invocation`` is the single relocation primitive: the derived
executor applies it for policy and execution inside the pipeline's CURRENT
context (like the built-in binding-target projection), so both see one
consistent relocated call while the model-facing arguments, call ids and
checkpoints stay untouched — restore replays re-relocate idempotently and
never diverge from the recorded tool arguments. The SDK adapter needs no
changes; approval display wraps the same primitive in the SDK loop.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from krim_agent.contracts import ServiceError, encoded, parse_json
from krim_agent.sandbox.filesystem import _parts
from krim_agent.tools.core import Invocation, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.pipeline import CURRENT, _calling
from krim_agent.tools.registry import ToolRegistry

ISOLATED_TOOL_NAMES = frozenset({"shell", "read", "list", "find", "search", "write", "edit"})
ISOLATED_VERSION = "development-isolated/1"
# Only the shell relocates a working directory; every other isolated tool
# relocates its single file path argument.
_CWD_TOOLS = frozenset({"shell"})
_DIRECTORY_PREFIX = ".krim/subagent-workspaces/"


def validate_isolated_directory(directory: str) -> str:
    """The assignment directory must be a managed project-relative directory.

    Only the domain-allocated namespace (``workspace.workspace_directory``)
    is accepted, so a host cannot silently point children at user files. The
    sandbox path grammar still applies on top (sensitive names, length,
    encoding); symlink escapes stay with the terminal worker and port checks.
    """
    if not isinstance(directory, str) or not directory.startswith(_DIRECTORY_PREFIX):
        raise ServiceError("invalid_input", 422)
    parts = directory.split("/")
    if (
        any(not part or part in {".", ".."} for part in parts)
        or directory.startswith("/")
        or "\\" in directory
        or ":" in directory
        or any(ord(char) < 32 for char in directory)
    ):
        raise ServiceError("invalid_input", 422)
    _parts(directory, directory=True)
    return directory


def _relocate(value: str, directory: str) -> str:
    """Relocate one project-relative path; already-relocated paths pass through."""
    if value == ".":
        return directory
    if value == directory or value.startswith(directory + "/"):
        return value
    return f"{directory}/{value}"


def _relocate_argument(value: Any, directory: str) -> Any:
    """Relocate a relative path; absolute and ``..`` forms keep their original
    rejection by the model and sandbox validators instead of being rewritten."""
    if not isinstance(value, str):
        return value
    if not value or value.startswith("/") or ".." in value.split("/"):
        return value
    relocated = _relocate(value, directory)
    _parts(relocated)  # fail fast when relocation exceeds the path grammar
    return relocated


def isolate_arguments(
    parameters: type[BaseModel], values: BaseModel, directory: str
) -> BaseModel:
    """Return parsed tool arguments relocated into ``directory`` (idempotent).

    Only the isolated development surfaces carry a relocated ``path`` or
    ``cwd`` field; other models pass through unchanged. ``shell`` relocates
    ``cwd`` so the worker's real working directory is the assignment
    directory; ``command``, globs and hashes stay untouched.
    """
    if not isinstance(values, BaseModel):
        return values
    updates: dict[str, str] = {}
    fields = type(values).model_fields
    for key in ("cwd", "path"):
        if key in fields:
            current = getattr(values, key)
            relocated = _relocate_argument(current, directory)
            if relocated != current:
                updates[key] = relocated
    return values.model_copy(update=updates) if updates else values


def isolate_invocation(invocation: Invocation, directory: str) -> Invocation:
    """Relocate the raw arguments JSON of one isolated tool invocation.

    The result is what policy, approval projection and the executor must all
    see: one consistent arguments JSON. Missing optional path/cwd keys are
    injected with the relocated default so the parsed model equals
    ``isolate_arguments`` of the originally parsed values. Non-isolated tools
    pass through unchanged.
    """
    if invocation.name not in ISOLATED_TOOL_NAMES:
        return invocation
    values = parse_json(invocation.arguments_json)
    if not isinstance(values, dict):
        raise ServiceError("invalid_input", 422)
    updated = dict(values)
    key = "cwd" if invocation.name in _CWD_TOOLS else "path"
    current = updated.get(key, ".")
    updated[key] = _relocate_argument(current, directory)
    return replace(invocation, arguments_json=encoded(updated))


async def _run_projected(
    projected: Invocation, handler: Callable[[Invocation], Awaitable[Any]]
) -> Any:
    """Run one handler with CURRENT bound to the relocated invocation.

    Mirrors the built-in binding-target projection in ``ToolExecutor``: the
    pipeline scope's ``invocation`` is the relocated call, so strict scope
    identity checks (``Scope.current``) pass for the relocated arguments while
    the model-facing call id stays the same.
    """
    with _calling(projected):
        current = CURRENT.get()
        token = CURRENT.set(
            replace(current, invocation=projected) if current is not None else None
        )
        try:
            return await handler(projected)
        finally:
            CURRENT.reset(token)


def _isolated_spec(spec: ToolSpec, directory: str, source: bytes) -> ToolSpec:
    """One derived specification with remapped execution semantics.

    The digest binds the relocation source, the original fingerprint and the
    directory, so two children bind separately and any change to either side
    produces a new specification instead of silently reusing approvals. The
    checkpoint keeps the model's original tool arguments: relocation happens
    here, after parse, so restore replays stay byte-identical.
    """
    original = spec.scoped_executor
    assert original is not None  # isolated candidates always have one

    async def execute_isolated(
        invocation: Invocation, parsed: BaseModel, context: Any
    ) -> Any:
        projected = isolate_invocation(invocation, directory)
        relocated = isolate_arguments(type(parsed), parsed, directory)
        return await _run_projected(
            projected, lambda call: original(call, relocated, context),
        )

    digest = hashlib.sha256()
    digest.update(source)
    digest.update(spec.fingerprint.encode("ascii"))
    digest.update(directory.encode("utf-8"))
    return replace(
        spec,
        scoped_executor=execute_isolated,
        version=ISOLATED_VERSION,
        implementation_digest=digest.hexdigest(),
        description=(spec.description + " Workspace isolation: file paths and the shell "
                     f"working directory are relocated into {directory}.").strip(),
    )


def isolated_registry(base: ToolRegistry, directory: str) -> ToolRegistry:
    """Derive the child tool registry with ``directory`` as its file root.

    Isolated candidates are replaced by their remapped specifications; every
    other tool is kept as the original object. The base registry stays a
    dependency, so its freeze fence keeps guarding the derived one.
    """
    validate_isolated_directory(directory)
    base.assert_current()
    source = Path(__file__).read_bytes()
    specs = tuple(
        _isolated_spec(spec, directory, source)
        if spec.name in ISOLATED_TOOL_NAMES and spec.scoped_executor is not None
        else spec
        for spec, _fingerprint in base._frozen
    )
    return ToolRegistry(specs, dependencies=(base,))


def isolated_executor(base: ToolExecutor, directory: str) -> ToolExecutor:
    """Derive the child executor: policy and execution share one relocated view.

    Admission keeps the derived specifications and their fingerprints. The
    wrapped policy runs inside a CURRENT context bound to the relocated
    invocation — the same projection the executor applies — but presents the
    ORIGINAL specification fingerprint: composed owners (``compose_executor``)
    key on ``(name, original fingerprint)``, while permission decisions
    themselves evaluate the relocated arguments by name and scope. Hooks,
    middleware and wrap are preserved untouched.
    """
    async def projected_policy(invocation: Invocation) -> Any:
        assert base.policy is not None
        projected = isolate_invocation(invocation, directory)
        if projected is invocation:
            return await base.policy(invocation)
        try:
            original_hash = base.registry.get(invocation.name).fingerprint
        except ServiceError:
            return await base.policy(projected)
        owner_view = replace(projected, tool_hash=original_hash)
        return await _run_projected(owner_view, base.policy)

    policy = projected_policy if base.policy is not None else None
    return ToolExecutor(
        isolated_registry(base.registry, directory),
        policy,
        base.pre,
        base.post,
        middleware=base.middleware,
        wrap=base.wrap,
    )
