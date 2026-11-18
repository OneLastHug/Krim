"""Live project ceiling and session confirmation, shared by all tool bundles."""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

from sqlalchemy import select

from krim_agent.contracts import ServiceError
from krim_agent.project_access.policy import resolve_project_access
from krim_agent.storage.execution_policy import (
    allow_rule_satisfied,
    arguments_digest,
    get_mode,
)
from krim_agent.storage.models import sessions
from krim_agent.storage.sandbox_models import sandbox_session_bindings
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.session import SessionToolScope

if TYPE_CHECKING:
    from krim_agent.storage.sqlite import SQLiteStore


def normal_project_command(arguments_json: str) -> bool:
    """Recognize a deliberately small set of ordinary build/test/install commands.

    No shell rewriting or remote classification. Shell syntax, expansion, scripts,
    Git writes and ambiguous commands fall back to confirmation, never denial.
    """
    from krim_agent.contracts import parse_json

    arguments = parse_json(arguments_json)
    command = arguments.get("command") if isinstance(arguments, dict) else None
    if not isinstance(command, str) or any(c in command for c in ";&|<>\n\r$`\\"):
        return False
    try:
        words = shlex.split(command)
    except ValueError:
        return False
    if not words:
        return False
    program, *args = words
    if program in {"pytest", "ruff", "mypy"}:
        return True
    if program in {"python", "python3"}:
        return len(args) >= 2 and args[:2] in (["-m", "pytest"], ["-m", "build"])
    if program in {"pip", "pip3"}:
        return bool(args) and args[0] in {"install", "check", "list", "show"}
    if program == "uv":
        return args[:2] in (["pip", "install"], ["pip", "check"]) or args == ["sync"]
    if program in {"npm", "pnpm", "yarn"}:
        return bool(args) and (args[0] in {"install", "ci", "test", "build", "lint"}
                               or args[:2] in (["run", "build"], ["run", "test"],
                                              ["run", "lint"], ["run", "check"]))
    return program == "make" and bool(args) and all(
        arg in {"build", "test", "check", "lint"} for arg in args
    )


def is_readonly(spec: ToolSpec, trusted_readonly_names: frozenset[str]) -> bool:
    current: ToolSpec | None = spec
    seen: set[int] = set()
    while current is not None:
        if id(current) in seen:
            raise ServiceError("tool_binding_changed")
        seen.add(id(current))
        if current.risk != "read":
            return False
        if (current.source is not None and current.source.kind == "mcp"
                and current.name not in trusted_readonly_names):
            return False
        current = current.binding_target
    return True


async def session_decision(
    store: SQLiteStore, scope: SessionToolScope, spec: ToolSpec, invocation: Invocation,
    *, trusted_readonly_names: frozenset[str] = frozenset(),
) -> Decision:
    """Read the latest values every time; no project/global mode cache exists."""
    if store.schema_version < 19:
        return "allow"  # historical readonly stores keep their original policy
    scope.check()

    async def read(conn: object) -> Decision:
        # AsyncConnection is kept inside the store's bounded read transaction.
        from typing import cast

        from sqlalchemy.ext.asyncio import AsyncConnection

        connection = cast(AsyncConnection, conn)
        row = (await connection.execute(select(sessions).where(
            sessions.c.id == scope.session_id,
        ))).mappings().first()
        if (row is None or row["owner_id"] != scope.actor.principal_id
                or row["workspace_id"] != scope.workspace_id):
            return "deny"
        binding = (await connection.execute(select(sandbox_session_bindings).where(
            sandbox_session_bindings.c.session_id == scope.session_id,
        ))).mappings().first()
        project_id = binding["project_id"] if binding is not None else None
        if project_id != scope.project_id:
            return "deny"
        readonly = is_readonly(spec, trusted_readonly_names)
        session_state = spec.capability in {"session_read", "session_state"}
        if project_id is not None:
            access = await resolve_project_access(connection, scope.actor, project_id)
            if access.access == "none":
                return "deny"
            if access.access == "read" and not readonly and not session_state:
                return "deny"
        if readonly or session_state:
            return "allow"
        # Unbound historical sessions cannot acquire write capabilities through
        # a newly introduced mode, even if corrupt data contains 'bypass'.
        if project_id is None:
            return "ask"
        mode = await get_mode(connection, scope.session_id)
        if mode == "bypass":
            return "allow"
        if await allow_rule_satisfied(
            connection, scope.session_id, tool_hash=spec.fingerprint,
            arguments_hash=arguments_digest(invocation.arguments_json),
        ):
            return "allow"
        if mode == "auto" and spec.source is None:
            if spec.name in {"write", "edit", "workspace_write", "sandbox_write", "sandbox_patch"}:
                return "allow"
            if spec.capability == "command" and normal_project_command(invocation.arguments_json):
                return "allow"
        return "ask"

    result = await store.work_read(read)
    scope.check()
    return result
