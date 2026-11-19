"""Host-resolved session identity and small tool bundles for shared capabilities.

Models never supply an actor, workspace, session, or project authority. The host
resolves these from the invocation immediately before policy and execution.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from krim_agent.security import PrincipalContext

if TYPE_CHECKING:
    from krim_agent.tools.core import Invocation, Policy, ToolSpec

# A separate capability avoids changing historical workspace plan_update hashes.
SESSION_CAPABILITIES: dict[str, str] = {
    "plan_read": "session_read",
    "plan_update": "session_state",
    "ask_user": "session_state",
    "enter_plan_mode": "session_state",
    "exit_plan_mode": "session_state",
    "tool_search": "session_read",
    "list_mcp_resources": "session_read",
    "list_mcp_resource_templates": "session_read",
    "read_mcp_resource": "session_read",
    "view_image": "session_read",
    "memory_search": "session_read",
    "memory_read": "session_read",
    "memory_write": "session_write",
    "memory_delete": "session_write",
    "goal_update": "session_state",
    "goal_status": "session_read",
    "compact_context": "session_state",
}
SESSION_RISKS = {
    "session_read": "read",
    "session_state": "write",
    "session_write": "write",
}


@dataclass(frozen=True)
class SessionToolScope:
    actor: PrincipalContext
    workspace_id: str
    session_id: str
    turn_id: str
    run_id: str
    project_id: str | None = None
    check: Callable[[], None] = lambda: None


type SessionScopeResolver = Callable[[Invocation], Awaitable[SessionToolScope]]


@dataclass(frozen=True)
class ToolBundle:
    specs: tuple[ToolSpec, ...]
    policy: Policy
