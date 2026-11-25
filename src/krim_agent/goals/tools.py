"""Session tools for durable goals; host-bound scope, never model-supplied authority.

``goal_update`` mutates the session's single goal with CAS and a request key.
Creation is accepted only from the current real user turn: the service loads
that turn itself and rejects host-internal continuation turns and runs whose
turn is not the scoped one. ``goal_status`` is a read-only projection.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel, JsonValue

from krim_agent.contracts import ServiceError
from krim_agent.planning.contracts import PAGE_SIZE
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.session import SessionScopeResolver, SessionToolScope, ToolBundle

from .contracts import GoalRead, GoalUpdate

if TYPE_CHECKING:
    from .service import GoalService


async def unavailable(parameters: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


def bundle(service: GoalService, resolve: SessionScopeResolver) -> ToolBundle:
    async def scope_for(invocation: Invocation) -> SessionToolScope:
        scope = await resolve(invocation)
        scope.check()
        if scope.run_id != invocation.run_id:
            raise ServiceError("tool_binding_changed")
        return scope

    async def policy(invocation: Invocation) -> Decision:
        if invocation.name not in {"goal_update", "goal_status"}:
            return "deny"
        try:
            scope = await scope_for(invocation)
            await service.authorize(scope.actor, scope.workspace_id, scope.session_id)
            scope.check()
        except ServiceError:
            return "deny"
        return "allow"

    async def execute(invocation: Invocation, parsed: BaseModel, context: object) -> JsonValue:
        scope = await scope_for(invocation)
        if invocation.name == "goal_update" and isinstance(parsed, GoalUpdate):
            try:
                result = await service.update_from_tool(scope, parsed)
            except ServiceError as error:
                if error.code != "revision_conflict":
                    raise
                current = await service.read(
                    scope.actor, scope.workspace_id, scope.session_id, GoalRead(),
                    check=scope.check,
                )
                scope.check()
                return {
                    "ok": False, "code": error.code,
                    "current_goal": current.model_dump(mode="json"),
                    "next_action": (
                        "The goal was not updated. Review current_goal before choosing a valid "
                        "action and expected_revision; only paused or blocked goals can be "
                        "replaced with action=set. Do not retry the same update unchanged. "
                        "Continue the user's task even if no goal update is needed."
                    ),
                }
        elif invocation.name == "goal_status" and isinstance(parsed, GoalRead):
            result = await service.read(
                scope.actor, scope.workspace_id, scope.session_id, parsed, check=scope.check
            )
        else:
            raise ServiceError("invalid_input", 422)
        scope.check()
        return cast(JsonValue, result.model_dump(mode="json"))

    digest = hashlib.sha256()
    root = Path(__file__).parent
    for path in (
        root / "contracts.py",
        root / "continuation.py",
        root / "service.py",
        Path(__file__),
        root.parent / "storage" / "goals.py",
        root.parent / "storage" / "goal_models.py",
    ):
        digest.update(path.read_bytes())
    implementation = digest.hexdigest()
    return ToolBundle(
        specs=(
            ToolSpec(
                name="goal_update",
                parameters=GoalUpdate,
                executor=unavailable,
                output_schema={"type": "object"},
                version="goals/1",
                implementation_digest=implementation,
                scoped_executor=execute,
                capability="session_state",
                risk="write",
                description=(
                    "Update this session's single goal with expected_revision from "
                    "goal_status and a unique request_key. Create (action=set) only when "
                    "the user's current message asks for a goal or continuation of this "
                    "task; never invent a new goal on your own and never treat an internal "
                    "continuation turn as new user authorization. Actions: set, progress "
                    "(report work plus evidence_refs when the goal moved forward), block "
                    "(concrete reason; user must adjust the goal), complete (only with "
                    "completion_criteria and evidence_refs), cancel (user asked to stop). "
                    "A complete plan or checked-off steps is not goal completion evidence. "
                    "pause/resume are owner operations, not model tools."
                ),
            ),
            ToolSpec(
                name="goal_status",
                parameters=GoalRead,
                executor=unavailable,
                output_schema={"type": "object"},
                version="goals/1",
                implementation_digest=implementation,
                scoped_executor=execute,
                capability="session_read",
                risk="read",
                concurrency="parallel",
                description=(
                    "Read the session goal: revision, status, objective, completion "
                    "criteria and evidence refs, plus the latest continuation state. Omit "
                    "revision for the latest; pass a revision for an immutable historical "
                    f"view. Plan counts (when present) are context only. Page size {PAGE_SIZE} "
                    "is not a goal-size limit."
                ),
            ),
        ),
        policy=policy,
    )
