"""Host-bound plan tools for ordinary and development session runtimes."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel, JsonValue

from krim_agent.contracts import ServiceError
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.session import SessionScopeResolver, SessionToolScope, ToolBundle

from .contracts import PlanRead, PlanUpdate

if TYPE_CHECKING:
    from .service import PlanningService


async def unavailable(parameters: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


def bundle(service: PlanningService, resolve: SessionScopeResolver) -> ToolBundle:
    async def scope_for(invocation: Invocation) -> SessionToolScope:
        scope = await resolve(invocation)
        scope.check()
        if scope.run_id != invocation.run_id:
            raise ServiceError("tool_binding_changed")
        return scope

    async def policy(invocation: Invocation) -> Decision:
        if invocation.name not in {"plan_read", "plan_update"}:
            return "deny"
        try:
            scope = await scope_for(invocation)
            await service.authorize(
                scope.actor,
                scope.workspace_id,
                scope.session_id,
                write=invocation.name == "plan_update",
                check=scope.check,
            )
        except ServiceError:
            return "deny"
        return "allow"

    async def execute(invocation: Invocation, parsed: BaseModel, context: object) -> JsonValue:
        scope = await scope_for(invocation)
        if invocation.name == "plan_read" and isinstance(parsed, PlanRead):
            result = await service.read(
                scope.actor,
                scope.workspace_id,
                scope.session_id,
                parsed,
                check=scope.check,
            )
        elif invocation.name == "plan_update" and isinstance(parsed, PlanUpdate):
            result = await service.update(
                scope.actor,
                scope.workspace_id,
                scope.session_id,
                parsed,
                check=scope.check,
            )
        else:
            raise ServiceError("invalid_input", 422)
        scope.check()
        return cast(JsonValue, result.model_dump(mode="json"))

    digest = hashlib.sha256()
    root = Path(__file__).parent
    for path in (
        root / "contracts.py",
        root / "service.py",
        Path(__file__),
        root.parent / "storage" / "planning.py",
        root.parent / "storage" / "planning_models.py",
    ):
        digest.update(path.read_bytes())
    implementation = digest.hexdigest()
    return ToolBundle(
        specs=(
            ToolSpec(
                name="plan_read",
                parameters=PlanRead,
                executor=unavailable,
                output_schema={"type": "object"},
                version="planning/1",
                implementation_digest=implementation,
                scoped_executor=execute,
                capability="session_read",
                risk="read",
                description=(
                    "Read the current session plan, counts and revision. Use statuses to focus "
                    "on pending/in_progress/blocked steps. Continue with next_offset and the "
                    "returned revision to read a stable page; omit revision to read the latest."
                ),
            ),
            ToolSpec(
                name="plan_update",
                parameters=PlanUpdate,
                executor=unavailable,
                output_schema={"type": "object"},
                version="planning/1",
                implementation_digest=implementation,
                scoped_executor=execute,
                capability="session_state",
                risk="write",
                description=(
                    "Replace the entire session plan using expected_revision from plan_read "
                    "and a unique request_key. Preserve steps you want to keep; an empty list "
                    "clears the plan. Reuse the same request key and body only to retry that "
                    "same update. States: pending, in_progress, completed, blocked. The result "
                    "contains counts and the first page; read remaining pages by revision. "
                    "Plan completion alone does not establish that the user goal is achieved."
                ),
            ),
        ),
        policy=policy,
    )
