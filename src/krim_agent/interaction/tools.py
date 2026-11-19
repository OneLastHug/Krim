"""Session tools yielding a durable parked request to the host."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from pydantic import BaseModel, JsonValue

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.session import SessionScopeResolver, ToolBundle

from .contracts import AskUserArgs, EnterPlanModeArgs, ExitPlanModeArgs, RequestPending

if TYPE_CHECKING:
    from .service import InteractionService


def interaction_bundle(service: InteractionService, resolve: SessionScopeResolver) -> ToolBundle:
    async def unavailable(_: BaseModel) -> JsonValue:
        raise ServiceError("workspace_contract_required")

    async def execute(invocation: Invocation, parsed: BaseModel, _: object) -> JsonValue:
        scope = await resolve(invocation)
        scope.check()
        # Host identity, never model-supplied request/session/run authority.
        request_key = "tool:" + fingerprint({
            "run_id": invocation.run_id, "call_id": invocation.call_id, "name": invocation.name
        })
        if invocation.name == "ask_user" and isinstance(parsed, AskUserArgs):
            result = await service.ask(scope, parsed, request_key)
        elif invocation.name == "enter_plan_mode" and isinstance(parsed, EnterPlanModeArgs):
            result = await service.enter_plan_mode(scope, parsed, request_key)
        elif invocation.name == "exit_plan_mode" and isinstance(parsed, ExitPlanModeArgs):
            result = await service.exit_plan_mode(scope, parsed, request_key)
        else:
            raise ServiceError("invalid_input", 422)
        scope.check()
        if result.get("status") == "pending":
            raise RequestPending(result)
        return cast(JsonValue, result)

    async def policy(invocation: Invocation) -> Decision:
        if invocation.name not in {"ask_user", "enter_plan_mode", "exit_plan_mode"}:
            return "deny"
        scope = await resolve(invocation)
        scope.check()
        await service.state(scope.actor, scope.workspace_id, scope.session_id)
        scope.check()
        return "allow"

    definitions = (
        ("ask_user", AskUserArgs,
         "Ask the session owner one or more single-choice, multiple-choice or text questions. "
         "The run pauses durably until answered or cancelled; use only when input is needed."),
        ("enter_plan_mode", EnterPlanModeArgs,
         "Enter read-only planning. Internal plans and user questions remain available; "
         "project changes and external side effects require a confirmed exit."),
        ("exit_plan_mode", ExitPlanModeArgs,
         "Present the proposed plan to the session owner for execution confirmation. "
         "The run pauses; execution remains blocked until the owner confirms."),
    )
    specs = tuple(ToolSpec(
        name=name, parameters=parameters, executor=unavailable,
        output_schema={"type": "object"}, version="interaction/1",
        implementation_digest=fingerprint({"name": name, "implementation": "interaction/1"}),
        risk="write", capability="session_state", scoped_executor=execute,
        description=description,
    ) for name, parameters, description in definitions)
    return ToolBundle(specs=specs, policy=policy)
