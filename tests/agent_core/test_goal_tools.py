"""Goal tool bundle: host scope, policy identity and no model-supplied authority."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.goals import GoalService, GoalUpdate
from krim_agent.goals.contracts import GoalRead, GoalSnapshot, goal_digest
from krim_agent.security import LOCAL
from krim_agent.tools.core import Invocation
from krim_agent.tools.session import SessionToolScope


def invocation(name="goal_status", *, run_id="run", call_id="call"):
    return Invocation(run_id, call_id, "span", 0, name, "{}", "a" * 64)


def empty():
    return GoalSnapshot(
        goal_id=None,
        session_id="session",
        workspace_id="local",
        owner_id=None,
        revision=0,
        status=None,
        objective=None,
        content_hash=goal_digest({"version": "goals/1", "goal": None}),
    )


class FakeGoals:
    def __init__(self):
        self.calls = []

    async def authorize(self, actor, workspace, session, *, check=lambda: None):
        check()
        self.calls.append(("authorize", actor, workspace, session))

    async def read(self, actor, workspace, session, query, *, check=lambda: None):
        check()
        self.calls.append(("read", actor, workspace, session, query))
        return empty()

    async def update_from_tool(self, scope, body):
        scope.check()
        self.calls.append(("update", scope, body))
        return empty()


def scope(**kwargs):
    return SessionToolScope(LOCAL, "local", "session", "turn", "run", **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("project_id", [None, "development-project"])
async def test_bundle_uses_host_scope_and_exact_capabilities(project_id):
    service = FakeGoals()
    resolutions = []

    async def resolve(call):
        resolutions.append(call.call_id)
        return scope(project_id=project_id)

    bundle = GoalService.bundle(service, resolve)
    specs = {spec.name: spec for spec in bundle.specs}
    assert set(specs) == {"goal_update", "goal_status"}
    assert specs["goal_status"].capability == "session_read"
    assert specs["goal_update"].capability == "session_state"
    assert specs["goal_status"].risk == "read"
    assert specs["goal_update"].risk == "write"
    assert specs["goal_update"].descriptor()["policy_version"] == "session/1"
    for spec in specs.values():
        schema = spec.parameters.model_json_schema()
        assert not {"actor", "session_id", "workspace_id", "project_id"} & set(schema["properties"])
    assert await bundle.policy(invocation()) == "allow"
    assert await bundle.policy(invocation("goal_update")) == "allow"
    assert await bundle.policy(invocation("plan_read")) == "deny"

    output = await specs["goal_status"].scoped_executor(invocation(), GoalRead(), None)
    assert output["version"] == "goals/1" and output["goal_id"] is None
    body = GoalUpdate(request_key="k", expected_revision=0, action="progress")
    await specs["goal_update"].scoped_executor(invocation("goal_update"), body, None)
    assert [call[0] for call in service.calls] == ["authorize", "authorize", "read", "update"]
    assert len(resolutions) == 4
    with pytest.raises(ServiceError, match="workspace_contract_required"):
        await specs["goal_status"].executor(GoalRead())


@pytest.mark.asyncio
async def test_run_binding_mismatch_and_denied_policy():
    service = FakeGoals()

    async def resolve(_call):
        return scope()

    bundle = GoalService.bundle(service, resolve)
    specs = {spec.name: spec for spec in bundle.specs}
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await specs["goal_status"].scoped_executor(
            invocation(run_id="different-run"), GoalRead(), None
        )
    # Policy fails closed on binding problems instead of surfacing the error.
    assert await bundle.policy(invocation("goal_update", run_id="different-run")) == "deny"

    class BreakScope:
        async def authorize(self, *args, **kwargs):
            raise ServiceError("not_found", 404)

    denied = GoalService.bundle(BreakScope(), resolve)
    assert await denied.policy(invocation()) == "deny"


@pytest.mark.asyncio
async def test_scope_check_after_execution_is_enforced():
    service = FakeGoals()
    calls = 0

    def check():
        nonlocal calls
        calls += 1
        if calls > 2:
            raise ServiceError("cancelled")

    async def resolve(_call):
        return scope(check=check)

    bundle = GoalService.bundle(service, resolve)
    specs = {spec.name: spec for spec in bundle.specs}
    with pytest.raises(ServiceError, match="cancelled"):
        await specs["goal_status"].scoped_executor(invocation(), GoalRead(), None)


@pytest.mark.asyncio
async def test_invalid_tool_input_shapes_are_rejected_before_execution():
    service = FakeGoals()

    async def resolve(_call):
        return scope()

    specs = {spec.name: spec for spec in GoalService.bundle(service, resolve).specs}
    with pytest.raises(ValidationError):
        specs["goal_update"].parameters.model_validate({"expected_revision": 0, "action": "set"})
    # objective is optional at wire level; set without it is rejected by the service.
    assert (
        specs["goal_update"]
        .parameters.model_validate({"request_key": "k", "expected_revision": 0, "action": "set"})
        .objective
        is None
    )
    with pytest.raises(ValidationError):
        specs["goal_update"].parameters.model_validate(
            {"request_key": "k", "expected_revision": 0, "action": "unknown_action"}
        )
    with pytest.raises(ServiceError, match="invalid_input"):
        await specs["goal_status"].scoped_executor(
            invocation(), GoalUpdate(request_key="k", expected_revision=0, action="progress"), None
        )
    with pytest.raises(ValidationError):
        specs["goal_status"].parameters.model_validate({"revision": -1})
    with pytest.raises(ServiceError, match="invalid_input"):
        await specs["goal_status"].scoped_executor(
            invocation(), GoalUpdate(request_key="k", expected_revision=0, action="progress"), None
        )
