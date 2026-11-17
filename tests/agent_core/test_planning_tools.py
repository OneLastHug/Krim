"""Session-scoped plan routing and strict parameter contracts."""

import pytest
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.planning import PlanningService, PlanRead, PlanUpdate
from krim_agent.planning.contracts import PlanCounts, PlanSnapshot, plan_digest
from krim_agent.security import LOCAL
from krim_agent.tools.core import Invocation
from krim_agent.tools.session import SessionToolScope


def invocation(name="plan_read", *, run_id="run", call_id="call"):
    return Invocation(run_id, call_id, "span", 0, name, "{}", "a" * 64)


def empty():
    return PlanSnapshot(
        session_id="session",
        revision=0,
        content_hash=plan_digest([]),
        updated_at=None,
        counts=PlanCounts(),
        total=0,
        matched_total=0,
        steps=(),
        offset=0,
        limit=50,
        next_offset=None,
    )


class FakePlanning:
    def __init__(self):
        self.calls = []

    async def authorize(self, actor, workspace, session, *, write, check):
        check()
        self.calls.append(("authorize", actor, workspace, session, write))

    async def read(self, actor, workspace, session, query, *, check):
        check()
        self.calls.append(("read", actor, workspace, session, query))
        return empty()

    async def update(self, actor, workspace, session, body, *, check):
        check()
        self.calls.append(("update", actor, workspace, session, body))
        return empty()


def scope(**kwargs):
    return SessionToolScope(LOCAL, "local", "session", "turn", "run", **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("project_id", [None, "development-project"])
async def test_bundle_uses_host_scope_without_work_frame(project_id):
    service = FakePlanning()
    resolutions = []

    async def resolve(call):
        resolutions.append(call.call_id)
        return scope(project_id=project_id)

    bundle = PlanningService.bundle(service, resolve)
    specs = {spec.name: spec for spec in bundle.specs}
    assert set(specs) == {"plan_read", "plan_update"}
    assert specs["plan_read"].capability == "session_read"
    assert specs["plan_update"].capability == "session_state"
    assert specs["plan_read"].risk == "read"
    assert specs["plan_update"].risk == "write"
    assert specs["plan_update"].descriptor()["policy_version"] == "session/1"
    for spec in specs.values():
        schema = spec.parameters.model_json_schema()
        assert not {"actor", "session_id", "workspace_id", "project_id"} & set(schema["properties"])
    assert await bundle.policy(invocation()) == "allow"
    assert await bundle.policy(invocation("plan_update")) == "allow"
    output = await specs["plan_read"].scoped_executor(invocation(), PlanRead(), object())
    assert output["version"] == "planning/1" and output["steps"] == []
    body = PlanUpdate(request_key="update", expected_revision=0, steps=())
    await specs["plan_update"].scoped_executor(invocation("plan_update"), body, None)
    assert [call[0] for call in service.calls] == ["authorize", "authorize", "read", "update"]
    assert service.calls[1][-1] is True
    assert service.calls[-1][1:4] == (LOCAL, "local", "session")
    assert len(resolutions) == 4
    with pytest.raises(ServiceError, match="workspace_contract_required"):
        await specs["plan_read"].executor(PlanRead())


@pytest.mark.asyncio
async def test_policy_rejects_unknown_tool_stale_run_and_revoked_scope():
    service = FakePlanning()
    current_scope = scope()

    async def resolve(call):
        return current_scope

    bundle = PlanningService.bundle(service, resolve)
    spec = bundle.specs[0]
    assert await bundle.policy(invocation("other_tool")) == "deny"
    assert await bundle.policy(invocation(run_id="other-run")) == "deny"
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await spec.scoped_executor(invocation(run_id="other-run"), PlanRead(), None)
    assert service.calls == []

    def revoked():
        raise ServiceError("permission_denied", 403)

    current_scope = scope(check=revoked)
    assert await bundle.policy(invocation()) == "deny"
    with pytest.raises(ServiceError, match="permission_denied"):
        await spec.scoped_executor(invocation(), PlanRead(), None)


@pytest.mark.asyncio
async def test_executor_rechecks_scope_after_policy_and_rejects_parameter_mismatch():
    service = FakePlanning()
    allowed = True

    def check():
        if not allowed:
            raise ServiceError("cancelled")

    async def resolve(call):
        return scope(check=check)

    bundle = PlanningService.bundle(service, resolve)
    assert await bundle.policy(invocation()) == "allow"
    allowed = False
    with pytest.raises(ServiceError, match="cancelled"):
        await bundle.specs[0].scoped_executor(invocation(), PlanRead(), None)
    assert len(service.calls) == 1
    allowed = True
    with pytest.raises(ServiceError, match="invalid_input"):
        await bundle.specs[0].scoped_executor(
            invocation(), PlanUpdate(request_key="k", expected_revision=0, steps=()), None
        )


def test_strict_frozen_dtos_no_legacy_states_and_revision_pinned_pagination():
    plan = PlanUpdate.model_validate_json(
        '{"request_key":"k","expected_revision":0,"steps":[{"id":"a","text":"do work",'
        '"status":"in_progress"}]}'
    )
    assert isinstance(plan.steps, tuple)
    with pytest.raises(ValidationError):
        plan.steps[0].status = "completed"
    for state in ("running", "failed"):
        with pytest.raises(ValidationError):
            PlanUpdate.model_validate_json(
                '{"request_key":"k","expected_revision":0,"steps":[{"id":"a","text":"x",'
                '"status":"' + state + '"}]}'
            )
    with pytest.raises(ValidationError):
        PlanRead(offset=1)
    with pytest.raises(ValidationError):
        PlanRead(statuses=("blocked", "blocked"))
    with pytest.raises(ValidationError):
        PlanRead(limit=201)
    with pytest.raises(ValidationError):
        PlanUpdate(request_key="x", expected_revision=0, steps=(plan.steps[0], plan.steps[0]))
    with pytest.raises(ValidationError):
        PlanRead.model_validate_json('{"actor":"forged"}')


@pytest.mark.asyncio
async def test_real_executor_admits_json_arrays_without_work_frame():
    from krim_agent.tools.executor import ToolContext, ToolExecutor
    from krim_agent.tools.registry import ToolRegistry

    service = FakePlanning()

    async def resolve(call):
        return scope()

    events = []

    async def emit(kind, data):
        events.append((kind, data))

    async def approve(*args):
        raise AssertionError("Session plan state must not ask for file-write approval")

    bundle = PlanningService.bundle(service, resolve)
    executor = ToolExecutor(ToolRegistry(bundle.specs), bundle.policy)
    context = ToolContext(emit=emit, approve=approve, timeout=5)
    inputs = {
        "plan_read": '{"statuses":["pending","blocked"]}',
        "plan_update": (
            '{"request_key":"update","expected_revision":0,"steps":['
            '{"id":"a","text":"Do work","status":"pending"}]}'
        ),
    }
    for spec in bundle.specs:
        call = Invocation("run", "call", "span", 0, spec.name, inputs[spec.name], spec.fingerprint)
        admitted = await executor.admit(call, context, timeout_seconds=5)
        assert admitted.decision == "allow"
        assert admitted.parsed.model_dump(mode="json")
        if spec.name == "plan_update":
            assert isinstance(admitted.parsed.steps, tuple)
        else:
            assert admitted.parsed.statuses == ("pending", "blocked")
        outcome = await executor.execute(call, context, admitted)
        assert outcome.status == "ok", outcome.code
        assert outcome.value["version"] == "planning/1"
    assert [kind for kind, _ in events] == [
        "tool_start",
        "tool_intent",
        "tool_end",
        "tool_start",
        "tool_intent",
        "tool_end",
    ]
