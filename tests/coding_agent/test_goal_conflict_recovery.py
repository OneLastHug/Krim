"""A rejected goal CAS returns facts to the SDK without ending the user task."""

import json

import ai
from ai import events, messages

from krim_agent.agent_sdk import SDKAdapter
from krim_agent.contracts import ServiceError
from krim_agent.goals import GoalService, GoalUpdate
from krim_agent.goals.contracts import GoalSnapshot, goal_digest
from krim_agent.security import LOCAL
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import SessionToolScope
from tests.coding_agent.sdk_fixtures import Scripted, done, model, setup


async def test_goal_conflict_does_not_abort_fail_on_tool_error_run():
    updates = []

    class Goals:
        async def authorize(self, *args, **kwargs):
            return None

        async def update_from_tool(self, scope, body):
            updates.append(body)
            raise ServiceError("revision_conflict", 409)

        async def read(self, *args, **kwargs):
            return GoalSnapshot(
                goal_id=None, session_id="session", workspace_id="local", owner_id=None,
                revision=0, status=None, objective=None,
                content_hash=goal_digest({"version": "goals/1", "goal": None}),
            )

    async def resolve(_call):
        return SessionToolScope(LOCAL, "local", "session", "turn", "run")

    bundle = GoalService.bundle(Goals(), resolve)
    executor = ToolExecutor(ToolRegistry(bundle.specs), bundle.policy)
    harness = setup(approval=False)

    @ai.tool
    async def goal_update(request_key: str, expected_revision: int, action: str):
        return None

    adapter = SDKAdapter(
        store=harness.store, operations=harness.operations, executor=executor,
        tools=[goal_update], environment=harness.environment, ownership=harness.ownership,
        read_result=harness.store.read_result, approval_request=harness.adapter().approval_request,
        fail_on_tool_error=True,
    )
    raw = json.dumps({"request_key": "goal-cas", "expected_revision": 4, "action": "progress"})
    call = messages.ToolCallPart(
        tool_call_id="stale-goal", tool_name="goal_update", tool_args=raw,
    )
    provider = Scripted([[
        events.StreamStart(),
        events.ToolStart(tool_call_id="stale-goal", tool_name="goal_update"),
        events.ToolDelta(tool_call_id="stale-goal", chunk=raw),
        events.ToolEnd(tool_call_id="stale-goal", tool_call=call),
        events.StreamEnd(finish_reason="tool_call"),
    ], done()])
    result = await adapter.run(model(provider), [ai.user_message("Continue the project")])
    assert result.status == "completed" and len(provider._seen) == 2
    observed = next(part.result for message in provider._seen[1]
                    for part in message.tool_results if part.tool_call_id == "stale-goal")
    assert observed["value"]["ok"] is False
    assert observed["value"]["code"] == "revision_conflict"
    assert observed["value"]["current_goal"]["revision"] == 0
    assert updates == [GoalUpdate(request_key="goal-cas", expected_revision=4, action="progress")]
