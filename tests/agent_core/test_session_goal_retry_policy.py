"""Goal-only deltas: plan-state allowance and durable permanent queue failures."""

from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError, StrictModel, fingerprint
from krim_agent.goals.continuation import ContinuationRequest
from krim_agent.interaction.service import plan_allows
from krim_agent.sessions.service import SessionService
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.goal_models import goal_continuations
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import ToolSource, ToolSpec


class Args(StrictModel):
    pass


async def unavailable(_args):
    return {}


async def scoped(_call, _args, _context):
    return {}


async def external(_call, _args):
    return {}


def test_plan_mode_allows_internal_goal_state_without_authorizing_external_actions():
    internal = ToolSpec(
        "goal_update", Args, unavailable, {"type": "object"}, "test/1", fingerprint("goal"),
        risk="write", capability="session_state", scoped_executor=scoped,
    )
    remote = ToolSpec(
        "remote_goal_update", Args, unavailable, {"type": "object"}, "test/1",
        fingerprint("remote"), risk="write", source=ToolSource("mcp", "server", "goal", "a" * 64),
        external_executor=external,
    )
    assert plan_allows(internal)
    assert not plan_allows(remote)
    assert not plan_allows(remote, trusted_readonly_names={remote.name})


@pytest.mark.parametrize(
    ("code", "attempts", "status"),
    [
        ("workspace_contract_required", 1, "failed"),
        ("permission_denied", 1, "failed"),
        ("input_revision_conflict", 1, "failed"),
        ("session_busy", 2, "queued"),
        ("capacity", 2, "queued"),
    ],
)
async def test_queue_recovery_stops_permanent_errors_and_retries_temporary_busy(
    tmp_path, code, attempts, status
):
    store = await SQLiteStore.open(tmp_path / "retry.sqlite")
    try:
        session = await session_sql.create_session(
            store, "local", "local", SessionCreate(request_key="session"), {},
            ContextPolicy(), Settings(),
        )
        request = ContinuationRequest(
            workspace_id="local", session_id=session.id, owner_id="local", goal_id="goal",
            goal_revision=1, continue_key="continue-key",
            turn_request_key="goal-continue:continue-key", origin_turn_id="origin",
            origin_run_id="origin-run", reason="goal_active", text="Continue authorized goal",
        )
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(insert(goal_continuations).values(
                session_id=session.id, request_key=request.continue_key, workspace_id="local",
                owner_id="local", goal_id="goal", goal_revision_base=1,
                origin_turn_id="origin", origin_run_id="origin-run", status="queued",
                internal=True, payload=request.model_dump(mode="json"),
                created_at=1.0, updated_at=1.0,
            ))
        service = object.__new__(SessionService)
        service.store, service.goals = store, object()
        service.runs = SimpleNamespace(tasks={})
        calls = []

        async def enqueue(candidate):
            calls.append(candidate)
            raise ServiceError(code)

        service.enqueue = enqueue
        await service._reconcile_goals()
        await service._reconcile_goals()
        assert calls == [request] * attempts
        async with store.lock, store.engine.begin() as conn:
            row = (await conn.execute(select(goal_continuations))).mappings().one()
        assert row["status"] == status
        assert row["code"] == (code if status == "failed" else None)
    finally:
        await store.close()
