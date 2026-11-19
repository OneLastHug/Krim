"""Live context projections pin plan pages and use the real current user turn."""

import json
from dataclasses import replace

import pytest
from sqlalchemy import update
from test_memory_tools import bind_projects, memory_environment

from krim_agent.context.serialization import fingerprint
from krim_agent.context.service import ContextService
from krim_agent.contracts import ServiceError
from krim_agent.extensions import MemoryCreate, Preference
from krim_agent.planning.contracts import PlanStep, PlanUpdate
from krim_agent.planning.service import PlanningService
from krim_agent.runtime.session_snapshots import SessionSnapshotProvider
from krim_agent.storage.models import runs, turns
from krim_agent.storage.planning_models import PLANNING_TABLES
from krim_agent.storage.sandbox_models import sandbox_session_bindings


async def setup_plan(store, identities, scope, total=103):
    async with store.lock, store.engine.begin() as conn:
        await conn.run_sync(
            lambda sync: PLANNING_TABLES[0].metadata.create_all(sync, tables=list(PLANNING_TABLES))
        )
        await conn.execute(update(runs).where(runs.c.id == scope.run_id).values(status="running"))
    plans = PlanningService(store, identities)
    await plans.update(
        scope.actor,
        scope.workspace_id,
        scope.session_id,
        PlanUpdate(
            request_key="plan",
            expected_revision=0,
            steps=tuple(PlanStep(id=f"s{index}", text=f"Step {index}") for index in range(total)),
        ),
    )
    return plans


async def test_provider_pins_plan_revision_and_recalls_current_turn(tmp_path):
    async with memory_environment(tmp_path / "snapshots.sqlite") as (
        store,
        identities,
        _,
        ext,
        memory,
        scope,
    ):
        plans = await setup_plan(store, identities, scope)
        saved = await ext.memory(scope.actor, MemoryCreate(request_key="coffee", body="深烘焙咖啡"))
        # A new revision appears after the first page; remaining pages must stay on revision 1.
        calls = []

        class ConcurrentPlans:
            async def snapshot(self, *args, **kwargs):
                calls.append(dict(kwargs))
                result = await plans.snapshot(*args, **kwargs)
                if len(calls) == 1:
                    await plans.update(
                        scope.actor,
                        scope.workspace_id,
                        scope.session_id,
                        PlanUpdate(
                            request_key="new-plan",
                            expected_revision=1,
                            steps=(PlanStep(id="other", text="new revision"),),
                        ),
                    )
                return result

        # Use separate matching tokens to exercise real relevance, not fabricated memory data.
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(turns).where(turns.c.id == scope.turn_id).values(text="深烘焙咖啡")
            )

        class MatchingMemory:
            async def recall(self, passed_scope, query, **kwargs):
                assert query == "深烘焙咖啡"
                return await memory.recall(passed_scope, query, **kwargs)

        provider = SessionSnapshotProvider(
            store, planning=ConcurrentPlans(), memory_tools=MatchingMemory(), plan_page_size=50
        )
        items = await provider(scope)
        assert isinstance(items, tuple) and [item.name for item in items] == ["plan", "memory"]
        plan = json.loads(items[0].content)
        assert items[0].revision == 1 and len(plan["steps"]) == 103
        assert plan["steps"][0]["id"] == "s0" and plan["steps"][-1]["id"] == "s102"
        assert [call.get("revision") for call in calls] == [None, 1, 1]
        assert [call.get("offset", 0) for call in calls] == [0, 50, 100]
        assert items[0].source_refs[0]["content_hash"] == plan["content_hash"]
        recalled = json.loads(items[1].content)
        assert recalled["items"][0]["id"] == saved["id"]
        assert items[1].revision == recalled["fingerprint"]
        assert items[1].source_refs[0]["preference_revision"] == 1
        assert (
            fingerprint({key: value for key, value in recalled.items() if key != "fingerprint"})
            == items[1].revision
        )


async def test_disabled_memory_keeps_explicit_projection_and_optional_goal(tmp_path):
    async with memory_environment(tmp_path / "disabled-provider.sqlite") as (
        store,
        identities,
        _,
        ext,
        memory,
        scope,
    ):
        await setup_plan(store, identities, scope, total=0)
        await ext.preference(
            scope.actor, Preference(request_key="disabled", expected_revision=1, enabled=False)
        )

        class Goals:
            async def snapshot(self, actor, workspace_id, session_id, *, check):
                check()
                assert actor == scope.actor and workspace_id == scope.workspace_id
                return {
                    "version": "goals/1",
                    "session_id": session_id,
                    "revision": 3,
                    "status": "paused",
                    "objective": "Finish the user task",
                }

        items = await SessionSnapshotProvider(store, memory_tools=memory, goals=Goals())(scope)
        assert [item.name for item in items] == ["memory", "goal"]
        snapshot = json.loads(items[0].content)
        assert snapshot["status"] == "disabled" and snapshot["items"] == []
        assert snapshot["next_offset"] is None and snapshot["preference_revision"] == 2
        assert items[1].revision == 3 and json.loads(items[1].content)["status"] == "paused"
        assert await SessionSnapshotProvider(store)(scope) == ()


async def test_provider_scope_cancellation_and_binding_guards(tmp_path):
    async with memory_environment(tmp_path / "guards.sqlite") as (
        store,
        identities,
        _,
        _,
        memory,
        scope,
    ):
        await setup_plan(store, identities, scope, total=0)
        provider = SessionSnapshotProvider(store, memory_tools=memory)
        for wrong in [
            replace(scope, turn_id="unknown"),
            replace(scope, run_id="unknown"),
            replace(scope, session_id="unknown"),
        ]:
            with pytest.raises(ServiceError, match="not_found"):
                await provider(wrong)
        bound = await bind_projects(store, scope)
        with pytest.raises(ServiceError, match="context_binding_changed"):
            await provider(scope)
        assert len(await provider(bound)) == 1
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(runs).where(runs.c.id == scope.run_id).values(stop_requested=True)
            )
        with pytest.raises(ServiceError, match="cancelled"):
            await provider(bound)


async def test_provider_rejects_broken_plan_pagination_and_foreign_goal(tmp_path):
    async with memory_environment(tmp_path / "bad-provider.sqlite") as (
        store,
        identities,
        _,
        _,
        _,
        scope,
    ):
        plans = await setup_plan(store, identities, scope, total=3)

        class StuckPlans:
            async def snapshot(self, *args, **kwargs):
                page = await plans.snapshot(*args, **kwargs)
                return page.model_copy(update={"next_offset": kwargs.get("offset", 0)})

        with pytest.raises(ServiceError, match="facts_corrupt"):
            await SessionSnapshotProvider(store, planning=StuckPlans(), plan_page_size=1)(scope)

        class ForeignGoals:
            async def snapshot(self, actor, workspace_id, session_id, *, check):
                return {"session_id": "other", "revision": 1}

        with pytest.raises(ServiceError, match="facts_corrupt"):
            await SessionSnapshotProvider(store, goals=ForeignGoals())(scope)


async def test_final_guard_rechecks_after_live_service_changes(tmp_path):
    async with memory_environment(tmp_path / "post-read.sqlite") as (
        store,
        identities,
        _,
        _,
        memory,
        scope,
    ):
        await setup_plan(store, identities, scope, total=0)

        class StoppingMemory:
            async def recall(self, current_scope, query, **kwargs):
                result = await memory.recall(current_scope, query, **kwargs)
                async with store.lock, store.engine.begin() as conn:
                    await conn.execute(
                        update(runs).where(runs.c.id == scope.run_id).values(stop_requested=True)
                    )
                return result

        with pytest.raises(ServiceError, match="cancelled"):
            await SessionSnapshotProvider(store, memory_tools=StoppingMemory())(scope)


async def test_provider_injected_into_real_context_boundary(tmp_path):
    async with memory_environment(tmp_path / "boundary-provider.sqlite") as (
        store,
        identities,
        _,
        _,
        memory,
        scope,
    ):
        plans = await setup_plan(store, identities, scope, total=4)
        provider = SessionSnapshotProvider(
            store, planning=plans, memory_tools=memory, plan_page_size=2
        )
        context = ContextService(store, identities, snapshots=provider)
        items = await context.boundary(
            scope.actor, scope.workspace_id, scope.session_id, scope.run_id
        ).prepare()
        assert [item.name for item in items] == ["plan", "memory"]
        assert len(json.loads(items[0].content)["steps"]) == 4
        assert json.loads(items[1].content)["session_id"] == scope.session_id


async def test_real_goal_snapshot_empty_and_active_use_original_revision(tmp_path):
    from krim_agent.goals.contracts import GoalUpdate
    from krim_agent.goals.service import GoalService
    from krim_agent.storage.goal_models import GOAL_TABLES

    async with memory_environment(tmp_path / "real-goals.sqlite") as (
        store,
        identities,
        _,
        _,
        _,
        scope,
    ):
        await setup_plan(store, identities, scope, total=0)
        async with store.lock, store.engine.begin() as conn:
            await conn.run_sync(
                lambda sync: GOAL_TABLES[0].metadata.create_all(sync, tables=list(GOAL_TABLES))
            )
        goals = GoalService(store, identities)
        provider = SessionSnapshotProvider(store, goals=goals)
        empty = (await provider(scope))[0]
        assert empty.name == "goal" and empty.revision == 0
        assert json.loads(empty.content)["owner_id"] is None
        saved = await goals.update_from_tool(
            scope,
            GoalUpdate(
                request_key="goal",
                expected_revision=0,
                action="set",
                objective="完成用户任务",
                completion_criteria=("验证通过",),
            ),
        )
        active = (await provider(scope))[0]
        assert active.revision == saved.revision == 1
        assert json.loads(active.content)["content_hash"] == saved.content_hash
        assert json.loads(active.content)["objective"] == "完成用户任务"


async def test_bound_project_context_boundary_uses_host_scope(tmp_path):
    async with memory_environment(tmp_path / "project-boundary.sqlite") as (
        store,
        identities,
        _,
        ext,
        memory,
        scope,
    ):
        await setup_plan(store, identities, scope, total=0)
        scope = await bind_projects(store, scope)
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(turns).where(turns.c.id == scope.turn_id).values(text="project marker")
            )
        saved = await ext.memory(
            scope.actor,
            MemoryCreate(request_key="project-memory", body="project marker"),
            workspace_id=scope.workspace_id,
            project_id=scope.project_id,
        )
        context = ContextService(
            store, identities, snapshots=SessionSnapshotProvider(store, memory_tools=memory)
        )
        boundary = context.boundary(
            scope.actor, scope.workspace_id, scope.session_id, scope.run_id, scope=scope
        )
        items = await boundary.prepare()
        assert len(items) == 1 and items[0].name == "memory"
        assert [item["id"] for item in json.loads(items[0].content)["items"]] == [saved["id"]]
        assert json.loads(items[0].content)["project_id"] == scope.project_id
        resolved = await context.boundary(
            scope.actor, scope.workspace_id, scope.session_id, scope.run_id
        ).prepare()
        assert json.loads(resolved[0].content)["project_id"] == scope.project_id
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(sandbox_session_bindings)
                .where(sandbox_session_bindings.c.session_id == scope.session_id)
                .values(project_id="project-two")
            )
        with pytest.raises(ServiceError, match="permission_denied"):
            await boundary.prepare()
        changed = await context.boundary(
            scope.actor, scope.workspace_id, scope.session_id, scope.run_id
        ).prepare()
        changed_memory = json.loads(changed[0].content)
        assert changed_memory["project_id"] == "project-two" and changed_memory["items"] == []
