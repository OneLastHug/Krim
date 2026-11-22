"""domain close/interrupt/dispose 集成验收（真实 SubagentService + SQLiteStore）。

只验证两个具体修复的域路径：并发 dispose 幂等（provider.dispose 恰一次）、
control close 的成员关闭 + queued 任务 aborted + 租约归还 + interrupt 的
cancel_delegate 记录；FakeRuntime/FakeProvider 只做记录桩，不造工具
producer、不调付费模型。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert, select, update
from test_subagent_team import write

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import RunRequest, ServiceError, fingerprint
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.storage.subagent_models import subagent_members, subagent_tasks
from krim_agent.subagents.configuration import (
    SubagentProfileConfiguration,
    SubagentsConfiguration,
)
from krim_agent.subagents.providers.base import ProviderCapabilities
from krim_agent.subagents.service import SubagentService
from krim_agent.subagents.types import ReserveAgent, TreeScope

WORKSPACE = "local"
OWNER = "local"
ACTOR = PrincipalContext(WORKSPACE, 1, (Membership(workspace_id=WORKSPACE, role="admin"),))


@dataclass
class FakeProvider:
    version: str = "v1"
    capabilities: ProviderCapabilities = field(default_factory=ProviderCapabilities)
    dispose_calls: list[str] = field(default_factory=list)

    async def dispose(self, activation_id: str) -> None:
        self.dispose_calls.append(activation_id)


@dataclass
class FakeRuntime:
    cancelled: list[str] = field(default_factory=list)

    async def cancel_delegate(self, run_id: str) -> Any:
        self.cancelled.append(run_id)
        return {"run_id": run_id}


async def service_for(tmp_path: Path, name: str = "lifecycle.sqlite") -> SimpleNamespace:
    store = await SQLiteStore.open(tmp_path / name)
    configuration = SubagentsConfiguration(enabled=True)
    provider = FakeProvider()
    service = SubagentService(store, configuration, session_tools=None)
    service.register_profile("worker", provider, SubagentProfileConfiguration())
    registration = service._registrations[("worker", "target-http")]
    profile_version = fingerprint(SubagentProfileConfiguration().model_dump(mode="json"))
    runtime = FakeRuntime()
    service.attach_runtime(runtime, None)
    authorizations: list[tuple[str, str, str, bool]] = []

    async def authorize_session(
        actor: Any, workspace_id: str, session_id: str, *, write: bool
    ) -> None:
        authorizations.append((actor.principal_id, workspace_id, session_id, write))

    service.authorize_session = authorize_session
    session = await session_sql.create_session(
        store,
        WORKSPACE,
        OWNER,
        SessionCreate(request_key="lifecycle-session"),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )
    tree = await service.repository.ensure_tree(
        TreeScope(
            session_id=session.id,
            workspace_id=WORKSPACE,
            owner_id=OWNER,
            request_key="root:lifecycle",
        )
    )
    root = await service.repository.reserve_agent(
        ReserveAgent(
            tree_id=tree["tree_id"],
            path="lead",
            role="root",
            provider=registration.name,
            profile="worker",
            provider_version=registration.version,
            profile_version=profile_version,
            request_key="spawn:lifecycle-root",
        )
    )
    reservation = await service.repository.reserve_agent(
        ReserveAgent(
            tree_id=tree["tree_id"],
            parent_id=root["agent"]["agent_id"],
            path="lead/w1",
            role="agent",
            provider=registration.name,
            profile="worker",
            provider_version=registration.version,
            profile_version=profile_version,
            max_depth=2,
            resident_limit=configuration.max_resident_agents,
            request_key="spawn:lifecycle-w1",
        )
    )
    return SimpleNamespace(
        store=store,
        ids=tree,
        session=session,
        service=service,
        repository=service.repository,
        budgets=service.budgets,
        provider=provider,
        runtime=runtime,
        authorizations=authorizations,
        registration=registration,
        reservation=reservation,
    )


async def seed_task_with_lease(
    env: SimpleNamespace, *, run_id: str | None = None
) -> tuple[str, str]:
    await env.budgets.ensure(env.ids["tree_id"], 4)
    await env.budgets.reserve(env.ids["tree_id"], "lease-close", 2)
    member = await env.repository.get_agent(env.reservation["agent"]["agent_id"])
    task_id = str(uuid.uuid4())
    body = {"goal": "queued work", "budget_key": "lease-close"}

    async def operation(conn: Any) -> None:
        ordinal = (
            await conn.scalar(
                select(subagent_tasks.c.ordinal).where(
                    subagent_tasks.c.agent_id == member["agent_id"]
                )
            )
            or 0
        ) + 1
        await conn.execute(
            insert(subagent_tasks).values(
                id=task_id,
                agent_id=member["agent_id"],
                tree_id=env.ids["tree_id"],
                ordinal=ordinal,
                status="accepted",
                run_id=run_id,
                parent_run_id=None,
                source_version=1,
                provider_version="v1",
                profile_version="v1",
                input=body,
                input_hash=fingerprint(body),
                request_key=f"seed:{task_id}@{member['agent_id']}",
                request_hash=fingerprint({"task": task_id}),
                revision=1,
                input_closed=False,
                created_at=time.time(),
                finished_at=None,
            )
        )
        await conn.execute(
            update(subagent_members)
            .where(subagent_members.c.id == member["agent_id"])
            .values(current_task_id=task_id)
        )

    await write(env.store, operation)
    return task_id, "lease-close"


async def root_member_id(store: SQLiteStore, tree_id: str) -> str:
    async def read(conn: Any) -> str:
        return str(
            await conn.scalar(
                select(subagent_members.c.id).where(
                    subagent_members.c.tree_id == tree_id,
                    subagent_members.c.role == "root",
                )
            )
        )

    return await store.work_read(read)


async def test_concurrent_dispose_calls_provider_once_and_both_released(tmp_path: Path) -> None:
    env = await service_for(tmp_path)
    try:
        activation_id = env.reservation["activation"]["activation_id"]
        first, second = await asyncio.gather(
            env.service.dispose(activation_id), env.service.dispose(activation_id)
        )
        assert first == {"activation_id": activation_id, "released": True}
        assert second == {"activation_id": activation_id, "released": True}
        assert env.provider.dispose_calls == [activation_id]
        again = await env.service.dispose(activation_id)
        assert again == {"activation_id": activation_id, "released": True}
        assert env.provider.dispose_calls == [activation_id]
        member = await env.repository.get_agent(env.reservation["agent"]["agent_id"])
        assert member["activation"]["status"] == "released"
    finally:
        await env.store.close()


async def test_control_close_confirms_member_and_aborts_queued_task_with_lease(
    tmp_path: Path,
) -> None:
    env = await service_for(tmp_path)
    try:
        agent_id = env.reservation["agent"]["agent_id"]
        activation_id = env.reservation["activation"]["activation_id"]
        task_id, _lease_key = await seed_task_with_lease(env)
        result = await env.service.control(
            ACTOR, WORKSPACE, env.session.id, agent_id, action="close"
        )
        assert result["closed"] is True
        member = await env.repository.get_agent(agent_id)
        assert member["status"] == "closed"
        settled = await env.repository.result(task_id)
        assert settled is not None
        assert settled["stop_reason"] == "aborted"
        assert settled["code"] == "member_closed"
        state = await env.budgets.state(env.ids["tree_id"])
        assert state["active_leases"] == []
        assert env.provider.dispose_calls == [activation_id]
        assert env.authorizations
        assert all(entry[3] is True for entry in env.authorizations)
        assert {entry[0] for entry in env.authorizations} == {OWNER}
    finally:
        await env.store.close()


async def test_control_verifies_identity_membership_and_root_target(tmp_path: Path) -> None:
    env = await service_for(tmp_path)
    try:
        agent_id = env.reservation["agent"]["agent_id"]
        outsider = PrincipalContext(
            "someone-else", 1, (Membership(workspace_id=WORKSPACE, role="admin"),)
        )
        with pytest.raises(ServiceError, match="not_found"):
            await env.service.control(
                outsider, WORKSPACE, env.session.id, agent_id, action="close"
            )
        with pytest.raises(ServiceError, match="permission_denied"):
            await env.service.control(
                ACTOR, WORKSPACE, env.session.id, agent_id, action="retire"
            )
        root_id = await root_member_id(env.store, env.ids["tree_id"])
        with pytest.raises(ServiceError, match="permission_denied"):
            await env.service.control(
                ACTOR, WORKSPACE, env.session.id, root_id, action="close"
            )
        assert env.runtime.cancelled == []
        assert env.provider.dispose_calls == []
    finally:
        await env.store.close()


async def test_control_interrupt_records_cancel_delegate(tmp_path: Path) -> None:
    env = await service_for(tmp_path)
    try:
        agent_id = env.reservation["agent"]["agent_id"]
        run, _ = await env.store.begin(
            RunRequest(request_key="lifecycle-child-run", text="run", target="demo"),
            {"key": "demo"},
        )
        task_id, _lease_key = await seed_task_with_lease(env, run_id=run.id)

        async def bind_run(conn: Any) -> None:
            await conn.execute(
                update(subagent_tasks).where(subagent_tasks.c.id == task_id).values(run_id=run.id)
            )

        await write(env.store, bind_run)
        result = await env.service.control(
            ACTOR, WORKSPACE, env.session.id, agent_id, action="interrupt"
        )
        assert result == {"agent_id": agent_id, "interrupted": True}
        assert env.runtime.cancelled == [run.id]
        assert env.provider.dispose_calls == []
    finally:
        await env.store.close()
