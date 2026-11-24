"""send_message(kind=task) 续聊必须继承该 agent 已持久化的隔离 workspace。

真实缺陷复现：spawn 首任务的 input 持久化 workspace/baseline/profile_snapshot；
同一 agent 的 send_message new_task 续聊此前只重建 history/profile_snapshot，
DevelopmentSDKLoop._isolated_assignment 读 task.input["workspace"] 得 None，
shell.cwd 退回共享根目录，隔离域静默丢失。验收契约：续聊任务从该 agent 的
持久化任务事实逐字继承同一 assignment/baseline（同 agent、同 project、基线
指纹一致；process profile 的任务文本同步携带 workspace 文档），shared 模式
不新增 workspace；事实缺失或损坏拒绝续派而不静默降级；同 send_key 幂等重放
不重建任务输入。全部走真实 SubagentService + SQLiteStore，不造抽象。
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest_asyncio
from sqlalchemy import insert, update

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import (
    RunRequest,
    ServiceError,
    fingerprint,
    parse_json,
)
from krim_agent.security import Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.account_models import accounts
from krim_agent.storage.sandbox_models import sandbox_environments, sandbox_projects
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.storage.subagent_models import subagent_members, subagent_tasks
from krim_agent.subagents.configuration import (
    SubagentProcessConfiguration,
    SubagentProfileConfiguration,
    SubagentsConfiguration,
)
from krim_agent.subagents.errors import SubagentError
from krim_agent.subagents.providers.base import ProviderCapabilities
from krim_agent.subagents.service import Caller, SubagentService
from krim_agent.subagents.tool_models import SendAgentMessage
from krim_agent.subagents.types import ReserveAgent, TreeScope
from krim_agent.subagents.workspace import WorkspaceIsolation, workspace_directory

PROJECT = "proj-1"
WORKSPACE = "local"
OWNER = "local"
ACTOR = PrincipalContext(WORKSPACE, 1, (Membership(workspace_id=WORKSPACE, role="admin"),))
TERMINAL = frozenset({"completed", "aborted", "error"})
_OPEN_STORES = []


@pytest_asyncio.fixture(autouse=True)
async def close_stores():
    yield
    while _OPEN_STORES:
        await _OPEN_STORES.pop().close()


@dataclass
class FakeProvider:
    version: str = "v1"
    capabilities: ProviderCapabilities = field(default_factory=ProviderCapabilities)

    @property
    def descriptor(self) -> dict[str, Any]:
        return {"system_prompt": "followup worker descriptor"}


class FakePort:
    """In-memory read surface for spawn-time baseline capture (no sandbox)."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files

    async def read(self, project_id: str, path: str, *, offset: int = 0, limit: int = 65536):
        import base64
        import hashlib

        data = self.files.get(path)
        if data is None:
            raise ServiceError("not_found", 404)
        chunk = data[offset:offset + limit]
        return {
            "path": path,
            "offset": offset,
            "size": len(data),
            "eof": offset + len(chunk) >= len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "data": base64.b64encode(chunk).decode(),
        }

    async def entries(self, project_id: str, path: str, *, offset: int = 0):
        prefix = "" if path == "." else path.rstrip("/") + "/"
        names: dict[str, str] = {}
        for candidate in sorted(self.files):
            if candidate.startswith(prefix):
                rest = candidate[len(prefix):]
                if "/" in rest:
                    names.setdefault(rest.split("/", 1)[0], "directory")
                else:
                    names[rest] = "file"
        ordered = [
            {"name": name, "type": kind, "size": 0} for name, kind in sorted(names.items())
        ]
        return {"path": path, "entries": ordered[offset:offset + 100], "next_offset": None}

    async def git_head(self, project_id: str) -> str | None:
        return None


async def isolated_facts(agent_id: str, *, project: str = PROJECT) -> tuple[dict, dict]:
    """A real, fingerprint-consistent spawn pair captured over the fake port."""
    port = FakePort({"README.md": b"source\n", "src/main.py": b"print(1)\n"})
    assignment, baseline = await WorkspaceIsolation(port).assign(
        "isolated_write", source_project_id=project, agent_id=agent_id, input_version=1,
    )
    return assignment.model_dump(mode="json"), baseline.model_dump(mode="json")


@dataclass
class Env:
    store: SQLiteStore
    service: SubagentService
    session: Any
    tree: dict
    agent_id: str
    root_id: str
    profile: SubagentProfileConfiguration
    authorizations: list


async def seed_project(store: SQLiteStore) -> None:
    """Account/environment/project rows backing the tree's project_id FK."""

    async def operation(conn: Any) -> None:
        now = time.time()
        await conn.execute(insert(accounts).values(
            principal_id=OWNER, display_name=OWNER, status="active", platform_role="admin",
            authorization_generation=1, revision=1, legacy=False,
            created_at=now, updated_at=now,
        ))
        await conn.execute(insert(sandbox_environments).values(
            environment_id="env-1", principal_id=OWNER, profile_name=None,
            quota_override={}, revision=1, status="active",
            created_at=now, updated_at=now,
        ))
        await conn.execute(insert(sandbox_projects).values(
            project_id=PROJECT, environment_id="env-1", principal_id=OWNER,
            workspace_id=WORKSPACE, title="demo", generation=0, status="active",
            lease_id=None, lease_expires_at=None, created_at=now, updated_at=now,
        ))

    async def confirm() -> None:
        return None

    await store.work_write(operation, confirm)


async def build(
    tmp_path: Path,
    profile: SubagentProfileConfiguration,
    *,
    processes: dict[str, SubagentProcessConfiguration] | None = None,
    name: str = "followup.sqlite",
) -> Env:
    store = await SQLiteStore.open(tmp_path / name)
    _OPEN_STORES.append(store)
    await seed_project(store)
    configuration = SubagentsConfiguration(
        enabled=True,
        profiles={"worker": profile},
        processes=processes or {},
        default_profile="worker",
    )
    service = SubagentService(store, configuration, session_tools=None)
    service.register_profile("worker", FakeProvider(), profile)
    registration = service._registrations[("worker", "target-http")]
    profile_version = fingerprint(profile.model_dump(mode="json"))
    service.attach_runtime(SimpleNamespace(), None)
    authorizations: list = []

    async def authorize_session(
        actor: Any, workspace_id: str, session_id: str, *, write: bool
    ) -> None:
        authorizations.append((actor.principal_id, workspace_id, session_id, write))

    service.authorize_session = authorize_session
    session = await session_sql.create_session(
        store,
        WORKSPACE,
        OWNER,
        SessionCreate(request_key="followup-session"),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )
    root_run, _ = await store.begin(
        RunRequest(text="root work", request_key="root:followup", target="target-http"),
        {"family": "test"},
    )
    tree = await service.repository.ensure_tree(TreeScope(
        session_id=session.id,
        run_id=root_run.id,
        workspace_id=WORKSPACE,
        owner_id=OWNER,
        project_id=PROJECT,
        request_key="root:followup",
    ))
    await service.budgets.ensure(tree["tree_id"], None)
    root = await service.repository.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"],
        path="lead",
        role="root",
        provider=registration.name,
        profile="worker",
        provider_version=registration.version,
        profile_version=profile_version,
        request_key="spawn:root",
    ))
    member = await service.repository.reserve_agent(ReserveAgent(
        tree_id=tree["tree_id"],
        parent_id=root["agent"]["agent_id"],
        path="lead/w1",
        role="agent",
        provider=registration.name,
        provider_version=registration.version,
        profile="worker",
        profile_version=profile_version,
        max_depth=configuration.max_depth,
        resident_limit=configuration.max_resident_agents,
        request_key="spawn:w1",
    ))
    return Env(
        store=store,
        service=service,
        session=session,
        tree=tree,
        agent_id=member["agent"]["agent_id"],
        root_id=root["agent"]["agent_id"],
        profile=profile,
        authorizations=authorizations,
    )


async def seed_task(
    env: Env,
    *,
    ordinal: int,
    extra: dict[str, Any] | None = None,
    status: str = "completed",
    run_id: str | None = None,
) -> str:
    """A persisted prior task exactly as spawn/续聊 writes it; binds current_task."""
    body: dict[str, Any] = {
        "text": f"task {ordinal}",
        "history": [],
        "profile_snapshot": env.profile.model_dump(mode="json"),
        "depth_limit": 1,
        "budget_key": f"lease-{ordinal}",
    }
    if extra:
        body.update(extra)
    task_id = str(uuid.uuid4())
    if run_id is None:
        run, _ = await env.store.begin(
            RunRequest(text=f"task {ordinal}", request_key=f"seed-run:{task_id}"),
            {"fixture": True},
        )
        run_id = run.id

    async def operation(conn: Any) -> None:
        await conn.execute(insert(subagent_tasks).values(
            id=task_id,
            agent_id=env.agent_id,
            tree_id=env.tree["tree_id"],
            ordinal=ordinal,
            status=status,
            run_id=run_id,
            parent_run_id=None,
            source_version=env.tree["input_version"],
            provider_version="v1",
            profile_version=fingerprint(env.profile.model_dump(mode="json")),
            input=body,
            input_hash=fingerprint(body),
            request_key=f"seed:{ordinal}@{env.agent_id}",
            request_hash=fingerprint({"seed": ordinal}),
            revision=1,
            input_closed=False,
            created_at=time.time(),
            finished_at=time.time() if status in TERMINAL else None,
        ))
        await conn.execute(
            update(subagent_members)
            .where(subagent_members.c.id == env.agent_id)
            .values(current_task_id=task_id)
        )

    async def confirm() -> None:
        return None

    await env.store.work_write(operation, confirm)
    return task_id


async def settle_task(env: Env, task_id: str) -> None:
    """Mark a follow-up task terminal so the next kind=task send is admitted."""

    async def operation(conn: Any) -> None:
        await conn.execute(
            update(subagent_tasks)
            .where(subagent_tasks.c.id == task_id)
            .values(status="completed", finished_at=time.time())
        )

    async def confirm() -> None:
        return None

    await env.store.work_write(operation, confirm)


async def host_followup(env: Env, *, request_key: str, text: str = "continue please") -> dict:
    receipt = await env.service.host_send_message(
        ACTOR,
        WORKSPACE,
        env.session.id,
        env.agent_id,
        text=text,
        new_task=True,
        wake="queue_only",
        request_key=request_key,
    )
    message = next(item for item in await env.service.repository.inbox(env.agent_id, after_seq=0)
                   if item["message_id"] == receipt["message_id"])
    return {**receipt, "task_id": message["task_id"]}


async def task_count(env: Env) -> int:
    return len(await env.service.repository.tasks(env.agent_id))


async def test_followup_task_inherits_isolated_workspace(tmp_path: Path) -> None:
    """续聊任务继承首任务的持久化 workspace/baseline；host 与模型入口一致。"""
    env = await build(tmp_path, SubagentProfileConfiguration(model="worker",
                                                             workspace="isolated_write"))
    workspace, baseline = await isolated_facts(env.agent_id)
    first = await seed_task(env, ordinal=1, extra={"workspace": workspace, "baseline": baseline})

    receipt = await host_followup(env, request_key="host-1")
    assert receipt["accepted"] is True
    followup = await env.service.repository.task(receipt["task_id"])
    assert receipt["task_id"] != first
    # 逐字继承同一隔离域事实：同目录、同基线，不重新切割也不降级为共享根。
    assert followup["input"]["workspace"] == workspace
    assert followup["input"]["baseline"] == baseline
    assert followup["input"]["workspace"]["directory"] == workspace_directory(env.agent_id)
    assert followup["input"]["workspace"]["agent_id"] == env.agent_id
    # 历史续聊与能力快照保持原有行为。
    assert "history" in followup["input"] and "profile_snapshot" in followup["input"]

    # 模型入口 send_message 走同一 _deliver，继承结果一致；先结算上一任务，
    # kind=task 准入要求当前任务已终结。
    await settle_task(env, receipt["task_id"])
    caller_member = await env.service.repository.get_agent(env.root_id)
    model_receipt = await env.service._deliver(
        Caller(caller_member, env.tree, SimpleNamespace(check=lambda: None)),
        SendAgentMessage(
            target="lead/w1", text="model continuation", wake="queue_only", new_task=True,
            task_id=None, send_key="model-1",
        ),
        parent_run_id=env.tree["root_run_id"],
    )
    model_task = await env.service.repository.task(model_receipt["task_id"])
    assert model_task["input"]["workspace"] == workspace
    assert model_task["input"]["baseline"] == baseline


async def test_inheritance_survives_legacy_followup_chain(tmp_path: Path) -> None:
    """冷恢复/历史链：中间续聊任务没有 workspace（修复前产生的旧数据），
    再续聊仍从首任务的域事实继承，不因最近任务缺键而降级。"""
    env = await build(tmp_path, SubagentProfileConfiguration(model="worker",
                                                             workspace="isolated_write"))
    workspace, baseline = await isolated_facts(env.agent_id)
    await seed_task(env, ordinal=1, extra={"workspace": workspace, "baseline": baseline})
    await seed_task(env, ordinal=2)

    receipt = await host_followup(env, request_key="host-legacy")
    followup = await env.service.repository.task(receipt["task_id"])
    assert followup["input"]["workspace"] == workspace
    assert followup["input"]["baseline"] == baseline


async def test_shared_profile_adds_no_workspace(tmp_path: Path) -> None:
    """shared_read_only worker 续聊不新增 workspace/baseline，即使旧任务带残留。"""
    env = await build(tmp_path, SubagentProfileConfiguration(model="worker"))
    workspace, baseline = await isolated_facts(env.agent_id)
    await seed_task(env, ordinal=1, extra={"workspace": workspace, "baseline": baseline})

    receipt = await host_followup(env, request_key="host-shared")
    followup = await env.service.repository.task(receipt["task_id"])
    assert "workspace" not in followup["input"]
    assert "baseline" not in followup["input"]
    assert "profile_snapshot" in followup["input"]


async def test_missing_workspace_fact_refuses_followup(tmp_path: Path) -> None:
    """隔离 profile 的事实缺失：拒绝续派，不静默降级为共享根目录。"""
    env = await build(tmp_path, SubagentProfileConfiguration(model="worker",
                                                             workspace="isolated_write"))
    await seed_task(env, ordinal=1)

    try:
        await host_followup(env, request_key="host-missing")
    except SubagentError as error:
        assert error.accepted is False
    else:
        raise AssertionError("follow-up without persisted workspace facts must be refused")
    assert await task_count(env) == 1


async def test_foreign_or_corrupt_facts_are_rejected(tmp_path: Path) -> None:
    """同 agent/同 project 校验承担真实约束：他人 workspace、异项目、坏事实
    一律拒绝，不继承也不降级。"""
    env = await build(tmp_path, SubagentProfileConfiguration(model="worker",
                                                             workspace="isolated_write"),
                      name="foreign.sqlite")
    # 事实属于另一个 agent。
    other, other_baseline = await isolated_facts("other-agent")
    await seed_task(env, ordinal=1, extra={"workspace": other, "baseline": other_baseline})
    try:
        await host_followup(env, request_key="host-foreign")
    except ServiceError as error:
        assert error.code == "facts_corrupt"
    else:
        raise AssertionError("another agent's workspace must not be inherited")
    assert await task_count(env) == 1

    # 同一 agent 但来源项目不同。
    env2 = await build(tmp_path, SubagentProfileConfiguration(model="worker",
                                                              workspace="isolated_write"),
                       name="foreign-project.sqlite")
    foreign, foreign_baseline = await isolated_facts(env2.agent_id, project="proj-2")
    await seed_task(env2, ordinal=1, extra={"workspace": foreign, "baseline": foreign_baseline})
    try:
        await host_followup(env2, request_key="host-foreign-project")
    except ServiceError as error:
        assert error.code == "facts_corrupt"
    else:
        raise AssertionError("a workspace cut from another project must not be inherited")
    assert await task_count(env2) == 1

    # 持久化事实损坏（无法按当前模型解析）。
    env3 = await build(tmp_path, SubagentProfileConfiguration(model="worker",
                                                              workspace="isolated_write"),
                       name="corrupt.sqlite")
    await seed_task(env3, ordinal=1, extra={"workspace": {"mode": 42}, "baseline": {}})
    try:
        await host_followup(env3, request_key="host-corrupt")
    except ServiceError as error:
        assert error.code == "facts_corrupt"
    else:
        raise AssertionError("corrupt persisted facts must not be silently dropped")
    assert await task_count(env3) == 1


async def test_send_key_replay_keeps_inherited_input(tmp_path: Path) -> None:
    """同 send_key 重放返回原回执：不重建任务、不改写已继承的输入。"""
    env = await build(tmp_path, SubagentProfileConfiguration(model="worker",
                                                             workspace="isolated_write"))
    workspace, baseline = await isolated_facts(env.agent_id)
    await seed_task(env, ordinal=1, extra={"workspace": workspace, "baseline": baseline})

    first = await host_followup(env, request_key="replay-1")
    retry = await host_followup(env, request_key="replay-1")
    assert retry["message_id"] == first["message_id"]
    assert retry["task_id"] == first["task_id"]
    assert await task_count(env) == 2
    stored = await env.service.repository.task(first["task_id"])
    assert stored["input"]["workspace"] == workspace
    assert stored["input"]["baseline"] == baseline
    inbox = await env.service.repository.inbox(env.agent_id, after_seq=0)
    assert len(inbox) == 1, "replay must not enqueue a duplicate message"


async def test_process_followup_delivers_workspace_document(tmp_path: Path) -> None:
    """process worker 续聊：任务文本同步携带 workspace 文档，运行前置检查可读。"""
    env = await build(
        tmp_path,
        SubagentProfileConfiguration(
            model="worker", provider="process", process="pv", workspace="isolated_write",
        ),
        processes={"pv": SubagentProcessConfiguration(argv=("echo", "hi"))},
    )
    workspace, baseline = await isolated_facts(env.agent_id)
    await seed_task(env, ordinal=1, extra={"workspace": workspace, "baseline": baseline})

    receipt = await host_followup(env, request_key="host-process", text="run again")
    followup = await env.service.repository.task(receipt["task_id"])
    assert followup["input"]["workspace"] == workspace
    assert followup["input"]["baseline"] == baseline
    document = parse_json(followup["input"]["text"])
    assert document["task"] == "run again"
    assert document["workspace"] == workspace
