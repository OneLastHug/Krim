"""共享任务板服务：依赖、归属、CAS 与证据，校验与变更在同一事务完成。

TeamService(store) 由宿主持有并传入 SQLiteStore；所有方法携带宿主解析的
tree_id + caller_id，事务内逐次重查树与成员：角色以数据库 role 为准，
模型不能通过调用参数伪装 root。任务板只改存储，绝不调用模型、发消息或
唤醒代理（需要行动时由宿主显式 send_message，见 plan §9.1）。

错误映射（复用已注册错误码，不改 errors.py/contracts.py）：
- ``revision_conflict``：expected_revision / expected_ownership_version 不匹配
  （并发迁移、转交后旧 owner 用旧归属版本完成）。
- ``subagent_task_closed``：任务状态不接受该迁移（重复 claim、终态再变更、
  blocked 上开始/完成、failed/cancelled 依赖未处理）。
- ``invalid_input``：依赖图无效（未知/跨树/自环/环）、依赖未满足、完成缺
  证据、空更新等请求前置不成立；ids/action 给出下一步。
- ``permission_denied``：caller 不是该树真实成员，或无 root/祖先协调权限。
- ``closed``：caller 成员已关闭，或转交目标成员已关闭。
- ``subagent_tree_closed``：树关闭后仍尝试修改（list 仍可读）。
- ``key_conflict``：同 request_key 重放但内容不同。
- ``not_found``：树、任务或转交目标不存在。

状态机：
- open → claimed：claim（CAS expected_revision；依赖全部 completed）。
- claimed → in_progress：owner 开始（owner 侧迁移需同时携带
  expected_ownership_version；依赖仍须全部 completed）。
- claimed/in_progress → completed：owner 或 root，需已有或随附
  artifact/check 证据；不存在任何"执行成功自动完成任务"的路径。
- claimed/in_progress → failed：owner 或 root；直系依赖它的非终态任务
  同事务置 blocked，归属不变（失败/离线不自动释放归属）。
- blocked：存在 failed/cancelled 依赖；只能 root 处理——替换依赖且新集合
  无 failed/cancelled 时自动回到 open，或显式 failed/cancelled。
- 任意非终态 → cancelled：root。终态不可再变更（root 仍可补验收证据）。

归属：claim 与 reassign 都使 ownership_version 递增；reassign 仅限 root
成员，或当前 owner 的祖先成员（subagent_members.parent_id 链即持久证据）。
转交保留全部旧证据行及其归属版本快照；任务板与委派 task/run 共用同一套
树、成员 ID，不维护第二套代理关系。
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field, JsonValue, field_validator
from sqlalchemy import delete, func, insert, select, update

from krim_agent.contracts import ServiceError, StrictModel, check_json, fingerprint
from krim_agent.storage.models import runs
from krim_agent.storage.subagent_models import subagent_members, subagent_tasks, subagent_trees
from krim_agent.storage.subagent_team_models import (
    subagent_team_task_deps,
    subagent_team_task_evidence,
    subagent_team_tasks,
)
from krim_agent.subagents.errors import SubagentError
from krim_agent.subagents.types import KEY_PATTERN, MAX_KEY_LENGTH

if TYPE_CHECKING:
    from krim_agent.storage.sqlite import SQLiteStore

TeamTaskStatus = Literal[
    "open", "claimed", "in_progress", "blocked", "completed", "failed", "cancelled"
]

NON_TERMINAL_TEAM_STATUS: frozenset[str] = frozenset(
    {"open", "claimed", "in_progress", "blocked"}
)
FAILED_TEAM_STATUS: frozenset[str] = frozenset({"failed", "cancelled"})
TERMINAL_TEAM_STATUS: frozenset[str] = frozenset({"completed", "failed", "cancelled"})

MAX_GOAL_LENGTH = 4000
MAX_DEPENDENCIES = 32
MAX_EVIDENCE_PER_CALL = 8
MAX_EVIDENCE_BODY_BYTES = 65536
MAX_CHAIN_DEPTH = 64


class TeamEvidence(StrictModel):
    """随 update 附着的产物或检查结果；request_key 承担重放幂等。"""

    kind: Literal["artifact", "check"]
    body: dict[str, JsonValue]
    request_key: str = Field(min_length=1, max_length=MAX_KEY_LENGTH, pattern=KEY_PATTERN)


class TeamTaskCreate(StrictModel):
    """创建板面工作项；依赖可为未完成任务，failed 依赖使任务直接 blocked。"""

    tree_id: str
    caller_id: str
    goal: str = Field(min_length=1, max_length=MAX_GOAL_LENGTH)
    dependencies: tuple[str, ...] = Field(default=(), max_length=MAX_DEPENDENCIES)
    source_task_id: str | None = None
    source_run_id: str | None = None
    request_key: str = Field(min_length=1, max_length=MAX_KEY_LENGTH, pattern=KEY_PATTERN)

    @field_validator("goal")
    @classmethod
    def meaningful(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("目标为空")
        value.encode("utf-8")
        return value


class TeamTaskQuery(StrictModel):
    """列出本树任务板；caller 必须是真实成员（树关闭后仍可读）。"""

    tree_id: str
    caller_id: str
    owner_id: str | None = None
    status: TeamTaskStatus | None = None


class TeamTaskClaim(StrictModel):
    """领取 open 任务；expected_revision 承担重复领取竞态。"""

    tree_id: str
    task_id: str
    caller_id: str
    expected_revision: int = Field(ge=1)


class TeamTaskUpdate(StrictModel):
    """板面变更：状态迁移、目标/依赖修订与证据附着；至少携带一项。"""

    tree_id: str
    task_id: str
    caller_id: str
    expected_revision: int = Field(ge=1)
    expected_ownership_version: int | None = Field(default=None, ge=1)
    status: TeamTaskStatus | None = None
    goal: str | None = Field(default=None, min_length=1, max_length=MAX_GOAL_LENGTH)
    dependencies: tuple[str, ...] | None = Field(default=None, max_length=MAX_DEPENDENCIES)
    evidence: tuple[TeamEvidence, ...] | None = Field(
        default=None, max_length=MAX_EVIDENCE_PER_CALL
    )

    @field_validator("goal")
    @classmethod
    def meaningful(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("目标为空")
        value.encode("utf-8")
        return value


class TeamTaskReassign(StrictModel):
    """协调者转交：new_owner_id=None 表示退回任务板（open）。"""

    tree_id: str
    task_id: str
    caller_id: str
    expected_revision: int = Field(ge=1)
    new_owner_id: str | None = None
    expected_ownership_version: int | None = Field(default=None, ge=1)


async def _context(
    conn: Any, tree_id: str, caller_id: str, *, mutation: bool
) -> dict[str, Any]:
    """逐次事务校验：树存在、（变更时）树开放、caller 是该树真实活跃成员。"""
    tree = (
        (await conn.execute(select(subagent_trees).where(subagent_trees.c.id == tree_id)))
        .mappings()
        .first()
    )
    if tree is None:
        raise ServiceError("not_found", 404)
    if mutation and tree["status"] != "open":
        raise SubagentError(
            "subagent_tree_closed",
            ids={"tree_id": tree_id},
            action="the tree is closed; only reads remain",
        )
    member = (
        (await conn.execute(select(subagent_members).where(subagent_members.c.id == caller_id)))
        .mappings()
        .first()
    )
    if member is None or member["tree_id"] != tree_id:
        raise SubagentError(
            "permission_denied",
            ids={"tree_id": tree_id},
            action="the caller must be an active member of this tree",
        )
    if member["status"] != "active":
        raise SubagentError(
            "closed",
            ids={"agent_id": caller_id},
            action="the calling member is closed",
        )
    return {"tree": dict(tree), "member": dict(member)}


async def _task(conn: Any, tree_id: str, task_id: str) -> Any:
    row = (
        (await conn.execute(select(subagent_team_tasks).where(subagent_team_tasks.c.id == task_id)))
        .mappings()
        .first()
    )
    if row is None or row["tree_id"] != tree_id:
        raise ServiceError("not_found", 404)
    return row


async def _statuses(conn: Any, task_ids: list[str]) -> dict[str, str]:
    if not task_ids:
        return {}
    rows = (
        await conn.execute(
            select(subagent_team_tasks.c.id, subagent_team_tasks.c.status).where(
                subagent_team_tasks.c.id.in_(task_ids)
            )
        )
    ).all()
    return {row[0]: row[1] for row in rows}


async def _adjacency(conn: Any, tree_id: str) -> dict[str, set[str]]:
    """本树全部依赖边（板面小，一次载入做环检测）。"""
    rows = (
        await conn.execute(
            select(subagent_team_task_deps.c.task_id, subagent_team_task_deps.c.depends_on_id)
            .join(
                subagent_team_tasks,
                subagent_team_tasks.c.id == subagent_team_task_deps.c.task_id,
            )
            .where(subagent_team_tasks.c.tree_id == tree_id)
        )
    ).all()
    edges: dict[str, set[str]] = {}
    for task_id, depends_on_id in rows:
        edges.setdefault(task_id, set()).add(depends_on_id)
    return edges


def _creates_cycle(adjacency: dict[str, set[str]], task_id: str, dep_ids: list[str]) -> bool:
    """新边只从 task 出发：沿依赖边能否回到 task。自环也覆盖。"""
    if task_id in dep_ids:
        return True
    seen: set[str] = set()
    stack = list(dep_ids)
    while stack:
        current = stack.pop()
        if current == task_id:
            return True
        if current in seen:
            continue
        seen.add(current)
        stack.extend(adjacency.get(current, ()))
    return False


async def _validated_dependencies(
    conn: Any, tree_id: str, task_id: str, dep_ids: list[str]
) -> list[str]:
    """依赖必须存在、同树、无重复、无（自）环；返回去重后的有序列表。"""
    if not dep_ids:
        return []
    if len(set(dep_ids)) != len(dep_ids):
        raise SubagentError(
            "invalid_input",
            ids={"task_id": task_id},
            action="remove duplicate dependencies",
        )
    found = {
        row[0]: row[1]
        for row in (
            await conn.execute(
                select(subagent_team_tasks.c.id, subagent_team_tasks.c.tree_id).where(
                    subagent_team_tasks.c.id.in_(dep_ids)
                )
            )
        ).all()
    }
    for dep_id in dep_ids:
        if found.get(dep_id) != tree_id:
            raise SubagentError(
                "invalid_input",
                ids={"task_id": task_id, "dependency_id": dep_id},
                action="dependencies must reference tasks of the same tree",
            )
    adjacency = await _adjacency(conn, tree_id)
    adjacency[task_id] = set(dep_ids)
    if _creates_cycle(adjacency, task_id, dep_ids):
        raise SubagentError(
            "invalid_input",
            ids={"task_id": task_id},
            action="remove the dependency cycle",
        )
    return dep_ids


async def _require_runnable(conn: Any, task_id: str) -> None:
    """claim/start/complete 前置：依赖全部 completed。"""
    dep_ids = [
        row[0]
        for row in (
            await conn.execute(
                select(subagent_team_task_deps.c.depends_on_id).where(
                    subagent_team_task_deps.c.task_id == task_id
                )
            )
        ).all()
    ]
    states = await _statuses(conn, dep_ids)
    failed = sorted(i for i, s in states.items() if s in FAILED_TEAM_STATUS)
    if failed:
        raise SubagentError(
            "subagent_task_closed",
            ids={"task_id": task_id},
            action="a dependency has failed; the task is blocked for the coordinator",
        )
    unmet = sorted(i for i, s in states.items() if s in NON_TERMINAL_TEAM_STATUS)
    if unmet:
        raise SubagentError(
            "invalid_input",
            ids={"task_id": task_id, "unmet": ",".join(unmet)},
            action="wait_dependencies",
        )


def _revision_conflict(task_id: str) -> SubagentError:
    return SubagentError(
        "revision_conflict",
        ids={"task_id": task_id},
        action="reload the board and retry with the current revision",
    )


def _validate_transition(
    from_status: str,
    to_status: str,
    *,
    root: bool,
    is_owner: bool,
    deps_changing: bool,
    task_id: str,
) -> None:
    """校验一次显式状态迁移的合法性与调用者权限。"""
    if to_status == from_status:
        # Even a no-op request must not let another active tree member impersonate
        # the owner or coordinator for an otherwise privileged state.
        if to_status == "in_progress" and not is_owner:
            raise SubagentError(
                "permission_denied",
                ids={"task_id": task_id},
                action="only the owner starts the task",
            )
        if to_status in {"completed", "failed"} and not (is_owner or root):
            raise SubagentError(
                "permission_denied",
                ids={"task_id": task_id},
                action="only the owner or the coordinator changes this task",
            )
        if to_status == "cancelled" and not root:
            raise SubagentError(
                "permission_denied",
                ids={"task_id": task_id},
                action="only the coordinator cancels tasks",
            )
        if to_status == "open" and not root:
            raise SubagentError(
                "permission_denied",
                ids={"task_id": task_id},
                action="only the coordinator reopens tasks",
            )
        if to_status == "blocked":
            raise SubagentError(
                "invalid_input",
                ids={"task_id": task_id},
                action="blocked is derived from failed dependencies",
            )
        return  # authorized no-op
    if from_status in TERMINAL_TEAM_STATUS:
        raise SubagentError(
            "subagent_task_closed",
            ids={"task_id": task_id},
            action="terminal tasks cannot change status",
        )
    if to_status == "open":
        if from_status != "blocked" or not root:
            raise SubagentError(
                "subagent_task_closed" if from_status != "blocked" else "permission_denied",
                ids={"task_id": task_id},
                action="only the coordinator reopens blocked tasks",
            )
        if not deps_changing:
            raise SubagentError(
                "invalid_input",
                ids={"task_id": task_id},
                action="replace dependencies to reopen a blocked task",
            )
    elif to_status == "claimed":
        raise SubagentError(
            "invalid_input",
            ids={"task_id": task_id},
            action="use the claim operation to take an open task",
        )
    elif to_status == "in_progress":
        if not is_owner:
            raise SubagentError(
                "permission_denied",
                ids={"task_id": task_id},
                action="only the owner starts the task",
            )
        if from_status != "claimed":
            raise SubagentError(
                "subagent_task_closed",
                ids={"task_id": task_id},
                action="only claimed tasks can be started",
            )
    elif to_status == "completed":
        if not (is_owner or root):
            raise SubagentError(
                "permission_denied",
                ids={"task_id": task_id},
                action="only the owner or the coordinator completes the task",
            )
        if from_status not in {"claimed", "in_progress"}:
            raise SubagentError(
                "subagent_task_closed",
                ids={"task_id": task_id},
                action="only claimed or in-progress tasks can be completed",
            )
    elif to_status == "failed":
        if from_status == "blocked":
            if not root:
                raise SubagentError(
                    "permission_denied",
                    ids={"task_id": task_id},
                    action="only the coordinator abandons a blocked task",
                )
        elif not (is_owner or root):
            raise SubagentError(
                "permission_denied",
                ids={"task_id": task_id},
                action="only the owner or the coordinator fails the task",
            )
        elif from_status not in {"claimed", "in_progress", "open"}:
            raise SubagentError(
                "subagent_task_closed",
                ids={"task_id": task_id},
                action="the task status does not allow failing",
            )
    elif to_status == "cancelled":
        if not root:
            raise SubagentError(
                "permission_denied",
                ids={"task_id": task_id},
                action="only the coordinator cancels tasks",
            )
    else:  # blocked 不接受显式设置：由依赖失败同事务产生
        raise SubagentError(
            "invalid_input",
            ids={"task_id": task_id},
            action="blocked is derived from failed dependencies, not set directly",
        )


async def _evidence_view(conn: Any, task_id: str) -> list[dict[str, Any]]:
    rows = (
        (
            await conn.execute(
                select(subagent_team_task_evidence)
                .where(subagent_team_task_evidence.c.task_id == task_id)
                .order_by(subagent_team_task_evidence.c.created_at,
                          subagent_team_task_evidence.c.id)
            )
        )
        .mappings()
        .all()
    )
    return [
        {
            "id": row["id"],
            "kind": row["kind"],
            "agent_id": row["agent_id"],
            "body": row["body"],
            "request_key": row["request_key"],
            "ownership_version": row["ownership_version"],
            "revision": row["revision"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]


async def _dependencies_view(conn: Any, task_id: str) -> list[dict[str, Any]]:
    rows = (
        (
            await conn.execute(
                select(
                    subagent_team_task_deps.c.depends_on_id,
                    subagent_team_task_deps.c.created_at,
                )
                .where(subagent_team_task_deps.c.task_id == task_id)
                .order_by(subagent_team_task_deps.c.created_at,
                          subagent_team_task_deps.c.depends_on_id)
            )
        )
        .all()
    )
    states = await _statuses(conn, [row[0] for row in rows])
    return [{"task_id": dep_id, "status": states.get(dep_id)} for dep_id, _ in rows]


async def _task_view(conn: Any, row: Any) -> dict[str, Any]:
    return {
        "id": row["id"],
        "tree_id": row["tree_id"],
        "ordinal": row["ordinal"],
        "goal": row["goal"],
        "status": row["status"],
        "owner_id": row["owner_id"],
        "created_by": row["created_by"],
        "source_task_id": row["source_task_id"],
        "source_run_id": row["source_run_id"],
        "revision": row["revision"],
        "ownership_version": row["ownership_version"],
        "request_key": row["request_key"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "completed_at": row["completed_at"],
        "dependencies": await _dependencies_view(conn, row["id"]),
        "evidence": await _evidence_view(conn, row["id"]),
    }


async def _is_ancestor(conn: Any, ancestor_id: str, descendant_id: str) -> bool:
    """父协调权限的证据：subagent_members.parent_id 链上确有该祖先。"""
    current: str | None = descendant_id
    for _ in range(MAX_CHAIN_DEPTH):
        if current is None:
            return False
        parent = (
            await conn.scalar(
                select(subagent_members.c.parent_id).where(subagent_members.c.id == current)
            )
        )
        if parent is None:
            return False
        if parent == ancestor_id:
            return True
        current = parent
    return False


async def _confirm_create(
    store: SQLiteStore, request: TeamTaskCreate, digest: str
) -> dict[str, Any] | None:
    """提交回程丢失时按原 request_key 核对：内容一致即返回原任务。"""

    async def probe(conn: Any) -> dict[str, Any] | None:
        row = (
            (
                await conn.execute(
                    select(subagent_team_tasks).where(
                        subagent_team_tasks.c.tree_id == request.tree_id,
                        subagent_team_tasks.c.request_key == request.request_key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None or row["request_hash"] != digest:
            return None
        return await _task_view(conn, row)

    return await store.work_read(probe)


async def _confirm_claim(store: SQLiteStore, request: TeamTaskClaim) -> dict[str, Any] | None:
    async def probe(conn: Any) -> dict[str, Any] | None:
        row = (
            (
                await conn.execute(
                    select(subagent_team_tasks).where(
                        subagent_team_tasks.c.id == request.task_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if (
            row is None
            or row["owner_id"] != request.caller_id
            or row["revision"] != request.expected_revision + 1
        ):
            return None
        return await _task_view(conn, row)

    return await store.work_read(probe)


async def _confirm_update(
    store: SQLiteStore, request: TeamTaskUpdate, outcome: dict[str, Any]
) -> dict[str, Any] | None:
    async def probe(conn: Any) -> dict[str, Any] | None:
        row = (
            (
                await conn.execute(
                    select(subagent_team_tasks).where(
                        subagent_team_tasks.c.id == request.task_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None or row["revision"] != outcome.get("revision"):
            return None
        if row["status"] != outcome.get("status"):
            return None
        return await _task_view(conn, row)

    return await store.work_read(probe)


async def _confirm_reassign(
    store: SQLiteStore, request: TeamTaskReassign
) -> dict[str, Any] | None:
    async def probe(conn: Any) -> dict[str, Any] | None:
        row = (
            (
                await conn.execute(
                    select(subagent_team_tasks).where(
                        subagent_team_tasks.c.id == request.task_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None or row["revision"] != request.expected_revision + 1:
            return None
        if request.new_owner_id is not None and row["owner_id"] != request.new_owner_id:
            return None
        return await _task_view(conn, row)

    return await store.work_read(probe)


class TeamService:
    """宿主侧任务板 API：create / list / claim / update / reassign。"""

    def __init__(self, store: SQLiteStore) -> None:
        self._store = store

    async def create(self, request: TeamTaskCreate) -> dict[str, Any]:
        digest = fingerprint(
            {
                "goal": request.goal,
                "dependencies": list(request.dependencies),
                "source_task_id": request.source_task_id,
                "source_run_id": request.source_run_id,
            }
        )

        async def operation(conn: Any) -> dict[str, Any]:
            await _context(conn, request.tree_id, request.caller_id, mutation=True)
            found = (
                (
                    await conn.execute(
                        select(subagent_team_tasks).where(
                            subagent_team_tasks.c.tree_id == request.tree_id,
                            subagent_team_tasks.c.request_key == request.request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if found is not None:
                if found["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                return await _task_view(conn, found)
            # 来源引用必须真实存在：委派任务须属同一棵树，run 须已落库。
            if request.source_task_id is not None:
                source_tree = await conn.scalar(
                    select(subagent_tasks.c.tree_id).where(
                        subagent_tasks.c.id == request.source_task_id
                    )
                )
                if source_tree != request.tree_id:
                    raise SubagentError(
                        "invalid_input",
                        ids={"task_id": request.source_task_id},
                        action="the source task must belong to this tree",
                    )
            if request.source_run_id is not None:
                found_run = (
                    await conn.scalar(
                        select(runs.c.id).where(runs.c.id == request.source_run_id)
                    )
                )
                if found_run is None:
                    raise SubagentError(
                        "invalid_input",
                        ids={"run_id": request.source_run_id},
                        action="the source run does not exist",
                    )
            task_id = str(uuid.uuid4())
            dep_ids = await _validated_dependencies(
                conn, request.tree_id, task_id, list(request.dependencies)
            )
            states = await _statuses(conn, dep_ids)
            now = time.time()
            ordinal = (
                await conn.scalar(
                    select(func.max(subagent_team_tasks.c.ordinal)).where(
                        subagent_team_tasks.c.tree_id == request.tree_id
                    )
                )
                or 0
            ) + 1
            status = (
                "blocked"
                if any(states[dep] in FAILED_TEAM_STATUS for dep in dep_ids)
                else "open"
            )
            await conn.execute(
                insert(subagent_team_tasks).values(
                    id=task_id,
                    tree_id=request.tree_id,
                    ordinal=ordinal,
                    goal=request.goal,
                    status=status,
                    owner_id=None,
                    created_by=request.caller_id,
                    source_task_id=request.source_task_id,
                    source_run_id=request.source_run_id,
                    revision=1,
                    ownership_version=1,
                    request_key=request.request_key,
                    request_hash=digest,
                    created_at=now,
                    updated_at=now,
                    completed_at=None,
                )
            )
            for dep_id in dep_ids:
                await conn.execute(
                    insert(subagent_team_task_deps).values(
                        task_id=task_id, depends_on_id=dep_id, created_at=now
                    )
                )
            row = (
                (
                    await conn.execute(
                        select(subagent_team_tasks).where(subagent_team_tasks.c.id == task_id)
                    )
                )
                .mappings()
                .one()
            )
            return await _task_view(conn, row)

        return await self._store.work_write(
            operation,
            lambda: _confirm_create(self._store, request, digest),
        )

    async def list(self, request: TeamTaskQuery) -> list[dict[str, Any]]:
        async def operation(conn: Any) -> list[dict[str, Any]]:
            await _context(conn, request.tree_id, request.caller_id, mutation=False)
            filters = [subagent_team_tasks.c.tree_id == request.tree_id]
            if request.owner_id is not None:
                filters.append(subagent_team_tasks.c.owner_id == request.owner_id)
            if request.status is not None:
                filters.append(subagent_team_tasks.c.status == request.status)
            rows = (
                (
                    await conn.execute(
                        select(subagent_team_tasks)
                        .where(*filters)
                        .order_by(subagent_team_tasks.c.ordinal)
                    )
                )
                .mappings()
                .all()
            )
            return [await _task_view(conn, row) for row in rows]

        return await self._store.work_read(operation)

    async def claim(self, request: TeamTaskClaim) -> dict[str, Any]:
        async def operation(conn: Any) -> dict[str, Any]:
            await _context(conn, request.tree_id, request.caller_id, mutation=True)
            task = await _task(conn, request.tree_id, request.task_id)
            if task["revision"] != request.expected_revision:
                raise _revision_conflict(request.task_id)
            if task["status"] != "open":
                raise SubagentError(
                    "subagent_task_closed",
                    ids={"task_id": request.task_id, "status": task["status"]},
                    action="only open tasks can be claimed",
                )
            await _require_runnable(conn, request.task_id)
            now = time.time()
            await conn.execute(
                update(subagent_team_tasks)
                .where(
                    subagent_team_tasks.c.id == request.task_id,
                    subagent_team_tasks.c.revision == request.expected_revision,
                )
                .values(
                    owner_id=request.caller_id,
                    status="claimed",
                    revision=task["revision"] + 1,
                    ownership_version=task["ownership_version"] + 1,
                    updated_at=now,
                )
            )
            row = (
                (
                    await conn.execute(
                        select(subagent_team_tasks).where(
                            subagent_team_tasks.c.id == request.task_id
                        )
                    )
                )
                .mappings()
                .one()
            )
            return await _task_view(conn, row)

        return await self._store.work_write(operation, lambda: _confirm_claim(self._store, request))

    async def update(self, request: TeamTaskUpdate) -> dict[str, Any]:
        if (
            request.status is None
            and request.goal is None
            and request.dependencies is None
            and not request.evidence
        ):
            raise SubagentError(
                "invalid_input",
                ids={"task_id": request.task_id},
                action="provide a status, goal, dependency or evidence change",
            )
        for item in request.evidence or ():
            try:
                check_json(item.body, max_bytes=MAX_EVIDENCE_BODY_BYTES)
            except ValueError:
                raise ServiceError("invalid_json") from None
        outcome: dict[str, Any] = {}

        async def operation(conn: Any) -> dict[str, Any]:
            context = await _context(conn, request.tree_id, request.caller_id, mutation=True)
            task = await _task(conn, request.tree_id, request.task_id)
            root = context["member"]["role"] == "root"
            is_owner = task["owner_id"] == request.caller_id
            terminal = task["status"] in TERMINAL_TEAM_STATUS

            # Authenticated tree members can observe the board, so return stale CAS
            # before role-specific write checks; this also fences old owners after
            # reassignment using both the board and ownership version.
            if task["revision"] != request.expected_revision:
                raise _revision_conflict(request.task_id)
            if (
                request.expected_ownership_version is not None
                and task["ownership_version"] != request.expected_ownership_version
            ):
                raise _revision_conflict(request.task_id)

            # 权限校验仍在读取或变更非公开细节之前完成。
            if request.goal is not None or request.dependencies is not None:
                if not root:
                    raise SubagentError(
                        "permission_denied",
                        ids={"task_id": request.task_id},
                        action="only the coordinator edits goal or dependencies",
                    )
                if terminal:
                    raise SubagentError(
                        "subagent_task_closed",
                        ids={"task_id": request.task_id},
                        action="terminal tasks cannot be re-planned",
                    )
            if request.evidence and not (root or (is_owner and not terminal)):
                raise SubagentError(
                    "subagent_task_closed" if terminal else "permission_denied",
                    ids={"task_id": request.task_id},
                    action=(
                        "completed tasks accept coordinator evidence only"
                        if terminal
                        else "only the owner or the coordinator attaches evidence"
                    ),
                )

            auto_status: str | None = None
            if request.dependencies is not None:
                dep_ids = await _validated_dependencies(
                    conn, request.tree_id, request.task_id, list(request.dependencies)
                )
                states = await _statuses(conn, dep_ids)
                dirty = any(states[dep] in FAILED_TEAM_STATUS for dep in dep_ids)
                if dirty and not terminal:
                    auto_status = "blocked"
                elif not dirty and task["status"] == "blocked":
                    auto_status = "open"

            if request.status is not None:
                _validate_transition(
                    auto_status or task["status"],
                    request.status,
                    root=root,
                    is_owner=is_owner,
                    deps_changing=request.dependencies is not None,
                    task_id=request.task_id,
                )
                final_status: str = request.status
            elif auto_status is not None:
                final_status = auto_status
            else:
                final_status = task["status"]

            if final_status == "completed" and task["status"] != "completed":
                existing = await conn.scalar(
                    select(func.count())
                    .select_from(subagent_team_task_evidence)
                    .where(subagent_team_task_evidence.c.task_id == request.task_id)
                )
                if (existing or 0) + len(request.evidence or ()) < 1:
                    raise SubagentError(
                        "invalid_input",
                        ids={"task_id": request.task_id},
                        action="attach an artifact or check result to complete",
                    )
            if final_status in {"in_progress", "completed"} and task["status"] != final_status:
                await _require_runnable(conn, request.task_id)

            touches_state = request.status is not None or bool(request.evidence)
            if touches_state and request.expected_ownership_version is None:
                raise SubagentError(
                    "invalid_input",
                    ids={"task_id": request.task_id},
                    action="owner-side changes must carry expected_ownership_version",
                )
            if (
                request.expected_ownership_version is not None
                and task["ownership_version"] != request.expected_ownership_version
            ):
                raise _revision_conflict(request.task_id)
            if task["revision"] != request.expected_revision:
                raise _revision_conflict(request.task_id)

            changed = (
                request.goal is not None
                or request.dependencies is not None
                or bool(request.evidence)
                or final_status != task["status"]
            )
            new_revision = task["revision"] + 1 if changed else task["revision"]
            outcome["revision"] = new_revision
            outcome["status"] = final_status
            now = time.time()
            if changed:
                values: dict[str, Any] = {"revision": new_revision, "updated_at": now}
                if request.goal is not None:
                    values["goal"] = request.goal
                if final_status != task["status"]:
                    values["status"] = final_status
                    if final_status == "completed":
                        values["completed_at"] = now
                await conn.execute(
                    update(subagent_team_tasks)
                    .where(subagent_team_tasks.c.id == request.task_id)
                    .values(**values)
                )
            if request.dependencies is not None:
                await conn.execute(
                    delete(subagent_team_task_deps).where(
                        subagent_team_task_deps.c.task_id == request.task_id
                    )
                )
                for dep_id in request.dependencies:
                    await conn.execute(
                        insert(subagent_team_task_deps).values(
                            task_id=request.task_id, depends_on_id=dep_id, created_at=now
                        )
                    )
            for item in request.evidence or ():
                item_digest = fingerprint({"kind": item.kind, "body": item.body})
                replay = (
                    await conn.scalar(
                        select(subagent_team_task_evidence.c.request_hash).where(
                            subagent_team_task_evidence.c.task_id == request.task_id,
                            subagent_team_task_evidence.c.request_key == item.request_key,
                        )
                    )
                )
                if replay is not None:
                    if replay != item_digest:
                        raise ServiceError("key_conflict")
                    continue
                await conn.execute(
                    insert(subagent_team_task_evidence).values(
                        id=str(uuid.uuid4()),
                        task_id=request.task_id,
                        agent_id=request.caller_id,
                        kind=item.kind,
                        body=item.body,
                        request_key=item.request_key,
                        request_hash=item_digest,
                        ownership_version=task["ownership_version"],
                        revision=task["revision"],
                        created_at=now,
                    )
                )
            if (
                final_status in FAILED_TEAM_STATUS
                and task["status"] not in FAILED_TEAM_STATUS
            ):
                dependents = (
                    (
                        await conn.execute(
                            select(subagent_team_task_deps.c.task_id).where(
                                subagent_team_task_deps.c.depends_on_id == request.task_id
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                for dependent_id in dependents:
                    dependent = (
                        (
                            await conn.execute(
                                select(subagent_team_tasks).where(
                                    subagent_team_tasks.c.id == dependent_id
                                )
                            )
                        )
                        .mappings()
                        .first()
                    )
                    if (
                        dependent is None
                        or dependent["status"] not in NON_TERMINAL_TEAM_STATUS
                        or dependent["status"] == "blocked"
                    ):
                        continue
                    await conn.execute(
                        update(subagent_team_tasks)
                        .where(subagent_team_tasks.c.id == dependent_id)
                        .values(
                            status="blocked",
                            revision=dependent["revision"] + 1,
                            updated_at=now,
                        )
                    )
            row = (
                (
                    await conn.execute(
                        select(subagent_team_tasks).where(
                            subagent_team_tasks.c.id == request.task_id
                        )
                    )
                )
                .mappings()
                .one()
            )
            return await _task_view(conn, row)

        return await self._store.work_write(
            operation, lambda: _confirm_update(self._store, request, outcome)
        )

    async def reassign(self, request: TeamTaskReassign) -> dict[str, Any]:
        async def operation(conn: Any) -> dict[str, Any]:
            context = await _context(conn, request.tree_id, request.caller_id, mutation=True)
            task = await _task(conn, request.tree_id, request.task_id)
            root = context["member"]["role"] == "root"
            owner_id = task["owner_id"]
            # 权限先于状态与版本核对：无权调用者得不到板面状态信息。
            if not root and not (
                owner_id is not None
                and await _is_ancestor(conn, request.caller_id, owner_id)
            ):
                raise SubagentError(
                    "permission_denied",
                    ids={"task_id": request.task_id},
                    action="only the root coordinator or an ancestor of the owner reassigns",
                )
            if task["revision"] != request.expected_revision:
                raise _revision_conflict(request.task_id)
            if task["status"] in TERMINAL_TEAM_STATUS:
                raise SubagentError(
                    "subagent_task_closed",
                    ids={"task_id": request.task_id},
                    action="terminal tasks cannot be reassigned",
                )
            if request.new_owner_id is None and owner_id is None:
                raise SubagentError(
                    "invalid_input",
                    ids={"task_id": request.task_id},
                    action="the task is already unassigned",
                )
            if owner_id is not None and request.expected_ownership_version is None:
                raise SubagentError(
                    "invalid_input",
                    ids={"task_id": request.task_id},
                    action="reassigning an owned task must carry expected_ownership_version",
                )
            if (
                request.expected_ownership_version is not None
                and task["ownership_version"] != request.expected_ownership_version
            ):
                raise _revision_conflict(request.task_id)
            new_owner = request.new_owner_id
            if new_owner is not None:
                if new_owner == owner_id:
                    raise SubagentError(
                        "invalid_input",
                        ids={"task_id": request.task_id},
                        action="the task already belongs to that member",
                    )
                target = (
                    (
                        await conn.execute(
                            select(subagent_members).where(subagent_members.c.id == new_owner)
                        )
                    )
                    .mappings()
                    .first()
                )
                if target is None:
                    raise ServiceError("not_found", 404)
                if target["tree_id"] != request.tree_id:
                    raise SubagentError(
                        "invalid_input",
                        ids={"task_id": request.task_id, "agent_id": new_owner},
                        action="the target member must belong to this tree",
                    )
                if target["status"] != "active":
                    raise SubagentError(
                        "closed",
                        ids={"agent_id": new_owner},
                        action="reassign to an active member",
                    )
            now = time.time()
            # 归属变化不解除依赖阻塞：blocked 任务转交后仍 blocked。
            status = (
                "blocked"
                if task["status"] == "blocked"
                else ("claimed" if new_owner is not None else "open")
            )
            await conn.execute(
                update(subagent_team_tasks)
                .where(subagent_team_tasks.c.id == request.task_id)
                .values(
                    owner_id=new_owner,
                    status=status,
                    revision=task["revision"] + 1,
                    ownership_version=task["ownership_version"] + 1,
                    updated_at=now,
                )
            )
            row = (
                (
                    await conn.execute(
                        select(subagent_team_tasks).where(
                            subagent_team_tasks.c.id == request.task_id
                        )
                    )
                )
                .mappings()
                .one()
            )
            return await _task_view(conn, row)

        return await self._store.work_write(
            operation, lambda: _confirm_reassign(self._store, request)
        )
