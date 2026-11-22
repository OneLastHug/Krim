"""子代理共享模型请求预算（计划 8.3）：树级持久预算、子租约划拨与计费台账。

预算只计真实模型请求次数；不伪造 usage/token，原始用量仍由既有 run 事件唯一
记账。所有写路径走 store.work_write（COMMIT 回程丢失只按稳定 attempt_id/lease
key 核实已提交事实，未确认则全 store 不健康，返回 storage_unavailable）；读路
径走 store.work_read。事务内不等待模型、沙箱或网络，父子不持 SQLite 锁等待。

语义：
- ensure：建立树预算（幂等）。initial_spent 计入旧根在首次 spawn 之前已有的
  model_start；limit=None 真正继承 unlimited，不发明商业配额。
- reserve：从根池或活跃父租约原子划拨子租约；同键同内容重放返回原租约，同键
  异内容 key_conflict。有限池默认划出约一半并为调用方保留至少 1 次，不足即时
  capacity（429），不等待；显式 requested 允许把池内剩余全部交给叶子调用方。
- charge：每次真实 ModelStep 恰好一次，attempt_id 独立（重新推理不按
  run/step 去重）；SDK 纯工具 replay 不经过此入口不计费。根用未预留余额，
  子用其租约余额；失败请求同样算已尝试（先 charge 后发起请求），排队超时
  根本不调用 charge。
- release：级联释放租约及其活跃后代；未用部分回到活跃父租约或根池，已花费
  不返还；幂等（重复 release 无变化）。
- state：树预算与活跃租约只读视图。

None 预算在整条链上真正继承 unlimited；租约 key 由调用方固化进 task.input，
冷恢复按 key 找回原租约，已花预算不重置。
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.storage.models import runs
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore
from krim_agent.storage.subagent_budget_models import (
    subagent_budget_charges,
    subagent_budget_leases,
    subagent_budgets,
)
from krim_agent.storage.subagent_models import subagent_trees
from krim_agent.subagents.errors import SubagentError

LEASE_ACTIVE = "active"
LEASE_RELEASED = "released"


def _check_key(key: str) -> str:
    if not isinstance(key, str) or not key or len(key) > 128:
        raise ServiceError("invalid_input", 422)
    return key


def _check_count(value: int) -> int:
    if type(value) is not int or value < 0:
        raise ServiceError("invalid_input", 422)
    return value


def _check_limit(limit: int | None) -> int | None:
    if limit is not None and (type(limit) is not int or limit < 0):
        raise ServiceError("invalid_input", 422)
    return limit


def _capacity(ids: dict[str, str], action: str) -> SubagentError:
    return SubagentError("capacity", accepted=False, ids=ids, action=action, status=429)


def _step_limit(ids: dict[str, str], action: str) -> SubagentError:
    return SubagentError("step_limit", accepted=False, ids=ids, action=action)


def _default_grant(pool: int, ids: dict[str, str]) -> int:
    """约一半可用且给调用方保留至少 1 次；池不足 2 时无法两全，即时拒绝。"""
    grant = min(pool // 2, pool - 1)
    if grant < 1:
        raise _capacity(ids, "可用预算不足以划出子租约：需同时给调用方保留至少 1 次")
    return grant


async def _tree_exists(conn: AsyncConnection, tree_id: str) -> None:
    found = await conn.scalar(select(subagent_trees.c.id).where(subagent_trees.c.id == tree_id))
    if found is None:
        raise ServiceError("not_found", 404)


async def _run_exists(conn: AsyncConnection, run_id: str) -> None:
    found = await conn.scalar(select(runs.c.id).where(runs.c.id == run_id))
    if found is None:
        raise ServiceError("not_found", 404)


async def _budget_opt(conn: AsyncConnection, tree_id: str) -> RowMapping | None:
    return (
        (await conn.execute(select(subagent_budgets).where(subagent_budgets.c.tree_id == tree_id)))
        .mappings()
        .first()
    )


async def _budget(conn: AsyncConnection, tree_id: str) -> RowMapping:
    row = await _budget_opt(conn, tree_id)
    if row is None:
        raise ServiceError("not_found", 404)
    return row


async def _lease_opt(conn: AsyncConnection, tree_id: str, key: str) -> RowMapping | None:
    return (
        (
            await conn.execute(
                select(subagent_budget_leases).where(
                    subagent_budget_leases.c.tree_id == tree_id,
                    subagent_budget_leases.c.key == key,
                )
            )
        )
        .mappings()
        .first()
    )


async def _charge_opt(conn: AsyncConnection, attempt_id: str) -> RowMapping | None:
    return (
        (
            await conn.execute(
                select(subagent_budget_charges).where(
                    subagent_budget_charges.c.attempt_id == attempt_id
                )
            )
        )
        .mappings()
        .first()
    )


async def _grant_sums(conn: AsyncConnection, tree_id: str) -> dict[str | None, int]:
    """活跃租约按父聚合的占位额；None 键为根池的直接子租约（outstanding）。"""
    rows = (
        await conn.execute(
            select(
                subagent_budget_leases.c.parent_key,
                func.coalesce(func.sum(subagent_budget_leases.c.request_limit), 0),
            )
            .where(
                subagent_budget_leases.c.tree_id == tree_id,
                subagent_budget_leases.c.status == LEASE_ACTIVE,
            )
            .group_by(subagent_budget_leases.c.parent_key)
        )
    ).all()
    return {row[0]: int(row[1]) for row in rows}


def _pool_remaining(limit: int | None, spent: int, children: int) -> int | None:
    return None if limit is None else limit - spent - children


def _tree_remaining(budget: RowMapping, sums: dict[str | None, int]) -> int | None:
    return _pool_remaining(budget["request_limit"], budget["spent"], sums.get(None, 0))


def _lease_remaining(lease: RowMapping, sums: dict[str | None, int]) -> int | None:
    return _pool_remaining(lease["request_limit"], lease["spent"], sums.get(lease["key"], 0))


def _charge_receipt(
    charge: RowMapping,
    budget: RowMapping,
    lease: RowMapping | None,
    sums: dict[str | None, int],
    *,
    created: bool,
) -> dict[str, Any]:
    tree_left = _tree_remaining(budget, sums)
    if lease is None:
        limit, spent, remaining = budget["request_limit"], budget["spent"], tree_left
    else:
        limit = lease["request_limit"]
        spent = lease["spent"]
        remaining = _lease_remaining(lease, sums)
    return {
        "attempt_id": charge["attempt_id"],
        "tree_id": charge["tree_id"],
        "run_id": charge["run_id"],
        "step_index": charge["step_index"],
        "lease_key": charge["lease_key"],
        "limit": limit,
        "spent": spent,
        "remaining": remaining,
        "tree_spent": budget["spent"],
        "tree_remaining": tree_left,
        "created": created,
    }


def _lease_receipt(
    budget: RowMapping, lease: RowMapping, sums: dict[str | None, int], *, created: bool
) -> dict[str, Any]:
    return {
        "key": lease["key"],
        "tree_id": lease["tree_id"],
        "parent_key": lease["parent_key"],
        "limit": lease["request_limit"],
        "spent": lease["spent"],
        "remaining": _lease_remaining(lease, sums),
        "tree_remaining": _tree_remaining(budget, sums),
        "created": created,
    }


class RequestBudgets:
    """树级共享请求预算仓库；方法返回 JSON 兼容 dict，可直接固化进事件。"""

    def __init__(self, store: SQLiteStore) -> None:
        if store.schema_version < SCHEMA_VERSION:
            raise ServiceError("schema_upgrade_required")
        self._store = store

    async def _submit(
        self,
        operation: Callable[[AsyncConnection], Awaitable[Any]],
        confirm: Callable[[], Awaitable[Any | None]],
    ) -> Any:
        store = self._store
        if store.closed:
            raise ServiceError("closed")
        if store.readonly:
            raise ServiceError("readonly")
        if not store.healthy:
            raise ServiceError("storage_unavailable", 503)
        return await store.work_write(operation, confirm)

    async def _query(self, operation: Callable[[AsyncConnection], Awaitable[Any]]) -> Any:
        return await self._store.work_read(operation)

    # ---------- 树预算 ----------

    async def ensure(
        self, tree_id: str, limit: int | None, initial_spent: int = 0
    ) -> dict[str, Any]:
        """建立树预算（幂等）；已存在时返回现状，不改 limit 与 spent。

        initial_spent 计入旧根首次 spawn 之前的真实请求；limit 是建立时的
        决定（调用方取 settings.max_steps 与 subagents.max_requests_per_tree
        的交集），允许 initial_spent 超过 limit：预算视为已经用尽。
        """
        _check_limit(limit)
        _check_count(initial_spent)
        now = time.time()

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            await _tree_exists(conn, tree_id)
            row = await _budget_opt(conn, tree_id)
            if row is None:
                await conn.execute(
                    insert(subagent_budgets).values(
                        tree_id=tree_id,
                        request_limit=limit,
                        spent=initial_spent,
                        created_at=now,
                        updated_at=now,
                    )
                )
                row = await _budget(conn, tree_id)
                created = True
            else:
                created = False
            sums = await _grant_sums(conn, tree_id)
            return {
                "tree_id": row["tree_id"],
                "limit": row["request_limit"],
                "spent": row["spent"],
                "remaining": _tree_remaining(row, sums),
                "created": created,
            }

        async def confirm() -> dict[str, Any] | None:
            async def read(conn: AsyncConnection) -> Any:
                row = await _budget_opt(conn, tree_id)
                if row is None:
                    return None
                sums = await _grant_sums(conn, tree_id)
                return {
                    "tree_id": row["tree_id"],
                    "limit": row["request_limit"],
                    "spent": row["spent"],
                    "remaining": _tree_remaining(row, sums),
                    "created": False,
                }

            return await self._query(read)

        return await self._submit(operation, confirm)

    # ---------- 租约划拨 ----------

    async def reserve(
        self, tree_id: str, key: str, requested: int | None, parent_key: str | None = None
    ) -> dict[str, Any]:
        """预留子租约；返回 {'key','limit','remaining',...}。

        有限池默认划出约一半并为调用方保留至少 1 次；requested 显式给出时
        允许取走池内全部剩余（叶子调用方接管预算）。不足即时 capacity(429)，
        不等待。父租约必须处于 active；已释放租约不再出借。None 预算继承
        unlimited：requested=None 的子租约同样不限额。
        """
        key = _check_key(key)
        if parent_key is not None:
            _check_key(parent_key)
        if requested is not None and (type(requested) is not int or requested < 1):
            raise ServiceError("invalid_input", 422)
        ids = {"tree_id": tree_id, "key": key}

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            await _tree_exists(conn, tree_id)
            budget = await _budget(conn, tree_id)
            existing = await _lease_opt(conn, tree_id, key)
            if existing is not None:
                if existing["requested"] != requested or existing["parent_key"] != parent_key:
                    raise ServiceError("key_conflict")
                sums = await _grant_sums(conn, tree_id)
                return _lease_receipt(budget, existing, sums, created=False)
            parent: RowMapping | None = None
            if parent_key is not None:
                parent = await _lease_opt(conn, tree_id, parent_key)
                if parent is None:
                    raise ServiceError("not_found", 404)
                if parent["status"] != LEASE_ACTIVE:
                    raise _capacity(
                        ids | {"parent_key": parent_key}, "父租约已释放；不再出借子租约"
                    )
            sums = await _grant_sums(conn, tree_id)
            if parent is None:
                pool = _tree_remaining(budget, sums)
            else:
                pool = _lease_remaining(parent, sums)
            if pool is not None:
                grant = requested if requested is not None else _default_grant(pool, ids)
                if grant > pool:
                    raise _capacity(
                        ids,
                        f"请求 {grant} 次超出可用 {pool} 次；缩小 requested 或先释放未用租约",
                    )
            else:
                grant = requested
            await conn.execute(
                insert(subagent_budget_leases).values(
                    tree_id=tree_id,
                    key=key,
                    parent_key=parent_key,
                    request_limit=grant,
                    requested=requested,
                    spent=0,
                    status=LEASE_ACTIVE,
                    created_at=time.time(),
                    released_at=None,
                )
            )
            fresh_budget = await _budget(conn, tree_id)
            fresh = await _lease_opt(conn, tree_id, key)
            assert fresh is not None
            sums = await _grant_sums(conn, tree_id)
            return _lease_receipt(fresh_budget, fresh, sums, created=True)

        async def confirm() -> dict[str, Any] | None:
            async def read(conn: AsyncConnection) -> Any:
                lease = await _lease_opt(conn, tree_id, key)
                if lease is None or lease["requested"] != requested:
                    return None
                budget = await _budget_opt(conn, tree_id)
                if budget is None:
                    return None
                sums = await _grant_sums(conn, tree_id)
                return _lease_receipt(budget, lease, sums, created=False)

            return await self._query(read)

        return await self._submit(operation, confirm)

    # ---------- 计费 ----------

    async def charge(
        self, tree_id: str, run_id: str, step_index: int, lease_key: str | None = None
    ) -> dict[str, Any]:
        """实际模型请求前调用；每次真实请求独立 attempt_id，恰好计费一次。

        根（lease_key=None）用未预留余额，子用其活跃租约余额；不足即时
        step_limit，不等待。COMMIT 回程丢失按 attempt_id 只读核实，不盲目
        重放（同一 run/step 的重新推理是新的真实请求，必须再次计费）。
        """
        _check_count(step_index)
        if lease_key is not None:
            _check_key(lease_key)
        attempt_id = str(uuid.uuid4())
        ids = {"tree_id": tree_id, "run_id": run_id, "attempt_id": attempt_id}

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            await _tree_exists(conn, tree_id)
            await _run_exists(conn, run_id)
            existing = await _charge_opt(conn, attempt_id)
            if existing is not None:
                return await self._rebuild(conn, existing)
            budget = await _budget(conn, tree_id)
            lease: RowMapping | None = None
            if lease_key is None:
                pool = _tree_remaining(budget, await _grant_sums(conn, tree_id))
            else:
                lease = await _lease_opt(conn, tree_id, lease_key)
                if lease is None:
                    raise ServiceError("not_found", 404)
                if lease["status"] != LEASE_ACTIVE:
                    raise _capacity(
                        ids | {"lease_key": lease_key}, "预算租约已释放；不得再计费"
                    )
                pool = _lease_remaining(lease, await _grant_sums(conn, tree_id))
            if pool is not None and pool < 1:
                raise _step_limit(
                    ids,
                    f"树请求预算已用尽（剩余 {pool}）；结束运行或先 release 未用租约",
                )
            now = time.time()
            await conn.execute(
                insert(subagent_budget_charges).values(
                    attempt_id=attempt_id,
                    tree_id=tree_id,
                    run_id=run_id,
                    lease_key=lease_key,
                    step_index=step_index,
                    created_at=now,
                )
            )
            await conn.execute(
                update(subagent_budgets)
                .where(subagent_budgets.c.tree_id == tree_id)
                .values(spent=subagent_budgets.c.spent + 1, updated_at=now)
            )
            if lease is not None:
                await conn.execute(
                    update(subagent_budget_leases)
                    .where(
                        subagent_budget_leases.c.tree_id == tree_id,
                        subagent_budget_leases.c.key == lease_key,
                    )
                    .values(spent=subagent_budget_leases.c.spent + 1)
                )
            charge = await _charge_opt(conn, attempt_id)
            assert charge is not None
            return await self._rebuild(conn, charge, created=True)

        async def confirm() -> dict[str, Any] | None:
            async def read(conn: AsyncConnection) -> Any:
                charge = await _charge_opt(conn, attempt_id)
                if charge is None:
                    return None
                return await self._rebuild(conn, charge, created=False)

            return await self._query(read)

        return await self._submit(operation, confirm)

    async def _rebuild(
        self, conn: AsyncConnection, charge: RowMapping, *, created: bool
    ) -> dict[str, Any]:
        budget = await _budget_opt(conn, charge["tree_id"])
        if budget is None:
            raise ServiceError("facts_corrupt")
        lease = (
            await _lease_opt(conn, charge["tree_id"], charge["lease_key"])
            if charge["lease_key"] is not None
            else None
        )
        sums = await _grant_sums(conn, charge["tree_id"])
        return _charge_receipt(charge, budget, lease, sums, created=created)

    # ---------- 释放 ----------

    async def release(self, tree_id: str, key: str) -> dict[str, Any]:
        """级联释放租约与其活跃后代；未用额度回到活跃父租约或根池。

        已花费不返还。幂等：重复 release 返回 released=False 且无变化。
        returned 是本次释放的未用次数合计（不确定时为 -1）。
        """
        key = _check_key(key)

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            await _tree_exists(conn, tree_id)
            await _budget(conn, tree_id)
            lease = await _lease_opt(conn, tree_id, key)
            if lease is None:
                raise ServiceError("not_found", 404)
            if lease["status"] == LEASE_RELEASED:
                sums = await _grant_sums(conn, tree_id)
                return {
                    "tree_id": tree_id,
                    "key": key,
                    "released": False,
                    "returned": 0,
                    "tree_remaining": _tree_remaining(await _budget(conn, tree_id), sums),
                }
            descendants = (
                select(subagent_budget_leases.c.key)
                .where(
                    subagent_budget_leases.c.tree_id == tree_id,
                    subagent_budget_leases.c.parent_key == key,
                    subagent_budget_leases.c.status == LEASE_ACTIVE,
                )
                .cte("budget_descendants", recursive=True)
            )
            descendants = descendants.union_all(
                select(subagent_budget_leases.c.key)
                .join(
                    descendants,
                    subagent_budget_leases.c.parent_key == descendants.c.key,
                )
                .where(
                    subagent_budget_leases.c.tree_id == tree_id,
                    subagent_budget_leases.c.status == LEASE_ACTIVE,
                )
            )
            cascade = [key]
            cascade.extend((await conn.execute(select(descendants.c.key))).scalars())
            targets = (
                (
                    await conn.execute(
                        select(subagent_budget_leases).where(
                            subagent_budget_leases.c.tree_id == tree_id,
                            subagent_budget_leases.c.key.in_(cascade),
                            subagent_budget_leases.c.status == LEASE_ACTIVE,
                        )
                    )
                )
                .mappings()
                .all()
            )
            freed = sum(
                row["request_limit"] - row["spent"]
                for row in targets
                if row["request_limit"] is not None
            )
            now = time.time()
            await conn.execute(
                update(subagent_budget_leases)
                .where(
                    subagent_budget_leases.c.tree_id == tree_id,
                    subagent_budget_leases.c.key.in_(cascade),
                    subagent_budget_leases.c.status == LEASE_ACTIVE,
                )
                .values(status=LEASE_RELEASED, released_at=now)
            )
            sums = await _grant_sums(conn, tree_id)
            return {
                "tree_id": tree_id,
                "key": key,
                "released": True,
                "returned": freed,
                "tree_remaining": _tree_remaining(await _budget(conn, tree_id), sums),
            }

        async def confirm() -> dict[str, Any] | None:
            async def read(conn: AsyncConnection) -> Any:
                lease = await _lease_opt(conn, tree_id, key)
                if lease is None or lease["status"] != LEASE_RELEASED:
                    return None
                budget = await _budget_opt(conn, tree_id)
                if budget is None:
                    return None
                sums = await _grant_sums(conn, tree_id)
                return {
                    "tree_id": tree_id,
                    "key": key,
                    "released": True,
                    "returned": -1,
                    "tree_remaining": _tree_remaining(budget, sums),
                }

            return await self._query(read)

        return await self._submit(operation, confirm)

    # ---------- 只读视图 ----------

    async def state(self, tree_id: str) -> dict[str, Any]:
        """树预算与活跃租约视图；冷恢复据此找回已花预算，不重置。"""

        async def operation(conn: AsyncConnection) -> dict[str, Any]:
            budget = await _budget(conn, tree_id)
            sums = await _grant_sums(conn, tree_id)
            rows = (
                (
                    await conn.execute(
                        select(subagent_budget_leases)
                        .where(subagent_budget_leases.c.tree_id == tree_id)
                        .order_by(
                            subagent_budget_leases.c.created_at, subagent_budget_leases.c.key
                        )
                    )
                )
                .mappings()
                .all()
            )
            leases = []
            for row in rows:
                if row["status"] != LEASE_ACTIVE:
                    continue
                leases.append(
                    {
                        "key": row["key"],
                        "parent_key": row["parent_key"],
                        "limit": row["request_limit"],
                        "spent": row["spent"],
                        "remaining": _lease_remaining(row, sums),
                        "created_at": row["created_at"],
                    }
                )
            return {
                "tree_id": tree_id,
                "limit": budget["request_limit"],
                "spent": budget["spent"],
                "remaining": _tree_remaining(budget, sums),
                "outstanding": sums.get(None, 0),
                "active_leases": leases,
            }

        return await self._query(operation)
