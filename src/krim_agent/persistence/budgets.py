"""SQLite 累计预算账本；写锁先于余额读取，所有 scope 原子预留/结算。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Literal, cast

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.budgets import (
    BudgetAccountView,
    BudgetAllocation,
    BudgetOwner,
    BudgetRequestView,
    BudgetState,
    TokenPrice,
    UsageBudgetConfig,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.usage import TokenUsage

from .database import (
    Database,
    RunRow,
    UsageBudgetAccountRow,
    UsageBudgetAllocationRow,
    UsageBudgetBindingRow,
    UsageBudgetRequestRow,
)


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


class UsageBudgetStore:
    def __init__(self, database: Database):
        self.database = database

    async def bind(
        self,
        kind: BudgetOwner,
        owner_id: str,
        config: UsageBudgetConfig | None,
        parents: Sequence[str] = (),
    ) -> tuple[str, ...]:
        if not owner_id or len(owner_id) > 128:
            raise ServiceError("budget_owner_invalid", "预算 owner 无效", 422)
        parents = tuple(dict.fromkeys(parents))
        fingerprint = digest(config.model_dump(mode="json") if config else None)
        if config is None and not parents:
            async with self.database.write_lock, self.database.sessions() as session:
                existing = await session.get(UsageBudgetBindingRow, (kind, owner_id))
                if existing is None:
                    return ()
                if existing.config_hash != fingerprint:
                    raise ServiceError(
                        "budget_definition_changed", "既有预算不能被当前空配置绕过", 409
                    )
                return tuple(str(value) for value in existing.account_ids)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            # 即使没有匹配行，SQLite 的 UPDATE 也先取得写锁，避免先读后抢额度。
            await session.execute(
                update(UsageBudgetBindingRow)
                .where(
                    UsageBudgetBindingRow.owner_kind == kind,
                    UsageBudgetBindingRow.owner_id == owner_id,
                )
                .values(config_hash=UsageBudgetBindingRow.config_hash)
            )
            binding = await session.get(UsageBudgetBindingRow, (kind, owner_id))
            if binding is not None:
                if binding.config_hash != fingerprint or (
                    parents and list(parents) != binding.parents
                ):
                    raise ServiceError("budget_definition_changed", "预算定义或父绑定已变化", 409)
                return tuple(str(value) for value in binding.account_ids)
            if config is None and not parents:
                return ()
            for account_id in parents:
                if await session.get(UsageBudgetAccountRow, account_id) is None:
                    raise ServiceError("budget_account_not_found", "父预算 account 不存在", 404)
            account_ids = list(parents)
            if config is not None:
                account_id = f"{kind}:{owner_id}"
                session.add(
                    UsageBudgetAccountRow(
                        account_id=account_id,
                        owner_kind=kind,
                        owner_id=owner_id,
                        config=config.model_dump(mode="json"),
                        revision=0,
                        spent_tokens=0,
                        spent_cost_micros=0,
                        closed=False,
                    )
                )
                account_ids.append(account_id)
            session.add(
                UsageBudgetBindingRow(
                    owner_kind=kind,
                    owner_id=owner_id,
                    config_hash=fingerprint,
                    parents=list(parents),
                    account_ids=account_ids,
                )
            )
            await session.flush()
            return tuple(account_ids)

    async def binding(self, kind: BudgetOwner, owner_id: str) -> tuple[str, ...]:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(UsageBudgetBindingRow, (kind, owner_id))
            return tuple(str(value) for value in row.account_ids) if row else ()

    async def _account(self, session: AsyncSession, account_id: str) -> BudgetAccountView:
        row = await session.get(UsageBudgetAccountRow, account_id)
        if row is None:
            raise ServiceError("budget_account_not_found", "预算 account 不存在", 404)
        config = UsageBudgetConfig.model_validate(row.config)
        held = await session.execute(
            select(
                func.coalesce(func.sum(UsageBudgetAllocationRow.reserved_tokens), 0),
                func.coalesce(func.sum(UsageBudgetAllocationRow.reserved_cost_micros), 0),
                func.count(UsageBudgetRequestRow.request_id),
            )
            .join(
                UsageBudgetRequestRow,
                UsageBudgetRequestRow.request_id == UsageBudgetAllocationRow.request_id,
            )
            .where(
                UsageBudgetAllocationRow.account_id == account_id,
                UsageBudgetRequestRow.state.in_(("reserved", "dispatched", "uncertain")),
            )
        )
        tokens, cost, _ = held.one()
        uncertain = await session.scalar(
            select(func.count())
            .select_from(UsageBudgetAllocationRow)
            .join(
                UsageBudgetRequestRow,
                UsageBudgetRequestRow.request_id == UsageBudgetAllocationRow.request_id,
            )
            .join(RunRow, RunRow.id == UsageBudgetRequestRow.run_id)
            .where(
                UsageBudgetAllocationRow.account_id == account_id,
                or_(
                    UsageBudgetRequestRow.state == "uncertain",
                    and_(
                        UsageBudgetRequestRow.state.in_(("reserved", "dispatched")),
                        RunRow.status.in_(("completed", "failed", "aborted", "interrupted")),
                    ),
                ),
            )
        )
        prior_overrun = await session.scalar(
            select(func.count())
            .select_from(UsageBudgetAllocationRow)
            .join(
                UsageBudgetRequestRow,
                UsageBudgetRequestRow.request_id == UsageBudgetAllocationRow.request_id,
            )
            .where(
                UsageBudgetAllocationRow.account_id == account_id,
                UsageBudgetRequestRow.overdrawn.is_(True),
            )
        )
        overdrawn = (
            bool(prior_overrun)
            or (
                config.max_total_tokens is not None
                and row.spent_tokens + tokens > config.max_total_tokens
            )
            or (
                config.max_cost_micros is not None
                and row.spent_cost_micros + cost > config.max_cost_micros
            )
        )
        return BudgetAccountView(
            account_id=account_id,
            owner_kind=cast(BudgetOwner, row.owner_kind),
            owner_id=row.owner_id,
            config=config,
            revision=row.revision,
            spent_tokens=row.spent_tokens,
            held_tokens=tokens,
            spent_cost_micros=row.spent_cost_micros if config.price else None,
            held_cost_micros=cost if config.price else None,
            uncertain_requests=uncertain or 0,
            overdrawn=overdrawn,
        )

    async def account(self, account_id: str) -> BudgetAccountView:
        async with self.database.write_lock, self.database.sessions() as session:
            return await self._account(session, account_id)

    async def accounts(self) -> list[BudgetAccountView]:
        async with self.database.write_lock, self.database.sessions() as session:
            ids = list(
                await session.scalars(
                    select(UsageBudgetAccountRow.account_id).order_by(
                        UsageBudgetAccountRow.account_id
                    )
                )
            )
            return [await self._account(session, value) for value in ids]

    async def _request(self, session: AsyncSession, request_id: str) -> BudgetRequestView:
        row = await session.get(UsageBudgetRequestRow, request_id)
        if row is None:
            raise ServiceError("budget_request_not_found", "预算 request 不存在", 404)
        allocations = list(
            await session.scalars(
                select(UsageBudgetAllocationRow)
                .where(UsageBudgetAllocationRow.request_id == request_id)
                .order_by(UsageBudgetAllocationRow.account_id)
            )
        )
        values = []
        for item in allocations:
            price = TokenPrice.model_validate(item.price) if item.price else None
            values.append(
                BudgetAllocation(
                    account_id=item.account_id,
                    reserved_tokens=item.reserved_tokens,
                    reserved_cost_micros=item.reserved_cost_micros,
                    charged_tokens=item.charged_tokens,
                    charged_cost_micros=item.charged_cost_micros,
                    currency=price.currency if price else None,
                )
            )
        return BudgetRequestView(
            request_id=request_id,
            run_id=row.run_id,
            step=row.step,
            state=cast(BudgetState, row.state),
            revision=row.revision,
            input_tokens=row.input_tokens,
            output_reserve=row.output_reserve,
            reported_usage=TokenUsage.model_validate(row.reported_usage)
            if row.reported_usage
            else None,
            source=cast(Literal["provider", "host_resolution"] | None, row.source),
            receipt=row.receipt,
            allocations=values,
            overdrawn=row.overdrawn,
        )

    async def request(self, request_id: str) -> BudgetRequestView:
        async with self.database.write_lock, self.database.sessions() as session:
            return await self._request(session, request_id)

    async def requests(self, run_id: str | None = None) -> list[BudgetRequestView]:
        async with self.database.write_lock, self.database.sessions() as session:
            query = select(UsageBudgetRequestRow.request_id).order_by(
                UsageBudgetRequestRow.request_id
            )
            if run_id is not None:
                query = query.where(UsageBudgetRequestRow.run_id == run_id)
            ids = list(await session.scalars(query))
            return [await self._request(session, value) for value in ids]

    async def reserve(
        self,
        run_id: str,
        step: int,
        accounts: Sequence[str],
        input_tokens: int,
        output_reserve: int,
        payload_hash: str,
    ) -> BudgetRequestView:
        if (
            type(input_tokens) is not int
            or type(output_reserve) is not int
            or input_tokens < 0
            or output_reserve <= 0
        ):
            raise ServiceError("budget_estimate_invalid", "预算预留需要非负输入/正数输出上限", 422)
        accounts = tuple(sorted(set(accounts)))
        if not accounts or len(accounts) > 32 or not 0 <= step <= 31:
            raise ServiceError("budget_binding_invalid", "预算 scope/step 无效", 422)
        request_id = f"{run_id}:{step}"
        request_hash = digest([accounts, input_tokens, output_reserve, payload_hash])
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(UsageBudgetAccountRow)
                .where(UsageBudgetAccountRow.account_id.in_(accounts))
                .values(revision=UsageBudgetAccountRow.revision + 1, updated_at=utc_now())
            )
            existing = await session.get(UsageBudgetRequestRow, request_id)
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise ServiceError(
                        "budget_request_conflict", "预算 request 已绑定其它模型输入", 409
                    )
                return await self._request(session, request_id)
            if await session.get(RunRow, run_id) is None:
                raise ServiceError("run_not_found", "预算 run 不存在", 404)
            allocations = []
            reserved_tokens = input_tokens + output_reserve
            for account_id in accounts:
                account = await self._account(session, account_id)
                row = await session.get(UsageBudgetAccountRow, account_id)
                assert row is not None
                if row.closed:
                    raise ServiceError("budget_account_closed", "预算 account 已关闭", 409)
                if account.uncertain_requests:
                    raise ServiceError("budget_usage_unknown", "预算存在未核实模型用量", 409)
                if account.overdrawn:
                    raise ServiceError(
                        "budget_account_overdrawn", "预算已发生报告超用，禁止继续派发", 409
                    )
                config = account.config
                if (
                    config.max_total_tokens is not None
                    and account.spent_tokens + account.held_tokens + reserved_tokens
                    > config.max_total_tokens
                ):
                    raise ServiceError("budget_tokens_exceeded", "累计 token 预算不足", 409)
                cost = config.price.cost(input_tokens, output_reserve) if config.price else None
                if (
                    account.spent_tokens + account.held_tokens + reserved_tokens
                    > 9_007_199_254_740_991
                    or (
                        (account.spent_cost_micros or 0)
                        + (account.held_cost_micros or 0)
                        + (cost or 0)
                        > 9_007_199_254_740_991
                    )
                ):
                    raise ServiceError("budget_estimate_overflow", "累计预留超过安全整数范围", 409)
                if (
                    config.max_cost_micros is not None
                    and (account.spent_cost_micros or 0)
                    + (account.held_cost_micros or 0)
                    + (cost or 0)
                    > config.max_cost_micros
                ):
                    raise ServiceError("budget_cost_exceeded", "累计费用预算不足", 409)
                allocations.append(
                    UsageBudgetAllocationRow(
                        request_id=request_id,
                        account_id=account_id,
                        reserved_tokens=reserved_tokens,
                        reserved_cost_micros=cost,
                        price=config.price.model_dump(mode="json") if config.price else None,
                    )
                )
            session.add(
                UsageBudgetRequestRow(
                    request_id=request_id,
                    run_id=run_id,
                    step=step,
                    request_hash=request_hash,
                    state="reserved",
                    revision=0,
                    input_tokens=input_tokens,
                    output_reserve=output_reserve,
                    overdrawn=False,
                )
            )
            await session.flush()
            session.add_all(allocations)
            await session.flush()
            return await self._request(session, request_id)

    async def transition(self, request_id: str, state: BudgetState) -> BudgetRequestView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(UsageBudgetRequestRow)
                .where(UsageBudgetRequestRow.request_id == request_id)
                .values(revision=UsageBudgetRequestRow.revision)
            )
            row = await session.get(UsageBudgetRequestRow, request_id)
            if row is None:
                raise ServiceError("budget_request_not_found", "预算 request 不存在", 404)
            allowed = {
                "dispatched": {"reserved"},
                "uncertain": {"reserved", "dispatched"},
                "released": {"reserved", "dispatched"},
            }
            if row.state != state:
                if row.state not in allowed.get(state, set()):
                    raise ServiceError("budget_transition_invalid", "预算状态不允许该转换", 409)
                row.state, row.revision, row.updated_at = state, row.revision + 1, utc_now()
            await session.flush()
            return await self._request(session, request_id)

    async def settle(
        self,
        request_id: str,
        usage: TokenUsage,
        *,
        receipt: str | None = None,
        revision: int | None = None,
    ) -> BudgetRequestView:
        usage = TokenUsage.model_validate(usage.model_dump(mode="json"))
        if any(
            getattr(usage, key) is None for key in ("input_tokens", "output_tokens", "total_tokens")
        ):
            raise ServiceError("budget_usage_unknown", "结算需要完整的输入/输出/总用量", 409)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(UsageBudgetRequestRow)
                .where(UsageBudgetRequestRow.request_id == request_id)
                .values(revision=UsageBudgetRequestRow.revision)
            )
            row = await session.get(UsageBudgetRequestRow, request_id)
            if row is None:
                raise ServiceError("budget_request_not_found", "预算 request 不存在", 404)
            if row.state == "settled":
                if row.reported_usage != usage.model_dump(mode="json") or row.receipt != receipt:
                    raise ServiceError("budget_settlement_conflict", "预算已有不同结算", 409)
                return await self._request(session, request_id)
            if receipt is None and row.state != "dispatched":
                raise ServiceError(
                    "budget_transition_invalid", "Provider 结算需要已派发 request", 409
                )
            if receipt is not None and (
                row.state != "uncertain"
                or revision != row.revision
                or not receipt.strip()
                or len(receipt) > 256
            ):
                raise ServiceError(
                    "budget_resolution_conflict", "核实状态/revision/receipt 无效", 409
                )
            overdrawn = False
            allocations = list(
                await session.scalars(
                    select(UsageBudgetAllocationRow).where(
                        UsageBudgetAllocationRow.request_id == request_id
                    )
                )
            )
            for allocation in allocations:
                account = await session.get(UsageBudgetAccountRow, allocation.account_id)
                assert account is not None
                config = UsageBudgetConfig.model_validate(account.config)
                price = TokenPrice.model_validate(allocation.price) if allocation.price else None
                cost = (
                    price.cost(cast(int, usage.input_tokens), cast(int, usage.output_tokens))
                    if price
                    else None
                )
                if (
                    account.spent_tokens + cast(int, usage.total_tokens) > 9_007_199_254_740_991
                    or account.spent_cost_micros + (cost or 0) > 9_007_199_254_740_991
                ):
                    raise ServiceError(
                        "budget_usage_overflow", "报告用量/费用超过安全整数范围，保留待核实", 409
                    )
                allocation.charged_tokens, allocation.charged_cost_micros = usage.total_tokens, cost
                account.spent_tokens += cast(int, usage.total_tokens)
                account.spent_cost_micros += cost or 0
                account.revision += 1
                account.updated_at = utc_now()
                overdrawn = overdrawn or cast(int, usage.total_tokens) > allocation.reserved_tokens
                overdrawn = overdrawn or (
                    cost is not None and cost > cast(int, allocation.reserved_cost_micros)
                )
                overdrawn = overdrawn or (
                    config.max_total_tokens is not None
                    and account.spent_tokens > config.max_total_tokens
                )
                overdrawn = overdrawn or (
                    config.max_cost_micros is not None
                    and account.spent_cost_micros > config.max_cost_micros
                )
            row.state, row.reported_usage = "settled", usage.model_dump(mode="json")
            row.source = "host_resolution" if receipt is not None else "provider"
            row.receipt, row.overdrawn = receipt, overdrawn
            row.revision, row.updated_at = row.revision + 1, utc_now()
            await session.flush()
            return await self._request(session, request_id)

    async def recover(self) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            # 只处理中断/终态 run；不能中断另一有效生产者的正常预留。
            await session.execute(
                update(UsageBudgetRequestRow)
                .where(
                    UsageBudgetRequestRow.state.in_(("reserved", "dispatched")),
                    UsageBudgetRequestRow.run_id.in_(
                        select(RunRow.id).where(
                            RunRow.status.in_(("completed", "failed", "aborted", "interrupted"))
                        )
                    ),
                )
                .values(
                    state="uncertain",
                    revision=UsageBudgetRequestRow.revision + 1,
                    updated_at=utc_now(),
                )
            )
