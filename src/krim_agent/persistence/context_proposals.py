"""压缩提案原摘要确认事务；TTL/来源/先前摘要/配置不匹配禁止保存。"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Sequence
from datetime import UTC
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.context_engine import ContextOptions, ContextProposalView, ContextResult
from krim_agent.protocol.domain import MessageView, ServiceError
from krim_agent.protocol.models import utc_now

from .database import ContextProposalRow, ContextSummaryRow, KrimRepository, RunRow, TopicRow
from .tree import MessageTree


def fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def message_snapshot(messages: Sequence[MessageView]) -> str:
    return fingerprint(
        [
            {"id": item.id, "role": item.role, "content": item.content, "details": item.details}
            for item in messages
        ]
    )


class ContextProposalStore:
    def __init__(self, repository: KrimRepository):
        self.repository = repository
        self.database = repository.database

    @staticmethod
    def _view(row: ContextProposalRow) -> ContextProposalView:
        return ContextProposalView.model_validate(
            {
                "id": row.id,
                "topic_id": row.topic_id,
                "run_id": row.run_id,
                "step": row.step,
                "branch_key": row.branch_key,
                "snapshot_hash": row.snapshot_hash,
                "prior_summary_hash": row.prior_summary_hash,
                "config_hash": row.config_hash,
                # 与 Repository.context_branch_key 的 ID 序列规范编码相同。
                "source_branch_key": fingerprint(
                    ContextResult.model_validate(row.payload["result"]).source_message_ids
                ),
                "status": row.status,
                "revision": row.revision,
                "expires_at": row.expires_at,
                "created_at": row.created_at.replace(tzinfo=UTC),
                "updated_at": row.updated_at.replace(tzinfo=UTC),
                **row.payload,
            }
        )

    async def _prior(
        self, session: AsyncSession, topic_id: str, messages: Sequence[MessageView]
    ) -> str:
        rows = list(
            await session.scalars(
                select(ContextSummaryRow).where(ContextSummaryRow.topic_id == topic_id)
            )
        )
        ids = [item.id for item in messages]
        matched = [
            row
            for row in rows
            if row.source_message_ids
            and row.source_message_ids == ids[: len(row.source_message_ids)]
            and len(row.source_message_ids) < len(messages)
            and messages[len(row.source_message_ids)].role == "user"
            and row.source_until_message_id == row.source_message_ids[-1]
            and row.branch_key
            == self.repository.context_branch_key(list(messages[: len(row.source_message_ids)]))
        ]
        row = max(matched, key=lambda item: len(item.source_message_ids), default=None)
        return fingerprint(
            {
                "id": row.id,
                "summary": row.summary,
                "sources": row.source_message_ids,
                "frozen": row.never_update,
                "strategy": row.strategy,
                "policy": row.policy,
                "updated_at": row.updated_at.isoformat(),
            }
            if row
            else None
        )

    async def prior(self, topic_id: str, messages: Sequence[MessageView]) -> str:
        async with self.database.write_lock, self.database.sessions() as session:
            return await self._prior(session, topic_id, messages)

    async def create(
        self,
        topic_id: str,
        messages: list[MessageView],
        options: ContextOptions,
        result: ContextResult,
        *,
        prior_hash: str,
        config_hash: str,
        run_id: str | None = None,
        step: int | None = None,
        ttl_seconds: float = 300,
    ) -> ContextProposalView:
        if (
            not result.compacted
            or not result.summary
            or not result.summary.strip()
            or len(result.summary.encode("utf-8")) > 65536
            or not result.source_message_ids
            or options.policy == "never"
            or options.strategy not in {"summarize_then_truncate", "annotated_compact"}
        ):
            raise ServiceError(
                "proposal_result_invalid", "提案需要实际摘要/完整来源与可更新策略", 400
            )
        if not 1 <= ttl_seconds <= 3600:
            raise ServiceError("proposal_ttl_invalid", "提案 TTL 必须在 1–3600 秒", 422)
        payload = {
            "options": options.model_dump(mode="json"),
            "result": result.model_dump(mode="json"),
        }
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(TopicRow)
                .where(TopicRow.id == topic_id)
                .values(updated_at=TopicRow.updated_at)
            )
            topic = await session.get(TopicRow, topic_id)
            if topic is None:
                raise ServiceError("topic_not_found", "提案话题不存在", 404)
            if run_id is None:
                await self.repository._require_idle(session, topic_id)
            else:
                run = await session.get(RunRow, run_id)
                if (
                    run is None
                    or run.topic_id != topic_id
                    or run.status not in ("queued", "running")
                    or run.stop_requested
                ):
                    raise ServiceError("proposal_run_closed", "提案运行已关闭或归属不匹配", 409)
            active = MessageTree(await self.repository._messages(session, topic_id)).active_path(
                topic.active_branches
            )
            if (
                message_snapshot(active) != message_snapshot(messages)
                or await self._prior(session, topic_id, active) != prior_hash
            ):
                raise ServiceError("summary_branch_changed", "提案计算期间来源或摘要发生变化", 409)
            sources = result.source_message_ids
            if (
                sources != [item.id for item in active[: len(sources)]]
                or len(sources) >= len(active)
                or active[len(sources)].role != "user"
            ):
                raise ServiceError(
                    "context_source_invalid", "提案必须覆盖完整历史前缀并保留最新用户组", 409
                )
            MessageTree(active).require_complete_tools(active[: len(sources)])
            existing = await session.scalar(
                select(ContextProposalRow)
                .where(
                    ContextProposalRow.topic_id == topic_id,
                    ContextProposalRow.run_id == run_id,
                    ContextProposalRow.status == "proposed",
                    ContextProposalRow.snapshot_hash == message_snapshot(messages),
                    ContextProposalRow.config_hash == config_hash,
                )
                .order_by(ContextProposalRow.created_at.desc())
            )
            if (
                existing is not None
                and existing.payload == payload
                and existing.expires_at > time.time()
            ):
                return self._view(existing)
            row = ContextProposalRow(
                id=str(uuid4()),
                topic_id=topic_id,
                run_id=run_id,
                step=step,
                branch_key=self.repository.context_branch_key(messages),
                snapshot_hash=message_snapshot(messages),
                prior_summary_hash=prior_hash,
                config_hash=config_hash,
                payload=payload,
                status="proposed",
                revision=0,
                expires_at=time.time() + ttl_seconds,
            )
            session.add(row)
            await session.flush()
            return self._view(row)

    async def _refresh(
        self, session: AsyncSession, row: ContextProposalRow, manual_config_hash: str | None = None
    ) -> None:
        if row.status != "proposed":
            return
        status = None
        if row.expires_at <= time.time():
            status = "expired"
        elif (
            row.run_id is None
            and manual_config_hash is not None
            and manual_config_hash != row.config_hash
        ):
            status = "stale"
        elif row.run_id is not None:
            run = await session.get(RunRow, row.run_id)
            if run is None or run.status not in {"queued", "running"} or run.stop_requested:
                status = "cancelled"
        if status is None:
            topic = await session.get(TopicRow, row.topic_id)
            if topic is None:
                status = "cancelled"
            else:
                active = MessageTree(
                    await self.repository._messages(session, row.topic_id)
                ).active_path(topic.active_branches)
                if (
                    message_snapshot(active) != row.snapshot_hash
                    or self.repository.context_branch_key(active) != row.branch_key
                    or await self._prior(session, row.topic_id, active) != row.prior_summary_hash
                ):
                    status = "stale"
        if status is not None:
            row.status, row.revision, row.updated_at = status, row.revision + 1, utc_now()

    async def get(
        self, topic_id: str, proposal_id: str, *, manual_config_hash: str | None = None
    ) -> ContextProposalView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(ContextProposalRow)
                .where(ContextProposalRow.id == proposal_id)
                .values(revision=ContextProposalRow.revision)
            )
            row = await session.get(ContextProposalRow, proposal_id)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("proposal_not_found", "提案不在该话题中", 404)
            await self._refresh(session, row, manual_config_hash)
            await session.flush()
            return self._view(row)

    async def list(
        self, topic_id: str, *, pending_only: bool = False, manual_config_hash: str | None = None
    ) -> list[ContextProposalView]:
        await self.repository.get_topic(topic_id)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(ContextProposalRow)
                .where(
                    ContextProposalRow.topic_id == topic_id,
                    ContextProposalRow.status == "proposed",
                    ContextProposalRow.expires_at <= time.time(),
                )
                .values(
                    status="expired", revision=ContextProposalRow.revision + 1, updated_at=utc_now()
                )
            )
            query = (
                select(ContextProposalRow)
                .where(ContextProposalRow.topic_id == topic_id)
                .order_by(ContextProposalRow.created_at.desc())
            )
            # 先刷新全部 pending，避免最近 128 条终态掩盖较早的待确认请求。
            pending = list(
                await session.scalars(
                    select(ContextProposalRow).where(
                        ContextProposalRow.topic_id == topic_id,
                        ContextProposalRow.status == "proposed",
                    )
                )
            )
            for row in pending:
                await self._refresh(session, row, manual_config_hash)
            await session.flush()
            if pending_only:
                query = query.where(ContextProposalRow.status == "proposed")
            return [self._view(row) for row in await session.scalars(query.limit(128))]

    async def decide(
        self,
        topic_id: str,
        proposal_id: str,
        revision: int,
        *,
        confirm: bool,
        config_hash: str | None = None,
    ) -> tuple[ContextProposalView, bool]:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(ContextProposalRow)
                .where(ContextProposalRow.id == proposal_id)
                .values(revision=ContextProposalRow.revision)
            )
            row = await session.get(ContextProposalRow, proposal_id)
            if row is None or row.topic_id != topic_id:
                raise ServiceError("proposal_not_found", "提案不在该话题中", 404)
            if row.status == ("completed" if confirm else "rejected") and revision in {
                row.revision,
                row.revision - 1,
            }:
                return self._view(row), False
            if row.status != "proposed" or row.revision != revision:
                raise ServiceError("proposal_state_conflict", "提案状态或 revision 已变化", 409)
            await self._refresh(session, row)
            if row.status != "proposed":
                await session.flush()
                return self._view(row), False
            if not confirm:
                row.status, row.revision, row.updated_at = "rejected", row.revision + 1, utc_now()
                await session.flush()
                return self._view(row), True
            topic = await session.get(TopicRow, topic_id)
            assert topic is not None
            if row.run_id is None:
                await self.repository._require_idle(session, topic_id)
            else:
                run = await session.get(RunRow, row.run_id)
                if (
                    run is None
                    or run.topic_id != topic_id
                    or run.status not in ("queued", "running")
                    or run.stop_requested
                ):
                    row.status, row.revision, row.updated_at = (
                        "cancelled",
                        row.revision + 1,
                        utc_now(),
                    )
                    await session.flush()
                    return self._view(row), False
            active = MessageTree(await self.repository._messages(session, topic_id)).active_path(
                topic.active_branches
            )
            if (
                message_snapshot(active) != row.snapshot_hash
                or self.repository.context_branch_key(active) != row.branch_key
                or await self._prior(session, topic_id, active) != row.prior_summary_hash
                or (config_hash is not None and config_hash != row.config_hash)
            ):
                row.status, row.revision, row.updated_at = "stale", row.revision + 1, utc_now()
                await session.flush()
                return self._view(row), False
            view = self._view(row)
            sources = view.result.source_message_ids
            MessageTree(active).require_complete_tools(active[: len(sources)])
            if (
                sources != [item.id for item in active[: len(sources)]]
                or len(sources) >= len(active)
                or active[len(sources)].role != "user"
            ):
                raise ServiceError("proposal_source_invalid", "提案来源不能拆开当前工具组", 409)
            key = self.repository.context_branch_key(active[: len(sources)])
            summary = await session.scalar(
                select(ContextSummaryRow).where(
                    ContextSummaryRow.topic_id == topic_id, ContextSummaryRow.branch_key == key
                )
            )
            if summary is not None and summary.never_update:
                raise ServiceError("summary_frozen", "已有摘要禁止更新", 409)
            if not view.result.summary or view.options.policy == "never":
                raise ServiceError("proposal_result_invalid", "提案摘要/策略无效", 409)
            if summary is None:
                summary = ContextSummaryRow(
                    id=str(uuid4()),
                    topic_id=topic_id,
                    branch_key=key,
                    source_until_message_id=sources[-1],
                    source_message_ids=list(sources),
                    summary=view.result.summary,
                    strategy=view.options.strategy,
                    policy=view.options.policy,
                    never_update=False,
                )
                session.add(summary)
            else:
                summary.summary, summary.source_message_ids = view.result.summary, list(sources)
                summary.source_until_message_id = sources[-1]
                summary.strategy, summary.policy, summary.updated_at = (
                    view.options.strategy,
                    view.options.policy,
                    utc_now(),
                )
            row.status, row.revision, row.updated_at = "completed", row.revision + 1, utc_now()
            await session.flush()
            return self._view(row), True

    async def cancel_run(self, run_id: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await session.execute(
                update(ContextProposalRow)
                .where(ContextProposalRow.run_id == run_id, ContextProposalRow.status == "proposed")
                .values(
                    status="cancelled",
                    revision=ContextProposalRow.revision + 1,
                    updated_at=utc_now(),
                )
            )
