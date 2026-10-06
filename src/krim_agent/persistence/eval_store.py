"""持久评估账本：原子 owner/epoch 保护与执行、评分、进度事务。"""

from __future__ import annotations

import json
import time
from datetime import UTC
from typing import Literal, cast
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.evaluation.jobs import (
    EvalEvent,
    EvalItem,
    EvalItemPage,
    EvalJobSpec,
    EvalJobView,
    JobStatus,
)
from krim_agent.evaluation.types import EvalCaseResult, EvalOutput, EvalReport, EvalScore
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import check_json, utc_now

from .database import Database
from .eval_models import EvalEventRow, EvalItemRow, EvalJobRow


class EvaluationStore:
    def __init__(
        self, database: Database, *, scope: str = "python", lease_seconds: float = 30
    ) -> None:
        if not scope or len(scope) > 64 or not 0.2 <= lease_seconds <= 600:
            raise ValueError("评估 scope/租约无效")
        self.database, self.scope, self.lease_seconds = database, scope, lease_seconds

    async def _job(self, session: AsyncSession, job_id: str) -> EvalJobRow:
        row = await session.get(EvalJobRow, job_id)
        if row is None or row.scope != self.scope:
            raise ServiceError("eval_not_found", "评估任务不存在", 404)
        return row

    async def _owned(
        self, session: AsyncSession, job_id: str, owner: str, epoch: int
    ) -> EvalJobRow:
        row = await session.scalar(
            update(EvalJobRow)
            .where(
                EvalJobRow.id == job_id,
                EvalJobRow.scope == self.scope,
                EvalJobRow.owner_id == owner,
                EvalJobRow.epoch == epoch,
                EvalJobRow.lease_expires_at > time.time(),
                EvalJobRow.status == "running",
            )
            .values(revision=EvalJobRow.revision + 1, updated_at=utc_now())
            .returning(EvalJobRow)
        )
        if row is None:
            raise ServiceError("eval_lease_lost", "评估所有权或执行代次已变化", 409)
        return row

    @staticmethod
    def _event(
        session: AsyncSession, row: EvalJobRow, kind: str, data: dict[str, JsonValue]
    ) -> None:
        check_json(data)
        session.add(
            EvalEventRow(
                job_id=row.id, seq=row.event_count, kind=kind, data=data, created_at=utc_now()
            )
        )
        row.event_count += 1

    @staticmethod
    def _item(row: EvalItemRow) -> EvalItem:
        return EvalItem.model_validate(
            {
                "job_id": row.job_id,
                "ordinal": row.ordinal,
                "case_id": row.case_id,
                "repetition": row.repetition,
                "generation": row.generation,
                "phase": row.phase,
                "bindings": row.bindings,
                "output": row.output,
                "scores": row.scores,
                "result": row.result,
                "history": row.history,
                "error_code": row.error_code,
                "duration_ms": row.duration_ms,
                "scoring_ms": row.scoring_ms,
                "updated_at": row.updated_at.replace(tzinfo=UTC),
            }
        )

    async def create(self, spec: EvalJobSpec, request_key: str | None = None) -> tuple[str, bool]:
        spec = EvalJobSpec.model_validate(spec.model_dump())
        key = request_key or str(uuid4())
        if not key or len(key) > 128:
            raise ValueError("评估请求键无效")
        async with self.database.write_lock, self.database.sessions.begin() as session:
            now = utc_now()
            row = await session.scalar(
                insert(EvalJobRow)
                .values(
                    id=str(uuid4()),
                    scope=self.scope,
                    request_key=key,
                    request_hash=spec.fingerprint,
                    snapshot=spec.model_dump(mode="json"),
                    status="queued",
                    revision=0,
                    epoch=0,
                    event_count=0,
                    created_at=now,
                    updated_at=now,
                )
                .on_conflict_do_nothing(index_elements=["scope", "request_key"])
                .returning(EvalJobRow)
            )
            if row is None:
                existing = await session.scalar(
                    select(EvalJobRow).where(
                        EvalJobRow.scope == self.scope, EvalJobRow.request_key == key
                    )
                )
                if existing is None or existing.request_hash != spec.fingerprint:
                    raise ServiceError("eval_idempotency_conflict", "请求键已绑定其他评估", 409)
                return existing.id, False
            ordinal = 0
            for case in spec.dataset.cases:
                for repetition in range(1, spec.config.repetitions + 1):
                    session.add(
                        EvalItemRow(
                            job_id=row.id,
                            ordinal=ordinal,
                            case_id=case.id,
                            repetition=repetition,
                            phase="pending",
                            generation=1,
                            bindings={},
                            scores=[],
                            history=[],
                            duration_ms=0,
                            scoring_ms=0,
                            updated_at=now,
                        )
                    )
                    ordinal += 1
            self._event(session, row, "created", {"total": ordinal})
            return row.id, True

    async def spec(self, job_id: str) -> EvalJobSpec:
        async with self.database.write_lock, self.database.sessions() as session:
            return EvalJobSpec.model_validate((await self._job(session, job_id)).snapshot)

    async def view(self, job_id: str) -> EvalJobView:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await self._job(session, job_id)
            phases = list(
                await session.scalars(select(EvalItemRow.phase).where(EvalItemRow.job_id == job_id))
            )
            return EvalJobView(
                id=row.id,
                status=cast(JobStatus, row.status),
                revision=row.revision,
                epoch=row.epoch,
                event_count=row.event_count,
                total=len(phases),
                pending=phases.count("pending"),
                completed=phases.count("done"),
                uncertain=phases.count("uncertain") + phases.count("grading_uncertain"),
                active=sum(p in {"executing", "executed", "scoring"} for p in phases),
                can_resume=row.status not in {"completed", "failed"}
                and (row.owner_id is None or (row.lease_expires_at or 0) <= time.time()),
                control=cast(Literal["pause", "cancel"] | None, row.control),
                error_code=row.error_code,
                created_at=row.created_at.replace(tzinfo=UTC),
                updated_at=row.updated_at.replace(tzinfo=UTC),
                finished_at=row.finished_at.replace(tzinfo=UTC) if row.finished_at else None,
            )

    async def list_jobs(self, *, limit: int = 100) -> list[EvalJobView]:
        if not 1 <= limit <= 1000:
            raise ValueError("评估目录 limit 无效")
        async with self.database.write_lock, self.database.sessions() as session:
            ids = list(
                await session.scalars(
                    select(EvalJobRow.id)
                    .where(EvalJobRow.scope == self.scope)
                    .order_by(EvalJobRow.created_at.desc())
                    .limit(limit)
                )
            )
        return [await self.view(job_id) for job_id in ids]

    async def items(self, job_id: str, *, offset: int = 0, limit: int = 100) -> EvalItemPage:
        if offset < 0 or not 1 <= limit <= 1000:
            raise ValueError("评估分页无效")
        async with self.database.write_lock, self.database.sessions() as session:
            spec = EvalJobSpec.model_validate((await self._job(session, job_id)).snapshot)
            rows = await session.scalars(
                select(EvalItemRow)
                .where(EvalItemRow.job_id == job_id)
                .order_by(EvalItemRow.ordinal)
                .offset(offset)
                .limit(limit)
            )
            return EvalItemPage(
                job_id=job_id,
                total=len(spec.dataset.cases) * spec.config.repetitions,
                offset=offset,
                items=[self._item(row) for row in rows],
            )

    async def item(self, job_id: str, ordinal: int) -> EvalItem:
        page = await self.items(job_id, offset=ordinal, limit=1)
        if not page.items or page.items[0].ordinal != ordinal:
            raise ServiceError("eval_item_not_found", "评估样本不存在", 404)
        return page.items[0]

    async def claim(self, job_id: str, owner: str) -> int:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            now = time.time()
            row = await session.scalar(
                update(EvalJobRow)
                .where(
                    EvalJobRow.id == job_id,
                    EvalJobRow.scope == self.scope,
                    EvalJobRow.status.not_in(("completed", "failed")),
                    or_(EvalJobRow.owner_id.is_(None), EvalJobRow.lease_expires_at <= now),
                )
                .values(
                    owner_id=owner,
                    lease_expires_at=now + self.lease_seconds,
                    epoch=EvalJobRow.epoch + 1,
                    revision=EvalJobRow.revision + 1,
                    status="running",
                    control=None,
                    updated_at=utc_now(),
                    finished_at=None,
                )
                .returning(EvalJobRow)
            )
            if row is None:
                await self._job(session, job_id)
                raise ServiceError("eval_busy_or_terminal", "评估已被持有或不能恢复", 409)
            self._event(session, row, "started", {"epoch": row.epoch})
            return row.epoch

    async def renew(self, job_id: str, owner: str, epoch: int) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.scalar(
                update(EvalJobRow)
                .where(
                    EvalJobRow.id == job_id,
                    EvalJobRow.scope == self.scope,
                    EvalJobRow.owner_id == owner,
                    EvalJobRow.epoch == epoch,
                    EvalJobRow.lease_expires_at > time.time(),
                    EvalJobRow.status == "running",
                )
                .values(lease_expires_at=time.time() + self.lease_seconds)
                .returning(EvalJobRow.id)
            )
            if row is None:
                raise ServiceError("eval_lease_lost", "评估租约已丢失", 409)

    async def control(self, job_id: str, action: str) -> None:
        if action not in {"pause", "cancel"}:
            raise ValueError("评估控制无效")
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.scalar(
                update(EvalJobRow)
                .where(
                    EvalJobRow.id == job_id,
                    EvalJobRow.scope == self.scope,
                    EvalJobRow.status.not_in(("completed", "failed", "cancelled")),
                )
                .values(control=action, revision=EvalJobRow.revision + 1, updated_at=utc_now())
                .returning(EvalJobRow)
            )
            if row is None:
                await self._job(session, job_id)
                return
            self._event(session, row, "control", {"action": action})
            # 无有效生产者时直接保存终态；活跃生产者必须先回收真正的 Run。
            if row.owner_id is None or (row.lease_expires_at or 0) <= time.time():
                row.status = "paused" if action == "pause" else "cancelled"
                row.owner_id = row.lease_expires_at = None
                row.finished_at = utc_now()
                self._event(session, row, "finished", {"status": row.status, "error_code": None})

    async def mutate(
        self,
        job_id: str,
        owner: str,
        epoch: int,
        ordinal: int,
        *,
        phase: str | None = None,
        bindings: dict[str, JsonValue] | None = None,
        output: EvalOutput | None = None,
        scores: list[EvalScore] | None = None,
        result: EvalCaseResult | None = None,
        error_code: str | None = None,
        duration_ms: float | None = None,
        scoring_ms: float | None = None,
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await self._owned(session, job_id, owner, epoch)
            item = await session.get(EvalItemRow, (job_id, ordinal))
            if item is None or item.phase == "done":
                raise ServiceError("eval_item_closed", "评估样本不存在或已完成", 409)
            if phase is not None:
                item.phase = phase
            if bindings is not None:
                check_json(bindings)
                item.bindings = bindings
            if output is not None:
                item.output = EvalOutput.model_validate(output.model_dump()).model_dump(mode="json")
            if scores is not None:
                item.scores = TypeAdapter(list[JsonValue]).validate_python(
                    [s.model_dump(mode="json") for s in scores]
                )
            if result is not None:
                if result.case_id != item.case_id or result.repetition != item.repetition:
                    raise ValueError("评估结果归属不正确")
                item.result = result.model_dump(mode="json")
                item.phase = "done"
                item.output = result.output.model_dump(mode="json") if result.output else None
                item.scores = TypeAdapter(list[JsonValue]).validate_python(
                    [s.model_dump(mode="json") for s in result.scores]
                )
                error_code, duration_ms, scoring_ms = (
                    result.error_code,
                    result.duration_ms,
                    result.scoring_ms,
                )
            item.error_code = error_code
            if duration_ms is not None:
                item.duration_ms = duration_ms
            if scoring_ms is not None:
                item.scoring_ms = scoring_ms
            item.updated_at = utc_now()
            self._event(
                session,
                row,
                "item",
                {"ordinal": ordinal, "phase": item.phase, "scored": len(item.scores)},
            )

    async def resolve(self, job_id: str, owner: str, epoch: int, ordinal: int, action: str) -> None:
        if action != "retry":
            raise ValueError("此入口只接受显式重试")
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await self._owned(session, job_id, owner, epoch)
            item = await session.get(EvalItemRow, (job_id, ordinal))
            if item is None or item.phase != "uncertain" or item.generation >= 20:
                raise ServiceError("eval_retry_invalid", "只有未知尝试可重试且最多20代", 409)
            history: JsonValue = {
                "generation": item.generation,
                "bindings": item.bindings,
                "output": item.output,
                "scores": item.scores,
                "error_code": item.error_code,
            }
            item.history = [*item.history, history]
            item.generation += 1
            item.phase, item.bindings, item.scores = "pending", {}, []
            item.output = item.result = item.error_code = None
            item.duration_ms = item.scoring_ms = 0
            self._event(session, row, "retry", {"ordinal": ordinal, "generation": item.generation})

    async def finish(
        self,
        job_id: str,
        owner: str,
        epoch: int,
        status: JobStatus,
        *,
        report: EvalReport | None = None,
        error_code: str | None = None,
    ) -> None:
        if status not in {"paused", "cancelled", "interrupted", "completed", "failed"}:
            raise ValueError("评估收束状态无效")
        if (status == "completed") != (report is not None):
            raise ValueError("只有完成任务必须保存完整报告")
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await self._owned(session, job_id, owner, epoch)
            if report is not None:
                validated = EvalReport.model_validate(report.model_dump())
                spec = EvalJobSpec.model_validate(row.snapshot)
                actual = list(
                    await session.scalars(
                        select(EvalItemRow)
                        .where(EvalItemRow.job_id == job_id)
                        .order_by(EvalItemRow.ordinal)
                    )
                )
                if (
                    validated.id != job_id
                    or spec
                    != EvalJobSpec(
                        dataset=validated.dataset,
                        target=validated.target,
                        config=validated.config,
                        scorers=validated.scorers,
                    )
                    or any(item.phase != "done" for item in actual)
                    or [item.result for item in actual]
                    != [result.model_dump(mode="json") for result in validated.results]
                ):
                    raise ValueError("完整报告必须对应冻结定义与实际已保存结果")
                row.report = validated.model_dump(mode="json")
            row.status, row.error_code = status, error_code
            row.owner_id = row.lease_expires_at = None
            row.finished_at = utc_now()
            self._event(session, row, "finished", {"status": status, "error_code": error_code})

    async def report(self, job_id: str) -> EvalReport:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await self._job(session, job_id)
            if row.report is None:
                raise ServiceError("eval_report_pending", "评估尚未形成完整报告", 409)
            return EvalReport.model_validate_json(json.dumps(row.report, ensure_ascii=False))

    async def events(self, job_id: str, from_seq: int = 0, limit: int = 100) -> list[EvalEvent]:
        if from_seq < 0 or not 1 <= limit <= 1000:
            raise ValueError("评估事件游标无效")
        async with self.database.write_lock, self.database.sessions() as session:
            row = await self._job(session, job_id)
            if from_seq > row.event_count:
                raise ServiceError("eval_cursor_invalid", "评估游标越界", 416)
            records = await session.scalars(
                select(EvalEventRow)
                .where(EvalEventRow.job_id == job_id, EvalEventRow.seq >= from_seq)
                .order_by(EvalEventRow.seq)
                .limit(limit)
            )
            return [
                EvalEvent(
                    job_id=r.job_id,
                    seq=r.seq,
                    kind=r.kind,
                    data=r.data,
                    created_at=r.created_at.replace(tzinfo=UTC),
                )
                for r in records
            ]
