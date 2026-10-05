"""有限观测记录的 SQLite 查询；写入故障由观测器隔离。"""

from __future__ import annotations

import re

from sqlalchemy import select

from krim_agent.observability import OwnerKind, SpanRecord, TraceView, owner_ref
from krim_agent.protocol.domain import ServiceError

from .database import Database, TraceSpanRow


class TraceStore:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def write(self, record: SpanRecord) -> None:
        record = SpanRecord.model_validate(record.model_dump())
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.get(TraceSpanRow, record.span_id)
            if row is None:
                session.add(
                    TraceSpanRow(
                        span_id=record.span_id,
                        trace_id=record.trace_id,
                        owner_kind=record.owner_kind,
                        owner_ref=record.owner_ref,
                        payload=record.model_dump(mode="json"),
                    )
                )
            elif row.trace_id != record.trace_id or row.owner_ref != record.owner_ref:
                raise ValueError("观测标识冲突")
            elif row.payload.get("end_ns") is None:
                row.payload = record.model_dump(mode="json")

    async def spans(self, owner_kind: OwnerKind, owner_id: str) -> list[SpanRecord]:
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(
                select(TraceSpanRow).where(
                    TraceSpanRow.owner_kind == owner_kind,
                    TraceSpanRow.owner_ref == owner_ref(owner_id),
                )
            )
            return sorted(
                [SpanRecord.model_validate(row.payload) for row in rows],
                key=lambda item: item.start_ns,
            )

    async def trace(self, trace_id: str) -> TraceView:
        if re.fullmatch(r"[0-9a-f]{32}", trace_id) is None:
            raise ServiceError("trace_id_invalid", "追踪标识格式不正确", 422)
        async with self.database.write_lock, self.database.sessions() as session:
            rows = list(
                await session.scalars(select(TraceSpanRow).where(TraceSpanRow.trace_id == trace_id))
            )
        if not rows:
            raise ServiceError("trace_not_found", "没有已保存的追踪事实", 404)
        spans = sorted(
            [SpanRecord.model_validate(row.payload) for row in rows], key=lambda item: item.start_ns
        )
        ids = {span.span_id for span in spans}
        complete = all(
            span.end_ns is not None and (span.parent_span_id is None or span.parent_span_id in ids)
            for span in spans
        )
        return TraceView(trace_id=trace_id, spans=spans, complete=complete)

    async def recover(self) -> None:
        import time

        async with self.database.write_lock, self.database.sessions.begin() as session:
            rows = await session.scalars(select(TraceSpanRow))
            for row in rows:
                record = SpanRecord.model_validate(row.payload)
                if record.end_ns is None:
                    row.payload = record.model_copy(
                        update={
                            "end_ns": max(time.time_ns(), record.start_ns),
                            "status": "interrupted",
                        }
                    ).model_dump(mode="json")
