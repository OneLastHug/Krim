"""原生清单批次：持久冻结准入，公开结果实时引用原文件操作账本。"""

from __future__ import annotations

import base64
import binascii
import json
import re
import secrets
import time
from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, cast
from uuid import uuid4

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace_change import PreparedChangeMetadata, WorkspaceChangeAction
from krim_agent.workspace_list import (
    NativeBatchDispatch,
    NativeBatchExecution,
    NativeBatchFence,
    NativeBatchHistoryPage,
    NativeBatchItemView,
    NativeBatchRequest,
    NativeBatchSummary,
    NativeBatchView,
)

from .database import (
    NativeWorkspaceBatchChildRow,
    NativeWorkspaceBatchRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceFileEffectRow,
)
from .workspace_list import NativeChangeListStore

if TYPE_CHECKING:
    from .workspace_edits import WorkspaceEditStore


INTERNAL_REQUEST_PREFIX = "krim-batch:"
_MAX_HISTORY = 5000
_BatchStatus = Literal[
    "running", "done", "partial", "failed", "uncertain", "cancelled", "interrupted"
]
_HistoryPosition = tuple[datetime, str]


def _history_cursor(scope_id: str, upper: _HistoryPosition, before: _HistoryPosition) -> str:
    value = {
        "v": 1,
        "scope": scope_id,
        "upper": [upper[0].replace(tzinfo=UTC).isoformat(), upper[1]],
        "before": [before[0].replace(tzinfo=UTC).isoformat(), before[1]],
    }
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _history_cursor_error() -> ServiceError:
    return ServiceError(
        "workspace_batch_history_cursor_invalid", "批次历史游标无效或作用域不匹配", 422
    )


def _history_position(value: object) -> _HistoryPosition:
    if (
        not isinstance(value, list)
        or len(value) != 2
        or not isinstance(value[0], str)
        or not isinstance(value[1], str)
        # ID只作为稳定排序键；沿用公开路径标识规则，不额外限制UUID版本。
        or re.fullmatch(r"[A-Za-z0-9_-]{1,36}", value[1]) is None
    ):
        raise _history_cursor_error()
    try:
        created = datetime.fromisoformat(value[0])
    except ValueError:
        raise _history_cursor_error() from None
    if created.tzinfo != UTC:
        raise _history_cursor_error()
    return created.replace(tzinfo=None), value[1]


def _decode_history_cursor(cursor: str, scope_id: str) -> tuple[_HistoryPosition, _HistoryPosition]:
    if (
        not isinstance(cursor, str)
        or not 1 <= len(cursor) <= 1024
        or re.fullmatch(r"[A-Za-z0-9_-]+", cursor) is None
    ):
        raise _history_cursor_error()
    try:
        encoded = cursor.encode()
        raw = base64.b64decode(encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        value = json.loads(raw)
    except (ValueError, UnicodeError, binascii.Error):
        raise _history_cursor_error() from None
    if (
        not isinstance(value, dict)
        or set(value) != {"v", "scope", "upper", "before"}
        or type(value["v"]) is not int
        or value["v"] != 1
        or value["scope"] != scope_id
    ):
        raise _history_cursor_error()
    upper, before = _history_position(value["upper"]), _history_position(value["before"])
    if before > upper or cursor != _history_cursor(scope_id, upper, before):
        raise _history_cursor_error()
    return upper, before


def _batch_status(producer_status: str, total: int, done: int, unresolved: int) -> _BatchStatus:
    if producer_status == "running":
        return "running"
    if unresolved:
        return "uncertain"
    if done == total:
        return "done"
    if producer_status in {"cancelled", "interrupted"}:
        return cast(Literal["cancelled", "interrupted"], producer_status)
    return "partial" if done else "failed"


def _assert_live(row: NativeWorkspaceBatchRow, owner_id: str) -> None:
    if (
        row.producer_status != "running"
        or row.owner_id != owner_id
        or row.lease_expires_at is None
        or row.lease_expires_at <= time.time()
    ):
        raise ServiceError("workspace_batch_fenced", "原生批次已结束或执行资格失效", 409)


async def _assert_undo_tip(session: AsyncSession, child: NativeWorkspaceBatchChildRow) -> None:
    from .workspace_edits import digest

    # SHA相等不证明没有后继；跨scope/资源检查真实物理文件当前仍生效的修改。
    rows = await session.execute(
        select(
            WorkspaceEditProposalRow.proposal_id,
            WorkspaceEditProposalRow.prepared,
            WorkspaceEditProposalRow.path,
            WorkspaceEditProposalRow.status,
            WorkspaceFileEffectRow.effect_sequence,
        )
        .outerjoin(
            WorkspaceFileEffectRow,
            (WorkspaceFileEffectRow.proposal_id == WorkspaceEditProposalRow.proposal_id)
            & (WorkspaceFileEffectRow.action == "apply"),
        )
        .where(
            WorkspaceEditProposalRow.status.in_(
                {"applying", "applied", "undoing", "keeping", "kept", "uncertain"}
            )
        )
    )
    for proposal_id, prepared, path, status, sequence in rows:
        if proposal_id == child.proposal_id:
            continue
        metadata = PreparedChangeMetadata.model_validate(prepared)
        key = digest(
            {
                "parent": [metadata.parent_device, metadata.parent_inode],
                "name": path.split("/")[-1],
            }
        )
        # 0026后新apply均有真实序号；已稳定legacy历史早于known，仍执行/未核实者不作此推断。
        if key == child.file_key and (
            child.apply_sequence is None
            or status in {"applying", "undoing", "keeping", "uncertain"}
            or (sequence is not None and sequence > child.apply_sequence)
        ):
            raise ServiceError(
                "workspace_batch_dependency", "同文件仍有后继或顺序未知的修改，须先处理", 409
            )


async def assert_batch_request(
    session: AsyncSession,
    owner_id: str,
    scope_id: str,
    request: WorkspaceChangeAction,
    resource_fingerprint: str,
    fence: NativeBatchFence | None,
    existing_operation_id: str | None = None,
) -> NativeWorkspaceBatchChildRow | None:
    """必须与原workspace reserve处于同一写事务；凭据不能来自公开参数。"""
    if fence is None:
        if existing_operation_id is None and request.request_key.startswith(
            INTERNAL_REQUEST_PREFIX
        ):
            raise ServiceError("workspace_batch_request_reserved", "该操作键仅供原生批次使用", 403)
        return None
    row = await session.get(NativeWorkspaceBatchRow, fence.batch_id)
    child = await session.get(NativeWorkspaceBatchChildRow, (fence.batch_id, fence.position))
    if (
        row is None
        or child is None
        or row.scope_id != scope_id
        or not secrets.compare_digest(row.token, fence.token)
        or row.resource_fingerprint != resource_fingerprint
        or row.action != request.action
        or child.proposal_id != request.proposal_id
        or child.expected_revision != request.expected_revision
        or child.request_key != request.request_key
    ):
        raise ServiceError("workspace_batch_fence_mismatch", "原生批次凭据或冻结目标不匹配", 403)
    if existing_operation_id is not None:
        if child.operation_id != existing_operation_id:
            raise ServiceError(
                "workspace_batch_operation_conflict", "批次不可认领其他操作回执", 409
            )
        return child
    _assert_live(row, owner_id)
    if (
        not child.admission_open
        or child.operation_id is not None
        or child.status != "pending"
        or child.error_code is not None
    ):
        raise ServiceError("workspace_batch_child_closed", "批次子项已关闭后续操作准入", 409)
    if request.action == "undo":
        await _assert_undo_tip(session, child)
    return child


async def assert_batch_execution(session: AsyncSession, owner_id: str, operation_id: str) -> None:
    """发布/保留前复核关联父批次；原非批次操作不受此资格约束。"""
    child = await session.scalar(
        select(NativeWorkspaceBatchChildRow).where(
            NativeWorkspaceBatchChildRow.operation_id == operation_id
        )
    )
    if child is not None:
        row = await session.get(NativeWorkspaceBatchRow, child.batch_id)
        assert row is not None
        _assert_live(row, owner_id)


class NativeBatchStore:
    def __init__(
        self, edits: WorkspaceEditStore, *, lease_seconds: float = 60, max_batches: int = 500
    ) -> None:
        if not 0.2 <= lease_seconds <= 600 or not 1 <= max_batches <= 5000:
            raise ValueError("原生批次租约或容量无效")
        self.edits, self.database = edits, edits.database
        self.owner_id = edits.owner_id
        self.lease_seconds, self.max_batches = lease_seconds, max_batches
        self.changes = NativeChangeListStore(edits)

    @staticmethod
    async def _children(session: AsyncSession, batch_id: str) -> list[NativeWorkspaceBatchChildRow]:
        return list(
            await session.scalars(
                select(NativeWorkspaceBatchChildRow)
                .where(NativeWorkspaceBatchChildRow.batch_id == batch_id)
                .order_by(NativeWorkspaceBatchChildRow.position)
            )
        )

    @staticmethod
    async def _row(session: AsyncSession, scope_id: str, batch_id: str) -> NativeWorkspaceBatchRow:
        row = await session.get(NativeWorkspaceBatchRow, batch_id)
        if row is None or row.scope_id != scope_id:
            raise ServiceError("workspace_batch_not_found", "原生修改批次不在该作用域中", 404)
        return row

    async def _close(
        self,
        session: AsyncSession,
        row: NativeWorkspaceBatchRow,
        status: Literal["finished", "cancelled", "interrupted"],
        reason: str | None,
    ) -> None:
        row.producer_status, row.stop_reason = status, reason
        row.owner_id, row.lease_expires_at = None, None
        row.updated_at = utc_now()
        for child in await self._children(session, row.batch_id):
            child.admission_open = False
            if child.operation_id is None and child.status == "pending":
                child.status = "skipped"
                child.error_code = reason or "workspace_batch_not_started"
        await session.flush()

    async def _expire(self, session: AsyncSession, row: NativeWorkspaceBatchRow) -> None:
        if row.producer_status == "running" and (
            row.lease_expires_at is None or row.lease_expires_at <= time.time()
        ):
            await self._close(session, row, "interrupted", "workspace_batch_lease_expired")

    async def _item(
        self, session: AsyncSession, child: NativeWorkspaceBatchChildRow
    ) -> NativeBatchItemView:
        operation = None
        status = cast(
            Literal["pending", "running", "done", "rejected", "uncertain", "skipped"], child.status
        )
        error_code = child.error_code
        if child.operation_id is not None:
            attempt = await session.get(WorkspaceEditAttemptRow, child.operation_id)
            assert attempt is not None
            operation = self.edits._attempt(attempt).view
            status = operation.status
            error_code = operation.receipt.error_code if operation.receipt is not None else None
        return NativeBatchItemView(
            position=child.position,
            proposal_id=child.proposal_id,
            expected_revision=child.expected_revision,
            path=child.path,
            apply_sequence=child.apply_sequence,
            status=status,
            operation=operation,
            error_code=error_code,
        )

    async def _view(self, session: AsyncSession, row: NativeWorkspaceBatchRow) -> NativeBatchView:
        items = [
            await self._item(session, child)
            for child in await self._children(session, row.batch_id)
        ]
        status = _batch_status(
            row.producer_status,
            len(items),
            sum(item.status == "done" for item in items),
            sum(item.status in {"running", "uncertain"} for item in items),
        )
        return NativeBatchView(
            batch_id=row.batch_id,
            scope_id=row.scope_id,
            request_key=row.request_key,
            snapshot_revision=row.snapshot_revision,
            action=cast(Literal["keep", "undo"], row.action),
            status=status,
            producer_status=cast(
                Literal["running", "finished", "cancelled", "interrupted"], row.producer_status
            ),
            items=items,
            stop_reason=row.stop_reason,
            created_at=row.created_at.replace(tzinfo=UTC),
            updated_at=max(
                [row.updated_at.replace(tzinfo=UTC)]
                + [item.operation.updated_at for item in items if item.operation is not None]
            ),
        )

    async def reserve(
        self,
        scope_id: str,
        request: NativeBatchRequest,
        resource_fingerprint: str,
        workspace_key: str,
    ) -> tuple[NativeBatchView, bool]:
        from .workspace_edits import digest, file_key

        request_hash = digest(
            {
                "request": request.model_dump(mode="json"),
                "resource": resource_fingerprint,
                "workspace_key": workspace_key,
            }
        )
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope_id, writing=False)
            previous = await session.scalar(
                select(NativeWorkspaceBatchRow).where(
                    NativeWorkspaceBatchRow.scope_id == scope_id,
                    NativeWorkspaceBatchRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise ServiceError(
                        "workspace_batch_idempotency_conflict", "批次键已绑定其他请求或资源", 409
                    )
                await self._expire(session, previous)
                return await self._view(session, previous), False
            await self.edits._scope(session, scope_id, writing=True)
            await self.edits._resource(session, scope_id, workspace_key, resource_fingerprint)
            active = list(
                await session.scalars(
                    select(NativeWorkspaceBatchRow).where(
                        NativeWorkspaceBatchRow.scope_id == scope_id,
                        NativeWorkspaceBatchRow.producer_status == "running",
                    )
                )
            )
            for row in active:
                await self._expire(session, row)
                if row.producer_status == "running":
                    raise ServiceError("workspace_batch_busy", "该作用域已有执行中的原生批次", 409)
            count = await session.scalar(select(func.count()).select_from(NativeWorkspaceBatchRow))
            if (count or 0) >= self.max_batches:
                raise ServiceError("workspace_batch_limit", "原生修改批次历史已达上限", 429)
            revision, items = await self.changes.snapshot(session, scope_id)
            if revision != request.snapshot_revision:
                raise ServiceError(
                    "workspace_list_revision_conflict", "原生修改清单快照已变化", 409
                )
            by_id = {item.proposal_id: item for item in items}
            frozen: list[NativeWorkspaceBatchChildRow] = []
            batch_id = str(uuid4())
            allowed = {"applied"} if request.action == "keep" else {"applied", "kept"}
            for target in request.targets:
                item = by_id.get(target.proposal_id)
                if item is None:
                    raise ServiceError(
                        "workspace_proposal_not_found", "批次目标不在该原生清单中", 404
                    )
                if item.revision != target.expected_revision or item.revision >= 9007199254740989:
                    raise ServiceError(
                        "workspace_revision_conflict", "批次目标提案版本已变化或达到上限", 409
                    )
                if item.status not in allowed or item.active_operation_id is not None:
                    raise ServiceError("workspace_change_state", "批次目标当前不允许该动作", 409)
                if (
                    item.resource_fingerprint != resource_fingerprint
                    or item.workspace_key != workspace_key
                ):
                    raise ServiceError("workspace_binding_changed", "批次目标资源版本不匹配", 409)
                proposal = await self.edits._proposal(session, scope_id, item.proposal_id)
                prepared = self.edits._change(proposal).prepared
                frozen.append(
                    NativeWorkspaceBatchChildRow(
                        batch_id=batch_id,
                        proposal_id=item.proposal_id,
                        expected_revision=item.revision,
                        path=item.path,
                        file_key=file_key(prepared),
                        apply_sequence=item.apply_sequence,
                        admission_open=True,
                        status="pending",
                    )
                )
            counts = Counter(child.file_key for child in frozen)
            if any(counts[child.file_key] > 1 and child.apply_sequence is None for child in frozen):
                raise ServiceError(
                    "workspace_batch_order_unknown", "同文件历史顺序未知，须逐项操作", 409
                )
            frozen.sort(key=lambda child: -(child.apply_sequence or 0))
            for position, child in enumerate(frozen):
                child.position = position
                child.request_key = f"{INTERNAL_REQUEST_PREFIX}{batch_id}:{position}"
            row = NativeWorkspaceBatchRow(
                batch_id=batch_id,
                scope_id=scope_id,
                resource_fingerprint=resource_fingerprint,
                workspace_key=workspace_key,
                request_key=request.request_key,
                request_hash=request_hash,
                snapshot_revision=revision,
                action=request.action,
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                token=secrets.token_hex(32),
                producer_status="running",
            )
            session.add(row)
            await session.flush()
            session.add_all(frozen)
            await session.flush()
            return await self._view(session, row), True

    async def execution(self, scope_id: str, batch_id: str) -> NativeBatchExecution:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope_id, writing=True)
            row = await self._row(session, scope_id, batch_id)
            _assert_live(row, self.owner_id)
            children = tuple(
                NativeBatchDispatch(
                    fence=NativeBatchFence(row.batch_id, child.position, row.token),
                    request=WorkspaceChangeAction(
                        proposal_id=child.proposal_id,
                        action=cast(Literal["keep", "undo"], row.action),
                        expected_revision=child.expected_revision,
                        request_key=child.request_key,
                    ),
                    file_key=child.file_key,
                )
                for child in await self._children(session, batch_id)
            )
            return NativeBatchExecution(await self._view(session, row), row.token, children)

    async def get(self, scope_id: str, batch_id: str) -> NativeBatchView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope_id, writing=False)
            row = await self._row(session, scope_id, batch_id)
            await self._expire(session, row)
            return await self._view(session, row)

    async def history(
        self, scope_id: str, *, cursor: str | None = None, limit: int = 32
    ) -> NativeBatchHistoryPage:
        """只读发现原批次；冻结创建上界，状态变化不使 keyset 游标失效。"""
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceError(
                "workspace_batch_history_limit_invalid", "批次历史每页须为1至32项", 422
            )
        decoded = _decode_history_cursor(cursor, scope_id) if cursor is not None else None
        batch, child, attempt = (
            NativeWorkspaceBatchRow,
            NativeWorkspaceBatchChildRow,
            WorkspaceEditAttemptRow,
        )
        # SQLite :memory: 常通过单连接共享；与批次续租/settle共用进程锁，
        # 避免只读会话在事件循环切换时与显式 BEGIN 重入同一连接。
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope_id, writing=False)
            # 先限定计数扫描；超过上限明确失败，不返回截断历史。
            bounded = (
                select(batch.batch_id)
                .where(batch.scope_id == scope_id)
                .limit(_MAX_HISTORY + 1)
                .subquery()
            )
            total = await session.scalar(select(func.count()).select_from(bounded)) or 0
            if total > _MAX_HISTORY:
                raise ServiceError(
                    "workspace_batch_history_too_large", "作用域批次历史超过5000项读取上限", 409
                )
            conditions = [batch.scope_id == scope_id]
            if decoded is not None:
                upper, before = decoded
                # 边界必须来自该作用域中的真实行，不能静默接受越界位置。
                for created, batch_id in (upper, before):
                    if (
                        await session.scalar(
                            select(batch.batch_id).where(
                                batch.scope_id == scope_id,
                                batch.created_at == created,
                                batch.batch_id == batch_id,
                            )
                        )
                        is None
                    ):
                        raise _history_cursor_error()
                conditions.append(
                    or_(
                        batch.created_at < upper[0],
                        and_(batch.created_at == upper[0], batch.batch_id <= upper[1]),
                    )
                )
                total = (
                    await session.scalar(select(func.count()).select_from(batch).where(*conditions))
                    or 0
                )
                conditions.append(
                    or_(
                        batch.created_at < before[0],
                        and_(batch.created_at == before[0], batch.batch_id < before[1]),
                    )
                )
            rows = (
                (
                    await session.execute(
                        select(
                            batch.batch_id,
                            batch.request_key,
                            batch.action,
                            batch.producer_status,
                            batch.snapshot_revision,
                            batch.stop_reason,
                            batch.created_at,
                            batch.updated_at,
                        )
                        .where(*conditions)
                        .order_by(batch.created_at.desc(), batch.batch_id.desc())
                        .limit(limit + 1)
                    )
                )
                .mappings()
                .all()
            )
            if not rows:
                if decoded is not None:
                    raise _history_cursor_error()
                return NativeBatchHistoryPage(
                    scope_id=scope_id, total=0, items=[], next_cursor=None
                )
            upper = (
                decoded[0] if decoded is not None else (rows[0]["created_at"], rows[0]["batch_id"])
            )
            selected = rows[:limit]
            effective_status = func.coalesce(attempt.status, child.status)
            # 仅聚合标量列；不加载 child 对象、文件快照、JSON 回执或恢复材料。
            counts = {
                row.batch_id: row
                for row in await session.execute(
                    select(
                        child.batch_id,
                        func.count().label("item_count"),
                        func.sum(case((effective_status == "done", 1), else_=0)).label(
                            "done_count"
                        ),
                        func.sum(
                            case((effective_status.in_(["running", "uncertain"]), 1), else_=0)
                        ).label("unresolved_count"),
                        func.max(attempt.updated_at).label("operation_updated_at"),
                    )
                    .outerjoin(attempt, attempt.operation_id == child.operation_id)
                    .where(child.batch_id.in_([row["batch_id"] for row in selected]))
                    .group_by(child.batch_id)
                )
            }
            items: list[NativeBatchSummary] = []
            for row in selected:
                count = counts.get(row["batch_id"])
                item_count = count.item_count if count is not None else 0
                done_count = count.done_count if count is not None else 0
                unresolved = count.unresolved_count if count is not None else 0
                updated = row["updated_at"]
                if count is not None and count.operation_updated_at is not None:
                    updated = max(updated, count.operation_updated_at)
                items.append(
                    NativeBatchSummary.model_validate(
                        {
                            **dict(row),
                            "status": _batch_status(
                                row["producer_status"], item_count, done_count, unresolved
                            ),
                            "item_count": item_count,
                            "done_count": done_count,
                            "created_at": row["created_at"].replace(tzinfo=UTC),
                            "updated_at": updated.replace(tzinfo=UTC),
                        }
                    )
                )
            last = selected[-1]
            return NativeBatchHistoryPage(
                scope_id=scope_id,
                total=total,
                items=items,
                next_cursor=(
                    _history_cursor(scope_id, upper, (last["created_at"], last["batch_id"]))
                    if len(rows) > limit
                    else None
                ),
            )

    async def by_key(self, scope_id: str, request_key: str) -> NativeBatchView | None:
        self.edits._key(request_key)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope_id, writing=False)
            row = await session.scalar(
                select(NativeWorkspaceBatchRow).where(
                    NativeWorkspaceBatchRow.scope_id == scope_id,
                    NativeWorkspaceBatchRow.request_key == request_key,
                )
            )
            if row is None:
                return None
            await self._expire(session, row)
            return await self._view(session, row)

    async def renew(self, batch_id: str, token: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            row = await session.get(NativeWorkspaceBatchRow, batch_id)
            if row is None or not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_batch_fence_mismatch", "原生批次执行凭据不匹配", 403)
            _assert_live(row, self.owner_id)
            await self.edits._scope(session, row.scope_id, writing=True)
            row.lease_expires_at = time.time() + self.lease_seconds
            row.updated_at = utc_now()

    async def finish_child(
        self,
        scope_id: str,
        fence: NativeBatchFence,
        *,
        error_code: str | None = None,
        skipped: bool = False,
    ) -> NativeBatchItemView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope_id, writing=False)
            row = await self._row(session, scope_id, fence.batch_id)
            child = await session.get(
                NativeWorkspaceBatchChildRow, (fence.batch_id, fence.position)
            )
            if child is None or not secrets.compare_digest(row.token, fence.token):
                raise ServiceError("workspace_batch_fence_mismatch", "原生批次子项凭据不匹配", 403)
            await self._expire(session, row)
            child.admission_open = False
            if child.operation_id is None and child.status == "pending":
                child.status = "skipped" if skipped else "rejected"
                child.error_code = error_code or (
                    "workspace_batch_dependency_failed"
                    if skipped
                    else "workspace_batch_not_executed"
                )
            row.updated_at = utc_now()
            await session.flush()
            return await self._item(session, child)

    async def finish(self, scope_id: str, batch_id: str, token: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope_id, writing=False)
            row = await self._row(session, scope_id, batch_id)
            if not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_batch_fence_mismatch", "原生批次执行凭据不匹配", 403)
            await self._expire(session, row)
            if row.producer_status == "running":
                _assert_live(row, self.owner_id)
                await self._close(session, row, "finished", None)

    async def cancel(self, scope_id: str, batch_id: str) -> NativeBatchView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            await self.edits._scope(session, scope_id, writing=False)
            row = await self._row(session, scope_id, batch_id)
            await self._expire(session, row)
            if row.producer_status == "running":
                await self._close(session, row, "cancelled", "workspace_batch_cancelled")
            return await self._view(session, row)

    async def recover_interrupted(self) -> int:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            rows = list(
                await session.scalars(
                    select(NativeWorkspaceBatchRow).where(
                        NativeWorkspaceBatchRow.producer_status == "running",
                    )
                )
            )
            recovered = 0
            for row in rows:
                await self._expire(session, row)
                if row.producer_status != "running":
                    recovered += 1
            return recovered
