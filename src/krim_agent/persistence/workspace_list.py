"""原生修改清单的只读投影；不复制文件状态，不读取私有内容。"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from typing import TYPE_CHECKING

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_list import NativeChangeItem, NativeChangePage, NativeListFilter

from .database import WorkspaceEditProposalRow, WorkspaceFileEffectRow

if TYPE_CHECKING:
    from .workspace_edits import WorkspaceEditStore


_MAX_PROPOSALS = 5000
_MAX_CURSOR_LENGTH = 1024
_VISIBLE_STATUSES = frozenset(
    {"applying", "applied", "undoing", "undone", "keeping", "kept", "uncertain"}
)


def _encode_cursor(scope_id: str, filter: NativeListFilter, revision: str, position: int) -> str:
    value = json.dumps(
        {"v": 1, "scope": scope_id, "filter": filter, "revision": revision, "position": position},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _decode_cursor(cursor: str, scope_id: str, filter: NativeListFilter) -> tuple[str, int]:
    def invalid() -> ServiceError:
        return ServiceError("workspace_list_cursor_invalid", "修改清单游标无效或作用域不匹配", 422)

    if (
        not isinstance(cursor, str)
        or not 1 <= len(cursor) <= _MAX_CURSOR_LENGTH
        or re.fullmatch(r"[A-Za-z0-9_-]+", cursor) is None
    ):
        raise invalid()
    try:
        encoded = cursor.encode()
        raw = base64.b64decode(encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        value = json.loads(raw)
    except (ValueError, UnicodeError, binascii.Error):
        raise invalid() from None
    if (
        not isinstance(value, dict)
        or set(value) != {"v", "scope", "filter", "revision", "position"}
        or type(value["v"]) is not int
        or value["v"] != 1
        or value["scope"] != scope_id
        or value["filter"] != filter
        or not isinstance(value["revision"], str)
        or re.fullmatch(r"[0-9a-f]{64}", value["revision"]) is None
        or type(value["position"]) is not int
        or not 1 <= value["position"] < _MAX_PROPOSALS
    ):
        raise invalid()
    revision, position = value["revision"], value["position"]
    # 拒绝重复JSON键、非标准编码及含混类型，游标只接受服务生成的规范格式。
    if cursor != _encode_cursor(scope_id, filter, revision, position):
        raise invalid()
    return revision, position


class NativeChangeListStore:
    def __init__(self, edits: WorkspaceEditStore) -> None:
        self.edits = edits

    async def snapshot(
        self, session: AsyncSession, scope_id: str
    ) -> tuple[str, list[NativeChangeItem]]:
        """在调用方事务内单次读取；调用方负责检查scope的访问资格。"""
        proposal, effect = WorkspaceEditProposalRow, WorkspaceFileEffectRow
        rows = (
            (
                await session.execute(
                    select(
                        proposal.proposal_id,
                        proposal.revision,
                        proposal.path,
                        proposal.preview["operation"].as_string().label("operation"),
                        proposal.status,
                        proposal.workspace_key,
                        proposal.resource_fingerprint,
                        proposal.active_operation_id,
                        proposal.request_hash,
                        proposal.expires_at,
                        effect.effect_sequence.label("apply_sequence"),
                    )
                    .outerjoin(
                        effect,
                        and_(
                            effect.proposal_id == proposal.proposal_id,
                            effect.scope_id == proposal.scope_id,
                            effect.action == "apply",
                        ),
                    )
                    .where(proposal.scope_id == scope_id)
                    .order_by(proposal.proposal_id)
                    .limit(_MAX_PROPOSALS + 1)
                )
            )
            .mappings()
            .all()
        )
        if len(rows) > _MAX_PROPOSALS:
            raise ServiceError("workspace_list_too_large", "作用域修改提案超过5000项读取上限", 409)
        # 未展示的proposed/rejected也进入摘要，不能让分页和批次冻结遗漏新增事实。
        revision = hashlib.sha256(
            json.dumps(
                {"version": 1, "scope": scope_id, "proposals": [dict(row) for row in rows]},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        items = [
            NativeChangeItem.model_validate(
                {
                    **{
                        key: value
                        for key, value in row.items()
                        if key not in {"request_hash", "expires_at"}
                    },
                    "order_known": row["apply_sequence"] is not None,
                }
            )
            for row in rows
            if row["status"] in _VISIBLE_STATUSES
        ]
        # 历史无效果序的记录排在已知顺序后；ID仅用于稳定分页，不代表发生时间。
        items.sort(
            key=lambda item: (
                item.apply_sequence is None,
                -(item.apply_sequence or 0),
                item.proposal_id,
            )
        )
        return revision, items

    async def page(
        self,
        scope_id: str,
        *,
        filter: NativeListFilter = "active",
        cursor: str | None = None,
        limit: int = 32,
    ) -> NativeChangePage:
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceError("workspace_list_limit_invalid", "修改清单每页须为1至32项", 422)
        if filter not in {"active", "history", "all"}:
            raise ServiceError("workspace_list_filter_invalid", "修改清单筛选无效", 422)
        decoded = _decode_cursor(cursor, scope_id, filter) if cursor is not None else None
        async with self.edits.database.sessions() as session:
            await self.edits._scope(session, scope_id, writing=False)
            revision, items = await self.snapshot(session, scope_id)
        if decoded is not None and decoded[0] != revision:
            raise ServiceError("workspace_list_stale", "修改清单已变化，请从第一页重新读取", 409)
        if filter == "active":
            items = [item for item in items if item.status != "undone"]
        elif filter == "history":
            items = [item for item in items if item.status == "undone"]
        position = decoded[1] if decoded is not None else 0
        if decoded is not None and position >= len(items):
            raise ServiceError("workspace_list_cursor_invalid", "修改清单游标位置越界", 422)
        selected = items[position : position + limit]
        next_position = position + len(selected)
        return NativeChangePage(
            scope_id=scope_id,
            snapshot_revision=revision,
            filter=filter,
            total=len(items),
            items=selected,
            next_cursor=(
                _encode_cursor(scope_id, filter, revision, next_position)
                if next_position < len(items)
                else None
            ),
        )
