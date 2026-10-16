"""缺失基线回退的原完成事实、当前批准版本与独立来源核实。"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Literal, cast

from pydantic import Field, JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory_restore import FINGERPRINT
from krim_agent.workspace_missing_directory_restore import DirectoryAbsenceSummary
from krim_agent.workspace_missing_directory_rollback import (
    MissingDirectoryRollbackSource,
    MissingDirectoryRollbackState,
    MissingDirectoryRollbackView,
)
from krim_agent.workspace_tree_move import (
    TreeMoveModelSource,
    TreeMovePreviewRequest,
    TreeMovePreviewView,
)
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot
from krim_agent.workspace_tree_writer import PreparedTreeMove

from .database import (
    WorkspaceMissingDirectoryRollbackRow,
    WorkspaceTreeMoveModelSourceRow,
    WorkspaceTreeMoveProposalRow,
)
from .missing_directory_restore_codec import decode_effect
from .tree_exchange_codec import _Model, _Versioned
from .workspace_directory_captures import WorkspaceDirectoryCaptureStore
from .workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from .workspace_edits import WorkspaceEditStore, digest
from .workspace_missing_directory_restores import WorkspaceMissingDirectoryRestoreStore

ID = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
MAX_METADATA = 8192


def _error(kind: str, message: str, status: int = 409) -> ServiceError:
    return ServiceError("workspace_missing_directory_rollback_" + kind, message, status)


def corrupt() -> ServiceError:
    return _error("corrupt", "原缺失基线回退来源、标记或完整材料损坏")


class _Source(_Model):
    run_id: str = Field(pattern=ID)
    call_id: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")


class _Origin(_Versioned):
    kind: Literal["workspace-missing-directory-baseline-rollback"]
    source_plan_id: str = Field(pattern=ID)
    source_operation_id: str = Field(pattern=ID)
    scope_id: str = Field(min_length=1, max_length=80)
    workspace_key: str = Field(min_length=1, max_length=64)
    resource_fingerprint: str = Field(pattern=FINGERPRINT)
    workspace_fingerprint: str = Field(pattern=FINGERPRINT)
    origin_request_hash: str = Field(pattern=FINGERPRINT)
    origin_execute_hash: str = Field(pattern=FINGERPRINT)
    origin_absence_hash: str = Field(pattern=FINGERPRINT)
    before_absence_fingerprint: str = Field(pattern=FINGERPRINT)
    published_fingerprint: str = Field(pattern=FINGERPRINT)
    approved_fingerprint: str = Field(pattern=FINGERPRINT)
    preview_model_source: _Source | None


def decode_origin(value: object, fingerprint: str) -> _Origin:
    try:
        if type(value) is not dict or len(json.dumps(value).encode()) > MAX_METADATA:
            raise corrupt()
        result = _Origin.model_validate(value)
        if result.model_dump(mode="json") != value or digest(value) != fingerprint:
            raise corrupt()
        return result
    except (ValueError, TypeError, AttributeError, RecursionError, ServiceError):
        raise corrupt() from None


def preview_hash(
    request: TreeMovePreviewRequest, resource: str, plan_id: str | None, source_hash: str | None
) -> str:
    value: dict[str, object] = {"request": request.model_dump(mode="json"), "resource": resource}
    if (plan_id is None) != (source_hash is None):
        raise corrupt()
    if plan_id is not None:
        if source_hash is None or len(source_hash) != 64:
            raise corrupt()
        value["missing_directory_rollback"] = {"plan_id": plan_id, "source_hash": source_hash}
    return digest(value)


def _date(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class MissingDirectoryRollbackSources:
    def __init__(self, edits: WorkspaceEditStore) -> None:
        self.edits = edits
        self.restores = WorkspaceMissingDirectoryRestoreStore(
            WorkspaceDirectoryCaptureStore(WorkspaceDirectoryCheckpointStore(edits))
        )

    async def _origin(
        self,
        sql: AsyncSession,
        scope: str,
        source: MissingDirectoryRollbackSource,
        request: TreeMovePreviewRequest,
        key: str,
        resource: str,
        model_source: TreeMoveModelSource | None,
    ) -> tuple[dict[str, JsonValue], WorkspaceTreeSnapshot]:
        if (
            type(source) is not MissingDirectoryRollbackSource
            or type(source.plan_id) is not str
            or re.fullmatch(ID, source.plan_id) is None
        ):
            raise _error("source", "缺失回退须使用原独立来源", 403)
        row, attempt, _ = await self.restores._base(sql, scope, source.plan_id)
        if row.phase != "finished" or attempt is None or attempt.status != "done":
            raise _error("incomplete", "回退须引用原已完成缺失恢复")
        prepared = self.restores._prepared(row)
        effect = decode_effect(prepared, attempt.effect_metadata)
        after = effect.observed_after
        if after is None or effect.receipt.status != "done":
            raise corrupt()
        if (row.workspace_key, row.resource_fingerprint, row.root_path) != (
            key,
            resource,
            request.source_path,
        ):
            raise _error("source", "原缺失基线不属于当前Scope/资源/根", 403)
        metadata: dict[str, object] = {
            "version": 1,
            "kind": "workspace-missing-directory-baseline-rollback",
            "source_plan_id": row.plan_id,
            "source_operation_id": attempt.operation_id,
            "scope_id": scope,
            "workspace_key": key,
            "resource_fingerprint": resource,
            "workspace_fingerprint": row.workspace_fingerprint,
            "origin_request_hash": row.request_hash,
            "origin_execute_hash": row.execute_hash,
            "origin_absence_hash": row.absence_hash,
            "before_absence_fingerprint": row.absence_fingerprint,
            "published_fingerprint": after.fingerprint,
            "approved_fingerprint": request.expected_fingerprint,
            "preview_model_source": {
                "run_id": model_source.run_id,
                "call_id": model_source.call_id,
            }
            if model_source is not None
            else None,
        }
        result = cast("dict[str, JsonValue]", metadata)
        decode_origin(result, digest(result))
        return result, after

    async def candidate(
        self,
        sql: AsyncSession,
        scope: str,
        key: str,
        resource: str,
        source: MissingDirectoryRollbackSource,
        request: TreeMovePreviewRequest,
        prepared: PreparedTreeMove | None,
        model_source: TreeMoveModelSource | None,
    ) -> tuple[dict[str, JsonValue], str]:
        metadata, after = await self._origin(
            sql, scope, source, request, key, resource, model_source
        )
        if prepared is not None:
            self._same_root(prepared, after)
        return metadata, digest(metadata)

    @staticmethod
    def _same_root(prepared: PreparedTreeMove, after: WorkspaceTreeSnapshot) -> None:
        if (
            type(prepared) is not PreparedTreeMove
            or not prepared.before.entries
            or not after.entries
        ):
            raise corrupt()
        root, original_root = prepared.before.entries[0].directory, after.entries[0].directory
        if (
            root is None
            or original_root is None
            or root.identity != original_root.identity
            or prepared.before.root_path != after.root_path
            or prepared.before.parent_identity != after.parent_identity
            or prepared.before.workspace_fingerprint != after.workspace_fingerprint
        ):
            raise _error("root_changed", "当前批准树须仍为原发布根和原父，不能冒领替换的根")

    async def validate(
        self,
        sql: AsyncSession,
        row: WorkspaceTreeMoveProposalRow,
        prepared: PreparedTreeMove,
    ) -> _Origin | None:
        link = await sql.get(WorkspaceMissingDirectoryRollbackRow, row.proposal_id)
        if row.missing_restore_plan_id is None and row.missing_restore_hash is None:
            if link is not None:
                raise corrupt()
            return None
        if row.generated_plan_id is not None or link is None or row.missing_restore_hash is None:
            raise corrupt()
        origin = decode_origin(link.source_metadata, row.missing_restore_hash)
        model_source = (
            TreeMoveModelSource(
                origin.preview_model_source.run_id, origin.preview_model_source.call_id
            )
            if origin.preview_model_source is not None
            else None
        )
        try:
            request = TreeMovePreviewRequest.model_validate(row.request)
            expected, after = await self._origin(
                sql,
                row.scope_id,
                MissingDirectoryRollbackSource(origin.source_plan_id),
                request,
                row.workspace_key,
                row.resource_fingerprint,
                model_source,
            )
            self._same_root(prepared, after)
            sources = list(
                await sql.scalars(
                    select(WorkspaceTreeMoveModelSourceRow).where(
                        WorkspaceTreeMoveModelSourceRow.proposal_id == row.proposal_id,
                        WorkspaceTreeMoveModelSourceRow.kind == "preview",
                    )
                )
            )
            if (
                link.source_plan_id != row.missing_restore_plan_id
                or origin.source_plan_id != row.missing_restore_plan_id
                or link.source_operation_id != origin.source_operation_id
                or link.scope_id != row.scope_id
                or origin.scope_id != row.scope_id
                or link.source_hash != row.missing_restore_hash
                or expected != link.source_metadata
                or prepared.before.fingerprint != origin.approved_fingerprint
                or _date(link.created_at) != _date(row.created_at)
                or len(sources) != int(model_source is not None)
                or model_source is not None
                and (sources[0].run_id, sources[0].call_id, sources[0].operation_id)
                != (model_source.run_id, model_source.call_id, None)
            ):
                raise corrupt()
        except (ValueError, TypeError, AttributeError, IndexError, ServiceError):
            raise corrupt() from None
        return origin

    @staticmethod
    def view(
        origin: _Origin, row: WorkspaceTreeMoveProposalRow, move: TreeMovePreviewView
    ) -> MissingDirectoryRollbackView:
        state = {
            "proposed": "awaiting_approval",
            "applying": "preserving",
            "applied": "preserved",
            "undoing": "restoring",
            "undone": "restored",
            "keeping": "keeping",
            "kept": "kept",
            "rejected": "rejected",
            "uncertain": "uncertain",
            "cancelled": "cancelled",
        }[row.status]
        return MissingDirectoryRollbackView(
            source_plan_id=origin.source_plan_id,
            source_operation_id=origin.source_operation_id,
            source_hash=cast(str, row.missing_restore_hash),
            baseline=DirectoryAbsenceSummary(fingerprint=origin.before_absence_fingerprint),
            published_fingerprint=origin.published_fingerprint,
            approved_fingerprint=origin.approved_fingerprint,
            rollback_state=cast(MissingDirectoryRollbackState, state),
            move=move,
        )
