"""回收只绑定真实已完成Missing来源；私有Stage/完整原材料不接受调用者上传。"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Literal, cast

from pydantic import Field, JsonValue
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_container_retention import (
    ContainerRetentionSource,
    StoredContainerRetentionSource,
)
from krim_agent.workspace_directory_restore import FINGERPRINT, IDENTIFIER

from .directory_restore_codec import decode_stage, encode_stage
from .directory_restore_sources import source_value
from .missing_directory_restore_codec import decode_effect
from .tree_exchange_codec import _Model, _Versioned
from .workspace_edits import digest
from .workspace_missing_directory_restores import WorkspaceMissingDirectoryRestoreStore

MAX_METADATA = 16384


def error(code: str, text: str, status: int = 409) -> ServiceError:
    return ServiceError("workspace_container_retention_" + code, text, status)


def corrupt() -> ServiceError:
    return error("corrupt", "原容器回收来源、Stage或材料关联损坏")


class _Source(_Model):
    run_id: str = Field(pattern=IDENTIFIER, max_length=36)
    call_id: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")


class _Origin(_Versioned):
    kind: Literal["workspace-empty-container-retention-source"]
    source_plan_id: str = Field(pattern=IDENTIFIER, max_length=36)
    source_operation_id: str = Field(pattern=IDENTIFIER, max_length=36)
    scope_id: str = Field(min_length=1, max_length=80)
    topic_id: str = Field(pattern=IDENTIFIER, max_length=36)
    root_path: str = Field(min_length=1, max_length=512)
    workspace_key: str = Field(min_length=1, max_length=64)
    resource_fingerprint: str = Field(pattern=FINGERPRINT)
    workspace_fingerprint: str = Field(pattern=FINGERPRINT)
    restore_request_hash: str = Field(pattern=FINGERPRINT)
    restore_execute_hash: str = Field(pattern=FINGERPRINT)
    restore_absence_hash: str = Field(pattern=FINGERPRINT)
    restore_prepared_hash: str = Field(pattern=FINGERPRINT)
    restore_effect_hash: str = Field(pattern=FINGERPRINT)
    target_material_id: str = Field(pattern=IDENTIFIER, max_length=36)
    published_fingerprint: str = Field(pattern=FINGERPRINT)
    preview_source: _Source | None
    execute_source: _Source | None
    verify_source: _Source | None
    stage_metadata: dict[str, JsonValue]


def decode_origin(value: object, expected_hash: str) -> _Origin:
    try:
        if (
            type(value) is not dict
            or len(json.dumps(value, ensure_ascii=True).encode()) > MAX_METADATA
        ):
            raise corrupt()
        result = _Origin.model_validate(value)
        if result.model_dump(mode="json") != value or digest(value) != expected_hash:
            raise corrupt()
        stage = decode_stage(result.stage_metadata, result.restore_request_hash)
        if (
            stage.root_path != result.root_path
            or result.source_plan_id == result.source_operation_id
        ):
            raise corrupt()
        return result
    except (ValueError, TypeError, AttributeError, RecursionError, ServiceError):
        raise corrupt() from None


class ContainerRetentionSources:
    def __init__(self, restores: WorkspaceMissingDirectoryRestoreStore) -> None:
        self.restores = restores

    async def read(
        self,
        sql: AsyncSession,
        scope: str,
        source_plan_id: str,
        *,
        workspace_key: str,
        resource_fingerprint: str,
        workspace_fingerprint: str,
    ) -> StoredContainerRetentionSource:
        row, attempt, origin = await self.restores._base(sql, scope, source_plan_id)
        if row.phase != "finished" or attempt is None or attempt.status != "done":
            raise error("incomplete", "空容器回收须引用原已完成缺失恢复")
        if (row.workspace_key, row.resource_fingerprint, row.workspace_fingerprint) != (
            workspace_key,
            resource_fingerprint,
            workspace_fingerprint,
        ):
            raise error("source", "原回收来源不属于当前授权工作区资源", 403)
        try:
            prepared = self.restores._prepared(row)
            effect = decode_effect(prepared, attempt.effect_metadata)
            after = effect.observed_after
            if after is None or effect.receipt.status != "done" or not row.cleanup_pending:
                raise corrupt()
            stage = self.restores._stage(row)
            if prepared.stage != stage or row.prepared_metadata is None:
                raise corrupt()
            metadata: dict[str, object] = {
                "version": 1,
                "kind": "workspace-empty-container-retention-source",
                "source_plan_id": row.plan_id,
                "source_operation_id": attempt.operation_id,
                "scope_id": scope,
                "topic_id": row.topic_id,
                "root_path": row.root_path,
                "workspace_key": row.workspace_key,
                "resource_fingerprint": row.resource_fingerprint,
                "workspace_fingerprint": row.workspace_fingerprint,
                "restore_request_hash": row.request_hash,
                "restore_execute_hash": row.execute_hash,
                "restore_absence_hash": row.absence_hash,
                "restore_prepared_hash": row.prepared_metadata["material_sha256"],
                "restore_effect_hash": digest(attempt.effect_metadata),
                "target_material_id": row.target_material_id,
                "published_fingerprint": after.fingerprint,
                "preview_source": source_value(origin.preview),
                "execute_source": source_value(origin.execute),
                "verify_source": source_value(origin.verify),
                "stage_metadata": encode_stage(stage, row.request_hash),
            }
            source_hash = digest(metadata)
            value = decode_origin(metadata, source_hash)
            summary = ContainerRetentionSource(
                source_plan_id=value.source_plan_id,
                source_operation_id=value.source_operation_id,
                scope_id=value.scope_id,
                topic_id=value.topic_id,
                root_path=value.root_path,
                workspace_key=value.workspace_key,
                resource_fingerprint=value.resource_fingerprint,
                source_hash=source_hash,
                published_fingerprint=value.published_fingerprint,
            )
            return StoredContainerRetentionSource(
                summary, replace(stage), cast(dict[str, object], value.model_dump(mode="json"))
            )
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError, ServiceError):
            raise corrupt() from None
