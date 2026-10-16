"""本次GC模型来源；清单与独立表双向校验，不能丢失标记后降为Host。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal

from pydantic import Field, JsonValue
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace_checkpoint import CheckpointModelSource
from krim_agent.workspace_directory_restore import FINGERPRINT, IDENTIFIER

from .container_retention_sources import corrupt, error
from .database import WorkspaceContainerRetentionModelSourceRow as Source
from .database import WorkspaceContainerRetentionRow as Row
from .directory_restore_sources import assert_new_call, valid_source
from .workspace_edits import WorkspaceEditStore, digest

MAX_MODEL_METADATA = 65536


def model_source(value: CheckpointModelSource | None) -> CheckpointModelSource | None:
    try:
        source = valid_source(value)
        if source is not None:
            if re.fullmatch(IDENTIFIER, source.run_id) is None:
                raise ValueError("原Run无效")
            source.call_id.encode("utf-8")
        return source
    except (ServiceError, ValueError, TypeError, AttributeError):
        raise error("model_context", "回收须保留原模型Run/call来源", 403) from None


class _Source(StrictModel):
    run_id: str = Field(pattern=IDENTIFIER, max_length=36)
    call_id: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")

    def source(self) -> CheckpointModelSource:
        value = model_source(CheckpointModelSource(self.run_id, self.call_id))
        assert value is not None
        return value


class _Verification(StrictModel):
    epoch: int = Field(ge=1, le=128)
    request_hash: str = Field(pattern=FINGERPRINT)
    source: _Source


class _Metadata(StrictModel):
    version: Literal[1]
    kind: Literal["workspace-container-retention-model-sources"]
    operation_id: str = Field(pattern=IDENTIFIER, max_length=36)
    scope_id: str = Field(min_length=1, max_length=80)
    source_hash: str = Field(pattern=FINGERPRINT)
    request_hash: str = Field(pattern=FINGERPRINT)
    remove: _Source | None
    verification: list[_Verification] = Field(max_length=128)


@dataclass(frozen=True)
class ContainerRetentionModelSources:
    remove: CheckpointModelSource | None
    verify: CheckpointModelSource | None
    verification: tuple[tuple[int, CheckpointModelSource], ...] = ()

    def at(self, epoch: int) -> CheckpointModelSource | None:
        if epoch == 0:
            return self.remove
        return next((source for number, source in self.verification if number == epoch), None)


def _metadata(row: Row) -> _Metadata | None:
    if row.model_sources_metadata is None:
        return None
    try:
        if (
            type(row.model_sources_metadata) is not dict
            or len(json.dumps(row.model_sources_metadata).encode()) > MAX_MODEL_METADATA
        ):
            raise corrupt()
        value = dict(row.model_sources_metadata)
        checksum = value.pop("material_sha256")
        if type(value.get("version")) is not int:
            raise corrupt()
        decoded = _Metadata.model_validate(value)
        if (
            checksum != digest(value)
            or decoded.model_dump(mode="json") != value
            or (decoded.operation_id, decoded.scope_id, decoded.source_hash, decoded.request_hash)
            != (row.operation_id, row.scope_id, row.source_hash, row.request_hash)
            or not decoded.scope_id.startswith("session:")
            or decoded.remove is None
            and not decoded.verification
        ):
            raise corrupt()
        return decoded
    except (TypeError, ValueError, AttributeError, KeyError):
        raise corrupt() from None


async def sources(
    sql: AsyncSession,
    edits: WorkspaceEditStore,
    row: Row,
    history: list[dict[str, JsonValue]],
) -> ContainerRetentionModelSources:
    stored = list(await sql.scalars(select(Source).where(Source.operation_id == row.operation_id)))
    metadata = _metadata(row)
    expected: set[tuple[str, int, str, str]] = set()
    current = original = None
    verification = []
    try:
        if metadata is not None:
            if metadata.remove is not None:
                original = metadata.remove.source()
                expected.add(("remove", 0, original.run_id, original.call_id))
            epochs = [item.epoch for item in metadata.verification]
            if epochs != sorted(set(epochs)):
                raise corrupt()
            for item in metadata.verification:
                if item.epoch > len(history) or (
                    item.request_hash != history[item.epoch - 1]["request_hash"]
                ):
                    raise corrupt()
                value = item.source.source()
                expected.add(("verify", item.epoch, value.run_id, value.call_id))
                verification.append((item.epoch, value))
                if item.epoch == row.epoch:
                    current = value
        actual = {(item.phase, item.epoch, item.run_id, item.call_id) for item in stored}
        if len(actual) != len(stored) or actual != expected:
            raise corrupt()
        for run_id in {item[2] for item in expected}:
            await edits._scope(sql, row.scope_id, writing=False, run_id=run_id)
        return ContainerRetentionModelSources(original, current, tuple(verification))
    except (ValueError, TypeError, AttributeError, IndexError, ServiceError):
        raise corrupt() from None


async def admit(
    sql: AsyncSession,
    edits: WorkspaceEditStore,
    row: Row,
    source: CheckpointModelSource | None,
    *,
    max_sources: int,
) -> None:
    source = model_source(source)
    if source is None:
        return
    if not row.scope_id.startswith("session:"):
        raise error("model_context", "模型回收须属于原session", 403)
    await edits._scope(sql, row.scope_id, writing=True, run_id=source.run_id)
    await assert_new_call(sql, source)
    if int(await sql.scalar(select(func.count()).select_from(Source)) or 0) >= max_sources:
        raise error("capacity", "回收模型来源数量已满", 429)
    metadata = _metadata(row)
    if metadata is None:
        metadata = _Metadata(
            version=1,
            kind="workspace-container-retention-model-sources",
            operation_id=row.operation_id,
            scope_id=row.scope_id,
            source_hash=row.source_hash,
            request_hash=row.request_hash,
            remove=None,
            verification=[],
        )
    value = _Source(run_id=source.run_id, call_id=source.call_id)
    if row.active_kind == "remove":
        if metadata.remove is not None or row.epoch != 0:
            raise error("model_context", "原回收模型来源不能替换", 403)
        metadata.remove = value
    else:
        metadata.verification.append(
            _Verification(
                epoch=row.epoch,
                request_hash=str(row.verification_metadata[row.epoch - 1]["request_hash"]),
                source=value,
            )
        )
    encoded = metadata.model_dump(mode="json")
    encoded["material_sha256"] = digest(encoded)
    if len(json.dumps(encoded).encode()) > MAX_MODEL_METADATA:
        raise error("capacity", "回收模型来源清单超过64KiB", 429)
    row.model_sources_metadata = encoded
    sql.add(
        Source(
            run_id=source.run_id,
            call_id=source.call_id,
            operation_id=row.operation_id,
            phase=row.active_kind,
            epoch=row.epoch,
        )
    )
