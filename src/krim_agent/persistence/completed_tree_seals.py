"""首次完整实际树封存；原来源同事务、完整物理条件和冷读零生产者。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from pydantic import JsonValue
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_tree import TreePlanModelSource
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot, WorkspaceTreeSnapshotReader

from .completed_tree_origins import CompletedTreeOriginVerifier, VerifiedTreeOrigin
from .database import WorkspaceTreeSealRow
from .directory_fences import directory_key, entry_key
from .tree_model_sources import run_scope, validate
from .tree_move_fences import assert_namespace_keys_available
from .tree_snapshot_codec import decode_snapshot, encode_snapshot
from .workspace_tree_plans import WorkspaceTreePlanStore


@dataclass(frozen=True)
class SealedTreeOrigin:
    origin: VerifiedTreeOrigin
    created_at: datetime
    model_source: TreePlanModelSource | None


def _corrupt() -> ServiceError:
    return ServiceError("workspace_tree_seal_corrupt", "原完整树封存或来源关联损坏", 409)


class CompletedTreeSealStore:
    def __init__(
        self,
        trees: WorkspaceTreePlanStore,
        *,
        max_seals: int = 500,
        max_snapshot_bytes: int = 536_870_912,
    ) -> None:
        if type(max_seals) is not int or not 1 <= max_seals <= 5000:
            raise ValueError("完整树封存数量上限无效")
        if type(max_snapshot_bytes) is not int or not 1024 <= max_snapshot_bytes <= 2_147_483_648:
            raise ValueError("完整树封存材料总容量无效")
        self.trees, self.database = trees, trees.database
        self.verifier = CompletedTreeOriginVerifier(trees)
        self.max_seals, self.max_snapshot_bytes = max_seals, max_snapshot_bytes

    async def _stored(
        self, sql: AsyncSession, scope: str, binding: WorkspaceBinding, row: WorkspaceTreeSealRow
    ) -> SealedTreeOrigin:
        return await self._stored_material(
            sql,
            scope,
            row,
            workspace_key=binding.key,
            resource_fingerprint=binding.fingerprint,
            workspace_manifest=binding.manifest(),
            workspace_fingerprint=binding.require().fingerprint,
        )

    async def _stored_material(
        self,
        sql: AsyncSession,
        scope: str,
        row: WorkspaceTreeSealRow,
        *,
        workspace_key: str,
        resource_fingerprint: str,
        workspace_manifest: dict[str, JsonValue] | None = None,
        workspace_fingerprint: str | None = None,
    ) -> SealedTreeOrigin:
        if row.scope_id != scope:
            raise ServiceError("workspace_tree_seal_not_found", "封存不在此作用域", 404)
        snapshot = decode_snapshot(row.metadata_value, row.payload)
        origin = await self.verifier.verify_material_in_transaction(
            sql,
            scope,
            row.plan_id,
            snapshot,
            workspace_key=workspace_key,
            resource_fingerprint=resource_fingerprint,
            workspace_manifest=workspace_manifest,
            workspace_fingerprint=workspace_fingerprint,
        )
        expected = {
            "view": origin.view.model_dump(mode="json"),
            "operation_ids": list(origin.operation_ids),
        }
        if (
            row.proof != expected
            or row.command_id != origin.view.command_id
            or row.source_hash != origin.view.source_hash
            or row.snapshot_revision != origin.view.snapshot_revision
            or row.tree_fingerprint != snapshot.fingerprint
            or row.workspace_key != workspace_key
            or row.resource_fingerprint != resource_fingerprint
            or row.topic_id != await self.trees.files._scope(sql, scope, writing=False)
            or (row.run_id is None) != (row.call_id is None)
        ):
            raise _corrupt()
        source = None
        if row.run_id is not None and row.call_id is not None:
            source = TreePlanModelSource(row.run_id, row.call_id)
            validate(source)
            await run_scope(sql, scope, source.run_id, live=False)
        created = (
            row.created_at.replace(tzinfo=UTC)
            if row.created_at.tzinfo is None
            else row.created_at.astimezone(UTC)
        )
        return SealedTreeOrigin(origin, created, source)

    async def get_material_in_transaction(
        self,
        sql: AsyncSession,
        scope: str,
        identifier: str,
        *,
        workspace_key: str,
        resource_fingerprint: str,
    ) -> SealedTreeOrigin | None:
        """供原账本同事务冷核实；不读取物理路径或宣称绑定当前可用。"""
        await self.trees.files._scope(sql, scope, writing=False)
        await self.trees.files._resource(sql, scope, workspace_key, resource_fingerprint)
        row = await sql.get(WorkspaceTreeSealRow, identifier)
        return (
            await self._stored_material(
                sql,
                scope,
                row,
                workspace_key=workspace_key,
                resource_fingerprint=resource_fingerprint,
            )
            if row is not None
            else None
        )

    async def get(
        self, scope: str, binding: WorkspaceBinding, identifier: str
    ) -> SealedTreeOrigin | None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.trees.files._scope(sql, scope, writing=False)
            await self.trees.files._resource(sql, scope, binding.key, binding.fingerprint)
            row = await sql.get(WorkspaceTreeSealRow, identifier)
            return await self._stored(sql, scope, binding, row) if row is not None else None

    async def seal(
        self,
        scope: str,
        binding: WorkspaceBinding,
        identifier: str,
        expected_fingerprint: str,
        snapshot: WorkspaceTreeSnapshot | None,
        *,
        model_source: TreePlanModelSource | None = None,
        cancelled: Callable[[], bool] | None = None,
        authorize: Callable[[], None] | None = None,
    ) -> tuple[SealedTreeOrigin, bool]:
        abort = cancelled or (lambda: False)

        def check() -> None:
            if abort():
                raise ServiceError("workspace_tree_seal_cancelled", "首次完整树封存已取消", 409)
            if authorize is not None:
                authorize()

        check()
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.trees.files._writer(sql)
            topic = await self.trees.files._scope(sql, scope, writing=True)
            await self.trees.files._resource(sql, scope, binding.key, binding.fingerprint)
            previous = await sql.get(WorkspaceTreeSealRow, identifier)
            if previous is not None:
                stored = await self._stored(sql, scope, binding, previous)
                if stored.origin.snapshot.fingerprint != expected_fingerprint:
                    raise ServiceError(
                        "workspace_tree_seal_conflict", "原封存已冻结其他完整版本", 409
                    )
                if model_source is not None and model_source != stored.model_source:
                    raise ServiceError(
                        "workspace_tree_seal_source_conflict", "原封存已绑定其他调用", 409
                    )
                return stored, False
            if not binding.require().writable:
                raise ServiceError("workspace_read_only", "首次封存须原可写授权", 403)
            if snapshot is None or snapshot.fingerprint != expected_fingerprint:
                raise ServiceError("workspace_tree_seal_conflict", "首次封存须原完整观察版本", 409)
            if model_source is not None:
                validate(model_source)
                await run_scope(sql, scope, model_source.run_id, live=True)
                if (
                    await sql.scalar(
                        select(WorkspaceTreeSealRow.plan_id).where(
                            WorkspaceTreeSealRow.run_id == model_source.run_id,
                            WorkspaceTreeSealRow.call_id == model_source.call_id,
                        )
                    )
                    is not None
                ):
                    raise ServiceError(
                        "workspace_tree_seal_call_conflict", "原调用已绑定其他封存", 409
                    )
            origin = await self.verifier.verify_in_transaction(
                sql, scope, binding, identifier, snapshot
            )
            keys = {
                ("entry", entry_key(snapshot.parent_identity, snapshot.root_path)),
                ("directory", directory_key(snapshot.parent_identity)),
                *(
                    ("directory", directory_key(entry.directory.identity))
                    for entry in snapshot.entries
                    if entry.directory is not None
                ),
            }
            await assert_namespace_keys_available(sql, keys)
            metadata, payload = encode_snapshot(snapshot)
            proof = {
                "view": origin.view.model_dump(mode="json"),
                "operation_ids": list(origin.operation_ids),
            }
            count = int(
                await sql.scalar(select(func.count()).select_from(WorkspaceTreeSealRow)) or 0
            )
            used = int(
                await sql.scalar(
                    select(
                        func.coalesce(
                            func.sum(
                                func.length(WorkspaceTreeSealRow.payload)
                                + func.length(WorkspaceTreeSealRow.metadata_value)
                                + func.length(WorkspaceTreeSealRow.proof)
                            ),
                            0,
                        )
                    )
                )
                or 0
            )
            if (
                count >= self.max_seals
                or used + len(payload) + len(json.dumps(metadata)) + len(json.dumps(proof))
                > self.max_snapshot_bytes
            ):
                raise ServiceError("workspace_tree_seal_limit", "完整树封存历史或材料容量已满", 413)
            check()
            await asyncio.to_thread(
                WorkspaceTreeSnapshotReader(binding.require()).verify,
                snapshot,
                authorize=check,
                cancelled=abort,
            )
            check()
            if model_source is not None:
                await run_scope(sql, scope, model_source.run_id, live=True)
            await self.trees.files._scope(sql, scope, writing=True)
            await self.trees.files._resource(sql, scope, binding.key, binding.fingerprint)
            row = WorkspaceTreeSealRow(
                plan_id=identifier,
                scope_id=scope,
                topic_id=topic,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                command_id=origin.view.command_id,
                source_hash=origin.view.source_hash,
                snapshot_revision=origin.view.snapshot_revision,
                tree_fingerprint=snapshot.fingerprint,
                metadata_value=cast(dict[str, JsonValue], metadata),
                payload=payload,
                proof=cast(dict[str, JsonValue], proof),
                run_id=model_source.run_id if model_source else None,
                call_id=model_source.call_id if model_source else None,
                created_at=utc_now(),
            )
            sql.add(row)
            await sql.flush()
            return await self._stored(sql, scope, binding, row), True
