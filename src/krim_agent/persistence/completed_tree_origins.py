"""完整已生成树的原来源核实；不补造旧inode、不执行或持久新副作用。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal

from pydantic import ConfigDict, Field, TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_directory import DirectorySnapshot
from krim_agent.workspace_tree import TreePlanRequest
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshot
from krim_agent.workspace_writer import observed_state

from .command_proposals import artifact_bytes, load_source
from .database import (
    WorkspaceDirectoryAttemptRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceFileEffectRow,
)
from .tree_fences import assert_source
from .workspace_directories import decode
from .workspace_edits import WorkspaceEditStore, digest
from .workspace_tree_plans import WorkspaceTreePlanStore


class CompletedTreeOriginView(StrictModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    plan_id: str
    scope_id: str
    command_id: str
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome: Literal["completed", "failed", "output_limit"]
    accept_failed: bool
    snapshot_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_path: str
    workspace_key: str
    resource_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    tree_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry_count: int = Field(ge=1, le=16384)
    byte_size: int = Field(ge=0, le=134_217_728)
    proof_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class VerifiedTreeOrigin:
    view: CompletedTreeOriginView
    snapshot: WorkspaceTreeSnapshot
    operation_ids: tuple[str, ...]


def _corrupt() -> ServiceError:
    return ServiceError("workspace_tree_origin_corrupt", "原完整树来源或实际观察不匹配", 409)


class CompletedTreeOriginVerifier:
    def __init__(self, trees: WorkspaceTreePlanStore) -> None:
        self.trees, self.database = trees, trees.database

    async def verify(
        self,
        scope: str,
        binding: WorkspaceBinding,
        identifier: str,
        snapshot: WorkspaceTreeSnapshot,
    ) -> VerifiedTreeOrigin:
        """可信Reader观察的来源前置；后续封存须调用同一写事务内的verify_in_transaction。"""
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            return await self.verify_in_transaction(sql, scope, binding, identifier, snapshot)

    async def verify_in_transaction(
        self,
        sql: AsyncSession,
        scope: str,
        binding: WorkspaceBinding,
        identifier: str,
        snapshot: WorkspaceTreeSnapshot,
    ) -> VerifiedTreeOrigin:
        try:
            row = await self.trees._row(sql, scope, identifier)
            await self.trees.files._resource(sql, scope, binding.key, binding.fingerprint)
            await assert_source(sql, row)
            view = await self.trees._view(sql, row)
            if view.producer_status != "finished" or view.status != "done":
                raise ServiceError(
                    "workspace_tree_origin_incomplete", "原树须全部实际完成后核实来源", 409
                )
            root_material = row.frozen["root_prepared"]
            if not isinstance(root_material, dict):
                raise _corrupt()
            if (
                row.workspace_key != binding.key
                or row.resource_fingerprint != binding.fingerprint
                or row.frozen["workspace"] != binding.manifest()
                or snapshot.root_path != row.root_path
                or snapshot.workspace_fingerprint != binding.require().fingerprint
                or snapshot.parent_identity != decode(root_material).parent_identity
            ):
                raise _corrupt()
            source = await load_source(sql, scope, row.command_id)
            request = TreePlanRequest.model_validate(row.frozen["request"])
            if source.outcome != "completed" and not request.accept_failed:
                raise _corrupt()
            steps = await self.trees._rows(sql, row)
            by_path = {entry.path: entry for entry in snapshot.entries}
            if (
                len(by_path) != len(snapshot.entries)
                or set(by_path) != {step.path for step in steps}
                or [entry.path for entry in snapshot.entries]
                != sorted(by_path, key=lambda path: tuple(path.split("/")))
            ):
                raise _corrupt()
            operation_ids = []
            for step in steps:
                projected = await self.trees._step_view(sql, row, step)
                if projected.status != "done" or projected.operation is None:
                    raise _corrupt()
                operation_id = projected.operation.operation_id
                operation_ids.append(operation_id)
                entry = by_path[step.path]
                parent = by_path.get(step.path.rpartition("/")[0])
                if step.source == "directory":
                    attempt = await sql.get(WorkspaceDirectoryAttemptRow, operation_id)
                    if attempt is None or entry.directory is None or entry.file is not None:
                        raise _corrupt()
                    observed = TypeAdapter(DirectorySnapshot).validate_json(
                        json.dumps(attempt.observed_after), strict=True
                    )
                    current = entry.directory
                    if (
                        current.identity != observed.identity
                        or current.mode != observed.mode
                        or current.uid != observed.uid
                        or current.gid != observed.gid
                    ):
                        raise _corrupt()
                    prepared_parent = decode(attempt.prepared).parent_identity
                else:
                    file_attempt = await sql.get(WorkspaceEditAttemptRow, operation_id)
                    proposal = await sql.get(WorkspaceEditProposalRow, step.file_proposal_id)
                    effect = await sql.scalar(
                        select(WorkspaceFileEffectRow).where(
                            WorkspaceFileEffectRow.operation_id == operation_id
                        )
                    )
                    if (
                        file_attempt is None
                        or proposal is None
                        or effect is None
                        or entry.file is None
                        or entry.directory is not None
                    ):
                        raise _corrupt()
                    node = WorkspaceEditStore._effect_node(effect, file_attempt, proposal)
                    expected_bytes = await artifact_bytes(sql, source, step.path)
                    if (
                        node is None
                        or effect.action != "apply"
                        or effect.scope_id != scope
                        or effect.resource_fingerprint != binding.fingerprint
                        or expected_bytes is None
                        or entry.file.data != expected_bytes
                        or observed_state(entry.file) != node.observed
                        or entry.file.mode != step.mode
                    ):
                        raise _corrupt()
                    prepared_parent = node.prepared.parent_identity
                if prepared_parent != (
                    snapshot.parent_identity
                    if step.position == 0
                    else parent.directory.identity
                    if parent is not None and parent.directory is not None
                    else None
                ):
                    raise _corrupt()
            fields = {
                "plan_id": row.plan_id,
                "scope_id": scope,
                "command_id": row.command_id,
                "source_hash": row.source_hash,
                "outcome": source.outcome,
                "accept_failed": request.accept_failed,
                "snapshot_revision": row.snapshot_revision,
                "root_path": row.root_path,
                "workspace_key": row.workspace_key,
                "resource_fingerprint": row.resource_fingerprint,
                "tree_fingerprint": snapshot.fingerprint,
                "entry_count": len(snapshot.entries),
                "byte_size": snapshot.total_bytes,
            }
            proof_hash = digest({**fields, "operation_ids": operation_ids})
            return VerifiedTreeOrigin(
                CompletedTreeOriginView.model_validate({**fields, "proof_hash": proof_hash}),
                snapshot,
                tuple(operation_ids),
            )
        except (ValueError, TypeError, KeyError):
            raise _corrupt() from None
