"""命令原结果到文件提案的原子来源关联；实际发布仍使用原文件账本。"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from pydantic import JsonValue, ValidationError
from sqlalchemy import CheckConstraint, Float, ForeignKey, Integer, String, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from krim_agent.execution.agent_models import AgentCommandRequest
from krim_agent.execution.directories import read_directory_manifest
from krim_agent.execution.models import ExecutionStatus, SandboxFile
from krim_agent.execution.publication import (
    ChangeKind,
    CommandDelta,
    CommandProposalRequest,
    CommandProposalView,
)
from krim_agent.execution.records import CommandDirectory, CommandFile, CommandSnapshot
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_change import ArtifactFileSpec
from krim_agent.workspace_tree import TreePlanFence
from krim_agent.workspace_writer import CapturedFile, PreparedFileChange

from .command_models import CommandFileRow, CommandRow
from .commands import CommandStore, digest, metadata
from .database import Base, WorkspaceEditProposalRow, WorkspaceFileEffectRow, WorkspaceFileLockRow
from .tree_fences import assert_preview, bind_proposal

if TYPE_CHECKING:
    from .workspace_edits import WorkspaceEditStore


class CommandProposalRow(Base):
    __tablename__ = "command_artifact_proposals"
    __table_args__ = (
        CheckConstraint("accept_failed IN (0,1)", name="ck_command_proposal_accept"),
        CheckConstraint("ttl_seconds > 0 AND ttl_seconds <= 86400", name="ck_command_proposal_ttl"),
        CheckConstraint(
            "effect_sequence IS NULL OR effect_sequence > 0", name="ck_command_proposal_effect"
        ),
    )
    proposal_id: Mapped[str] = mapped_column(
        ForeignKey("workspace_edit_proposals.proposal_id"), primary_key=True
    )
    command_id: Mapped[str] = mapped_column(ForeignKey("command_executions.command_id"), index=True)
    source_hash: Mapped[str] = mapped_column(String(64))
    path: Mapped[str] = mapped_column(String(512))
    file_key: Mapped[str] = mapped_column(String(64))
    effect_sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ttl_seconds: Mapped[float] = mapped_column(Float)
    accept_failed: Mapped[int] = mapped_column(Integer)


@dataclass(frozen=True)
class CommandSource:
    row: CommandRow
    snapshot: CommandSnapshot
    source_hash: str
    workspace: dict[str, JsonValue]
    inputs: dict[str, SandboxFile]
    captures: dict[str, CapturedFile]
    artifacts: dict[str, CommandFile]
    directories: dict[str, CommandDirectory] | None

    @property
    def outcome(self) -> ExecutionStatus:
        assert self.row.result is not None
        return cast(ExecutionStatus, self.row.result["status"])

    def delta(self, path: str) -> CommandDelta:
        old, new = self.inputs.get(path), self.artifacts.get(path)
        if old is None and new is None:
            raise ServiceError("execution_file_not_found", "原命令没有该输入或产物", 404)
        before = metadata(path, old) if old is not None else None
        kind: ChangeKind = (
            "added"
            if before is None
            else "deleted"
            if new is None
            else "unchanged"
            if before == new
            else "modified"
        )
        return CommandDelta(
            path=path,
            kind=kind,
            before_sha256=before.sha256 if before else None,
            after_sha256=new.sha256 if new else None,
            before_bytes=before.byte_size if before else None,
            after_bytes=new.byte_size if new else None,
            before_mode=before.mode if before else None,
            after_mode=new.mode if new else None,
        )


async def load_source(session: AsyncSession, scope: str, identifier: str) -> CommandSource:
    from .workspace_checkpoints import decode_capture

    row = await session.get(CommandRow, identifier)
    if row is None or row.scope_id != scope:
        raise ServiceError("execution_not_found", "命令不在该作用域中", 404)
    snapshot = CommandStore._snapshot(row)
    result = row.result
    if (
        row.phase != "finished"
        or result is None
        or not row.started
        or result.get("artifacts_complete") is not True
        or result.get("status") not in {"completed", "failed", "output_limit"}
    ):
        raise ServiceError("execution_artifacts_incomplete", "命令未形成已收束的完整产物集", 409)
    origin = snapshot.origin
    if origin is None or not isinstance(origin.get("workspace"), dict):
        raise ServiceError("execution_source_unbound", "命令没有可核验的原工作区来源", 409)
    try:
        request = AgentCommandRequest.model_validate(origin["request"])
        workspace = cast(dict[str, JsonValue], origin["workspace"])
        if (
            request.command != snapshot.request.command
            or request.request_key != snapshot.request.request_key
            or sorted(request.directories) != sorted(snapshot.directories)
        ):
            raise ValueError("request")
        input_rows = list(
            await session.scalars(
                select(CommandFileRow).where(
                    CommandFileRow.command_id == identifier, CommandFileRow.kind == "input"
                )
            )
        )
        inputs = {item.path: SandboxFile(item.content, item.mode) for item in input_rows}
        expected = {item.path: item for item in snapshot.inputs}
        if (
            len(expected) != len(snapshot.inputs)
            or set(expected) != set(inputs)
            or set(inputs) != set(request.input_paths)
            or any(metadata(path, value) != expected[path] for path, value in inputs.items())
            or any(item.sha256 != expected[item.path].sha256 for item in input_rows)
        ):
            raise ValueError("inputs")
        raw_captures = origin["captures"]
        if not isinstance(raw_captures, list) or len(raw_captures) != len(inputs):
            raise ValueError("captures")
        captures = {}
        for encoded in raw_captures:
            if not isinstance(encoded, str):
                raise ValueError("capture")
            value = json.loads(encoded)
            path = value["path"]
            if path not in inputs or path in captures:
                raise ValueError("path")
            capture = decode_capture(value, inputs[path].data)
            if capture.snapshot is None or capture.snapshot.mode != inputs[path].mode:
                raise ValueError("snapshot")
            captures[path] = capture
        raw_artifacts = result["artifacts"]
        if not isinstance(raw_artifacts, list):
            raise ValueError("artifacts")
        values = [CommandFile.model_validate(value) for value in raw_artifacts]
        artifacts = {item.path: item for item in values}
        if len(artifacts) != len(values) or len(values) != result["artifact_files"]:
            raise ValueError("artifacts")
        manifest = read_directory_manifest(result, set(artifacts), snapshot.limits)
        directories = {item.path: item for item in manifest.entries} if manifest else None
    except (KeyError, ValueError, TypeError, ValidationError):
        raise ServiceError("execution_source_corrupt", "命令来源或产物清单校验失败", 409) from None
    return CommandSource(
        row,
        snapshot,
        digest({"request": row.request_hash, "result": result}),
        workspace,
        inputs,
        captures,
        artifacts,
        directories,
    )


async def artifact_bytes(session: AsyncSession, source: CommandSource, path: str) -> bytes | None:
    wanted = source.artifacts.get(path)
    row = await session.get(CommandFileRow, (source.row.command_id, "artifact", path))
    if wanted is None:
        if row is not None:
            raise ServiceError("execution_source_corrupt", "产物记录与完整清单不匹配", 409)
        return None
    if (
        row is None
        or metadata(path, SandboxFile(row.content, row.mode)) != wanted
        or row.sha256 != wanted.sha256
    ):
        raise ServiceError("execution_source_corrupt", "产物字节或模式校验失败", 409)
    return row.content


async def effect_tip(session: AsyncSession, key: str) -> int | None:
    value = await session.scalar(
        select(func.max(WorkspaceFileEffectRow.effect_sequence)).where(
            WorkspaceFileEffectRow.file_key == key
        )
    )
    return int(value) if value is not None else None


async def assert_command_proposal(
    session: AsyncSession,
    proposal: WorkspaceEditProposalRow,
    prepared: PreparedFileChange,
    *,
    applying: bool,
) -> bool:
    from .workspace_edits import file_key

    link = await session.get(CommandProposalRow, proposal.proposal_id)
    spec = prepared.spec
    if link is None:
        if isinstance(spec, ArtifactFileSpec):
            raise ServiceError("execution_proposal_source", "产物提案缺少真实来源关联", 409)
        return False
    source = await load_source(session, proposal.scope_id, link.command_id)
    source.delta(link.path)
    if (
        not isinstance(spec, ArtifactFileSpec)
        or spec.command_id != link.command_id
        or spec.source_hash != link.source_hash
        or source.source_hash != link.source_hash
        or spec.path != link.path
        or proposal.path != link.path
        or file_key(prepared) != link.file_key
        or source.workspace.get("key") != proposal.workspace_key
        or source.workspace.get("fingerprint") != proposal.resource_fingerprint
    ):
        raise ServiceError("execution_proposal_source", "文件提案不属于原命令产物", 409)
    if source.outcome != "completed" and not link.accept_failed:
        raise ServiceError("execution_failed_artifacts", "失败命令产物未被明确接受", 409)
    before = source.captures.get(spec.path)
    after = source.artifacts.get(spec.path)
    if (
        prepared.before != (before.snapshot if before else None)
        or prepared.after != await artifact_bytes(session, source, spec.path)
        or spec.mode != (after.mode if after else None)
        or (
            before is not None
            and (
                prepared.parent_identity != before.parent_identity
                or prepared.workspace_fingerprint != before.workspace_fingerprint
            )
        )
    ):
        raise ServiceError("execution_proposal_source", "文件提案与冻结输入/产物不一致", 409)
    if applying and await effect_tip(session, link.file_key) != link.effect_sequence:
        raise ServiceError("workspace_conflict", "产物提案之后已有文件效果记录", 409)
    return True


class CommandProposalStore:
    def __init__(self, edits: WorkspaceEditStore) -> None:
        self.edits, self.database = edits, edits.database

    async def source(self, scope: str, identifier: str) -> CommandSource:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope, writing=False)
            return await load_source(session, scope, identifier)

    async def data(self, scope: str, source: CommandSource, path: str) -> bytes | None:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope, writing=False)
            return await artifact_bytes(session, source, path)

    @staticmethod
    def request_hash(request: CommandProposalRequest, binding: WorkspaceBinding) -> str:
        return digest(
            {"command_proposal": request.model_dump(mode="json"), "workspace": binding.manifest()}
        )

    async def _view(
        self, session: AsyncSession, proposal: WorkspaceEditProposalRow
    ) -> CommandProposalView:
        stored = self.edits._change(proposal)
        link = await session.get(CommandProposalRow, proposal.proposal_id)
        if link is None:
            raise ServiceError("execution_proposal_source", "该提案不是命令产物来源", 409)
        await assert_command_proposal(session, proposal, stored.prepared, applying=False)
        source = await load_source(session, proposal.scope_id, link.command_id)
        return CommandProposalView(
            command_id=link.command_id,
            source_hash=link.source_hash,
            change=source.delta(link.path),
            accept_failed=bool(link.accept_failed),
            proposal=stored.view,
        )

    async def find(
        self,
        scope: str,
        request: CommandProposalRequest,
        binding: WorkspaceBinding,
        *,
        run_id: str | None = None,
        tree_fence: TreePlanFence | None = None,
    ) -> CommandProposalView | None:
        self.edits._key(request.request_key)
        async with self.database.write_lock, self.database.sessions() as session:
            await self.edits._scope(session, scope, writing=True, run_id=run_id)
            await self.edits._resource(session, scope, binding.key, binding.fingerprint)
            row = await session.scalar(
                select(WorkspaceEditProposalRow).where(
                    WorkspaceEditProposalRow.scope_id == scope,
                    WorkspaceEditProposalRow.request_key == request.request_key,
                )
            )
            await assert_preview(
                session,
                self.edits.owner_id,
                scope,
                binding.fingerprint,
                "file",
                request,
                tree_fence,
                existing_proposal_id=row.proposal_id if row else None,
                run_id=run_id,
            )
            if row is None:
                return None
            if row.request_hash != self.request_hash(request, binding):
                raise ServiceError("workspace_idempotency_conflict", "提案键已绑定其他请求", 409)
            return await self._view(session, row)

    async def create(
        self,
        scope: str,
        request: CommandProposalRequest,
        binding: WorkspaceBinding,
        prepared: PreparedFileChange,
        *,
        run_id: str | None = None,
        tree_fence: TreePlanFence | None = None,
    ) -> CommandProposalView:
        from .workspace_edits import encode_prepared, file_key

        self.edits._key(request.request_key)
        self.edits._validate_prepared(prepared, original_intent=False)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.edits._writer(session)
            topic = await self.edits._scope(session, scope, writing=True, run_id=run_id)
            await self.edits._resource(session, scope, binding.key, binding.fingerprint)
            if not binding.require().writable:
                raise ServiceError("workspace_read_only", "工作区未授权发布产物", 403)
            previous = await session.scalar(
                select(WorkspaceEditProposalRow).where(
                    WorkspaceEditProposalRow.scope_id == scope,
                    WorkspaceEditProposalRow.request_key == request.request_key,
                )
            )
            request_hash = self.request_hash(request, binding)
            tree_step = await assert_preview(
                session,
                self.edits.owner_id,
                scope,
                binding.fingerprint,
                "file",
                request,
                tree_fence,
                prepared=prepared,
                existing_proposal_id=previous.proposal_id if previous else None,
                run_id=run_id,
            )
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise ServiceError(
                        "workspace_idempotency_conflict", "提案键已绑定其他请求", 409
                    )
                return await self._view(session, previous)
            source = await load_source(session, scope, request.command_id)
            spec = prepared.spec
            if (
                not isinstance(spec, ArtifactFileSpec)
                or spec.command_id != request.command_id
                or spec.path != request.path
                or spec.source_hash != source.source_hash
                or source.workspace != binding.manifest()
                or prepared.workspace_fingerprint != binding.workspace.fingerprint
            ):
                raise ServiceError("execution_proposal_source", "产物提案的工作区或来源不匹配", 409)
            if source.outcome != "completed" and not request.accept_failed:
                raise ServiceError(
                    "execution_failed_artifacts", "失败命令产物需要显式accept_failed", 409
                )
            key = file_key(prepared)
            if await session.get(WorkspaceFileLockRow, key) is not None:
                raise ServiceError("workspace_file_busy", "目标有未收束的文件操作", 409)
            count = await session.scalar(select(func.count()).select_from(WorkspaceEditProposalRow))
            if (count or 0) >= self.edits.max_changes:
                raise ServiceError("workspace_proposal_limit", "文件提案数量已达上限", 429)
            await self.edits._bytes_available(
                session,
                len(prepared.before.data if prepared.before else b"") + len(prepared.after or b""),
            )
            proposal = WorkspaceEditProposalRow(
                proposal_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                request_key=request.request_key,
                request_hash=request_hash,
                resource_fingerprint=binding.fingerprint,
                workspace_key=binding.key,
                path=request.path,
                prepared=encode_prepared(prepared),
                preview=prepared.preview().model_dump(mode="json"),
                before_content=prepared.before.data if prepared.before else None,
                after_content=prepared.after,
                status="proposed",
                revision=0,
                expires_at=time.time() + request.ttl_seconds,
            )
            session.add(proposal)
            await session.flush()
            bind_proposal(tree_step, "file", proposal.proposal_id)
            await session.flush()
            session.add(
                CommandProposalRow(
                    proposal_id=proposal.proposal_id,
                    command_id=request.command_id,
                    source_hash=source.source_hash,
                    path=request.path,
                    file_key=key,
                    effect_sequence=await effect_tip(session, key),
                    ttl_seconds=request.ttl_seconds,
                    accept_failed=int(request.accept_failed),
                )
            )
            await session.flush()
            binding.require()
            return await self._view(session, proposal)
