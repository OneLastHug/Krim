"""0037命令目录来源与0036提案同事务保存；发布时复核原来源。"""

from __future__ import annotations

import time
from typing import Literal, cast
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.execution.directory_publication import (
    CommandDirectoryChangesPage,
    CommandDirectoryChangesRequest,
    CommandDirectoryDelta,
    CommandDirectoryEntry,
    CommandDirectoryProposalRequest,
    CommandDirectoryProposalView,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_directory import ArtifactDirectorySpec, PreparedDirectoryChange

from .command_proposals import CommandSource, load_source
from .database import CommandDirectoryProposalRow, WorkspaceDirectoryProposalRow
from .directory_fences import entry_key
from .workspace_directories import WorkspaceDirectoryStore, decode, encode, validate
from .workspace_edits import digest


def directory_entry(source: CommandSource, path: str) -> CommandDirectoryEntry:
    if source.directories is None:
        raise ServiceError("execution_directories_unrecorded", "原命令尚无可靠目录来源", 409)
    value = source.directories.get(path)
    if value is None:
        raise ServiceError("execution_directory_not_found", "原命令目录集合没有该路径", 404)
    nested = path + "/"
    return CommandDirectoryEntry(
        path=path,
        mode=value.mode,
        empty=not any(name.startswith(nested) for name in (*source.directories, *source.artifacts)),
        creatable=value.mode & 0o700 == 0o700,
    )


async def assert_directory_source(
    session: AsyncSession, proposal: WorkspaceDirectoryProposalRow
) -> bool:
    prepared = decode(proposal.prepared)
    spec = prepared.spec
    link = await session.get(CommandDirectoryProposalRow, proposal.proposal_id)
    if link is None:
        if isinstance(spec, ArtifactDirectorySpec):
            raise ServiceError("execution_proposal_source", "目录提案缺少真实命令来源关联", 409)
        return False
    source = await load_source(session, proposal.scope_id, link.command_id)
    try:
        observed = directory_entry(source, link.path)
    except ServiceError as error:
        if error.code not in {"execution_directory_not_found", "execution_directories_unrecorded"}:
            raise
        raise ServiceError("execution_proposal_source", "目录提案原来源不可核实", 409) from None
    if (
        not isinstance(spec, ArtifactDirectorySpec)
        or spec.operation != "create"
        or spec.command_id != link.command_id
        or spec.source_hash != link.source_hash
        or source.source_hash != link.source_hash
        or prepared.before is not None
        or prepared.restore_from is not None
        or spec.path != link.path
        or proposal.path != link.path
        or spec.mode != link.mode
        or observed.mode != link.mode
        or not observed.creatable
        or link.entry_key != entry_key(prepared.parent_identity, spec.path)
        or source.workspace.get("key") != proposal.workspace_key
        or source.workspace.get("fingerprint") != proposal.resource_fingerprint
    ):
        raise ServiceError("execution_proposal_source", "目录提案不属于原命令目录来源", 409)
    if source.outcome != "completed" and not link.accept_failed:
        raise ServiceError("execution_failed_artifacts", "失败命令目录尚未被明确接受", 409)
    return True


class CommandDirectoryProposalStore:
    def __init__(self, directories: WorkspaceDirectoryStore) -> None:
        self.directories, self.database = directories, directories.database

    @staticmethod
    def request_hash(request: CommandDirectoryProposalRequest, binding: WorkspaceBinding) -> str:
        return digest(
            {
                "command_directory_proposal": request.model_dump(mode="json"),
                "workspace": binding.manifest(),
            }
        )

    async def source(self, scope: str, command_id: str) -> CommandSource:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.directories.files._scope(session, scope, writing=False)
            return await load_source(session, scope, command_id)

    async def page(
        self, scope: str, request: CommandDirectoryChangesRequest
    ) -> CommandDirectoryChangesPage:
        async with self.database.write_lock, self.database.sessions() as session:
            await self.directories.files._scope(session, scope, writing=False)
            source = await load_source(session, scope, request.command_id)
            if source.directories is None:
                raise ServiceError(
                    "execution_directories_unrecorded", "原命令尚无可靠目录来源", 409
                )
            if request.after is not None and request.after not in source.directories:
                raise ServiceError("execution_directory_not_found", "目录游标不属于原命令来源", 404)
            paths = [
                path
                for path in sorted(source.directories)
                if request.after is None or path > request.after
            ]
            page = [directory_entry(source, path) for path in paths[: request.limit]]
            proposal_ids: dict[str, str] = {}
            rows = await session.scalars(
                select(WorkspaceDirectoryProposalRow)
                .join(
                    CommandDirectoryProposalRow,
                    CommandDirectoryProposalRow.proposal_id
                    == WorkspaceDirectoryProposalRow.proposal_id,
                )
                .where(
                    WorkspaceDirectoryProposalRow.scope_id == scope,
                    WorkspaceDirectoryProposalRow.path.in_([item.path for item in page]),
                    CommandDirectoryProposalRow.command_id == request.command_id,
                    CommandDirectoryProposalRow.source_hash == source.source_hash,
                )
                .order_by(
                    WorkspaceDirectoryProposalRow.created_at,
                    WorkspaceDirectoryProposalRow.proposal_id,
                )
            )
            for row in rows:
                await assert_directory_source(session, row)
                proposal_ids[row.path] = row.proposal_id
            return CommandDirectoryChangesPage(
                command_id=request.command_id,
                source_hash=source.source_hash,
                outcome=cast(Literal["completed", "failed", "output_limit"], source.outcome),
                entries=[
                    CommandDirectoryDelta(
                        path=item.path,
                        mode=item.mode,
                        empty=item.empty,
                        creatable=item.creatable,
                        proposal_id=proposal_ids.get(item.path),
                    )
                    for item in page
                ],
                next_after=page[-1].path if len(paths) > request.limit else None,
            )

    async def _view(
        self, session: AsyncSession, proposal: WorkspaceDirectoryProposalRow
    ) -> CommandDirectoryProposalView:
        await assert_directory_source(session, proposal)
        link = await session.get(CommandDirectoryProposalRow, proposal.proposal_id)
        if link is None:
            raise ServiceError("execution_proposal_source", "该目录提案没有命令来源", 409)
        source = await load_source(session, proposal.scope_id, link.command_id)
        return CommandDirectoryProposalView(
            command_id=link.command_id,
            source_hash=link.source_hash,
            outcome=source.outcome,
            accept_failed=bool(link.accept_failed),
            directory=directory_entry(source, link.path),
            proposal=self.directories._view(proposal).view,
        )

    async def find(
        self,
        scope: str,
        request: CommandDirectoryProposalRequest,
        binding: WorkspaceBinding,
        *,
        run_id: str | None = None,
    ) -> CommandDirectoryProposalView | None:
        self.directories.files._key(request.request_key)
        async with self.database.write_lock, self.database.sessions() as session:
            await self.directories.files._scope(session, scope, writing=True, run_id=run_id)
            await self.directories.files._resource(session, scope, binding.key, binding.fingerprint)
            row = await session.scalar(
                select(WorkspaceDirectoryProposalRow).where(
                    WorkspaceDirectoryProposalRow.scope_id == scope,
                    WorkspaceDirectoryProposalRow.request_key == request.request_key,
                )
            )
            if row is None:
                return None
            if row.request_hash != self.request_hash(request, binding):
                raise ServiceError(
                    "workspace_idempotency_conflict", "目录来源键已绑定其他意图", 409
                )
            return await self._view(session, row)

    async def create(
        self,
        scope: str,
        request: CommandDirectoryProposalRequest,
        binding: WorkspaceBinding,
        prepared: PreparedDirectoryChange,
        *,
        run_id: str | None = None,
    ) -> CommandDirectoryProposalView:
        self.directories.files._key(request.request_key)
        validate(prepared, original=True)
        spec = prepared.spec
        if not isinstance(spec, ArtifactDirectorySpec):
            raise ServiceError("execution_proposal_source", "命令目录须使用内部来源规格", 409)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self.directories.files._writer(session)
            topic = await self.directories.files._scope(session, scope, writing=True, run_id=run_id)
            await self.directories.files._resource(session, scope, binding.key, binding.fingerprint)
            if not binding.require().writable:
                raise ServiceError("workspace_read_only", "原工作区未授权目录发布", 403)
            previous = await session.scalar(
                select(WorkspaceDirectoryProposalRow).where(
                    WorkspaceDirectoryProposalRow.scope_id == scope,
                    WorkspaceDirectoryProposalRow.request_key == request.request_key,
                )
            )
            wanted = self.request_hash(request, binding)
            if previous is not None:
                if previous.request_hash != wanted:
                    raise ServiceError(
                        "workspace_idempotency_conflict", "目录来源键已绑定其他意图", 409
                    )
                return await self._view(session, previous)
            source = await load_source(session, scope, request.command_id)
            observed = directory_entry(source, request.path)
            if (
                spec.command_id != request.command_id
                or spec.source_hash != source.source_hash
                or spec.path != request.path
                or spec.mode != observed.mode
                or source.workspace != binding.manifest()
                or prepared.workspace_fingerprint != binding.workspace.fingerprint
            ):
                raise ServiceError("execution_proposal_source", "目录准备与原命令/资源不一致", 409)
            if not observed.creatable:
                raise ServiceError("execution_artifact_mode", "目录模式不支持本宿主受控创建", 422)
            if source.outcome != "completed" and not request.accept_failed:
                raise ServiceError(
                    "execution_failed_artifacts", "失败命令目录须显式accept_failed", 409
                )
            key = await self.directories._fence(session, prepared)
            count = await session.scalar(
                select(func.count()).select_from(WorkspaceDirectoryProposalRow)
            )
            if (count or 0) >= self.directories.max_changes:
                raise ServiceError("workspace_directory_limit", "目录提案数量已达上限", 429)
            row = WorkspaceDirectoryProposalRow(
                proposal_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                request_key=request.request_key,
                request_hash=wanted,
                resource_fingerprint=binding.fingerprint,
                workspace_key=binding.key,
                path=request.path,
                prepared=encode(prepared),
                status="proposed",
                revision=0,
                expires_at=time.time() + request.ttl_seconds,
                active_operation_id=None,
                created_at=utc_now(),
                updated_at=utc_now(),
            )
            session.add(row)
            await session.flush()
            session.add(
                CommandDirectoryProposalRow(
                    proposal_id=row.proposal_id,
                    command_id=request.command_id,
                    source_hash=source.source_hash,
                    path=request.path,
                    mode=observed.mode,
                    entry_key=key,
                    ttl_seconds=request.ttl_seconds,
                    accept_failed=int(request.accept_failed),
                )
            )
            await session.flush()
            binding.require()
            return await self._view(session, row)
