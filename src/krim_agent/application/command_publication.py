"""命令产物的私有来源、真实差异与条件提案；不代替workspace_change发布。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from krim_agent.execution.binding import ExecutionBinding
from krim_agent.execution.publication import (
    CommandChangesPage,
    CommandChangesRequest,
    CommandProposalRequest,
    CommandProposalView,
)
from krim_agent.persistence.command_proposals import CommandProposalStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext
from krim_agent.tools.workspace import workspace_io
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_change import ArtifactFileSpec
from krim_agent.workspace_writer import WorkspaceWriter

from .workspace_changes import WorkspaceChangeService

Guard = Callable[[], Awaitable[tuple[str, ExecutionBinding, WorkspaceBinding | None]]]


class CommandPublicationService:
    def __init__(self, changes: WorkspaceChangeService) -> None:
        self.changes = changes
        self.store = CommandProposalStore(changes.store)

    async def changes_page(
        self, scope: str, identifier: str, request: CommandChangesRequest
    ) -> CommandChangesPage:
        source = await self.store.source(scope, identifier)
        entries = [
            source.delta(path) for path in sorted(set(source.inputs) | set(source.artifacts))
        ]
        entries = [
            entry
            for entry in entries
            if (request.include_unchanged or entry.kind != "unchanged")
            and (request.after is None or entry.path > request.after)
        ]
        page = entries[: request.limit]
        return CommandChangesPage(
            command_id=identifier,
            source_hash=source.source_hash,
            outcome=source.outcome,
            requires_failed_acceptance=source.outcome != "completed",
            entries=page,
            next_after=page[-1].path if len(entries) > len(page) else None,
        )

    async def propose(
        self, request: CommandProposalRequest, context: ToolContext, guard: Guard
    ) -> CommandProposalView:
        scope, execution, current_workspace = await guard()
        edit_scope, workspace = await self.changes._guard(context)
        if scope != edit_scope or workspace is not current_workspace:
            raise ServiceError("execution_proposal_source", "命令与发布作用域不匹配", 403)
        run_id = context.run_id if context.run_id != "local" else None
        previous = await self.store.find(scope, request, workspace, run_id=run_id)
        if previous is not None:
            return previous
        source = await self.store.source(scope, request.command_id)
        if (
            source.workspace != workspace.manifest()
            or source.snapshot.resource != execution.resource
        ):
            raise ServiceError(
                "execution_proposal_source", "命令来源与当前工作区或执行资源不匹配", 403
            )
        if source.outcome != "completed" and not request.accept_failed:
            raise ServiceError(
                "execution_failed_artifacts", "失败命令产物需要显式accept_failed", 409
            )
        delta = source.delta(request.path)
        after = await self.store.data(scope, source, request.path)
        spec = ArtifactFileSpec(
            command_id=request.command_id,
            source_hash=source.source_hash,
            path=request.path,
            expected_sha256=delta.before_sha256,
            mode=delta.after_mode,
        )
        loop = asyncio.get_running_loop()

        async def check() -> None:
            actual_scope, actual_execution, actual_workspace = await guard()
            await self.changes._guard(context)
            if (
                actual_scope != scope
                or actual_execution is not execution
                or actual_workspace is not workspace
            ):
                raise ServiceError("execution_proposal_source", "提案准备期间授权发生变化", 403)

        def authorize() -> None:
            future = asyncio.run_coroutine_threadsafe(check(), loop)
            try:
                future.result(timeout=5)
            except ServiceError:
                raise
            except Exception:
                future.cancel()
                raise ServiceError(
                    "execution_proposal_unavailable", "产物提案资格无法核实", 503
                ) from None

        prepared = await workspace_io(
            WorkspaceWriter(workspace.require()).prepare_artifact,
            spec,
            source.captures.get(request.path),
            after,
            authorize=authorize,
        )
        await check()
        return await self.store.create(scope, request, workspace, prepared, run_id=run_id)
