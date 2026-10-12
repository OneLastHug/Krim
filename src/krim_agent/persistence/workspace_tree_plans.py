"""0041目录树父/步骤冻结与原事务准入；真实回程投影原proposal/attempt。"""

from __future__ import annotations

import json
import math
import secrets
import time
from datetime import UTC
from typing import cast
from uuid import uuid4

from pydantic import JsonValue
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.execution.directory_publication import CommandDirectoryProposalRequest
from krim_agent.execution.publication import CommandProposalRequest
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now
from krim_agent.workspace import WorkspaceBinding
from krim_agent.workspace_change import (
    ArtifactFileSpec,
    WorkspaceAttemptView,
    WorkspaceChangeAction,
)
from krim_agent.workspace_directory import (
    ArtifactDirectorySpec,
    DirectoryAttemptView,
    DirectoryChangeAction,
    PreparedDirectoryChange,
)
from krim_agent.workspace_tree import (
    TreePhase,
    TreePlanDispatch,
    TreePlanExecution,
    TreePlanFence,
    TreePlanRequest,
    TreePlanStatus,
    TreePlanStepPage,
    TreePlanStepView,
    TreePlanView,
    TreeProducerStatus,
    TreeSource,
    TreeStepStatus,
)
from krim_agent.workspace_writer import PreparedFileChange

from .command_proposals import load_source
from .database import (
    AgentSessionRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
    WorkspaceTreePlanRow,
    WorkspaceTreeStepRow,
)
from .tree_fences import assert_live, assert_source, parent_identity, step_key
from .workspace_directories import WorkspaceDirectoryStore, decode, encode, validate
from .workspace_edits import WorkspaceEditStore, digest


def _corrupt() -> ServiceError:
    return ServiceError("workspace_tree_corrupt", "目录树冻结快照或原步骤关联损坏", 409)


def _metadata_bytes(value: dict[str, JsonValue]) -> int:
    return len(json.dumps(value).encode("utf-8"))


class WorkspaceTreePlanStore:
    def __init__(
        self,
        files: WorkspaceEditStore,
        directories: WorkspaceDirectoryStore,
        *,
        max_plans: int = 500,
        max_metadata_bytes: int = 33_554_432,
        lease_seconds: float = 60,
    ) -> None:
        if files.database is not directories.database or files.owner_id != directories.owner_id:
            raise ValueError("目录树须共用原文件/目录Database和owner")
        if type(max_plans) is not int or not 1 <= max_plans <= 5000:
            raise ValueError("目录树历史容量无效")
        if type(max_metadata_bytes) is not int or not 1024 <= max_metadata_bytes <= 134_217_728:
            raise ValueError("目录树元数据容量无效")
        self.files, self.directories = files, directories
        self.database, self.owner_id = files.database, files.owner_id
        self.max_plans, self.max_metadata_bytes = max_plans, max_metadata_bytes
        if (
            type(lease_seconds) not in {int, float}
            or not math.isfinite(lease_seconds)
            or not 0.2 <= lease_seconds <= 600
        ):
            raise ValueError("目录树执行租约无效")
        self.lease_seconds = lease_seconds

    @staticmethod
    def _request_hash(request: TreePlanRequest, binding: WorkspaceBinding) -> str:
        return digest(
            {"tree_plan": request.model_dump(mode="json"), "workspace": binding.manifest()}
        )

    async def reserve(
        self,
        scope: str,
        binding: WorkspaceBinding,
        request: TreePlanRequest,
        prepared: PreparedDirectoryChange | None,
    ) -> tuple[TreePlanView, bool]:
        request = TreePlanRequest.model_validate(request.model_dump())
        self.files._key(request.request_key)
        wanted = self._request_hash(request, binding)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=True)
            await self.files._resource(sql, scope, binding.key, binding.fingerprint)
            previous = await sql.scalar(
                select(WorkspaceTreePlanRow).where(
                    WorkspaceTreePlanRow.scope_id == scope,
                    WorkspaceTreePlanRow.request_key == request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != wanted:
                    raise ServiceError(
                        "workspace_tree_idempotency_conflict", "树请求键已绑定其他参数", 409
                    )
                return await self._view(sql, previous), False
            if not binding.require().writable:
                raise ServiceError("workspace_read_only", "工作区未授权目录树准备", 403)
            source = await load_source(sql, scope, request.command_id)
            if source.source_hash != request.source_hash:
                raise ServiceError("workspace_tree_source_conflict", "目录树原命令来源已变化", 409)
            if source.workspace != binding.manifest():
                raise ServiceError("workspace_binding_changed", "目录树不属于原工作区资源", 403)
            if scope.startswith("session:"):
                session = await sql.get(AgentSessionRow, scope.removeprefix("session:"))
                assert session is not None
                if session.permissions.get("execution") != source.snapshot.resource.model_dump(
                    mode="json"
                ):
                    raise ServiceError("execution_binding_changed", "目录树原执行资源已变化", 403)
            if source.directories is None:
                raise ServiceError(
                    "execution_directories_unrecorded", "原命令没有可靠目录来源", 409
                )
            root = source.directories.get(request.root_path)
            if root is None:
                raise ServiceError("workspace_tree_root_not_found", "原来源没有所选目录根", 404)
            if source.outcome != "completed" and not request.accept_failed:
                raise ServiceError("execution_failed_artifacts", "失败目录树成果须显式接受", 409)
            if prepared is None:
                raise ServiceError(
                    "workspace_tree_root_required", "目录树须冻结原根目标准备材料", 403
                )
            validate(prepared, original=True)
            if prepared.workspace_fingerprint != binding.workspace.fingerprint:
                raise ServiceError("workspace_binding_changed", "树根准备属于其他工作区", 403)
            spec = prepared.spec
            if (
                not isinstance(spec, ArtifactDirectorySpec)
                or spec.path != request.root_path
                or spec.command_id != request.command_id
                or spec.source_hash != request.source_hash
                or spec.mode != root.mode
                or spec.operation != "create"
                or prepared.before is not None
                or prepared.restore_from is not None
                or prepared.target_parent_identity is not None
            ):
                raise ServiceError(
                    "workspace_tree_root_mismatch", "树根准备与冻结来源条件不匹配", 409
                )
            prefix = request.root_path + "/"

            def inside(path: str) -> bool:
                return path == request.root_path or path.startswith(prefix)

            if any(inside(path) for path in source.inputs):
                raise ServiceError(
                    "workspace_tree_existing_input", "新树计划不能丢弃原宿主输入基线", 409
                )
            directories = [item for item in source.directories.values() if inside(item.path)]
            files = [item for item in source.artifacts.values() if inside(item.path)]
            if len(directories) + len(files) > request.max_entries:
                raise ServiceError("workspace_tree_step_limit", "树步骤超过原请求容量", 409)
            if any(item.mode & 0o700 != 0o700 for item in directories):
                raise ServiceError("execution_artifact_mode", "原目录模式不支持受控创建", 422)
            if any(item.byte_size > binding.workspace.max_file_bytes for item in files):
                raise ServiceError("workspace_file_limit", "目录树原文件超过工作区上限", 422)
            ordered = sorted(
                [(item.path, "directory", item.mode, None, None) for item in directories]
                + [(item.path, "file", item.mode, item.sha256, item.byte_size) for item in files],
                key=lambda item: (item[0].count("/"), item[1] != "directory", item[0]),
            )
            positions = {item[0]: position for position, item in enumerate(ordered)}
            steps: list[dict[str, JsonValue]] = []
            for position, (path, kind, mode, sha, size) in enumerate(ordered):
                parent = None if position == 0 else positions.get(path.rpartition("/")[0])
                if position > 0 and (parent is None or ordered[parent][1] != "directory"):
                    raise ServiceError("workspace_tree_source_conflict", "目录树缺少原父闭包", 409)
                steps.append(
                    {
                        "position": position,
                        "parent_position": parent,
                        "source": kind,
                        "path": path,
                        "mode": mode,
                        "sha256": sha,
                        "byte_size": size,
                    }
                )
            await self.directories._fence(sql, prepared)
            frozen: dict[str, JsonValue] = {
                "request": request.model_dump(mode="json"),
                "scope_id": scope,
                "workspace": binding.manifest(),
                "execution": source.snapshot.resource.model_dump(mode="json"),
                "root_prepared": encode(prepared),
                "steps": cast(list[JsonValue], steps),
                "anchor_path": request.root_path.rpartition("/")[0] or ".",
                "anchor_identity": list(prepared.parent_identity),
            }
            metadata_bytes = _metadata_bytes(frozen)
            if metadata_bytes > 2_097_152:
                raise ServiceError("workspace_tree_metadata_limit", "单树冻结元数据超过2MiB", 409)
            count = await sql.scalar(select(func.count()).select_from(WorkspaceTreePlanRow)) or 0
            total = await sql.scalar(select(func.sum(WorkspaceTreePlanRow.metadata_bytes))) or 0
            if count >= self.max_plans or total + metadata_bytes > self.max_metadata_bytes:
                raise ServiceError("workspace_tree_limit", "树历史数量或总元数据容量已达上限", 429)
            row = WorkspaceTreePlanRow(
                plan_id=str(uuid4()),
                scope_id=scope,
                command_id=request.command_id,
                source_hash=request.source_hash,
                root_path=request.root_path,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                request_key=request.request_key,
                request_hash=wanted,
                snapshot_revision=digest(frozen),
                frozen=frozen,
                metadata_bytes=metadata_bytes,
                step_count=len(steps),
                producer_status="planned",
                created_owner_id=self.owner_id,
                owner_id=None,
                lease_expires_at=None,
                token=secrets.token_hex(32),
                stop_reason=None,
                created_at=utc_now(),
                updated_at=utc_now(),
            )
            sql.add(row)
            await sql.flush()
            for step in steps:
                sql.add(
                    WorkspaceTreeStepRow(
                        plan_id=row.plan_id,
                        **step,
                        file_proposal_id=None,
                        directory_proposal_id=None,
                        expected_revision=None,
                        file_operation_id=None,
                        directory_operation_id=None,
                        admission_open=False,
                        status="pending",
                        error_code=None,
                    )
                )
            await sql.flush()
            return await self._view(sql, row), True

    @staticmethod
    def _frozen(row: WorkspaceTreePlanRow) -> list[dict[str, JsonValue]]:
        try:
            if (
                digest(row.frozen) != row.snapshot_revision
                or _metadata_bytes(row.frozen) != row.metadata_bytes
            ):
                raise ValueError()
            value = row.frozen
            request = TreePlanRequest.model_validate(value["request"])
            workspace = value["workspace"]
            if not isinstance(workspace, dict):
                raise ValueError()
            if (
                value["scope_id"] != row.scope_id
                or request.command_id != row.command_id
                or request.source_hash != row.source_hash
                or request.root_path != row.root_path
                or request.request_key != row.request_key
                or workspace["key"] != row.workspace_key
                or workspace["fingerprint"] != row.resource_fingerprint
                or digest({"tree_plan": request.model_dump(mode="json"), "workspace": workspace})
                != row.request_hash
            ):
                raise ValueError()
            root = value["root_prepared"]
            if not isinstance(root, dict):
                raise ValueError()
            prepared = decode(root)
            validate(prepared, original=True)
            spec = prepared.spec
            if not isinstance(spec, ArtifactDirectorySpec) or (
                spec.command_id != row.command_id
                or spec.source_hash != row.source_hash
                or spec.path != row.root_path
                or prepared.before is not None
                or value["anchor_identity"] != list(prepared.parent_identity)
                or value["anchor_path"] != (row.root_path.rpartition("/")[0] or ".")
            ):
                raise ValueError()
            steps = value["steps"]
            if not isinstance(steps, list) or len(steps) != row.step_count or not steps:
                raise ValueError()
            result: list[dict[str, JsonValue]] = []
            for position, item in enumerate(steps):
                if not isinstance(item, dict) or item["position"] != position:
                    raise ValueError()
                TreePlanStepView.model_validate({**item, "status": "pending"})
                if position == 0:
                    if (
                        item["source"] != "directory"
                        or item["path"] != row.root_path
                        or item["parent_position"] is not None
                        or item["mode"] != spec.mode
                    ):
                        raise ValueError()
                else:
                    parent = item["parent_position"]
                    if type(parent) is not int or not 0 <= parent < position:
                        raise ValueError()
                    if (
                        result[parent]["source"] != "directory"
                        or result[parent]["path"] != str(item["path"]).rpartition("/")[0]
                    ):
                        raise ValueError()
                result.append(item)
            return result
        except (KeyError, TypeError, ValueError, ServiceError):
            raise _corrupt() from None

    async def _rows(
        self, sql: AsyncSession, row: WorkspaceTreePlanRow
    ) -> list[WorkspaceTreeStepRow]:
        frozen = self._frozen(row)
        steps = list(
            await sql.scalars(
                select(WorkspaceTreeStepRow)
                .where(WorkspaceTreeStepRow.plan_id == row.plan_id)
                .order_by(WorkspaceTreeStepRow.position)
            )
        )
        if len(steps) != len(frozen):
            raise _corrupt()
        for step, expected in zip(steps, frozen, strict=True):
            actual = {name: getattr(step, name) for name in expected}
            if actual != expected:
                raise _corrupt()
            await self._step_view(sql, row, step)
        return steps

    @staticmethod
    async def _step_view(
        sql: AsyncSession, row: WorkspaceTreePlanRow, step: WorkspaceTreeStepRow
    ) -> TreePlanStepView:
        try:
            return await WorkspaceTreePlanStore._project_step(sql, row, step)
        except (KeyError, TypeError, ValueError):
            raise _corrupt() from None

    @staticmethod
    async def _project_step(
        sql: AsyncSession, row: WorkspaceTreePlanRow, step: WorkspaceTreeStepRow
    ) -> TreePlanStepView:
        from .command_directory_proposals import assert_directory_source
        from .command_proposals import CommandProposalRow, assert_command_proposal
        from .database import CommandDirectoryProposalRow

        if step.source not in {"file", "directory"}:
            raise _corrupt()
        proposal_id = step.file_proposal_id if step.source == "file" else step.directory_proposal_id
        operation_id = (
            step.file_operation_id if step.source == "file" else step.directory_operation_id
        )
        if step.admission_open and (
            row.producer_status != "running" or step.status != "pending" or operation_id is not None
        ):
            raise _corrupt()
        if row.producer_status == "planned" and (
            proposal_id is not None or operation_id is not None
        ):
            raise _corrupt()
        operation: WorkspaceAttemptView | DirectoryAttemptView | None = None
        prepared: PreparedFileChange | PreparedDirectoryChange | None = None
        status = step.status
        error = step.error_code
        if proposal_id is not None:
            proposal = (
                await sql.get(WorkspaceEditProposalRow, proposal_id)
                if step.source == "file"
                else await sql.get(WorkspaceDirectoryProposalRow, proposal_id)
            )
            if (
                proposal is None
                or proposal.scope_id != row.scope_id
                or proposal.workspace_key != row.workspace_key
                or proposal.resource_fingerprint != row.resource_fingerprint
                or proposal.path != step.path
                or step.expected_revision != 0
                or proposal.request_key != step_key(row.plan_id, step.position, "preview")
            ):
                raise _corrupt()
            original_request = TreePlanRequest.model_validate(row.frozen["request"])
            if isinstance(proposal, WorkspaceEditProposalRow):
                prepared = WorkspaceEditStore._change(proposal).prepared
                await assert_command_proposal(sql, proposal, prepared, applying=False)
                link = await sql.get(CommandProposalRow, proposal_id)
                spec = prepared.spec
                if (
                    link is None
                    or link.command_id != row.command_id
                    or link.source_hash != row.source_hash
                    or link.accept_failed != original_request.accept_failed
                    or not isinstance(spec, ArtifactFileSpec)
                    or spec.mode != step.mode
                    or prepared.after_sha256 != step.sha256
                    or len(prepared.after or b"") != step.byte_size
                    or prepared.before is not None
                ):
                    raise _corrupt()
                preview_request: CommandProposalRequest | CommandDirectoryProposalRequest = (
                    CommandProposalRequest(
                        command_id=row.command_id,
                        path=step.path,
                        request_key=proposal.request_key,
                        accept_failed=original_request.accept_failed,
                    )
                )
                request_kind = "command_proposal"
            else:
                await assert_directory_source(sql, proposal)
                directory_link = await sql.get(CommandDirectoryProposalRow, proposal_id)
                prepared = WorkspaceDirectoryStore._view(proposal).prepared
                directory_spec = prepared.spec
                if (
                    directory_link is None
                    or directory_link.accept_failed != original_request.accept_failed
                    or not isinstance(directory_spec, ArtifactDirectorySpec)
                    or directory_spec.command_id != row.command_id
                    or directory_spec.source_hash != row.source_hash
                    or directory_spec.mode != step.mode
                    or prepared.before is not None
                ):
                    raise _corrupt()
                preview_request = CommandDirectoryProposalRequest(
                    command_id=row.command_id,
                    path=step.path,
                    request_key=proposal.request_key,
                    accept_failed=original_request.accept_failed,
                )
                request_kind = "command_directory_proposal"
            if proposal.request_hash != digest(
                {
                    request_kind: preview_request.model_dump(mode="json"),
                    "workspace": row.frozen["workspace"],
                }
            ):
                raise _corrupt()
        elif operation_id is not None or step.expected_revision is not None:
            raise _corrupt()
        if operation_id is not None:
            attempt = (
                await sql.get(WorkspaceEditAttemptRow, operation_id)
                if step.source == "file"
                else await sql.get(WorkspaceDirectoryAttemptRow, operation_id)
            )
            if (
                attempt is None
                or attempt.scope_id != row.scope_id
                or attempt.proposal_id != proposal_id
                or attempt.action != "apply"
                or attempt.expected_revision != step.expected_revision
                or attempt.request_key != step_key(row.plan_id, step.position, "change")
                or attempt.run_id is not None
                or step.status != "pending"
            ):
                raise _corrupt()
            if isinstance(attempt, WorkspaceEditAttemptRow):
                stored_file = WorkspaceEditStore._attempt(attempt)
                operation = stored_file.view
                if (
                    stored_file.prepared != prepared
                    or operation.receipt is not None
                    and (
                        operation.receipt.before_sha256 is not None
                        or operation.receipt.after_sha256 != step.sha256
                    )
                ):
                    raise _corrupt()
            else:
                stored_directory = WorkspaceDirectoryStore._attempt(attempt)
                operation = stored_directory.view
                if (
                    stored_directory.prepared != prepared
                    or operation.status == "uncertain"
                    and operation.receipt is None
                    and (attempt.owner_id is not None or attempt.lease_expires_at is not None)
                    or operation.receipt is not None
                    and (
                        operation.receipt.operation_token != stored_directory.prepared.token
                        or operation.receipt.before_fingerprint is not None
                    )
                ):
                    raise _corrupt()
            if attempt.request_hash != digest(
                {
                    "request": {
                        "proposal_id": proposal_id,
                        "action": "apply",
                        "expected_revision": step.expected_revision,
                        "request_key": attempt.request_key,
                    },
                    "resource": row.resource_fingerprint,
                }
            ):
                raise _corrupt()
            status, error = (
                operation.status,
                operation.receipt.error_code if operation.receipt else None,
            )
        return TreePlanStepView(
            position=step.position,
            parent_position=step.parent_position,
            source=cast(TreeSource, step.source),
            path=step.path,
            mode=step.mode,
            sha256=step.sha256,
            byte_size=step.byte_size,
            status=cast(TreeStepStatus, status),
            error_code=error,
            proposal_id=proposal_id,
            operation=operation,
        )

    async def _view(self, sql: AsyncSession, row: WorkspaceTreePlanRow) -> TreePlanView:
        values = [await self._step_view(sql, row, item) for item in await self._rows(sql, row)]
        done = sum(item.status == "done" for item in values)
        unresolved = any(item.status in {"running", "uncertain"} for item in values)
        status = (
            row.producer_status
            if row.producer_status in {"planned", "running"}
            else (
                "uncertain"
                if unresolved
                else "done"
                if done == row.step_count
                else row.producer_status
                if row.producer_status in {"cancelled", "interrupted"}
                else "partial"
                if done
                else "failed"
            )
        )
        return TreePlanView(
            plan_id=row.plan_id,
            scope_id=row.scope_id,
            command_id=row.command_id,
            source_hash=row.source_hash,
            root_path=row.root_path,
            workspace_key=row.workspace_key,
            resource_fingerprint=row.resource_fingerprint,
            request_key=row.request_key,
            snapshot_revision=row.snapshot_revision,
            status=cast(TreePlanStatus, status),
            producer_status=cast(TreeProducerStatus, row.producer_status),
            step_count=row.step_count,
            done_count=done,
            stop_reason=row.stop_reason,
            created_at=row.created_at.replace(tzinfo=UTC),
            updated_at=row.updated_at.replace(tzinfo=UTC),
        )

    async def _row(self, sql: AsyncSession, scope: str, identifier: str) -> WorkspaceTreePlanRow:
        await self.files._scope(sql, scope, writing=False)
        row = await sql.get(WorkspaceTreePlanRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError("workspace_tree_not_found", "目录树计划不在该作用域", 404)
        return row

    async def get(self, scope: str, identifier: str) -> TreePlanView:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._row(sql, scope, identifier)
            await self._expire(sql, row)
            return await self._view(sql, row)

    async def by_key(self, scope: str, key: str) -> TreePlanView | None:
        self.files._key(key)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=False)
            row = await sql.scalar(
                select(WorkspaceTreePlanRow).where(
                    WorkspaceTreePlanRow.scope_id == scope,
                    WorkspaceTreePlanRow.request_key == key,
                )
            )
            if row is not None:
                await self._expire(sql, row)
                return await self._view(sql, row)
            return None

    async def steps(
        self, scope: str, identifier: str, *, after: int | None = None, limit: int = 32
    ) -> TreePlanStepPage:
        if (
            type(limit) is not int
            or not 1 <= limit <= 32
            or after is not None
            and (type(after) is not int or after < 0)
        ):
            raise ServiceError("workspace_tree_page", "树步骤分页参数无效", 422)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._row(sql, scope, identifier)
            await self._expire(sql, row)
            values = await self._rows(sql, row)
            selected = [step for step in values if after is None or step.position > after]
            if after is not None and after >= row.step_count:
                raise ServiceError("workspace_tree_page", "树步骤游标不属于原计划", 422)
            return TreePlanStepPage(
                plan_id=identifier,
                scope_id=scope,
                items=[await self._step_view(sql, row, item) for item in selected[:limit]],
                next_after=selected[limit - 1].position if len(selected) > limit else None,
            )

    async def cancel(self, scope: str, identifier: str) -> TreePlanView:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._row(sql, scope, identifier)
            await self._rows(sql, row)
            if row.producer_status in {"planned", "running"}:
                await self._close(sql, row, "cancelled", "workspace_tree_cancelled")
            return await self._view(sql, row)

    async def _close(
        self, sql: AsyncSession, row: WorkspaceTreePlanRow, status: str, reason: str | None
    ) -> None:
        values = await self._rows(sql, row)
        row.producer_status, row.stop_reason = status, reason
        row.owner_id, row.lease_expires_at, row.updated_at = None, None, utc_now()
        for step in values:
            step.admission_open = False
            if (
                step.file_operation_id is None
                and step.directory_operation_id is None
                and step.status == "pending"
            ):
                step.status, step.error_code = "skipped", reason

    async def _expire(self, sql: AsyncSession, row: WorkspaceTreePlanRow) -> None:
        if row.producer_status == "running" and (row.lease_expires_at or 0) <= time.time():
            await self._close(sql, row, "interrupted", "workspace_tree_lease_expired")

    async def claim(self, scope: str, identifier: str) -> TreePlanExecution:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            await self.files._scope(sql, scope, writing=True)
            row = await self._row(sql, scope, identifier)
            if row.producer_status != "planned":
                raise ServiceError("workspace_tree_claimed", "树计划已被接受或关闭，不可复活", 409)
            await self.files._resource(sql, scope, row.workspace_key, row.resource_fingerprint)
            await assert_source(sql, row)
            await self._rows(sql, row)
            row.producer_status, row.owner_id = "running", self.owner_id
            row.lease_expires_at, row.updated_at = time.time() + self.lease_seconds, utc_now()
            return TreePlanExecution(await self._view(sql, row), row.token)

    async def renew(self, scope: str, identifier: str, token: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._row(sql, scope, identifier)
            if not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_tree_fence_mismatch", "树凭据不匹配", 403)
            assert_live(row, self.owner_id)
            await self.files._scope(sql, scope, writing=True)
            await self.files._resource(sql, scope, row.workspace_key, row.resource_fingerprint)
            await assert_source(sql, row)
            row.lease_expires_at, row.updated_at = time.time() + self.lease_seconds, utc_now()

    async def dispatch(
        self, scope: str, identifier: str, token: str, position: int
    ) -> TreePlanDispatch:
        if type(position) is not int or not 0 <= position < 16384:
            raise ServiceError("workspace_tree_step_not_found", "树步骤位置无效", 404)
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._row(sql, scope, identifier)
            if not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_tree_fence_mismatch", "树凭据不匹配", 403)
            assert_live(row, self.owner_id)
            await self.files._scope(sql, scope, writing=True)
            await self.files._resource(sql, scope, row.workspace_key, row.resource_fingerprint)
            await assert_source(sql, row)
            await self._rows(sql, row)
            step = await sql.get(WorkspaceTreeStepRow, (identifier, position))
            if step is None:
                raise ServiceError("workspace_tree_step_not_found", "树步骤不存在", 404)
            if (
                step.status != "pending"
                or step.file_operation_id is not None
                or step.directory_operation_id is not None
            ):
                raise ServiceError("workspace_tree_step_closed", "树步骤已接纳或关闭", 409)
            await parent_identity(sql, row, step)
            proposal = (
                step.file_proposal_id if step.source == "file" else step.directory_proposal_id
            )
            phase: TreePhase = "preview" if proposal is None else "change"
            step.admission_open = True
            fence = TreePlanFence(identifier, position, cast(TreeSource, step.source), phase, token)
            request: (
                CommandProposalRequest
                | CommandDirectoryProposalRequest
                | WorkspaceChangeAction
                | DirectoryChangeAction
            )
            if phase == "preview":
                preview_type = (
                    CommandProposalRequest
                    if step.source == "file"
                    else CommandDirectoryProposalRequest
                )
                request = preview_type(
                    command_id=row.command_id,
                    path=step.path,
                    request_key=step_key(identifier, position, "preview"),
                    accept_failed=TreePlanRequest.model_validate(
                        row.frozen["request"]
                    ).accept_failed,
                )
                name = "command_propose" if step.source == "file" else "directory_source_preview"
            else:
                if proposal is None or step.expected_revision is None:
                    raise _corrupt()
                action_type = (
                    WorkspaceChangeAction if step.source == "file" else DirectoryChangeAction
                )
                request = action_type(
                    proposal_id=proposal,
                    action="apply",
                    expected_revision=step.expected_revision,
                    request_key=step_key(identifier, position, "change"),
                )
                name = "workspace_change" if step.source == "file" else "directory_change"
            return TreePlanDispatch(fence, name, request)

    async def finish_step(
        self,
        scope: str,
        identifier: str,
        token: str,
        position: int,
        *,
        error_code: str | None = None,
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._row(sql, scope, identifier)
            if not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_tree_fence_mismatch", "树凭据不匹配", 403)
            assert_live(row, self.owner_id)
            step = await sql.get(WorkspaceTreeStepRow, (identifier, position))
            if step is None:
                raise ServiceError("workspace_tree_step_not_found", "树步骤不存在", 404)
            step.admission_open = False
            if step.file_operation_id is None and step.directory_operation_id is None:
                step.status, step.error_code = (
                    "rejected",
                    error_code or "workspace_tree_original_operation_missing",
                )

    async def finish(self, scope: str, identifier: str, token: str) -> TreePlanView:
        async with self.database.write_lock, self.database.sessions.begin() as sql:
            await self.files._writer(sql)
            row = await self._row(sql, scope, identifier)
            if not secrets.compare_digest(row.token, token):
                raise ServiceError("workspace_tree_fence_mismatch", "树凭据不匹配", 403)
            assert_live(row, self.owner_id)
            await self._close(sql, row, "finished", None)
            return await self._view(sql, row)
