"""从原0030完整命令账本冷读目录依赖；不写proposal/attempt或宿主文件。"""

from __future__ import annotations

from typing import Literal, cast

from pydantic import ValidationError

from krim_agent.execution.directory_dependencies import (
    DirectoryDependencyEntry,
    build_directory_dependency_plan,
)
from krim_agent.execution.directory_publication import (
    CommandDirectoryDependencyRequest,
    CommandDirectoryDependencyView,
)
from krim_agent.protocol.domain import ServiceError

from .command_proposals import load_source
from .workspace_edits import WorkspaceEditStore


class CommandDirectoryDependencyStore:
    def __init__(self, files: WorkspaceEditStore) -> None:
        self.files, self.database = files, files.database

    async def plan(
        self, scope: str, request: CommandDirectoryDependencyRequest
    ) -> CommandDirectoryDependencyView:
        request = CommandDirectoryDependencyRequest.model_validate(request.model_dump())
        async with self.database.write_lock, self.database.sessions() as sql:
            await self.files._scope(sql, scope, writing=False)
            source = await load_source(sql, scope, request.command_id)
            key, resource = source.workspace.get("key"), source.workspace.get("fingerprint")
            if not isinstance(key, str) or not isinstance(resource, str):
                raise ServiceError("execution_source_corrupt", "原命令工作区来源无效", 409)
            await self.files._resource(sql, scope, key, resource)
            if source.directories is None:
                raise ServiceError(
                    "execution_directories_unrecorded", "原命令没有已采集的目录清单", 409
                )
            if request.source_hash is not None and request.source_hash != source.source_hash:
                raise ServiceError(
                    "execution_directory_dependency_source_conflict", "原命令成果来源已变化", 409
                )
            if source.outcome != "completed" and not request.accept_failed:
                raise ServiceError(
                    "execution_failed_artifacts", "失败命令目录成果须显式accept_failed", 409
                )
            total = len(source.directories) + len(source.artifacts)
            if total == 0:
                raise ServiceError(
                    "execution_directory_dependency_empty", "原命令没有可规划的目录或文件成果", 409
                )
            if total > request.max_entries:
                raise ServiceError(
                    "execution_directory_dependency_limit", "目录依赖条目超过调用方上限", 409
                )
            if any(item.mode & 0o700 != 0o700 for item in source.directories.values()):
                raise ServiceError("execution_artifact_mode", "原目录模式不支持宿主受控创建", 422)
            try:
                entries = [
                    DirectoryDependencyEntry(path=item.path, kind="directory", mode=item.mode)
                    for item in source.directories.values()
                ]
                entries.extend(
                    DirectoryDependencyEntry(path=item.path, kind="file")
                    for item in source.artifacts.values()
                )
                plan = build_directory_dependency_plan(entries, max_entries=request.max_entries)
            except ValidationError:
                raise ServiceError(
                    "execution_source_corrupt", "目录计划原路径或模式无效", 409
                ) from None
            return CommandDirectoryDependencyView(
                command_id=source.row.command_id,
                scope_id=scope,
                source_hash=source.source_hash,
                workspace_key=key,
                resource_fingerprint=resource,
                execution_resource=source.snapshot.resource,
                outcome=cast(Literal["completed", "failed", "output_limit"], source.outcome),
                accept_failed=request.accept_failed,
                plan=plan,
                files=[source.artifacts[path] for path in sorted(source.artifacts)],
            )
