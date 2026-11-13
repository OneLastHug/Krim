"""sandbox/1 通过普通会话的现有审批停靠链执行，不伪造工作区完成证明。"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import BaseModel, JsonValue
from sqlalchemy import select

from krim_agent.context.types import SessionContext
from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.sandbox.command import SandboxCommand
from krim_agent.sandbox.file_models import (
    DirectoryCreate,
    FileFind,
    FileList,
    FilePatch,
    FileRead,
    FileRemove,
    FileRename,
    FileReplacePreview,
    FileSearch,
    FileWrite,
)
from krim_agent.security import PrincipalContext
from krim_agent.storage.models import sessions
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.pipeline import CURRENT, Scope
from krim_agent.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from krim_agent.sandbox.service import SandboxService


@dataclass(frozen=True)
class SessionCommandScope:
    """与ToolExecutor共享调用围栏接口，不含旧WorkFrame/产物revision。"""

    actor: PrincipalContext
    workspace_id: str
    session_id: str
    operations: SandboxOperations
    check: Callable[[], None]
    initial_deadline: float
    execution_budget: ExecutionBudget | None = None
    invocation: Invocation | None = None
    project_id: str | None = None
    binding_revision: int = 0

    @property
    def deadline(self) -> float:
        return self.execution_budget.deadline if self.execution_budget else self.initial_deadline


class SandboxOperations:
    def __init__(self, service: SandboxService) -> None:
        self.service = service

    async def scope(self, run_id: str, check: Callable[[], None], budget: ExecutionBudget) -> Scope:
        snapshot = await self.service.store.snapshot(run_id)
        context = snapshot.session_context
        if not isinstance(context, SessionContext):
            raise ServiceError("workspace_contract_required")
        async with self.service.store.lock, self.service.store.engine.connect() as conn:
            session = (
                (await conn.execute(select(sessions).where(sessions.c.id == context.session_id)))
                .mappings()
                .one()
            )
        actor = self.service.identities.resolve(session["owner_id"])
        from krim_agent.sandbox.bindings import read_binding

        binding = await read_binding(self.service, actor, context.session_id)
        actor.member(context.workspace_id, write=True)
        existing = next(
            (event.data for event in snapshot.events if event.kind == "sandbox_authorization"), None
        )
        if existing is not None and (
            existing.get("principal_id") != actor.principal_id
            or existing.get("generation") != actor.generation
            or existing.get("workspace_id") != context.workspace_id
            or existing.get("project_id") != binding["project_id"]
            or existing.get("binding_revision", 0) != binding["revision"]
        ):
            raise ServiceError("permission_denied")
        if existing is None:
            await self.service.store.append(
                run_id,
                "sandbox_authorization",
                {
                    "principal_id": actor.principal_id,
                    "generation": actor.generation,
                    "workspace_id": context.workspace_id,
                    "project_id": binding["project_id"],
                    "binding_revision": binding["revision"],
                },
                event_id="sandbox-authorization",
            )
        accepted = actor.generation

        def fence() -> None:
            check()
            current = self.service._live(actor)
            if current.generation != accepted or time.monotonic() >= budget.deadline:
                raise ServiceError("permission_denied")
            current.member(context.workspace_id, write=True)

        # Scope只作为executor的共同调用协议使用；引擎不将此对象交给旧verifier/compactor。
        return cast(
            Scope,
            SessionCommandScope(
                actor,
                context.workspace_id,
                context.session_id,
                self,
                fence,
                budget.deadline,
                budget,
                project_id=binding["project_id"],
                binding_revision=binding["revision"],
            ),
        )

    def current(self, invocation: Invocation) -> SessionCommandScope:
        scope = CURRENT.get()
        if not isinstance(scope, SessionCommandScope) or scope.operations is not self:
            raise ServiceError("workspace_contract_required")
        if scope.invocation != invocation:
            raise ServiceError("permission_denied")
        scope.check()
        return scope

    def selected(self, scope: SessionCommandScope, value: BaseModel) -> Any:
        selected = getattr(value, "project_id", None) or scope.project_id
        if selected is None:
            raise ServiceError("workspace_contract_required")
        if scope.project_id is not None and selected != scope.project_id:
            raise ServiceError("permission_denied")
        return value.model_copy(update={"project_id": selected})

    async def policy(self, invocation: Invocation) -> Decision:
        scope = self.current(invocation)
        model = self.models().get(invocation.name)
        if model is None:
            return "deny"
        parsed = model.model_validate_json(invocation.arguments_json)
        if invocation.name == "sandbox_project":
            return "allow"
        values = self.selected(scope, parsed)
        write = isinstance(values, (SandboxCommand, DirectoryCreate, FilePatch, FileRename))
        project = await self.service.project(scope.actor, values.project_id, write=write)
        if project["workspace_id"] != scope.workspace_id:
            raise ServiceError("permission_denied")
        if write:
            if project["status"] != "ready":
                raise ServiceError("workspace_busy")
            if project["generation"] != values.expected_generation:
                raise ServiceError("input_revision_conflict")
        scope.check()
        return "ask" if write else "allow"

    async def execute(
        self, invocation: Invocation, parameters: BaseModel, context: object
    ) -> JsonValue:
        scope = self.current(invocation)
        if invocation.name == "sandbox_project":
            from krim_agent.sandbox.bindings import read_binding

            binding = await read_binding(self.service, scope.actor, scope.session_id)
            if binding["project_id"] is not None:
                binding["project"] = await self.service.project(scope.actor, binding["project_id"])
            else:
                binding["projects"] = await self.service.list_projects(scope.actor, limit=20)
            scope.check()
            return cast(JsonValue, binding)
        values = self.selected(scope, parameters)
        project_id = values.project_id
        if isinstance(values, (DirectoryCreate, SandboxCommand, FilePatch, FileRename)):
            from krim_agent.sandbox.execution import execute_command

            if isinstance(values, SandboxCommand) and values.wall_seconds + 5 > min(
                30, scope.deadline - time.monotonic()
            ):
                raise ServiceError("run_timeout")
            result = await execute_command(self.service, scope.actor, values, invocation, scope)
            if result.get("code") is not None:
                raise ServiceError(result["code"])
        elif isinstance(values, FileFind):
            result = await self.service.find_files(
                scope.actor,
                project_id,
                path=values.path,
                include_glob=values.include_glob,
                limit=values.limit,
                offset=values.offset,
            )
            if result.get("truncated"):
                result["next_offset"] = None
        elif isinstance(values, FileReplacePreview):
            result = await self.service.preview_replace(
                scope.actor,
                project_id,
                path=values.path,
                expected_generation=values.expected_generation,
                expected_sha256=values.expected_sha256,
                find=values.find,
                replacement=values.replacement,
                occurrence=values.occurrence,
            )
        elif isinstance(values, FileSearch):
            result = await self.service.search_files(
                scope.actor,
                project_id,
                query=values.query,
                path=values.path,
                limit=values.limit,
                offset=values.offset,
                include_glob=values.include_glob,
                mode=values.mode,
                case_sensitive=values.case_sensitive,
                before=values.before,
                after=values.after,
            )
            result["max_result_bytes"] = 64 * 1024
            if values.mode == "regex":
                result["regex_limit_bytes"] = 64
                result["max_regex_scan_bytes"] = 512 * 1024
                result["max_regex_line_chars"] = 1024
        elif isinstance(values, FileList):
            result = await self.service.directory(
                scope.actor, project_id, path=values.path, limit=values.limit, offset=values.offset
            )
        elif isinstance(values, FileRead):
            result = await self.service.read_file(
                scope.actor, project_id, path=values.path, offset=values.offset, limit=values.limit
            )
        else:
            raise ServiceError("invalid_input", 422)
        scope.check()
        return cast(JsonValue, result)

    @staticmethod
    def models() -> dict[str, type[BaseModel]]:
        return {
            "sandbox_project": StrictModel,
            "sandbox_command_run": SandboxCommand,
            "sandbox_read": FileRead,
            "sandbox_list": FileList,
            "sandbox_find": FileFind,
            "sandbox_search": FileSearch,
            "sandbox_preview_replace": FileReplacePreview,
            "sandbox_write": FileWrite,
            "sandbox_mkdir": DirectoryCreate,
            "sandbox_remove": FileRemove,
            "sandbox_patch": FilePatch,
            "sandbox_rename": FileRename,
        }

    async def observe(self, invocation: Invocation) -> dict[str, JsonValue] | None:
        from krim_agent.sandbox.execution import observe_command

        return await observe_command(self.service, invocation)

    def registry(self) -> ToolRegistry:
        async def unavailable(_: BaseModel) -> JsonValue:
            raise ServiceError("workspace_contract_required")

        def _output_schema(name: str) -> dict[str, JsonValue]:
            if name == "sandbox_find":
                return {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "include_glob": {"type": "string"},
                        "files": {"type": "array", "items": {"type": "object"}},
                        "offset": {"type": "integer"},
                        "next_offset": {"type": ["integer", "null"]},
                        "total": {"type": ["integer", "null"]},
                        "truncated": {"type": "boolean"},
                        "entry_cap_reached": {"type": "boolean"},
                        "depth_cap_reached": {"type": "boolean"},
                        "project_id": {"type": "string"},
                        "generation": {"type": "integer"},
                        "may_change": {"type": "boolean"},
                    },
                }
            if name == "sandbox_preview_replace":
                return {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "generation": {"type": "integer"},
                        "source_sha256": {"type": "string"},
                        "result_sha256": {"type": "string"},
                        "occurrence": {"type": "integer"},
                        "matches": {"type": "integer"},
                        "patch": {"type": "object"},
                        "diff": {"type": "string"},
                        "diff_truncated": {"type": "boolean"},
                        "preview_only": {"const": True},
                    },
                }
            return {"type": "object"}

        digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        result = []
        for name, model in self.models().items():
            write = name in {
                "sandbox_command_run",
                "sandbox_write",
                "sandbox_mkdir",
                "sandbox_remove",
                "sandbox_patch",
                "sandbox_rename",
            }
            capability = (
                "command" if name == "sandbox_command_run" else "write" if write else "read"
            )
            risk: Literal["read", "write", "exec"] = (
                "exec" if capability == "command" else "write" if write else "read"
            )
            result.append(
                ToolSpec(
                    name,
                    model,
                    unavailable,
                    (
                        {
                            "type": "object",
                            "properties": {
                                "path": {"type": "string"},
                                "project_id": {"type": "string"},
                                "generation": {"type": "integer"},
                                "may_change": {"type": "boolean"},
                                "offset": {"type": "integer"},
                                "matches": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "path": {"type": "string"},
                                            "line": {"type": "integer"},
                                            "preview": {"type": "string"},
                                            "before": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                            "after": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                            },
                                        },
                                        "required": ["path", "line", "preview", "before", "after"],
                                        "additionalProperties": False,
                                    },
                                },
                                "total_matches": {"type": ["integer", "null"]},
                                "next_offset": {"type": ["integer", "null"]},
                                "truncated": {"type": "boolean"},
                                "entry_cap_reached": {"type": "boolean"},
                                "match_cap_reached": {"type": "boolean"},
                                "skipped": {"type": "integer"},
                                "scanned_bytes": {"type": "integer"},
                                "byte_cap_reached": {"type": "boolean"},
                                "depth_cap_reached": {"type": "boolean"},
                                "max_result_bytes": {"type": "integer"},
                                "regex_limit_bytes": {"type": "integer"},
                                "max_regex_scan_bytes": {"type": "integer"},
                                "max_regex_line_chars": {"type": "integer"},
                            },
                        }
                        if name == "sandbox_search"
                        else _output_schema(name)
                    ),
                    "sandbox/1",
                    digest,
                    risk,
                    self.execute,
                    capability,
                    description="访问会话绑定的持久项目；写操作需批准，失败和取消保留已发生改动。",
                )
            )
        return ToolRegistry(tuple(result), max_tools=24)
