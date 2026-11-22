"""Explicit root integration over the existing approved sandbox file surface."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel, Field, JsonValue

from krim_agent.contracts import ServiceError, StrictModel, encoded, fingerprint
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.sandbox.file_models import DirectoryCreate, FileRemove, FileWrite
from krim_agent.sandbox.operations import SandboxOperations, SessionCommandScope
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.executor import ToolContext
from krim_agent.tools.pipeline import CURRENT, _calling
from krim_agent.tools.session import SessionScopeResolver, ToolBundle

from .workspace import SandboxWorkspaceReader


class WorkspaceDiff(StrictModel):
    task_id: str = Field(min_length=1, max_length=36)


class WorkspaceIntegrate(WorkspaceDiff):
    expected_patch_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class ApprovedIntegrationPort(SandboxWorkspaceReader):
    """An already-approved root operation, with a receipt for each file mutation.

    The outer tool carries the exact patch fingerprint through session policy
    and approval. Each mutation still uses SandboxOperations project/generation
    validation and durable broker receipts. No host filesystem is exposed.
    """

    def __init__(
        self, service: Any, caller: Any, invocation: Invocation, check: Callable[[], None]
    ) -> None:
        super().__init__(service, caller.scope.actor)
        self.service = service
        self.caller = caller
        self.invocation = invocation
        self.check = check
        self.operations = SandboxOperations(service)
        self.budget = ExecutionBudget(None)

    async def _change(self, operation: str, values: BaseModel) -> dict[str, Any]:
        self.check()
        if getattr(values, "project_id", None) != self.caller.tree["project_id"]:
            raise ServiceError("permission_denied")
        project = await self.service.project(
            self.caller.scope.actor, self.caller.tree["project_id"], write=True
        )
        values = values.model_copy(update={"expected_generation": project["generation"]})
        binding = await self.service.store.snapshot(self.invocation.run_id)
        if binding.run.stop_requested or binding.run.status != "running":
            raise ServiceError("cancelled")
        from krim_agent.sandbox.bindings import read_binding

        selected = await read_binding(
            self.service, self.caller.scope.actor, self.caller.scope.session_id
        )
        command = replace(
            self.invocation,
            name=operation,
            call_id="integrate-"
            + fingerprint(
                {
                    "outer": self.invocation.call_id,
                    "operation": operation,
                    "path": getattr(values, "path", ""),
                }
            )[:48],
            arguments_json=encoded(values.model_dump(mode="json")),
        )
        scope = SessionCommandScope(
            actor=self.caller.scope.actor,
            workspace_id=self.caller.scope.workspace_id,
            session_id=self.caller.scope.session_id,
            operations=self.operations,
            check=self.check,
            initial_deadline=None,
            execution_budget=self.budget,
            invocation=command,
            project_id=selected["project_id"],
            binding_revision=selected["revision"],
        )
        with _calling(command):
            token = CURRENT.set(cast(Any, scope))
            try:
                result = await self.operations.execute(command, values, None)
            finally:
                CURRENT.reset(token)
        if not isinstance(result, dict):
            raise ServiceError("invalid_output")
        if result.get("status") == "unknown" or result.get("executor_started") is None:
            raise ServiceError("execution_unknown")
        if result.get("code") or result.get("exit_code") not in {None, 0}:
            raise ServiceError(str(result.get("code") or "execution_error"))
        self.check()
        return result

    async def write(
        self, project_id: str, path: str, content: str, *, expected_sha256: str | None = None
    ) -> dict[str, Any]:
        return await self._change(
            "sandbox_write",
            FileWrite(
                project_id=project_id,
                path=path,
                content=content,
                expected_sha256=expected_sha256,
                expected_generation=0,
            ),
        )

    async def remove(self, project_id: str, path: str, *, expected_sha256: str) -> dict[str, Any]:
        return await self._change(
            "sandbox_remove",
            FileRemove(
                project_id=project_id,
                path=path,
                expected_sha256=expected_sha256,
                expected_generation=0,
            ),
        )

    async def make_directory(self, project_id: str, path: str) -> dict[str, Any]:
        return await self._change(
            "sandbox_mkdir",
            DirectoryCreate(
                project_id=project_id,
                path=path,
                expected_generation=0,
            ),
        )


async def _unavailable(parameters: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


def bundle(service: Any, resolve: SessionScopeResolver) -> ToolBundle:
    async def policy(invocation: Invocation) -> Decision:
        if invocation.name not in {"workspace_diff", "workspace_integrate"}:
            return "deny"
        scope = await resolve(invocation)
        scope.check()
        if scope.project_id is None:
            return "deny"
        return "ask" if invocation.name == "workspace_integrate" else "allow"

    async def execute(invocation: Invocation, parsed: BaseModel, context: object) -> JsonValue:
        caller = await service._caller(invocation)
        if invocation.name == "workspace_diff" and isinstance(parsed, WorkspaceDiff):
            result = await service.workspace_diff(caller, parsed.task_id)
            return cast(JsonValue, result)
        if invocation.name != "workspace_integrate" or not isinstance(parsed, WorkspaceIntegrate):
            raise ServiceError("invalid_input", 422)
        if not isinstance(context, ToolContext) or service.sandbox_service is None:
            raise ServiceError("workspace_contract_required")
        from .integration import root_merge

        def check() -> None:
            caller.scope.check()
            context.assert_target_current()

        port = ApprovedIntegrationPort(service.sandbox_service, caller, invocation, check)
        result = await root_merge(
            service.store,
            caller,
            invocation,
            parsed.task_id,
            parsed.expected_patch_fingerprint,
            port,
        )
        check()
        return cast(JsonValue, result)

    digest = hashlib.sha256()
    for name in ("workspace_tools.py", "workspace.py", "integration.py"):
        digest.update(Path(__file__).with_name(name).read_bytes())
    common = dict(
        executor=_unavailable,
        output_schema={"type": "object"},
        version="subagent-workspace/1",
        implementation_digest=digest.hexdigest(),
        scoped_executor=execute,
    )
    return ToolBundle(
        specs=(
            ToolSpec(
                name="workspace_diff",
                parameters=WorkspaceDiff,
                capability="session_read",
                risk="read",
                concurrency="parallel",
                description="Read a completed worker's isolated patch and baseline by task ID.",
                **common,
            ),
            ToolSpec(
                name="workspace_integrate",
                parameters=WorkspaceIntegrate,
                capability="session_write",
                risk="write",
                description=(
                    "Explicitly integrate a descendant's patch into the root "
                    "project using its expected fingerprint. "
                    "Baseline or artifact drift leaves both "
                    "versions intact. Run relevant tests on the returned post_fingerprint before "
                    "claiming the integrated change works."
                ),
                **common,
            ),
        ),
        policy=policy,
    )
