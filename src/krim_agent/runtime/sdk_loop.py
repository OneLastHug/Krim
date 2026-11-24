"""One SDK-owned loop for the explicit development target.

The existing RunService claims/parks/finishes runs. This adapter supplies scope,
validated model steps and tools to ai.Agent; it does not dispatch a second loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any, cast

from pydantic import JsonValue, ValidationError

from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.config import Settings
from krim_agent.context.compaction import Compactor
from krim_agent.context.compaction_source import RunGroupSource
from krim_agent.context.types import (
    ChildRunContext,
    ContextPolicy,
    DevelopmentSessionContext,
    SessionContext,
)
from krim_agent.contracts import (
    Message,
    RunEvent,
    ServiceError,
    ToolObservation,
    fingerprint,
    parse_json,
)
from krim_agent.development.contracts import EnvironmentHandle, OperationReceipt, RunOwnership
from krim_agent.providers.types import ModelProvider
from krim_agent.storage.development import DevelopmentStore
from krim_agent.storage.development_context import DevelopmentCompactionSource
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.subagents.isolated_bootstrap import prepare_isolated_workspace
from krim_agent.subagents.isolated_tools import isolate_invocation, isolated_executor
from krim_agent.subagents.workspace import WorkspaceAssignment
from krim_agent.tools.executor import Approve, ToolContext, ToolExecutor
from krim_agent.tools.pipeline import Scope
from krim_agent.tools.session import SessionToolScope

from .contracts import ExecutionBudget
from .engine import AgentLoop, ApprovalRequest, _no_inprocess_wait
from .recovery import BootstrapResume
from .recovery import SDKResume as SDKResume
from .sdk_execution import SDKExecutionPlan, SDKExecutionPolicy, SDKHistory, execute_sdk


@dataclass(frozen=True)
class DevelopmentRunScope:
    environment: EnvironmentHandle
    ownership: RunOwnership
    tools: Scope | None


class DevelopmentSDKLoop(AgentLoop):
    def __init__(
        self,
        store: SQLiteStore,
        provider: ModelProvider,
        executor: ToolExecutor,
        settings: Settings,
        *,
        resolve_scope: Callable[
            [str, Callable[[], None], ExecutionBudget], Awaitable[DevelopmentRunScope]
        ],
        repositories: DevelopmentStore,
        cleanup_run: Callable[[str], Awaitable[None]],
        descriptor: dict[str, JsonValue],
        approval_projection: Callable[
            [Any, dict[str, JsonValue]], Awaitable[dict[str, JsonValue]]
        ] | None = None,
        media_for_run: Callable[
            [str], Callable[[list[Message]], Awaitable[list[Message]]]
        ] | None = None,
    ) -> None:
        super().__init__(store, provider, executor, settings)
        self.development_scope = resolve_scope
        self.development_store = repositories
        self.development_cleanup = cleanup_run
        self.development_descriptor = descriptor
        # Wrap once here: the binding descriptor fingerprints this callable by
        # identity, so it must not be replaced per run.
        self.development_approval_projection = (
            self._isolated_projection(approval_projection)
            if approval_projection is not None else None
        )
        self.development_media_for_run = media_for_run

    async def _isolated_assignment(
        self, context: ChildRunContext
    ) -> WorkspaceAssignment | None:
        """The isolated workspace assignment of a child task, if any.

        Read from the child task's frozen input; the model never supplies
        paths. Prepared state lives in the ``workspace_prepared`` run event
        (see ``subagents.isolated_bootstrap``), never in this input.
        """
        subagents = self.subagents
        if subagents is None:
            return None
        task = await subagents.repository.task(context.task_id)
        raw = task["input"].get("workspace")
        if raw is None:
            profile = task["input"].get("profile_snapshot")
            if isinstance(profile, dict) and profile.get("workspace") == "isolated_write":
                raise ServiceError("facts_corrupt")
            return None
        try:
            assignment = WorkspaceAssignment.model_validate(raw)
        except (ValidationError, ValueError):
            raise ServiceError("facts_corrupt") from None
        if assignment.mode != "isolated_write" or not assignment.directory:
            return None
        return assignment

    async def _invocation_directory(self, run_id: str) -> str | None:
        snapshot = await self.store.snapshot(run_id)
        if not isinstance(snapshot.session_context, ChildRunContext):
            return None
        assignment = await self._isolated_assignment(snapshot.session_context)
        return assignment.directory if assignment is not None else None

    def _isolated_projection(
        self,
        projection: Callable[[Any, dict[str, JsonValue]], Awaitable[dict[str, JsonValue]]],
    ) -> Callable[[Any, dict[str, JsonValue]], Awaitable[dict[str, JsonValue]]]:
        """Approval display for isolated children: relocated view plus root.

        The stored tool input and the authorization digest of the real request
        are untouched; only the projection payload gains the relocated
        arguments and an explicit workspace field.
        """
        async def project(invocation: Any, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
            try:
                directory = await self._invocation_directory(invocation.run_id)
            except ServiceError:
                return await projection(invocation, data)
            if directory is None:
                return await projection(invocation, data)
            projected = isolate_invocation(invocation, directory)
            return await projection(projected, {
                **data,
                "arguments": parse_json(projected.arguments_json),
                "workspace": {"mode": "isolated_write", "directory": directory},
            })

        return project

    async def run(
        self,
        run_id: str,
        approval_request: ApprovalRequest | Approve,
        assert_target_current: Callable[[], None] = lambda: None,
        execution_budget: ExecutionBudget | None = None,
        resume: SDKResume | BootstrapResume | None = None,
        executor: ToolExecutor | None = None,
        run_tools: Any | None = None,
        session_scope: SessionToolScope | None = None,
        startup_gate: Callable[[], asyncio.Lock | None] | None = None,
    ) -> None:
        if execution_budget is None:
            raise ServiceError("execution_error")
        assert_target_current()
        snapshot = await self.store.snapshot(run_id)
        if snapshot.run.status != "running" or snapshot.run.stop_requested:
            raise ServiceError("cancelled" if snapshot.run.stop_requested else "run_closed")
        if not isinstance(
            snapshot.session_context, (SessionContext, DevelopmentSessionContext, ChildRunContext)
        ):
            raise ServiceError("workspace_contract_required")
        development_context = (
            snapshot.session_context
            if isinstance(snapshot.session_context, DevelopmentSessionContext)
            else None
        )
        child_context = (snapshot.session_context
                         if isinstance(snapshot.session_context, ChildRunContext) else None)
        if (child_context is None and self.settings.development_history_compaction
                != (development_context is not None)):
            raise ServiceError("facts_corrupt")
        scope = await self.development_scope(run_id, assert_target_current, execution_budget)
        if (
            scope.ownership.run_id != run_id
            or development_context is not None
            and (
                scope.ownership.owner_id != development_context.owner_id
                or scope.ownership.session_id != development_context.session_id
                or scope.environment.project_id != development_context.project_id
            )
        ):
            raise ServiceError("permission_denied")
        active = executor or self.executor
        bootstrap_resume = resume if isinstance(resume, BootstrapResume) else None
        # The bootstrap envelope never reaches the SDK restore path; a child
        # that has not sampled yet restarts execute_sdk from its own input.
        resume_for_sdk = None if bootstrap_resume is not None else resume
        assignment: WorkspaceAssignment | None = None
        child_task_id = ""
        if child_context is not None:
            assignment = await self._isolated_assignment(child_context)
            child_task_id = child_context.task_id
        source = (DevelopmentCompactionSource(development_context.frozen)
                  if development_context is not None else None)

        async def verify_source() -> None:
            if source is not None:
                await cast(SQLiteStore, self.store).work_read(
                    lambda conn: source.verify(conn, run_id)
                )
            assert_target_current()

        async def emit(kind: str, data: dict[str, JsonValue]) -> RunEvent:
            assert_target_current()
            if kind == "model_start":
                await verify_source()
            event = await self.store.append(run_id, kind, data)
            if kind == "model_start":
                await verify_source()
            return event

        policy = SDKExecutionPolicy.from_settings(self.settings)
        if child_context is not None:
            policy = replace(policy, fail_on_tool_error=False)
        context = ToolContext(
            emit,
            _no_inprocess_wait,
            policy.tool_timeout,
            assert_target_current,
            scope.tools,
            policy.approval_timeout,
        )
        if assignment is not None and assignment.directory:
            # 先经真实准入/审批链准备副本（workspace_prepared 事件为凭），
            # 再派生重定位执行面；隔离子代理的第一个模型采样前目录已就绪。
            await prepare_isolated_workspace(
                run_id=run_id,
                store=self.store,
                operations=self.development_store.operations,
                executor=active,
                context=context,
                environment=scope.environment,
                ownership=scope.ownership,
                emit=emit,
                approval_request=approval_request,
                resume=bootstrap_resume,
                budget=execution_budget,
                assignment=assignment,
                task_id=child_task_id,
                check=assert_target_current,
            )
            active = isolated_executor(active, assignment.directory)
            if run_tools is not None:
                # HostModelProvider and SDKAdapter must agree on the same
                # derived schema/digests, including across approval restores.
                run_tools.executor = active
                run_tools._registry_hash = fingerprint(active.registry.schemas())
        legacy_context = (snapshot.session_context
                          if isinstance(snapshot.session_context, SessionContext) else None)
        context_policy = (
            ContextPolicy(max_bytes=self.settings.context_max_bytes,
                          max_messages=self.settings.context_max_messages)
            if child_context is not None else
            development_context.frozen.policy if development_context is not None
            else cast(SessionContext, legacy_context).context.policy
        )

        async def build_history(stepper: Any, request_limit: int | None) -> SDKHistory:
            nonlocal session_scope
            history = None
            if development_context is not None:
                provider_identity = snapshot.target.get("provider")
                if not isinstance(provider_identity, dict):
                    raise ServiceError("facts_corrupt")
                assert source is not None
                compactor = Compactor.from_source(
                    cast(SQLiteStore, self.store),
                    source,
                    stepper,
                    assert_target_current,
                    development_context.frozen.system_prompt,
                    request_limit,
                    provider_identity,
                )
                boundary = None
                if self.context_service is not None:
                    if session_scope is None and self.session_tool_host is not None:
                        session_scope = await self.session_tool_host.by_run(
                            run_id, assert_target_current,
                        )
                    if session_scope is not None:
                        boundary = self.context_service.boundary(
                            session_scope.actor, session_scope.workspace_id,
                            session_scope.session_id, run_id, scope=session_scope,
                        )
                history = CompactorHistory(
                    compactor, run_id, boundary=boundary,
                    live_memory=(boundary is not None and self.context_service is not None
                                 and self.context_service.snapshots is not None),
                    run_source=RunGroupSource(
                        development_context.frozen.turn_id, source.verify,
                    ),
                )
            return SDKHistory(history, context_policy)

        async def read_receipt(receipt: OperationReceipt) -> JsonValue:
            if (
                receipt.identity.ownership != scope.ownership
                or receipt.identity.environment != scope.environment
                or receipt.state != "completed"
                or receipt.result_ref is None
            ):
                raise ServiceError("facts_corrupt")
            stored = await self.development_store.operations.lookup(receipt.identity)
            if stored != receipt:
                raise ServiceError("facts_corrupt")
            current = await self.store.snapshot(run_id)
            event = next(
                (
                    item
                    for item in reversed(current.events)
                    if item.kind == "tool_end" and item.data.get("call_id") == receipt.result_ref
                ),
                None,
            )
            if event is None:
                raise ServiceError("facts_corrupt")
            observed = ToolObservation.model_validate(event.data)
            if (
                observed.call_id != receipt.identity.call_id
                or observed.status != "ok"
                or observed.result_available is not True
            ):
                raise ServiceError("facts_corrupt")
            return cast(JsonValue, observed.model_dump(mode="json"))

        async def unused_result(_ref: str) -> JsonValue:
            raise ServiceError("facts_corrupt")

        initial_messages = (
            [Message(role="system", content=self.settings.system_prompt),
             *child_context.messages, Message(role="user", content=snapshot.request.text)]
            if child_context is not None else
            [development_context.frozen.current]
            if development_context is not None
            else list(cast(SessionContext, legacy_context).context.messages)
        )
        # SDK resume may replay a pending approved tool before another model call.
        # Recheck the frozen source's live authorization before entering that path.
        await verify_source()
        host_media = (
            self.session_media.for_run(run_id, assert_target_current)
            if self.session_media is not None else
            self.development_media_for_run(run_id)
            if self.development_media_for_run is not None
            else None
        )
        await execute_sdk(SDKExecutionPlan(
            store=self.store,
            provider=self.provider,
            executor=active,
            settings=self.settings,
            run_id=run_id,
            model_id="krim-development",
            initial_history=initial_messages,
            policy=policy,
            execution_budget=execution_budget,
            tool_context=context,
            emit=emit,
            check=assert_target_current,
            approval_request=approval_request,
            read_result=unused_result,
            read_receipt=read_receipt,
            build_history=build_history,
            operations=self.development_store.operations,
            environment=scope.environment,
            ownership=scope.ownership,
            run_tools=run_tools,
            host_media=host_media,
            startup_gate=startup_gate,
            resume=resume_for_sdk,
            workspace=development_context is not None,
            on_completed=verify_source,
            subagents=self.subagents,
        ))
        if assignment is not None and assignment.directory and self.subagents is not None:
            from krim_agent.subagents.workspace import SandboxWorkspaceReader, WorkspaceIsolation

            tools_scope = scope.tools
            sandbox_service = getattr(getattr(tools_scope, "operations", None), "sandbox", None)
            actor = getattr(tools_scope, "actor", None)
            if sandbox_service is None or actor is None:
                raise ServiceError("workspace_contract_required")
            baseline = await self.subagents.workspace_baseline(run_id)
            patch = await WorkspaceIsolation(SandboxWorkspaceReader(
                sandbox_service, actor,
            )).collect(assignment, baseline)
            assert_target_current()
            await emit("workspace_patch", {
                "patch_fingerprint": patch.fingerprint,
                "baseline_fingerprint": patch.baseline_fingerprint,
                "source_project_id": patch.source_project_id,
                "workspace_project_id": patch.workspace_project_id,
                "directory": patch.directory, "mode": patch.mode,
                "input_version": patch.input_version, "truncated": patch.truncated,
                "changes": [{"path": change.path, "kind": change.kind,
                    "base_sha256": change.base_sha256, "result_sha256": change.result_sha256,
                    "size": change.size, "binary": change.binary} for change in patch.changes],
            })
