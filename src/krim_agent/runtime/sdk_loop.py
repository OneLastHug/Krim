"""One SDK-owned loop for the explicit development target.

The existing RunService claims/parks/finishes runs. This adapter supplies scope,
validated model steps and tools to ai.Agent; it does not dispatch a second loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

import ai
from pydantic import JsonValue

from krim_agent.agent_sdk.adapter import ProbeStopped, SDKAdapter
from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import HostModelProvider, agent_tools, sdk_messages
from krim_agent.config import Settings
from krim_agent.context.compaction import Compactor
from krim_agent.context.types import DevelopmentSessionContext, SessionContext
from krim_agent.contracts import Message, RunEvent, ServiceError, ToolObservation, fingerprint
from krim_agent.development.contracts import EnvironmentHandle, OperationReceipt, RunOwnership
from krim_agent.providers.types import ModelProvider
from krim_agent.storage.development import DevelopmentStore
from krim_agent.storage.development_context import DevelopmentCompactionSource
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.executor import Approve, ToolContext, ToolExecutor
from krim_agent.tools.pipeline import Scope

from .contracts import ExecutionBudget
from .engine import AgentLoop, ApprovalRequest, Parked, _no_inprocess_wait
from .model_step import ModelStep
from .recovery import SDKResume as SDKResume


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
        self.development_approval_projection = approval_projection
        self.development_media_for_run = media_for_run

    async def run(
        self,
        run_id: str,
        approval_request: ApprovalRequest | Approve,
        assert_target_current: Callable[[], None] = lambda: None,
        execution_budget: ExecutionBudget | None = None,
        resume: SDKResume | None = None,
        executor: ToolExecutor | None = None,
    ) -> None:
        if execution_budget is None:
            raise ServiceError("execution_error")
        assert_target_current()
        snapshot = await self.store.snapshot(run_id)
        if snapshot.run.status != "running" or snapshot.run.stop_requested:
            raise ServiceError("cancelled" if snapshot.run.stop_requested else "run_closed")
        if not isinstance(
            snapshot.session_context, (SessionContext, DevelopmentSessionContext)
        ):
            raise ServiceError("workspace_contract_required")
        development_context = (
            snapshot.session_context
            if isinstance(snapshot.session_context, DevelopmentSessionContext)
            else None
        )
        if self.settings.development_history_compaction != (development_context is not None):
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

        context = ToolContext(
            emit,
            _no_inprocess_wait,
            self.settings.tool_timeout,
            assert_target_current,
            scope.tools,
            self.settings.approval_timeout,
        )
        stepper = ModelStep(
            self.provider,
            emit,
            assert_target_current,
            max_steps=self.settings.max_steps,
            max_output=self.settings.max_output_bytes,
            deadline=execution_budget.deadline,
            workspace=development_context is not None,
            execution_budget=execution_budget,
        )
        request_limit = min(
            self.settings.max_request_bytes,
            getattr(
                getattr(self.provider, "config", None),
                "max_request_bytes",
                self.settings.max_request_bytes,
            ),
        )
        history = None
        legacy_context = (snapshot.session_context
                          if isinstance(snapshot.session_context, SessionContext) else None)
        context_policy = (development_context.frozen.policy if development_context is not None
                          else cast(SessionContext, legacy_context).context.policy)
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
            history = CompactorHistory(compactor, run_id)
        provider = HostModelProvider(
            stepper,
            active.registry,
            max_group=self.settings.max_tools_per_step,
            max_calls=self.settings.max_tool_calls,
            request_bytes=request_limit,
            context_policy=context_policy,
            history=history,
            host_media=(
                self.development_media_for_run(run_id)
                if self.development_media_for_run is not None
                else None
            ),
        )

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

        host = HostExecution(
            context,
            execution_budget,
            read_receipt,
            provider.step_index,
            provider.checkpoint,
            provider.restore,
        )
        adapter = SDKAdapter(
            store=self.store,
            operations=self.development_store.operations,
            executor=active,
            tools=agent_tools(active.registry),
            environment=scope.environment,
            ownership=scope.ownership,
            read_result=unused_result,
            approval_request=cast(ApprovalRequest, approval_request),
            host=host,
        )
        if resume is not None and not isinstance(resume, SDKResume):
            raise ServiceError("facts_corrupt")
        if isinstance(resume, SDKResume):
            # Validate the service's exact checkpoint before SDK.run loads it.
            if fingerprint(resume.checkpoint) != resume.fingerprint:
                raise ServiceError("facts_corrupt")
        initial_messages = (
            [development_context.frozen.current]
            if development_context is not None
            else list(cast(SessionContext, legacy_context).context.messages)
        )
        # SDK resume may replay a pending approved tool before another model call.
        # Recheck the frozen source's live authorization before entering that path.
        await verify_source()
        try:
            result = await adapter.run(
                ai.Model(id="krim-development", provider=provider),
                sdk_messages(initial_messages),
                resume=resume is not None,
                replies=resume.replies if resume else None,
            )
        except ProbeStopped:
            raise ServiceError("cancelled") from None
        except BaseExceptionGroup as group:

            def first(error: BaseException) -> BaseException:
                if isinstance(error, BaseExceptionGroup):
                    return first(error.exceptions[0])
                return error

            cause = first(group)
            if isinstance(cause, (ServiceError, asyncio.CancelledError, TimeoutError)):
                raise cause from None
            raise ServiceError("execution_error") from None
        if result.status == "awaiting":
            raise Parked()
