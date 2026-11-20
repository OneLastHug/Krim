"""Runtime host for the SDK-owned model and tool loop."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any, cast

from pydantic import JsonValue
from sqlalchemy import select

from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.config import Settings
from krim_agent.context import builder, compaction
from krim_agent.context.types import OrdinarySessionContext, SessionContext
from krim_agent.contracts import Message, RunEvent, ServiceError, encoded, fingerprint
from krim_agent.providers.types import ModelProvider
from krim_agent.sandbox.operations import SandboxOperations
from krim_agent.storage.session_context import OrdinaryCompactionSource
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import Approve, ToolContext, ToolExecutor
from krim_agent.tools.session import SessionToolScope

from . import model_step
from .contracts import ExecutionBudget, RunStore
from .recovery import SDKResume
from .sdk_execution import (
    ApprovalRequest,
    SDKExecutionPlan,
    SDKExecutionPolicy,
    SDKHistory,
    execute_sdk,
)
from .sdk_execution import Parked as Parked


async def _no_inprocess_wait(invocation: Invocation, timeout_seconds: float | None) -> bool:
    """Approvals park the run and resume through the persisted SDK checkpoint."""
    raise ServiceError("approval_unavailable")


class AgentLoop:
    def __init__(
        self,
        store: RunStore,
        provider: ModelProvider,
        executor: ToolExecutor,
        settings: Settings,
        *,
        sandbox: SandboxOperations | None = None,
    ) -> None:
        self.store, self.provider, self.executor, self.settings = (
            store,
            provider,
            executor,
            settings,
        )
        self.sandbox = sandbox
        self.context_service: Any | None = None
        self.session_tool_host: Any | None = None
        self.session_media: Any | None = None
        self._d3_seal = (
            compaction.binding_token(), model_step.binding_token(), builder.binding_token()
        )

    async def run(
        self,
        run_id: str,
        approval_request: ApprovalRequest | Approve,
        assert_target_current: Callable[[], None] = lambda: None,
        execution_budget: ExecutionBudget | None = None,
        resume: SDKResume | None = None,
        executor: ToolExecutor | None = None,
        run_tools: Any | None = None,
        session_scope: SessionToolScope | None = None,
        startup_gate: Callable[[], asyncio.Lock | None] | None = None,
    ) -> None:
        active_executor = executor or self.executor

        def check() -> None:
            assert_target_current()
            if self._d3_seal != (
                compaction.binding_token(), model_step.binding_token(), builder.binding_token()
            ):
                raise ServiceError("target_binding_changed")

        check()
        snapshot = await self.store.snapshot(run_id)
        check()
        messages = [
            Message(role="system", content=self.settings.system_prompt),
            Message(role="user", content=snapshot.request.text),
        ]
        ordinary = (snapshot.session_context
                    if isinstance(snapshot.session_context, OrdinarySessionContext) else None)
        ordinary_source = None
        if ordinary is not None:
            if session_scope is None:
                if self.session_tool_host is None:
                    raise ServiceError("workspace_contract_required")
                session_scope = await self.session_tool_host.by_run(run_id, check)
            if (ordinary.owner_id != session_scope.actor.principal_id
                    or ordinary.workspace_id != session_scope.workspace_id
                    or ordinary.session_id != session_scope.session_id
                    or ordinary.turn_id != session_scope.turn_id
                    or ordinary.frozen.run_id != run_id):
                raise ServiceError("permission_denied")
            ordinary_source = OrdinaryCompactionSource(
                ordinary.frozen, legacy_actor=session_scope.actor,
            )
        if ordinary is not None:
            messages = [ordinary.frozen.current]
        elif snapshot.session_context is not None:
            if not isinstance(snapshot.session_context, SessionContext):
                raise ServiceError("workspace_contract_required")
            messages = list(snapshot.session_context.context.messages)
        if execution_budget is None:
            timeout = self.settings.run_timeout
            deadline = (None if timeout is None else
                        asyncio.get_running_loop().time() + max(
                            0, snapshot.run.created_at + timeout - time.time()
                        ))
            execution_budget = ExecutionBudget(deadline)
        scope = None
        if self.sandbox is not None:
            scope = await self.sandbox.scope(run_id, check, execution_budget)
        policy = SDKExecutionPolicy.from_settings(self.settings)
        compressor: compaction.Compactor | None = None

        async def emit(kind: str, data: dict[str, JsonValue]) -> RunEvent:
            if kind == "model_start" and ordinary_source is not None:
                await cast(SQLiteStore, self.store).work_read(
                    lambda conn: ordinary_source.verify(conn, run_id)
                )
            event = await self.store.append(run_id, kind, data)
            if kind == "model_start" and compressor is not None:
                await compressor.store.work_read(compressor.verify)
            check()
            return event

        async def build_history(stepper: Any, request_limit: int | None) -> SDKHistory:
            nonlocal compressor, session_scope
            if ordinary is not None and ordinary_source is not None:
                identity = snapshot.target.get("provider")
                if not isinstance(identity, dict):
                    raise ServiceError("facts_corrupt")
                compressor = compaction.Compactor.from_source(
                    cast(SQLiteStore, self.store), ordinary_source, stepper, check,
                    ordinary.frozen.system_prompt, request_limit, identity,
                )

            context_policy = (
                compressor.policy
                if compressor is not None
                else snapshot.session_context.context.policy
                if isinstance(snapshot.session_context, SessionContext)
                else None
            )
            session_sources = (
                snapshot.session_context.context.sources
                if isinstance(snapshot.session_context, SessionContext)
                else None
            )
            boundary = None
            if (compressor is not None and self.context_service is not None
                    and snapshot.session_context is not None):
                if session_scope is None and self.session_tool_host is not None:
                    session_scope = await self.session_tool_host.by_run(run_id, check)
                if session_scope is not None:
                    boundary = self.context_service.boundary(
                        session_scope.actor, session_scope.workspace_id,
                        session_scope.session_id, run_id, scope=session_scope,
                    )
            history = (CompactorHistory(
                compressor, run_id, boundary=boundary,
                live_memory=(boundary is not None and self.context_service is not None
                             and self.context_service.snapshots is not None),
            ) if compressor is not None else None)
            return SDKHistory(history, context_policy, session_sources)

        tool_context = ToolContext(
            emit,
            _no_inprocess_wait,
            policy.tool_timeout,
            check,
            scope,
            policy.approval_timeout,
        )

        async def reject_read_result(_result_ref: str) -> JsonValue:
            raise ServiceError("facts_corrupt")

        async def reject_read_receipt(_receipt: Any) -> JsonValue:
            raise ServiceError("facts_corrupt")

        async def on_completed() -> None:
            if self.sandbox is not None and snapshot.session_context is not None:
                descriptor = snapshot.target.get("context", {})
                completion_hash = (
                    descriptor.get("sandbox_completion_fingerprint")
                    if isinstance(descriptor, dict) else None
                )
                if completion_hash is not None:
                    if scope is None:
                        raise ServiceError("workspace_contract_required")
                    from krim_agent.sandbox.tasks import SandboxCompletion, verify_completion
                    from krim_agent.storage.sandbox_models import sandbox_turn_contracts

                    session_context = snapshot.session_context
                    sandbox_store = self.sandbox.service.store
                    async with sandbox_store.lock, sandbox_store.engine.connect() as conn:
                        contract_row = (
                            await conn.execute(
                                select(sandbox_turn_contracts).where(
                                    sandbox_turn_contracts.c.turn_id == session_context.turn_id
                                )
                            )
                        ).mappings().first()
                    if (
                        contract_row is None
                        or contract_row["fingerprint"] != completion_hash
                        or fingerprint(contract_row["completion"]) != completion_hash
                    ):
                        raise ServiceError("target_binding_changed")
                    contract = SandboxCompletion.model_validate_json(
                        encoded(contract_row["completion"])
                    )
                    completion_result = await verify_completion(
                        self.sandbox.service,
                        run_id,
                        session_context.session_id,
                        contract,
                        deadline=scope.deadline,
                        check=scope.check,
                    )
                    if completion_result["state"] != "passed":
                        raise ServiceError("completion_unverified")

        if ordinary_source is not None:
            await cast(SQLiteStore, self.store).work_read(
                lambda conn: ordinary_source.verify(conn, run_id)
            )
        await execute_sdk(SDKExecutionPlan(
            store=self.store,
            provider=self.provider,
            executor=active_executor,
            settings=self.settings,
            run_id=run_id,
            model_id="krim-host",
            initial_history=messages,
            policy=policy,
            execution_budget=execution_budget,
            tool_context=tool_context,
            emit=emit,
            check=check,
            approval_request=approval_request,
            read_result=reject_read_result,
            read_receipt=reject_read_receipt,
            build_history=build_history,
            run_tools=run_tools,
            host_media=(self.session_media.for_run(run_id, check)
                        if self.session_media is not None
                        and snapshot.session_context is not None else None),
            startup_gate=startup_gate,
            resume=cast(SDKResume | None, resume),
            workspace=ordinary is not None,
            on_completed=on_completed,
        ))
