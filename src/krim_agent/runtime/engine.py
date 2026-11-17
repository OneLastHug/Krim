"""Runtime host for the SDK-owned model and tool loop."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any, cast

import ai
from pydantic import JsonValue
from sqlalchemy import select

from krim_agent.agent_sdk.adapter import ProbeStopped, SDKAdapter
from krim_agent.agent_sdk.context import CompactorHistory
from krim_agent.agent_sdk.host import HostExecution
from krim_agent.agent_sdk.model import HostModelProvider, agent_tools, sdk_messages
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
from krim_agent.workspace.operations import WorkspaceOperations
from krim_agent.workspace.verification import Verifier, stage_result

from . import model_step
from .contracts import ExecutionBudget, RunStore
from .recovery import SDKResume

ApprovalRequest = Callable[[Invocation, float | None], Awaitable[dict[str, JsonValue]]]


async def _no_inprocess_wait(invocation: Invocation, timeout_seconds: float) -> bool:
    """Approvals park the run and resume through the persisted SDK checkpoint."""
    raise ServiceError("approval_unavailable")


class Parked(Exception):
    """The SDK saved an approval checkpoint and the run must await a reply."""


class AgentLoop:
    def __init__(
        self,
        store: RunStore,
        provider: ModelProvider,
        executor: ToolExecutor,
        settings: Settings,
        *,
        work: WorkspaceOperations | None = None,
        sandbox: SandboxOperations | None = None,
    ) -> None:
        self.store, self.provider, self.executor, self.settings = (
            store,
            provider,
            executor,
            settings,
        )
        if work is not None and sandbox is not None:
            raise ServiceError("sandbox_config_invalid", 422)
        self.sandbox = sandbox
        self.work = work
        self.context_service: Any | None = None
        self.session_tool_host: Any | None = None
        self.session_media: Any | None = None
        self._verifier = Verifier(self.store, self.work.artifacts) if self.work else None
        self._d4_seal = self._verifier.assert_current if self._verifier else None
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
    ) -> None:
        active_executor = executor or self.executor

        def check() -> None:
            assert_target_current()
            if self._d3_seal != (
                compaction.binding_token(), model_step.binding_token(), builder.binding_token()
            ):
                raise ServiceError("target_binding_changed")
            if self._d4_seal is not None:
                self._d4_seal()

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
        operations = self.work
        frame = None
        runtime: dict[str, JsonValue] = {}
        scope = None
        if operations is not None:
            frame = await operations.prepare(run_id)
            check()
            runtime = cast(dict[str, JsonValue], frame.conditions["runtime"])
            messages = [Message(role="user", content=snapshot.request.text)]
        elif ordinary is not None:
            messages = [ordinary.frozen.current]
        elif snapshot.session_context is not None:
            if not isinstance(snapshot.session_context, SessionContext):
                raise ServiceError("workspace_contract_required")
            messages = list(snapshot.session_context.context.messages)
        if execution_budget is None:
            timeout = (
                cast(float, runtime["run_timeout_seconds"]) if frame else self.settings.run_timeout
            )
            deadline = (None if timeout is None else
                        asyncio.get_running_loop().time() + max(
                            0, snapshot.run.created_at + timeout - time.time()
                        ))
            execution_budget = ExecutionBudget(deadline)
        if frame is not None and operations is not None:
            legacy_deadline = execution_budget.deadline
            if legacy_deadline is None:
                raise ServiceError("facts_corrupt")
            scope = operations.scope(frame, check, legacy_deadline, execution_budget)
        if self.sandbox is not None:
            scope = await self.sandbox.scope(run_id, check, execution_budget)
        max_steps = cast(int, runtime["model_steps"]) if frame else self.settings.max_steps
        max_group = (cast(int, runtime["max_tools_per_step"])
                     if frame else self.settings.max_tools_per_step)
        max_calls = cast(int, runtime["tool_calls"]) if frame else self.settings.max_tool_calls
        tool_timeout = (
            cast(float, runtime["tool_timeout_seconds"]) if frame else self.settings.tool_timeout
        )
        approval_timeout = (
            cast(float, runtime["approval_timeout_seconds"])
            if frame else self.settings.approval_timeout
        )
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

        stepper = model_step.ModelStep(
            self.provider,
            emit,
            check,
            max_steps=max_steps,
            max_output=self.settings.max_output_bytes,
            # Legacy frozen workspace frames retain their historical 20s step budget.
            model_timeout=20 if frame else self.settings.model_timeout,
            deadline=execution_budget.deadline,
            workspace=frame is not None or ordinary is not None,
            execution_budget=execution_budget,
        )
        request_limit = self.settings.request_capacity(self.provider)
        if frame is not None and scope is not None:
            compressor = compaction.Compactor(
                cast(SQLiteStore, self.store),
                frame,
                scope.access,
                stepper,
                check,
                self.settings.system_prompt,
                request_limit,
                cast(dict[str, JsonValue], snapshot.target["provider"]),
                source_anchor=cast(
                    str, getattr(snapshot.session_context, "source_fingerprint", None)
                ),
            )

        if ordinary is not None and ordinary_source is not None:
            identity = snapshot.target.get("provider")
            if not isinstance(identity, dict):
                raise ServiceError("facts_corrupt")
            compressor = compaction.Compactor.from_source(
                cast(SQLiteStore, self.store), ordinary_source, stepper, check,
                ordinary.frozen.system_prompt, request_limit, identity,
            )

        if resume is not None and not isinstance(resume, SDKResume):
            raise ServiceError("facts_corrupt")
        if isinstance(resume, SDKResume) and fingerprint(resume.checkpoint) != resume.fingerprint:
            raise ServiceError("facts_corrupt")

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
        host_provider = HostModelProvider(
            stepper,
            active_executor.registry,
            max_group=max_group,
            max_calls=max_calls,
            request_bytes=request_limit,
            context_policy=context_policy,
            history=history,
            session_sources=session_sources,
            run_tools=run_tools,
            host_media=(self.session_media.for_run(run_id, check)
                        if self.session_media is not None
                        and snapshot.session_context is not None else None),
        )
        tool_context = ToolContext(
            emit,
            _no_inprocess_wait,
            tool_timeout,
            check,
            scope,
            approval_timeout,
        )

        async def reject_read_result(_result_ref: str) -> JsonValue:
            raise ServiceError("facts_corrupt")

        async def reject_read_receipt(_receipt: Any) -> JsonValue:
            raise ServiceError("facts_corrupt")

        host = HostExecution(
            tool_context,
            execution_budget,
            reject_read_receipt,
            host_provider.step_index,
            host_provider.checkpoint,
            host_provider.restore,
        )
        adapter = SDKAdapter(
            store=self.store,
            operations=None,
            executor=active_executor,
            tools=agent_tools(active_executor.registry),
            environment=None,
            ownership=None,
            run_id=run_id,
            read_parallelism=(
                cast(int, runtime["read_parallelism"]) if frame else self.settings.read_parallelism
            ),
            batch_approvals=True,
            fail_on_tool_error=True,
            read_result=reject_read_result,
            approval_request=cast(ApprovalRequest, approval_request),
            host=host,
        )
        if ordinary_source is not None:
            await cast(SQLiteStore, self.store).work_read(
                lambda conn: ordinary_source.verify(conn, run_id)
            )
        try:
            result = await adapter.run(
                ai.Model(id="krim-host", provider=host_provider),
                sdk_messages(messages),
                resume=isinstance(resume, SDKResume),
                replies=resume.replies if isinstance(resume, SDKResume) else None,
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
        if result.status != "completed":
            raise ServiceError("execution_error")

        check()
        if frame is not None:
            assert self._verifier is not None and scope is not None
            verdict = await self._verifier.verify(
                run_id, frame, scope.access, check=scope.check, deadline=scope.deadline
            )
            scope.check()
            producer = asyncio.current_task()
            assert producer is not None
            stage_result(
                verdict,
                issuer=self._verifier,
                store=self.store,
                producer=producer,
                access=scope.access,
                check=scope.check,
                run_deadline=scope.deadline,
            )
            scope.check()
            if verdict.state != "passed":
                raise ServiceError("completion_unverified")
        elif self.sandbox is not None and snapshot.session_context is not None:
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
