"""SRC-OSS-002：FastAPI 路由、OpenAPI 和异步 StreamingResponse。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import quote
from uuid import uuid4

from fastapi import FastAPI, Header, Query, Request
from fastapi import Path as PathParameter
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException

from krim_agent import __version__
from krim_agent.agents import (
    AgentDescriptor,
    AgentProfileUpdate,
    AgentProfileVersion,
    AgentResourceView,
)
from krim_agent.agents.sessions import AgentSessionControl, AgentSessionView, AgentTurnView
from krim_agent.application.agents import (
    AgentInventoryView,
    AgentPromptRequest,
    AgentService,
    AgentSpawnRequest,
    AgentView,
)
from krim_agent.application.change_lists import ChangeListService
from krim_agent.application.checkpoints import CheckpointService
from krim_agent.application.host import HostSessions
from krim_agent.application.streams import MultiplexReader
from krim_agent.application.workflow import WorkflowServices
from krim_agent.application.workflow_runtime import WorkflowRuntime
from krim_agent.attachment import AttachmentContent, AttachmentUpload, AttachmentView
from krim_agent.budgets import BudgetAccountView, BudgetRequestView, BudgetResolveRequest
from krim_agent.change_list import (
    ChangeClick,
    ChangeExecution,
    ChangeInvoke,
    ChangeInvokeRequest,
    ChangeItemClick,
    ChangeListView,
    ChangeOperation,
    ChangeReceipt,
    ChangeRevision,
    ChangeSync,
)
from krim_agent.checkpoint import (
    CheckpointCanRollback,
    CheckpointCanRollbackRequest,
    CheckpointCatalog,
    CheckpointCheck,
    CheckpointCheckResult,
    CheckpointCreate,
    CheckpointCreateRequest,
    CheckpointExecution,
    CheckpointOperation,
    CheckpointReceipt,
    CheckpointRollback,
    CheckpointRollbackRequest,
)
from krim_agent.config import Settings
from krim_agent.context_engine import (
    ContextCompactRequest,
    ContextCompactResponse,
    ContextError,
    ContextProposalDecision,
    ContextProposalView,
)
from krim_agent.drag import DragPreview, DragState
from krim_agent.evaluation import (
    AgentTarget,
    ConversationTarget,
    EvalRemoteResumeRequest,
    EvalRemoteStartRequest,
    EvalStatistics,
    GenerationTarget,
    analyze_report,
    builtin_scorer,
    builtin_scorers,
)
from krim_agent.evaluation.jobs import (
    EvalEvent,
    EvalItemPage,
    EvalJobView,
    EvalPartialReport,
    EvalResumeRequest,
)
from krim_agent.evaluation.runner import EvalTarget
from krim_agent.evaluation.scorers import EvalScorer
from krim_agent.evaluation.service import EvaluationService
from krim_agent.evaluation.types import EvalReport
from krim_agent.host import HostCommand, HostSessionView, HostSide
from krim_agent.interactions import InteractionReply, InteractionView
from krim_agent.link import LinkResolve, LinkTarget, link_origin, normalize_link
from krim_agent.locale import LocaleChange, LocaleView
from krim_agent.observability import ObservationStatus, SpanRecord, TraceView
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.attachments import AttachmentRepository
from krim_agent.persistence.change_lists import ChangeListRepository
from krim_agent.persistence.checkpoints import CheckpointRepository
from krim_agent.prompt_context import ContextTagInput, MentionInput, MentionRef, PromptContext
from krim_agent.protocol.domain import (
    BranchRequest,
    ChatRequest,
    EditRequest,
    MessageTreeView,
    MessageView,
    RegenerateRequest,
    RunView,
    ServiceError,
    StreamEventView,
    TopicCreate,
    TopicView,
)
from krim_agent.protocol.models import ChannelResult, ProtocolEnvelope
from krim_agent.providers import ModelProvider, create_provider
from krim_agent.providers.factory import OwnedProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.skills import SkillBatch, SkillCatalogView, SkillReadInput, SkillReadResult
from krim_agent.streams import ActiveRunView, MultiplexRequest
from krim_agent.task_state import TaskKind, TaskOperationView, TaskSnapshot, TaskWriteRequest
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import ToolAudit, ToolError, ToolInfo
from krim_agent.tools.workspace import ListFilesInput, ReadFileInput, SearchFilesInput
from krim_agent.ui_locale import LocaleResources, locale_resources
from krim_agent.usage import RunUsageView
from krim_agent.workflow import (
    WorkflowError,
    WorkflowGraph,
    WorkflowRerunRequest,
    WorkflowRestoreRequest,
    WorkflowResumeRequest,
    WorkflowRunView,
    WorkflowStartRequest,
)
from krim_agent.workspace import WorkspaceFile, WorkspaceListing, WorkspaceSearch, WorkspaceView
from krim_agent.workspace_change import (
    WorkspaceAttemptView,
    WorkspaceChangeAction,
    WorkspaceChangeView,
    WorkspaceOperationProofView,
    WorkspaceOperationRequest,
    WorkspacePreviewRequest,
)
from krim_agent.workspace_list import (
    NativeBatchHistoryPage,
    NativeBatchRequest,
    NativeBatchView,
    NativeChangePage,
    NativeListFilter,
)

from .boundary import LocalBoundary
from .web import mount_browser_ui

WorkspaceIdentifier = Annotated[
    str, PathParameter(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$")
]


class TopicsResponse(BaseModel):
    topics: list[TopicView]


class MessagesResponse(BaseModel):
    messages: list[MessageView]


class ToolsResponse(BaseModel):
    tools: list[ToolInfo]


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    service: Literal["krim-agent"] = "krim-agent"
    protocol_version: Literal[2] = 2
    version: str = __version__
    provider: str = "fake-offline"


class ErrorDetails(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetails


def create_app(
    settings: Settings | None = None,
    *,
    runtime: ChatRuntime | None = None,
    eval_scorers: Mapping[str, EvalScorer] | None = None,
    agent_providers: Mapping[str, ModelProvider] | None = None,
    workspace_root: str | Path | None = None,
    workspace_version: str = "local-v1",
    workspace_writable: bool = False,
) -> FastAPI:
    config = settings or (runtime.settings if runtime else Settings.from_env())
    owns_database = runtime is None
    owned_provider: OwnedProvider | None = None
    if runtime is None:
        database = Database(config.database_url)
        repository = KrimRepository(database)
        owned_provider = create_provider(config)
        runtime = ChatRuntime(
            repository, owned_provider, builtin_registry(config.tool_timeout_seconds), config
        )
    else:
        repository = runtime.repository
        database = repository.database
    service = runtime
    workflow_service = WorkflowRuntime(
        database,
        scope="api",
        services=WorkflowServices(
            tools=service.tools.fork(),
            models={"default": PythonAI(service)},
            usage_budget=config.workflow_usage_budget,
            budget_store=service.budget_store,
            tracer=service.tracer,
        ),
        max_active_runs=config.max_active_runs,
    )
    agent_service = AgentService(service, config, providers=agent_providers)
    if workspace_root is not None:
        agent_service.register_workspace(
            "local",
            workspace_root,
            version=workspace_version,
            default=True,
            writable=workspace_writable,
        )
    evaluation_service = EvaluationService(
        database, scope="api", max_concurrency=config.max_active_runs
    )
    evaluation_ai = PythonAI(service)
    host_sessions = HostSessions()
    change_repository = ChangeListRepository(database)
    change_service = ChangeListService(change_repository)
    checkpoint_repository = CheckpointRepository(database)
    checkpoint_service = CheckpointService(checkpoint_repository)
    attachment_repository = AttachmentRepository(database)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            await database.initialize()
            await agent_service.assert_inventory_available()
            await agent_service.recover_workspace_edits()
            await change_repository.recover_interrupted()
            await checkpoint_repository.recover_interrupted()
            if owns_database:
                await repository.recover_interrupted()
                await agent_service.sessions.recover_transient()
                await service.tracer.store.recover()
            await service.budget_store.recover()
            await agent_service.restore_persistent()
            yield
        finally:
            await evaluation_service.close()
            await change_service.close()
            await checkpoint_service.close()
            await host_sessions.shutdown()
            try:
                await workflow_service.close()
            finally:
                try:
                    await agent_service.close()
                finally:
                    try:
                        await service.close()
                    finally:
                        if owns_database:
                            try:
                                await database.close()
                            finally:
                                if owned_provider is not None:
                                    await owned_provider.close()

    app = FastAPI(
        title="Krim Agent",
        version=__version__,
        lifespan=lifespan,
        description="Krim Agent 本地 Python 运行时，支持消息分支与持久 SSE；默认离线 provider。",
        responses={
            code: {"model": ErrorResponse}
            for code in (400, 401, 403, 404, 409, 410, 413, 416, 422, 429, 502, 503, 504)
        },
    )
    app.add_middleware(LocalBoundary, settings=config)
    app.state.runtime = service
    app.state.repository = repository
    app.state.workflow = workflow_service
    app.state.agents = agent_service
    app.state.host_sessions = host_sessions
    app.state.change_lists = change_service
    app.state.checkpoints = checkpoint_service
    app.state.attachments = attachment_repository
    app.state.evaluation = evaluation_service
    mount_browser_ui(app)

    @app.exception_handler(ServiceError)
    async def service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse({"error": {"code": exc.code, "message": str(exc)}}, exc.status)

    @app.exception_handler(ContextError)
    async def context_error(_request: Request, exc: ContextError) -> JSONResponse:
        status = (
            403
            if exc.code == "context_denied"
            else 502
            if exc.code == "context_summary_failed"
            else 400
        )
        return JSONResponse({"error": {"code": exc.code, "message": str(exc)}}, status)

    @app.exception_handler(WorkflowError)
    async def workflow_error(_request: Request, exc: WorkflowError) -> JSONResponse:
        status = (
            404
            if exc.code == "checkpoint_missing"
            else 416
            if exc.code == "invalid_cursor"
            else 409
            if exc.code
            in {
                "checkpoint_busy",
                "workflow_busy",
                "not_paused",
                "idempotency_conflict",
                "workflow_restore_required",
                "recovery_requires_confirmation",
            }
            else 429
            if exc.code == "workflow_limit"
            else 400
        )
        return JSONResponse({"error": {"code": exc.code, "message": str(exc)}}, status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": "invalid_request", "message": "请求字段校验失败"}}, 422
        )

    @app.exception_handler(HTTPException)
    async def http_error(_request: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": "http_error", "message": str(exc.detail)}}, exc.status_code
        )

    @app.get("/healthz")
    async def healthz() -> HealthResponse:
        return HealthResponse(provider=service.provider.name)

    async def evaluation_target(
        payload: EvalRemoteStartRequest | EvalRemoteResumeRequest,
    ) -> EvalTarget:
        if payload.target == "conversation":
            return ConversationTarget(
                evaluation_ai, version=payload.version, prompt_prefix=payload.prompt_prefix
            )
        if payload.target == "text":
            return GenerationTarget(
                evaluation_ai, version=payload.version, prompt_prefix=payload.prompt_prefix
            )
        try:
            definition = await agent_service.definition(payload.agent_name)
        except (ToolError, ServiceError) as error:
            if isinstance(error, ServiceError) and error.code != "agent_profile_not_found":
                raise
            raise ServiceError("eval_target_not_found", "评估 Agent 未登记", 404) from None
        return AgentTarget(
            agent_service.manager,
            definition,
            version=payload.version,
            prompt_prefix=payload.prompt_prefix,
        )

    def evaluation_scorers(names: list[str]) -> list[EvalScorer]:
        try:
            if eval_scorers is None:
                return builtin_scorers(names)
            if not names or len(set(names)) != len(names):
                raise ValueError("评分器名称必须唯一")
            return [
                eval_scorers[name] if name in eval_scorers else builtin_scorer(name)
                for name in names
            ]
        except ValueError:
            raise ServiceError("eval_scorer_invalid", "评分目录或名称无效", 422) from None

    @app.post("/v1/evals", status_code=202)
    async def start_evaluation(payload: EvalRemoteStartRequest) -> EvalJobView:
        handle = await evaluation_service.start(
            payload.dataset,
            await evaluation_target(payload),
            scorers=evaluation_scorers(payload.scorers),
            config=payload.config,
            request_key=payload.request_key,
        )
        return await handle.view()

    @app.get("/v1/evals")
    async def list_evaluations(limit: int = Query(default=100, ge=1, le=1000)) -> list[EvalJobView]:
        return await evaluation_service.store.list_jobs(limit=limit)

    @app.get("/v1/evals/{eval_id}")
    async def get_evaluation(eval_id: str) -> EvalJobView:
        return await evaluation_service.handle(eval_id).view()

    @app.get("/v1/evals/{eval_id}/items")
    async def evaluation_items(
        eval_id: str,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> EvalItemPage:
        return await evaluation_service.handle(eval_id).items(offset=offset, limit=limit)

    @app.get("/v1/evals/{eval_id}/events")
    async def evaluation_events(
        eval_id: str,
        from_seq: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[EvalEvent]:
        return await evaluation_service.store.events(eval_id, from_seq=from_seq, limit=limit)

    @app.get("/v1/evals/{eval_id}/stream")
    async def evaluation_event_stream(
        eval_id: str, from_seq: int = Query(default=0, ge=0)
    ) -> StreamingResponse:
        await evaluation_service.store.events(eval_id, from_seq=from_seq, limit=1)

        async def stream() -> AsyncIterator[str]:
            async for event in evaluation_service.handle(eval_id).events(from_seq):
                if event is None:
                    yield ": heartbeat\n\n"
                else:
                    yield f"id: {event.seq}\nevent: eval-event\ndata: {event.model_dump_json()}\n\n"

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/v1/evals/{eval_id}/report")
    async def evaluation_report(eval_id: str) -> EvalReport:
        return await evaluation_service.handle(eval_id).report()

    @app.get("/v1/evals/{eval_id}/partial-report")
    async def evaluation_partial_report(eval_id: str) -> EvalPartialReport:
        return await evaluation_service.handle(eval_id).partial_report()

    @app.get("/v1/evals/{eval_id}/statistics")
    async def evaluation_statistics(eval_id: str) -> EvalStatistics:
        return analyze_report(await evaluation_service.handle(eval_id).report())

    @app.post("/v1/evals/{eval_id}/pause")
    async def pause_evaluation(eval_id: str) -> EvalJobView:
        return await evaluation_service.handle(eval_id).pause()

    @app.post("/v1/evals/{eval_id}/cancel")
    async def cancel_evaluation(eval_id: str) -> EvalJobView:
        return await evaluation_service.handle(eval_id).cancel()

    @app.post("/v1/evals/{eval_id}/resume")
    async def resume_evaluation(eval_id: str, payload: EvalRemoteResumeRequest) -> EvalJobView:
        handle = await evaluation_service.resume(
            eval_id,
            target=await evaluation_target(payload),
            scorers=evaluation_scorers(payload.scorers),
            request=EvalResumeRequest(
                retry_uncertain=payload.retry_uncertain, skip_uncertain=payload.skip_uncertain
            ),
        )
        return await handle.view()

    @app.get("/v1/tools")
    async def list_tools() -> ToolsResponse:
        return ToolsResponse(tools=service.tools.list_tools())

    @app.post("/v1/host/sessions", status_code=201)
    async def create_host_session() -> HostSessionView:
        return await host_sessions.create()

    @app.get("/v1/host/sessions/{session_id}")
    async def host_session(session_id: str) -> HostSessionView:
        return host_sessions.session(session_id).view()

    @app.post("/v1/host/sessions/{session_id}/ready")
    async def host_ready(session_id: str) -> HostSessionView:
        return await host_sessions.mark_ready(session_id)

    @app.post("/v1/host/sessions/{session_id}/commands")
    async def host_command(session_id: str, request: HostCommand) -> ChannelResult:
        return await host_sessions.command(session_id, request)

    @app.post("/v1/host/sessions/{session_id}/reply")
    async def host_reply(
        session_id: str, envelope: ProtocolEnvelope, side: HostSide = "app"
    ) -> ChannelResult:
        return await host_sessions.reply(session_id, envelope, side)

    @app.post("/v1/host/sessions/{session_id}/requests")
    async def app_host_request(session_id: str, command: HostCommand) -> ChannelResult:
        if command.envelope.channel == "changeList/invoke":
            if command.envelope.kind != "call":
                raise ServiceError("host_channel_invalid", "变更操作必须是 call", 422)
            try:
                request = ChangeInvokeRequest.model_validate(command.envelope.data)
            except ValueError:
                raise ServiceError(
                    "change_input_invalid", "变更操作必须绑定话题、revision 和幂等键", 422
                ) from None
            payload = ChangeInvoke.model_validate(
                {
                    **request.model_dump(exclude={"topic_id"}),
                    "host_session_id": session_id,
                    "timeout_seconds": min(command.timeout_seconds, request.timeout_seconds),
                }
            )
            operation = await invoke_change(request.topic_id, payload)
            return ChannelResult(ok=True, data=operation.model_dump(mode="json"))
        if command.envelope.channel in {
            "checkpoint/create",
            "checkpoint/rollback",
            "checkpoint/canRollback",
        }:
            if command.envelope.kind != "call":
                raise ServiceError("host_channel_invalid", "Checkpoint 操作必须是 call", 422)
            try:
                if command.envelope.channel == "checkpoint/create":
                    checkpoint_request = CheckpointCreateRequest.model_validate(
                        command.envelope.data
                    )
                    checkpoint_payload = CheckpointCreate.model_validate(
                        {
                            **checkpoint_request.model_dump(exclude={"topic_id"}),
                            "host_session_id": session_id,
                            "timeout_seconds": min(
                                command.timeout_seconds, checkpoint_request.timeout_seconds
                            ),
                        }
                    )
                    checkpoint_operation = await create_checkpoint(
                        checkpoint_request.topic_id, checkpoint_payload
                    )
                    return ChannelResult(ok=True, data=checkpoint_operation.model_dump(mode="json"))
                if command.envelope.channel == "checkpoint/rollback":
                    rollback_request = CheckpointRollbackRequest.model_validate(
                        command.envelope.data
                    )
                    rollback_payload = CheckpointRollback.model_validate(
                        {
                            **rollback_request.model_dump(exclude={"topic_id"}),
                            "host_session_id": session_id,
                            "timeout_seconds": min(
                                command.timeout_seconds, rollback_request.timeout_seconds
                            ),
                        }
                    )
                    checkpoint_operation = await rollback_checkpoint(
                        rollback_request.topic_id,
                        rollback_request.checkpoint_id,
                        rollback_payload,
                    )
                    return ChannelResult(ok=True, data=checkpoint_operation.model_dump(mode="json"))
                can_request = CheckpointCanRollbackRequest.model_validate(command.envelope.data)
                can_payload = CheckpointCanRollback.model_validate(
                    {
                        **can_request.model_dump(exclude={"topic_id"}),
                        "host_session_id": session_id,
                        "timeout_seconds": min(
                            command.timeout_seconds, can_request.timeout_seconds
                        ),
                    }
                )
                answer = await can_rollback_checkpoint(can_request.topic_id, can_payload)
                return ChannelResult(ok=True, data=answer.model_dump(mode="json"))
            except ValueError:
                raise ServiceError(
                    "checkpoint_input_invalid", "Checkpoint 请求结构无效", 422
                ) from None
        return await host_sessions.request(session_id, command)

    @app.post("/v1/host/sessions/{session_id}/cancel/{request_id}")
    async def app_host_cancel(session_id: str, request_id: str) -> ChannelResult:
        return await host_sessions.cancel(session_id, request_id)

    @app.post("/v1/host/sessions/{session_id}/notify")
    async def host_notify(session_id: str, envelope: ProtocolEnvelope) -> ChannelResult:
        if envelope.channel == "changeList/item/click":
            try:
                click = ChangeItemClick.model_validate(envelope.data)
            except ValueError:
                raise ServiceError(
                    "host_notification_invalid", "变更点击通知结构无效", 422
                ) from None
            current = await change_repository.get(click.topic_id)
            if current.revision != click.revision or click.item not in current.items:
                raise ServiceError("change_revision_conflict", "变更点击快照已失效", 409)
        await host_sessions.notify(session_id, envelope)
        return ChannelResult(ok=True, data=None)

    @app.post("/v1/links/resolve")
    async def resolve_link(payload: LinkResolve, request: Request) -> LinkTarget:
        try:
            origin = str(request.base_url)
            base = payload.base_url or origin
            if link_origin(base) != link_origin(origin):
                raise ValueError("base 不属于当前服务 Origin")
            normalized = normalize_link(payload.url, base_url=base)
            external = link_origin(normalized) != link_origin(origin)
            return LinkTarget(url=normalized, external=external)
        except ValueError:
            raise ServiceError("link_invalid", "Link URL 无效或不在 Web 白名单", 422) from None

    @app.delete("/v1/host/sessions/{session_id}")
    async def host_close(session_id: str) -> ChannelResult:
        await host_sessions.close(session_id)
        return ChannelResult(ok=True, data=None)

    @app.get("/v1/host/sessions/{session_id}/events")
    async def host_events(
        session_id: str, side: HostSide = "app", from_seq: int = Query(default=0, ge=0)
    ) -> StreamingResponse:
        session = host_sessions.session(session_id)
        if from_seq > len(session.frames[side]):
            raise ServiceError("host_cursor_invalid", "宿主事件游标无效", 416)

        async def stream() -> AsyncIterator[str]:
            async for frame in host_sessions.events(session_id, side, from_seq):
                if frame is None:
                    yield ": heartbeat\n\n"
                else:
                    yield (
                        f"id: {frame.seq}\nevent: {frame.kind}\ndata: {frame.model_dump_json()}\n\n"
                    )

        return StreamingResponse(
            stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
        )

    @app.get("/v1/runs/{run_id}/usage")
    async def run_usage(run_id: str) -> RunUsageView:
        return await repository.usage_view(run_id)

    @app.get("/v1/budgets/accounts")
    async def budget_accounts() -> list[BudgetAccountView]:
        return await service.budget_store.accounts()

    @app.get("/v1/budgets/accounts/{account_id}")
    async def budget_account(account_id: str) -> BudgetAccountView:
        return await service.budget_store.account(account_id)

    @app.get("/v1/runs/{run_id}/budget")
    async def run_budget(run_id: str) -> list[BudgetRequestView]:
        await repository.get_run(run_id)
        return await service.budget_store.requests(run_id)

    @app.get("/v1/budgets/requests/{request_id}")
    async def budget_request(request_id: str) -> BudgetRequestView:
        return await service.budget_store.request(request_id)

    @app.post("/v1/budgets/requests/{request_id}/resolve")
    async def budget_resolve(request_id: str, payload: BudgetResolveRequest) -> BudgetRequestView:
        return await service.budget_store.settle(
            request_id, payload.usage, receipt=payload.receipt, revision=payload.revision
        )

    @app.get("/v1/runs/{run_id}/interactions")
    async def run_interactions(run_id: str, pending_only: bool = False) -> list[InteractionView]:
        return await repository.list_interactions(run_id, pending_only=pending_only)

    @app.get("/v1/runs/{run_id}/interactions/{interaction_id}")
    async def interaction(run_id: str, interaction_id: str) -> InteractionView:
        return await repository.get_interaction(run_id, interaction_id)

    @app.post("/v1/runs/{run_id}/interactions/{interaction_id}/respond")
    async def interaction_respond(
        run_id: str, interaction_id: str, request: InteractionReply
    ) -> InteractionView:
        return await service.interactions.reply(run_id, interaction_id, request)

    @app.get("/v1/agents/catalog")
    async def agent_catalog() -> list[AgentDescriptor]:
        return await agent_service.catalog_all()

    @app.get("/v1/agents/inventory")
    async def agent_inventory() -> list[AgentInventoryView]:
        return await agent_service.inventory()

    @app.get("/v1/agents/resources")
    async def agent_resources() -> list[AgentResourceView]:
        return agent_service.resource_inventory()

    @app.get("/v1/agents/profiles")
    async def agent_profiles() -> list[AgentProfileVersion]:
        return await agent_service.profile_list()

    @app.get("/v1/agents/profiles/{name}")
    async def agent_profile(
        name: str, revision: int | None = Query(default=None, ge=1)
    ) -> AgentProfileVersion:
        return await agent_service.profile_get(name, revision)

    @app.put("/v1/agents/profiles/{name}")
    async def put_agent_profile(name: str, payload: AgentProfileUpdate) -> AgentProfileVersion:
        if payload.config.name != name:
            raise ServiceError("agent_profile_name", "路径名称与配置名称不一致", 422)
        return await agent_service.profile_put(payload)

    @app.post("/v1/agents", status_code=201)
    async def spawn_agent(payload: AgentSpawnRequest) -> AgentView:
        return await agent_service.spawn(
            payload.name, persistent=payload.persistent, profile_revision=payload.profile_revision
        )

    @app.get("/v1/agents")
    async def list_agents() -> list[AgentView]:
        return await agent_service.views()

    @app.get("/v1/agents/{agent_id}")
    async def get_agent(agent_id: str) -> AgentView:
        return await agent_service.get(agent_id)

    @app.post("/v1/agents/{agent_id}/restore")
    async def restore_agent_instance(agent_id: str) -> AgentView:
        return await agent_service.restore(agent_id)

    @app.get("/v1/agents/{agent_id}/session")
    async def agent_session(agent_id: str) -> AgentSessionView:
        return await agent_service.session(agent_id)

    @app.get("/v1/agents/{agent_id}/turns")
    async def agent_turns(agent_id: str) -> list[AgentTurnView]:
        return await agent_service.session_turns(agent_id)

    @app.get("/v1/agents/{agent_id}/tasks/{kind}")
    async def read_agent_tasks(agent_id: str, kind: TaskKind) -> TaskSnapshot:
        return await agent_service.task_snapshot(agent_id, kind)

    @app.get("/v1/agents/{agent_id}/workspace")
    async def agent_workspace(agent_id: str) -> WorkspaceView:
        return await agent_service.workspace_info(agent_id)

    @app.get("/v1/agents/{agent_id}/workspace/read")
    async def agent_workspace_read(
        agent_id: str,
        path: str = Query(min_length=1, max_length=512),
        start_line: int = Query(default=1, ge=1),
        max_lines: int = Query(default=200, ge=1, le=1000),
    ) -> WorkspaceFile:
        return await agent_service.workspace_read(
            agent_id, ReadFileInput(path=path, start_line=start_line, max_lines=max_lines)
        )

    @app.get("/v1/agents/{agent_id}/workspace/list")
    async def agent_workspace_list(
        agent_id: str,
        path: str = Query(default=".", max_length=512),
        limit: int = Query(default=200, ge=1, le=1000),
    ) -> WorkspaceListing:
        return await agent_service.workspace_list(agent_id, ListFilesInput(path=path, limit=limit))

    @app.post("/v1/agents/{agent_id}/workspace/search")
    async def agent_workspace_search(agent_id: str, payload: SearchFilesInput) -> WorkspaceSearch:
        return await agent_service.workspace_search(agent_id, payload)

    @app.post("/v1/agents/{agent_id}/workspace/changes/preview")
    async def agent_workspace_preview(
        agent_id: WorkspaceIdentifier,
        payload: WorkspacePreviewRequest,
        confirmed: bool = Query(default=False),
    ) -> WorkspaceChangeView:
        return await agent_service.workspace_preview(agent_id, payload, confirmed=confirmed)

    @app.post("/v1/agents/{agent_id}/workspace/changes")
    async def agent_workspace_change(
        agent_id: WorkspaceIdentifier,
        payload: WorkspaceChangeAction,
        confirmed: bool = Query(default=False),
    ) -> WorkspaceAttemptView:
        return await agent_service.workspace_change(agent_id, payload, confirmed=confirmed)

    @app.get("/v1/agents/{agent_id}/workspace/changes")
    async def agent_workspace_changes(
        agent_id: WorkspaceIdentifier,
        offset: int = Query(default=0, ge=0, le=9007199254740991),
        limit: int = Query(default=50, ge=1, le=100),
    ) -> list[WorkspaceChangeView]:
        return await agent_service.workspace_proposals(agent_id, offset=offset, limit=limit)

    @app.get("/v1/agents/{agent_id}/workspace/changes/{proposal_id}")
    async def agent_workspace_proposal(
        agent_id: WorkspaceIdentifier, proposal_id: WorkspaceIdentifier
    ) -> WorkspaceChangeView:
        return await agent_service.workspace_proposal(agent_id, proposal_id)

    @app.get("/v1/agents/{agent_id}/workspace/changes/{proposal_id}/history")
    async def agent_workspace_history(
        agent_id: WorkspaceIdentifier, proposal_id: WorkspaceIdentifier
    ) -> list[WorkspaceAttemptView]:
        return await agent_service.workspace_history(agent_id, proposal_id)

    @app.get("/v1/agents/{agent_id}/workspace/operations/{operation_id}")
    async def agent_workspace_operation(
        agent_id: WorkspaceIdentifier, operation_id: WorkspaceIdentifier
    ) -> WorkspaceAttemptView:
        return await agent_service.workspace_operation(agent_id, operation_id)

    @app.get("/v1/agents/{agent_id}/workspace/operations/{operation_id}/proof")
    async def agent_workspace_operation_proof(
        agent_id: WorkspaceIdentifier, operation_id: WorkspaceIdentifier
    ) -> WorkspaceOperationProofView:
        return await agent_service.workspace_operation_proof(agent_id, operation_id)

    @app.post("/v1/agents/{agent_id}/workspace/operations/{operation_id}/reconcile")
    async def agent_workspace_reconcile(
        agent_id: WorkspaceIdentifier,
        operation_id: WorkspaceIdentifier,
        payload: WorkspaceOperationRequest | None = None,
        confirmed: bool = Query(default=False),
    ) -> WorkspaceAttemptView:
        request = payload or WorkspaceOperationRequest(operation_id=operation_id)
        if request.operation_id != operation_id:
            raise ServiceError("workspace_operation_mismatch", "路径与操作标识不一致", 422)
        return await agent_service.workspace_reconcile(agent_id, request, confirmed=confirmed)

    @app.post("/v1/agents/{agent_id}/workspace/operations/{operation_id}/finalize")
    async def agent_workspace_finalize(
        agent_id: WorkspaceIdentifier,
        operation_id: WorkspaceIdentifier,
        payload: WorkspaceOperationRequest | None = None,
        confirmed: bool = Query(default=False),
    ) -> WorkspaceAttemptView:
        request = payload or WorkspaceOperationRequest(operation_id=operation_id)
        if request.operation_id != operation_id:
            raise ServiceError("workspace_operation_mismatch", "路径与操作标识不一致", 422)
        return await agent_service.workspace_finalize(agent_id, request, confirmed=confirmed)

    # 原生清单与批次接口独立于旧 workspace/changes 路由。清单游标包含快照指纹，
    # 因此服务端只接受明确的 filter/cursor/limit，不把旧 offset 语义混用。
    @app.get("/v1/agents/{agent_id}/workspace/native-changes")
    async def agent_workspace_native_changes(
        agent_id: WorkspaceIdentifier,
        filter: Annotated[NativeListFilter, Query()] = "active",
        cursor: str | None = Query(default=None, max_length=1024),
        limit: int = Query(default=32, ge=1, le=32),
    ) -> NativeChangePage:
        return await agent_service.workspace_change_list(
            agent_id, filter=filter, cursor=cursor, limit=limit
        )

    @app.post("/v1/agents/{agent_id}/workspace/native-batches", status_code=202)
    async def agent_workspace_native_batch_start(
        agent_id: WorkspaceIdentifier,
        payload: NativeBatchRequest,
        confirmed: bool = Query(default=False),
    ) -> NativeBatchView:
        return await agent_service.workspace_start_batch(agent_id, payload, confirmed=confirmed)

    # history 和 by-key 必须排在通用 batch_id 路由之前。
    @app.get("/v1/agents/{agent_id}/workspace/native-batches/history")
    async def agent_workspace_native_batch_history(
        agent_id: WorkspaceIdentifier,
        cursor: str | None = Query(default=None, max_length=1024),
        limit: int = Query(default=32, ge=1, le=32),
    ) -> NativeBatchHistoryPage:
        return await agent_service.workspace_batch_history(agent_id, cursor=cursor, limit=limit)

    @app.get("/v1/agents/{agent_id}/workspace/native-batches/by-key")
    async def agent_workspace_native_batch_by_key(
        agent_id: WorkspaceIdentifier,
        request_key: str = Query(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f]+$"),
    ) -> NativeBatchView | None:
        return await agent_service.workspace_batch_by_key(agent_id, request_key)

    @app.get("/v1/agents/{agent_id}/workspace/native-batches/{batch_id}")
    async def agent_workspace_native_batch(
        agent_id: WorkspaceIdentifier, batch_id: WorkspaceIdentifier
    ) -> NativeBatchView:
        return await agent_service.workspace_batch(agent_id, batch_id)

    @app.post("/v1/agents/{agent_id}/workspace/native-batches/{batch_id}/wait")
    async def agent_workspace_native_batch_wait(
        agent_id: WorkspaceIdentifier,
        batch_id: WorkspaceIdentifier,
        timeout_seconds: float = Query(default=30, ge=0, le=300),
    ) -> NativeBatchView:
        return await agent_service.workspace_wait_batch(
            agent_id, batch_id, timeout_seconds=timeout_seconds
        )

    @app.post("/v1/agents/{agent_id}/workspace/native-batches/{batch_id}/cancel")
    async def agent_workspace_native_batch_cancel(
        agent_id: WorkspaceIdentifier, batch_id: WorkspaceIdentifier
    ) -> NativeBatchView:
        return await agent_service.workspace_cancel_batch(agent_id, batch_id)

    @app.post("/v1/agents/{agent_id}/tasks/{kind}")
    async def write_agent_tasks(
        agent_id: str, kind: TaskKind, payload: TaskWriteRequest
    ) -> TaskSnapshot:
        return await agent_service.task_mutate(agent_id, kind, payload)

    @app.get("/v1/agents/{agent_id}/tasks/{kind}/history")
    async def read_agent_task_history(
        agent_id: str,
        kind: TaskKind,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=100),
    ) -> list[TaskOperationView]:
        return await agent_service.task_history(agent_id, kind, offset=offset, limit=limit)

    @app.post("/v1/agents/{agent_id}/session/close")
    async def close_agent_session(agent_id: str, payload: AgentSessionControl) -> AgentSessionView:
        return await agent_service.close_session(agent_id, payload.expected_revision)

    @app.post("/v1/agents/{agent_id}/session/reopen")
    async def reopen_agent_session(agent_id: str, payload: AgentSessionControl) -> AgentSessionView:
        return await agent_service.reopen_session(agent_id, payload.expected_revision)

    @app.post("/v1/agents/{agent_id}/execute")
    async def execute_agent(agent_id: str, payload: AgentPromptRequest) -> AgentView:
        return await agent_service.execute(agent_id, payload)

    @app.post("/v1/agents/{agent_id}/start", status_code=202)
    async def start_agent(agent_id: str, payload: AgentPromptRequest) -> AgentView:
        return await agent_service.start(agent_id, payload)

    @app.post("/v1/agents/{agent_id}/wait")
    async def wait_agent(agent_id: str) -> AgentView:
        return await agent_service.wait(agent_id)

    @app.post("/v1/agents/{agent_id}/resume")
    async def resume_agent(agent_id: str, payload: AgentPromptRequest) -> AgentView:
        return await agent_service.resume(agent_id, payload)

    @app.post("/v1/agents/{agent_id}/stop")
    async def stop_agent(agent_id: str) -> AgentView:
        return await agent_service.stop(agent_id)

    @app.delete("/v1/agents/{agent_id}")
    async def destroy_agent(agent_id: str) -> AgentView:
        return await agent_service.destroy(agent_id)

    @app.get("/v1/agents/{agent_id}/events")
    async def agent_events(
        agent_id: str,
        from_seq: int = Query(default=0, ge=0),
        run_id: str | None = Query(default=None, min_length=1, max_length=36),
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    ) -> StreamingResponse:
        if last_event_id is not None:
            try:
                cursor = int(last_event_id)
                if cursor < 0:
                    raise ValueError
                from_seq = max(from_seq, cursor + 1)
            except ValueError as exc:
                raise ServiceError("invalid_cursor", "Last-Event-ID 必须是非负整数", 416) from exc

        reader, state = await agent_service.stream_state(agent_id, from_seq, run_id)

        async def event_stream() -> AsyncIterator[str]:
            async for frame in reader.subscribe(state, from_seq):
                if frame is None:
                    yield ": heartbeat\n\n"
                else:
                    payload = frame.model_dump_json(exclude_none=True)
                    for delimiter, escaped in (
                        ("\u0085", "\\u0085"),
                        ("\u2028", "\\u2028"),
                        ("\u2029", "\\u2029"),
                    ):
                        payload = payload.replace(delimiter, escaped)
                    yield f"id: {frame.seq}\nevent: {frame.kind}\ndata: {payload}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "X-Krim-Agent-Run-ID": state.run.id,
            },
        )

    @app.post("/v1/topics", status_code=201)
    async def create_topic(payload: TopicCreate) -> TopicView:
        return await repository.create_topic(payload.title, payload.locale)

    @app.get("/v1/topics")
    async def list_topics() -> TopicsResponse:
        return TopicsResponse(topics=await repository.list_topics())

    @app.get("/v1/locales")
    async def get_locale_resources(locale: str = "zh-CN") -> LocaleResources:
        try:
            payload = LocaleChange(locale=locale)
        except ValueError:
            raise ServiceError("locale_invalid", "Locale 标签无效", 422) from None
        return locale_resources(payload.locale)

    @app.patch("/v1/topics/{topic_id}/locale")
    async def change_locale(topic_id: str, payload: LocaleChange) -> LocaleView:
        return await repository.set_topic_locale(topic_id, payload.locale)

    @app.get("/v1/topics/{topic_id}/prompt-context")
    async def prompt_context(topic_id: str) -> PromptContext:
        return await repository.get_prompt_context(topic_id)

    @app.get("/v1/topics/{topic_id}/attachments")
    async def list_attachments(topic_id: str) -> list[AttachmentView]:
        return await attachment_repository.list(topic_id)

    @app.post("/v1/topics/{topic_id}/attachments", status_code=201)
    async def upload_attachment(topic_id: str, payload: AttachmentUpload) -> AttachmentView:
        return await attachment_repository.upload(topic_id, payload)

    @app.get("/v1/topics/{topic_id}/attachments/{attachment_id}")
    async def attachment(topic_id: str, attachment_id: str) -> AttachmentView:
        return await attachment_repository.get(topic_id, attachment_id)

    @app.get("/v1/topics/{topic_id}/attachments/{attachment_id}/content")
    async def attachment_content(topic_id: str, attachment_id: str) -> AttachmentContent:
        return await attachment_repository.content(topic_id, attachment_id)

    @app.get("/v1/topics/{topic_id}/attachments/{attachment_id}/download")
    async def download_attachment(topic_id: str, attachment_id: str) -> Response:
        import base64

        content = await attachment_repository.content(topic_id, attachment_id)
        filename = quote(content.attachment.filename, safe="")
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{filename}",
            "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": "default-src 'none'",
            "Cache-Control": "no-store",
        }
        return Response(
            base64.b64decode(content.content_base64),
            media_type=content.attachment.media_type,
            headers=headers,
        )

    @app.delete("/v1/topics/{topic_id}/attachments/{attachment_id}")
    async def delete_attachment(topic_id: str, attachment_id: str) -> dict[str, bool]:
        await attachment_repository.delete(topic_id, attachment_id)
        return {"deleted": True}

    @app.get("/v1/topics/{topic_id}/change-list")
    async def change_list(topic_id: str) -> ChangeListView:
        return await change_repository.get(topic_id)

    async def checkpoint_execute(
        execution: CheckpointExecution, session_id: str | None, timeout_seconds: float
    ) -> CheckpointReceipt:
        if session_id is None:
            raise ServiceError("host_not_ready", "Checkpoint 操作需要可信 Python Host", 409)
        result = await host_sessions.request(
            session_id,
            HostCommand(
                envelope=ProtocolEnvelope(
                    kind="call",
                    id=execution.operation_id,
                    channel=f"checkpoint/{execution.action}",
                    data=execution.model_dump(mode="json"),
                ),
                timeout_seconds=timeout_seconds,
            ),
        )
        if not result.ok:
            raise ServiceError("executor_failed", "Checkpoint 宿主未返回有效业务回执", 502)
        return CheckpointReceipt.model_validate(result.data)

    @app.get("/v1/topics/{topic_id}/checkpoints")
    async def checkpoints(topic_id: str) -> CheckpointCatalog:
        return await checkpoint_repository.catalog(topic_id)

    @app.post("/v1/topics/{topic_id}/checkpoints")
    async def create_checkpoint(topic_id: str, payload: CheckpointCreate) -> CheckpointOperation:
        async def execute(execution: CheckpointExecution) -> CheckpointReceipt:
            return await checkpoint_execute(
                execution, payload.host_session_id, payload.timeout_seconds
            )

        return await checkpoint_service.create(topic_id, payload, execute)

    @app.post("/v1/topics/{topic_id}/checkpoints/{checkpoint_id}/rollback")
    async def rollback_checkpoint(
        topic_id: str, checkpoint_id: str, payload: CheckpointRollback
    ) -> CheckpointOperation:
        if checkpoint_id != payload.checkpoint_id:
            raise ServiceError("checkpoint_input_invalid", "Checkpoint 路由与请求 ID 不匹配", 409)

        async def execute(execution: CheckpointExecution) -> CheckpointReceipt:
            return await checkpoint_execute(
                execution, payload.host_session_id, payload.timeout_seconds
            )

        return await checkpoint_service.rollback(topic_id, payload, execute)

    @app.post("/v1/topics/{topic_id}/checkpoints/can-rollback")
    async def can_rollback_checkpoint(
        topic_id: str, payload: CheckpointCanRollback
    ) -> CheckpointCheckResult:
        async def execute(check: CheckpointCheck) -> CheckpointCheckResult:
            if payload.host_session_id is None:
                raise ServiceError("host_not_ready", "Checkpoint 校验需要可信 Python Host", 409)
            result = await host_sessions.request(
                payload.host_session_id,
                HostCommand(
                    envelope=ProtocolEnvelope(
                        kind="call",
                        id=str(uuid4()),
                        channel="checkpoint/canRollback",
                        data=check.model_dump(mode="json"),
                    ),
                    timeout_seconds=payload.timeout_seconds,
                ),
            )
            if not result.ok:
                raise ServiceError("executor_failed", "Checkpoint 宿主校验失败", 502)
            return CheckpointCheckResult.model_validate(result.data)

        return await checkpoint_service.can_rollback(topic_id, payload, execute)

    @app.get("/v1/topics/{topic_id}/checkpoints/operations/{operation_id}")
    async def checkpoint_operation(topic_id: str, operation_id: str) -> CheckpointOperation:
        return await checkpoint_repository.operation(topic_id, operation_id)

    @app.get("/v1/topics/{topic_id}/checkpoints/operations")
    async def checkpoint_operations(topic_id: str) -> list[CheckpointOperation]:
        return await checkpoint_repository.operations(topic_id)

    @app.get("/v1/topics/{topic_id}/checkpoints/operations/{operation_id}/history")
    async def checkpoint_history(topic_id: str, operation_id: str) -> list[CheckpointReceipt]:
        return await checkpoint_repository.history(topic_id, operation_id)

    @app.post("/v1/topics/{topic_id}/checkpoints/operations/{operation_id}/cancel")
    async def checkpoint_cancel(topic_id: str, operation_id: str) -> CheckpointOperation:
        return await checkpoint_service.cancel(topic_id, operation_id)

    @app.post("/v1/topics/{topic_id}/checkpoints/operations/{operation_id}/resolve")
    async def checkpoint_resolve(
        topic_id: str, operation_id: str, payload: CheckpointReceipt
    ) -> CheckpointOperation:
        if payload.topic_id != topic_id or payload.operation_id != operation_id:
            raise ServiceError(
                "checkpoint_receipt_mismatch", "Checkpoint 核实回执与路由不匹配", 409
            )
        return await checkpoint_service.resolve(payload)

    @app.put("/v1/topics/{topic_id}/change-list")
    async def sync_change_list(topic_id: str, payload: ChangeSync) -> ChangeListView:
        return await change_repository.sync(topic_id, payload)

    @app.post("/v1/topics/{topic_id}/change-list/clear")
    async def clear_change_list(topic_id: str, payload: ChangeRevision) -> ChangeListView:
        return await change_repository.sync(topic_id, ChangeSync(revision=payload.revision))

    @app.post("/v1/topics/{topic_id}/change-list/invoke")
    async def invoke_change(topic_id: str, payload: ChangeInvoke) -> ChangeOperation:
        async def execute(execution: ChangeExecution) -> ChangeReceipt:
            if payload.host_session_id is None:
                raise ServiceError("host_not_ready", "变更执行需要可信 Python Host session", 409)
            result = await host_sessions.request(
                payload.host_session_id,
                HostCommand(
                    envelope=ProtocolEnvelope(
                        kind="call",
                        id=execution.operation_id,
                        channel="changeList/invoke",
                        data=execution.model_dump(mode="json"),
                    ),
                    timeout_seconds=payload.timeout_seconds,
                ),
            )
            if not result.ok:
                raise ServiceError("executor_failed", "宿主执行未返回有效业务回执", 502)
            return ChangeReceipt.model_validate(result.data)

        return await change_service.invoke(topic_id, payload, execute)

    @app.get("/v1/topics/{topic_id}/change-list/operations")
    async def change_operations(topic_id: str) -> list[ChangeOperation]:
        return await change_repository.operations(topic_id)

    @app.get("/v1/topics/{topic_id}/change-list/operations/{operation_id}")
    async def change_operation(topic_id: str, operation_id: str) -> ChangeOperation:
        return await change_repository.operation(topic_id, operation_id)

    @app.get("/v1/topics/{topic_id}/change-list/operations/{operation_id}/history")
    async def change_history(topic_id: str, operation_id: str) -> list[ChangeReceipt]:
        return await change_repository.history(topic_id, operation_id)

    @app.post("/v1/topics/{topic_id}/change-list/operations/{operation_id}/cancel")
    async def cancel_change(topic_id: str, operation_id: str) -> ChangeOperation:
        return await change_service.cancel(topic_id, operation_id)

    @app.post("/v1/topics/{topic_id}/change-list/operations/{operation_id}/resolve")
    async def resolve_change(
        topic_id: str, operation_id: str, payload: ChangeReceipt
    ) -> ChangeOperation:
        if payload.operation_id != operation_id or payload.topic_id != topic_id:
            raise ServiceError("change_receipt_mismatch", "核实回执与路由不匹配", 409)
        return await change_repository.settle(payload, resolve=True)

    @app.post("/v1/topics/{topic_id}/change-list/click")
    async def click_change(topic_id: str, payload: ChangeClick) -> ChangeItemClick:
        current = await change_repository.get(topic_id)
        if current.revision != payload.revision:
            raise ServiceError("change_revision_conflict", "变更清单已更新，请重新读取", 409)
        item = next((item for item in current.items if item.id == payload.item_id), None)
        if item is None:
            raise ServiceError("change_item_not_found", "变更条目不在该话题中", 404)
        click = ChangeItemClick(topic_id=topic_id, revision=current.revision, item=item)
        if payload.host_session_id is not None:
            await host_sessions.notify(
                payload.host_session_id,
                ProtocolEnvelope(
                    kind="notify",
                    id=str(uuid4()),
                    channel="changeList/item/click",
                    data=click.model_dump(mode="json"),
                ),
            )
        return click

    @app.get("/v1/topics/{topic_id}/skills")
    async def skills(topic_id: str) -> SkillCatalogView:
        return await repository.skill_catalog(topic_id)

    @app.post("/v1/topics/{topic_id}/skills")
    async def inject_skills(topic_id: str, payload: SkillBatch) -> SkillCatalogView:
        return await repository.inject_skills(topic_id, payload)

    @app.get("/v1/topics/{topic_id}/skills/{name}")
    async def read_skill(topic_id: str, name: str, resource: str | None = None) -> SkillReadResult:
        try:
            payload = SkillReadInput(name=name, resource=resource)
        except ValueError:
            raise ServiceError("skill_input_invalid", "技能名称或资源键无效", 422) from None
        return await repository.read_skill(topic_id, payload.name, payload.resource)

    @app.delete("/v1/topics/{topic_id}/skills/{name}")
    async def remove_skill(topic_id: str, name: str) -> SkillCatalogView:
        return await repository.remove_skill(topic_id, name)

    @app.delete("/v1/topics/{topic_id}/skills")
    async def clear_skills(topic_id: str) -> SkillCatalogView:
        return await repository.remove_skill(topic_id)

    @app.post("/v1/topics/{topic_id}/mentions", status_code=201)
    async def add_mention(topic_id: str, payload: MentionInput) -> MentionRef:
        return await repository.add_mention(topic_id, payload)

    @app.post("/v1/topics/{topic_id}/drag/enter")
    async def drag_enter(topic_id: str, payload: DragPreview) -> DragState:
        await repository.get_topic(topic_id)
        return DragState(active=True, preview=payload)

    @app.post("/v1/topics/{topic_id}/drag/leave")
    async def drag_leave(topic_id: str) -> DragState:
        await repository.get_topic(topic_id)
        return DragState(active=False)

    @app.post("/v1/topics/{topic_id}/drag/drop", status_code=201)
    async def drag_drop(topic_id: str, payload: MentionInput) -> MentionRef:
        return await repository.add_mention(topic_id, payload)

    @app.delete("/v1/topics/{topic_id}/mentions/{mention_id}")
    async def remove_mention(topic_id: str, mention_id: str) -> PromptContext:
        return await repository.remove_mention(topic_id, mention_id)

    @app.delete("/v1/topics/{topic_id}/mentions")
    async def clear_mentions(topic_id: str) -> PromptContext:
        return await repository.remove_mention(topic_id)

    @app.post("/v1/topics/{topic_id}/context-tags")
    async def add_context_tag(topic_id: str, payload: ContextTagInput) -> PromptContext:
        return await repository.add_context_tag(topic_id, payload)

    @app.delete("/v1/topics/{topic_id}/context-tags")
    async def remove_context_tag(
        topic_id: str, name: str | None = Query(default=None, min_length=1, max_length=120)
    ) -> PromptContext:
        return await repository.remove_context_tag(topic_id, name)

    @app.get("/v1/topics/{topic_id}/messages")
    async def list_messages(topic_id: str) -> MessagesResponse:
        return MessagesResponse(messages=await repository.list_messages(topic_id))

    @app.get("/v1/topics/{topic_id}/tree")
    async def message_tree(topic_id: str) -> MessageTreeView:
        return await repository.message_tree(topic_id)

    @app.post("/v1/topics/{topic_id}/compact")
    async def compact_topic(
        topic_id: str, payload: ContextCompactRequest
    ) -> ContextCompactResponse:
        return await service.compact(topic_id, payload)

    @app.get("/v1/topics/{topic_id}/compact-proposals")
    async def context_proposals(
        topic_id: str, pending_only: bool = False
    ) -> list[ContextProposalView]:
        return await service.context_proposals(topic_id, pending_only=pending_only)

    @app.get("/v1/topics/{topic_id}/compact-proposals/{proposal_id}")
    async def context_proposal(topic_id: str, proposal_id: str) -> ContextProposalView:
        return await service.context_proposal(topic_id, proposal_id)

    @app.post("/v1/topics/{topic_id}/compact-proposals/{proposal_id}/decision")
    async def decide_context_proposal(
        topic_id: str, proposal_id: str, payload: ContextProposalDecision
    ) -> ContextProposalView:
        return await service.decide_context_proposal(topic_id, proposal_id, payload)

    @app.put("/v1/topics/{topic_id}/branch")
    async def select_branch(topic_id: str, payload: BranchRequest) -> TopicView:
        return await repository.select_branch(topic_id, payload.message_id)

    @app.post("/v1/topics/{topic_id}/messages/{message_id}/edit", status_code=202)
    async def edit_message(topic_id: str, message_id: str, payload: EditRequest) -> RunView:
        return await service.edit(topic_id, message_id, payload)

    @app.post("/v1/topics/{topic_id}/messages/{message_id}/regenerate", status_code=202)
    async def regenerate_message(
        topic_id: str, message_id: str, payload: RegenerateRequest
    ) -> RunView:
        return await service.regenerate(topic_id, message_id, payload)

    @app.post("/v1/topics/{topic_id}/messages", status_code=202)
    async def send_message(topic_id: str, payload: ChatRequest) -> RunView:
        return await service.start(topic_id, payload)

    @app.get("/v1/runs/active")
    async def active_runs() -> list[ActiveRunView]:
        return [
            ActiveRunView(run=run, events=await repository.event_summary(run.id))
            for run in await repository.active_runs()
        ]

    @app.post("/v1/runs/events")
    async def multiplex_events(payload: MultiplexRequest) -> StreamingResponse:
        reader = MultiplexReader(service)
        prepared = await reader.prepare(payload)

        async def event_stream() -> AsyncIterator[str]:
            async for item in reader.subscribe(prepared):
                if item is None:
                    yield ": heartbeat\n\n"
                else:
                    data = item.model_dump_json(exclude_none=True)
                    for delimiter, escaped in (
                        ("\u0085", "\\u0085"),
                        ("\u2028", "\\u2028"),
                        ("\u2029", "\\u2029"),
                    ):
                        data = data.replace(delimiter, escaped)
                    yield (
                        f"id: {item.frame.stream_id}:{item.frame.seq}\n"
                        "event: run-frame\n"
                        f"data: {data}\n\n"
                    )

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/v1/runs/{run_id}")
    async def get_run(run_id: str) -> RunView:
        return await repository.get_run(run_id)

    @app.get("/v1/runs/{run_id}/audits")
    async def get_audits(run_id: str) -> list[ToolAudit]:
        return await repository.audits(run_id)

    @app.get("/v1/observability/status")
    async def observation_status() -> ObservationStatus:
        return service.tracer.status()

    @app.get("/v1/runs/{run_id}/trace")
    async def run_trace(run_id: str) -> list[SpanRecord]:
        await repository.get_run(run_id)
        await service.tracer.flush(0.5)
        return await service.tracer.store.spans("run", run_id)

    @app.get("/v1/traces/{trace_id}")
    async def trace_view(trace_id: str) -> TraceView:
        await service.tracer.flush(0.5)
        return await service.tracer.store.trace(trace_id)

    @app.get("/v1/workflows/runs/{run_id}/trace")
    async def workflow_trace(run_id: str) -> list[SpanRecord]:
        await workflow_service.get(run_id)
        await service.tracer.flush(0.5)
        return await service.tracer.store.spans("workflow", run_id)

    @app.get("/v1/runs/{run_id}/events/summary")
    async def event_summary(run_id: str) -> StreamEventView:
        return await repository.event_summary(run_id)

    @app.get("/v1/runs/{run_id}/messages")
    async def run_messages(run_id: str) -> MessagesResponse:
        return MessagesResponse(messages=await repository.run_messages(run_id))

    @app.post("/v1/workflows/runs", status_code=202)
    async def start_workflow(payload: WorkflowStartRequest) -> WorkflowRunView:
        return await workflow_service.start(payload)

    @app.get("/v1/workflows/runs")
    async def workflow_runs() -> list[WorkflowRunView]:
        return await workflow_service.list()

    @app.get("/v1/workflows/runs/{run_id}")
    async def workflow_run(run_id: str) -> WorkflowRunView:
        return await workflow_service.get(run_id)

    @app.get("/v1/workflows/runs/{run_id}/graph")
    async def workflow_graph(run_id: str) -> WorkflowGraph:
        return await workflow_service.graph(run_id)

    @app.get("/v1/workflows/runs/{run_id}/events")
    async def workflow_events(
        run_id: str,
        from_seq: int = Query(default=0, ge=0),
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    ) -> StreamingResponse:
        if last_event_id is not None:
            try:
                last = int(last_event_id)
                if last < 0:
                    raise ValueError
                from_seq = max(from_seq, last + 1)
            except ValueError as exc:
                raise WorkflowError("invalid_cursor", "Last-Event-ID 必须是非负整数") from exc
        await workflow_service.validate_cursor(run_id, from_seq)

        async def event_stream() -> AsyncIterator[str]:
            async for event in workflow_service.events(run_id, from_seq):
                if event is None:
                    yield ": heartbeat\n\n"
                else:
                    payload = event.model_dump_json(exclude_none=True)
                    for delimiter, escaped in (
                        ("\u0085", "\\u0085"),
                        ("\u2028", "\\u2028"),
                        ("\u2029", "\\u2029"),
                    ):
                        payload = payload.replace(delimiter, escaped)
                    yield (f"id: {event.seq}\nevent: {event.type}\ndata: {payload}\n\n")

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/v1/workflows/runs/{run_id}/pause")
    async def pause_workflow(run_id: str) -> WorkflowRunView:
        return await workflow_service.pause(run_id)

    @app.post("/v1/workflows/runs/{run_id}/resume")
    async def resume_workflow(run_id: str, payload: WorkflowResumeRequest) -> WorkflowRunView:
        return await workflow_service.resume(run_id, payload)

    @app.post("/v1/workflows/runs/{run_id}/restore", status_code=202)
    async def restore_workflow(run_id: str, payload: WorkflowRestoreRequest) -> WorkflowRunView:
        return await workflow_service.restore(run_id, payload)

    @app.post("/v1/workflows/runs/{run_id}/stop")
    async def stop_workflow(run_id: str) -> WorkflowRunView:
        return await workflow_service.stop(run_id)

    @app.post("/v1/workflows/runs/{run_id}/rerun")
    async def rerun_workflow(run_id: str, payload: WorkflowRerunRequest) -> WorkflowRunView:
        return await workflow_service.rerun(run_id, payload)

    @app.post("/v1/runs/{run_id}/stop")
    async def stop_run(run_id: str) -> RunView:
        return await service.stop(run_id)

    @app.get("/v1/runs/{run_id}/resume")
    @app.get("/v1/runs/{run_id}/events")
    async def run_events(
        run_id: str,
        from_seq: int = Query(default=0, ge=0),
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    ) -> StreamingResponse:
        if last_event_id is not None:
            try:
                cursor = int(last_event_id)
                if cursor < 0:
                    raise ValueError
                from_seq = max(from_seq, cursor + 1)
            except ValueError as exc:
                raise ServiceError("invalid_cursor", "Last-Event-ID 必须是非负整数", 416) from exc
        state = await service.stream_state(run_id, from_seq)

        async def event_stream() -> AsyncIterator[str]:
            async for frame in service.subscribe(state, from_seq):
                if frame is None:
                    yield ": heartbeat\n\n"
                else:
                    payload = frame.model_dump_json(exclude_none=True)
                    for delimiter, escaped in (
                        ("\u0085", "\\u0085"),
                        ("\u2028", "\\u2028"),
                        ("\u2029", "\\u2029"),
                    ):
                        payload = payload.replace(delimiter, escaped)
                    yield (f"id: {frame.seq}\nevent: {frame.kind}\ndata: {payload}\n\n")

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app
