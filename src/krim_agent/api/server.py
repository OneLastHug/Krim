"""SRC-OSS-002：FastAPI 路由、OpenAPI 和异步 StreamingResponse。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException

from krim_agent import __version__
from krim_agent.agents import AgentDescriptor
from krim_agent.application.agents import (
    AgentPromptRequest,
    AgentService,
    AgentSpawnRequest,
    AgentView,
)
from krim_agent.application.host import HostSessions
from krim_agent.application.workflow import WorkflowServices
from krim_agent.application.workflow_runtime import WorkflowRuntime
from krim_agent.config import Settings
from krim_agent.context_engine import ContextCompactRequest, ContextCompactResponse, ContextError
from krim_agent.host import HostCommand, HostSessionView, HostSide
from krim_agent.interactions import InteractionReply, InteractionView
from krim_agent.locale import LocaleChange, LocaleView
from krim_agent.persistence import Database, KrimRepository
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
from krim_agent.providers import create_provider
from krim_agent.providers.factory import OwnedProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.skills import SkillBatch, SkillCatalogView, SkillReadInput, SkillReadResult
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import ToolAudit, ToolInfo
from krim_agent.ui_locale import LocaleResources, locale_resources
from krim_agent.workflow import (
    WorkflowError,
    WorkflowGraph,
    WorkflowRerunRequest,
    WorkflowRestoreRequest,
    WorkflowResumeRequest,
    WorkflowRunView,
    WorkflowStartRequest,
)

from .boundary import LocalBoundary
from .web import mount_browser_ui


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


def create_app(settings: Settings | None = None, *, runtime: ChatRuntime | None = None) -> FastAPI:
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
            tools=service.tools.fork(), models={"default": PythonAI(service)}
        ),
        max_active_runs=config.max_active_runs,
    )
    agent_service = AgentService(service, config)
    host_sessions = HostSessions()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            await database.initialize()
            if owns_database:
                await repository.recover_interrupted()
            yield
        finally:
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
    async def host_reply(session_id: str, envelope: ProtocolEnvelope) -> ChannelResult:
        return await host_sessions.reply(session_id, envelope)

    @app.post("/v1/host/sessions/{session_id}/notify")
    async def host_notify(session_id: str, envelope: ProtocolEnvelope) -> ChannelResult:
        await host_sessions.notify(session_id, envelope)
        return ChannelResult(ok=True, data=None)

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
        return agent_service.catalog_views()

    @app.post("/v1/agents", status_code=201)
    async def spawn_agent(payload: AgentSpawnRequest) -> AgentView:
        return await agent_service.spawn(payload.name)

    @app.get("/v1/agents")
    async def list_agents() -> list[AgentView]:
        return await agent_service.views()

    @app.get("/v1/agents/{agent_id}")
    async def get_agent(agent_id: str) -> AgentView:
        return await agent_service.get(agent_id)

    @app.post("/v1/agents/{agent_id}/execute")
    async def execute_agent(agent_id: str, payload: AgentPromptRequest) -> AgentView:
        return await agent_service.execute(agent_id, payload)

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

        handle, state = await agent_service.stream_state(agent_id, from_seq)

        async def event_stream() -> AsyncIterator[str]:
            async for frame in handle.runtime.subscribe(state, from_seq):
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

    @app.get("/v1/runs/{run_id}")
    async def get_run(run_id: str) -> RunView:
        return await repository.get_run(run_id)

    @app.get("/v1/runs/{run_id}/audits")
    async def get_audits(run_id: str) -> list[ToolAudit]:
        return await repository.audits(run_id)

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
