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
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import (
    ChatRequest,
    MessageView,
    RunView,
    ServiceError,
    TopicCreate,
    TopicView,
)
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.core import ToolAudit, ToolInfo

from .boundary import LocalBoundary


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
    if runtime is None:
        database = Database(config.database_url)
        repository = KrimRepository(database)
        runtime = ChatRuntime(
            repository, FakeProvider(), builtin_registry(config.tool_timeout_seconds), config
        )
    else:
        repository = runtime.repository
        database = repository.database
    service = runtime

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await database.initialize()
        if owns_database:
            await repository.recover_interrupted()
        try:
            yield
        finally:
            await service.close()
            if owns_database:
                await database.close()

    app = FastAPI(
        title="Krim Agent",
        version=__version__,
        lifespan=lifespan,
        description="M1 单进程 Python 离线演示；不是完整 Agent 产品。",
        responses={
            code: {"model": ErrorResponse}
            for code in (400, 401, 403, 404, 409, 410, 413, 416, 422, 429, 503)
        },
    )
    app.add_middleware(LocalBoundary, settings=config)
    app.state.runtime = service
    app.state.repository = repository

    @app.exception_handler(ServiceError)
    async def service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse({"error": {"code": exc.code, "message": str(exc)}}, exc.status)

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

    @app.post("/v1/topics", status_code=201)
    async def create_topic(payload: TopicCreate) -> TopicView:
        return await repository.create_topic(payload.title)

    @app.get("/v1/topics")
    async def list_topics() -> TopicsResponse:
        return TopicsResponse(topics=await repository.list_topics())

    @app.get("/v1/topics/{topic_id}/messages")
    async def list_messages(topic_id: str) -> MessagesResponse:
        return MessagesResponse(messages=await repository.list_messages(topic_id))

    @app.post("/v1/topics/{topic_id}/messages", status_code=202)
    async def send_message(topic_id: str, payload: ChatRequest) -> RunView:
        return await service.start(topic_id, payload)

    @app.get("/v1/runs/{run_id}")
    async def get_run(run_id: str) -> RunView:
        return await repository.get_run(run_id)

    @app.get("/v1/runs/{run_id}/audits")
    async def get_audits(run_id: str) -> list[ToolAudit]:
        return await repository.audits(run_id)

    @app.post("/v1/runs/{run_id}/stop")
    async def stop_run(run_id: str) -> RunView:
        return await service.stop(run_id)

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
