"""薄 HTTP/SSE：所有业务操作调用同一 RunService。"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import JsonValue

from krim_agent import __version__
from krim_agent.composition import Application, open_application
from krim_agent.config import Settings
from krim_agent.contracts import (
    ApprovalReply,
    RunEvent,
    RunFacts,
    RunRequest,
    RunView,
    ServiceError,
    encoded,
)
from krim_agent.evaluation.service import EvaluationService
from krim_agent.evaluation.types import (
    Candidate,
    DatasetRevision,
    EvalRequest,
    EvalView,
    FreezeRequest,
)
from krim_agent.observability import TraceView
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.security import PrincipalContext

from .boundary import LocalBoundary
from .web import mount_browser_ui


def create_app(
    settings: Settings | None = None,
    *,
    application: Application | None = None,
    http_config: OpenAICompatibleConfig | None = None,
) -> FastAPI:
    if application is not None and http_config is not None:
        raise ServiceError("provider_config", 422)
    config = (
        settings
        if settings is not None
        else (application.settings if application is not None else Settings.from_env())
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned = application is None
        active = (
            await open_application(config.database, settings=config, http_config=http_config)
            if owned
            else application
        )
        assert active is not None
        app.state.application = active
        try:
            yield
        finally:
            if owned:
                await active.close()

    def active(request: Request) -> Application:
        return cast(Application, request.app.state.application)

    def evaluation(request: Request) -> EvaluationService:
        service = active(request).evaluations
        assert service is not None
        return service

    async def authorize_legacy(request: Request) -> None:
        if not request.url.path.startswith("/v1/"):
            return
        principal = cast(PrincipalContext, request.state.principal)
        if not principal.legacy:
            raise ServiceError("not_found", 404)
        if request.method not in {"GET", "HEAD"} and all(
            m.role == "viewer" for m in principal.memberships
        ):
            raise ServiceError("permission_denied", 403)

        async def check_source(run_id: str) -> None:
            snapshot = await active(request).store.snapshot(run_id)
            source = snapshot.session_context
            if source is not None:
                session = active(request).sessions
                assert session is not None
                await session.get(
                    principal,
                    source.workspace_id,
                    source.session_id,
                    write=request.method not in {"GET", "HEAD"},
                    approve="/approvals/" in request.url.path,
                )

        if request.url.path.startswith("/v1/runs"):
            if request.method == "POST" and request.url.path == "/v1/runs":
                body = await request.json()
                if (
                    isinstance(body, dict)
                    and isinstance(body.get("request_key"), str)
                    and body["request_key"].startswith("ac-turn.")
                ):
                    raise ServiceError("invalid_input", 422)
            run_id = request.path_params.get("run_id")
            if isinstance(run_id, str):
                await check_source(run_id)
            if "/by-key/" in request.url.path:
                found = await active(request).store.by_key(str(request.path_params["key"]))
                if found is not None:
                    await check_source(found.id)
        if request.method == "POST" and request.url.path == "/v1/datasets":
            body = await request.json()
            if isinstance(body, dict) and isinstance(body.get("cases"), list):
                for selection in body["cases"]:
                    if isinstance(selection, dict) and selection.get("source_kind") == "run":
                        run_id = selection.get("source_run_id")
                        if not isinstance(run_id, str):
                            raise ServiceError("invalid_input", 422)
                        await check_source(run_id)

    app = FastAPI(
        title="Krim Agent",
        version=__version__,
        lifespan=lifespan,
        dependencies=[Depends(authorize_legacy)],
    )
    app.add_middleware(LocalBoundary, settings=config)

    @app.exception_handler(ServiceError)
    async def service_error(_request: Request, error: ServiceError) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": error.code, "message": "请求未完成，请检查操作及运行状态。"}},
            status_code=error.status,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, _error: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": "invalid_input", "message": "输入不符合此接口要求。"}},
            status_code=422,
        )

    @app.get("/healthz")
    async def health(request: Request) -> dict[str, JsonValue]:
        current = active(request)
        enabled = "target-http" in current.service.bindings or isinstance(
            current.provider, OpenAICompatibleProvider
        )
        return {
            "status": "healthy" if current.store.healthy else "degraded",
            "edition": "agent-core",
            "provider": "script+http" if enabled else "offline-script",
            "http_enabled": enabled,
            "active": sum(not t.done() for t in current.service.tasks.values()),
        }

    @app.post("/v1/runs", status_code=202)
    async def start(body: RunRequest, request: Request) -> RunView:
        return await active(request).service.start(body)

    @app.get("/v1/runs/by-key/{key}")
    async def by_key(key: str, request: Request) -> RunView:
        return await active(request).service.by_key(key)

    @app.get("/v1/runs/{run_id}")
    async def get(run_id: str, request: Request) -> RunView:
        return await active(request).service.get(run_id)

    @app.get("/v1/runs/{run_id}/result")
    async def result(run_id: str, request: Request) -> RunFacts:
        return await active(request).service.result(run_id)

    @app.get("/v1/runs/{run_id}/trace")
    async def trace(run_id: str, request: Request) -> TraceView:
        return await active(request).service.trace(run_id)

    @app.get("/v1/runs/{run_id}/events")
    async def events(
        run_id: str,
        request: Request,
        from_seq: int = Query(default=0, ge=0),
        limit: int = Query(default=64, ge=1, le=64),
    ) -> list[RunEvent]:
        return await active(request).service.events(run_id, from_seq, limit)

    @app.get("/v1/runs/{run_id}/stream")
    async def stream(
        run_id: str, request: Request, from_seq: int = Query(default=0, ge=0)
    ) -> StreamingResponse:
        service = active(request).service
        await service.events(run_id, from_seq, 1)

        async def frames() -> AsyncIterator[str]:
            try:
                async for event in service.stream(run_id, from_seq):
                    yield (
                        ": heartbeat\n\n"
                        if event is None
                        else "id: "
                        + str(event.seq)
                        + "\nevent: "
                        + event.kind
                        + "\ndata: "
                        + encoded(event.model_dump(mode="json"))
                        + "\n\n"
                    )
            except Exception:
                yield 'event: error\ndata: {"code":"storage_unavailable"}\n\n'

        return StreamingResponse(
            frames(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    @app.post("/v1/runs/{run_id}/cancel")
    async def cancel(run_id: str, request: Request) -> RunView:
        return await active(request).service.cancel(run_id)

    @app.get("/v1/runs/{run_id}/approvals")
    async def approvals(run_id: str, request: Request) -> list[dict[str, JsonValue]]:
        return await active(request).service.approvals(run_id)

    @app.post("/v1/runs/{run_id}/approvals/{request_id}")
    async def reply(
        run_id: str, request_id: str, body: ApprovalReply, request: Request
    ) -> dict[str, JsonValue]:
        return await active(request).service.reply_approval(run_id, request_id, body)

    mount_browser_ui(app)
    from .sessions import mount_sessions

    mount_sessions(app)

    @app.get("/v1/eval-targets")
    async def targets(request: Request) -> list[dict[str, JsonValue]]:
        return evaluation(request).targets()

    @app.get("/v1/eval-rules")
    async def rules(request: Request) -> list[dict[str, JsonValue]]:
        return evaluation(request).rules()

    @app.get("/v1/runs/{run_id}/candidate")
    async def candidate(run_id: str, request: Request, include_input: bool = False) -> Candidate:
        return await evaluation(request).candidate(run_id, include_input)

    @app.post("/v1/datasets")
    async def freeze(body: FreezeRequest, request: Request) -> DatasetRevision:
        return await evaluation(request).freeze(body)

    @app.get("/v1/datasets/{name}/{version}")
    async def dataset(name: str, version: str, request: Request) -> DatasetRevision:
        return await active(request).store.dataset(name, version)

    @app.get("/v1/evals", include_in_schema=False)
    async def no_eval_listing() -> JSONResponse:
        # 不新增批次枚举；保留M1对这个未支持读入口的404约定。
        return JSONResponse({"error": {"code": "not_found"}}, status_code=404)

    @app.post("/v1/evals", status_code=202)
    async def evaluate(body: EvalRequest, request: Request) -> EvalView:
        return await evaluation(request).start(body)

    @app.get("/v1/evals/by-key/{key}")
    async def eval_by_key(key: str, request: Request) -> EvalView:
        return await evaluation(request).by_key(key)

    @app.get("/v1/evals/compare")
    async def compare(
        request: Request,
        baseline: str,
        candidate: str,
        baseline_revision: int = Query(ge=1),
        candidate_revision: int = Query(ge=1),
        baseline_hash: str = Query(pattern=r"^[0-9a-f]{64}$"),
        candidate_hash: str = Query(pattern=r"^[0-9a-f]{64}$"),
    ) -> dict[str, JsonValue]:
        return await evaluation(request).compare(
            baseline,
            candidate,
            baseline_revision,
            candidate_revision,
            baseline_hash,
            candidate_hash,
        )

    @app.get("/v1/evals/{eval_id}")
    async def eval_get(eval_id: str, request: Request) -> EvalView:
        return await evaluation(request).get(eval_id)

    @app.get("/v1/evals/{eval_id}/items")
    async def eval_items(
        eval_id: str,
        request: Request,
        revision: int = Query(ge=1),
        from_index: int = Query(default=0, ge=0),
        limit: int = Query(default=8, ge=1, le=8),
    ) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            await evaluation(request).items(eval_id, revision, from_index, limit),
        )

    @app.get("/v1/evals/{eval_id}/report")
    async def report(
        eval_id: str, request: Request, revision: int | None = Query(default=None, ge=1)
    ) -> dict[str, JsonValue]:
        return cast(dict[str, JsonValue], await evaluation(request).report(eval_id, revision))

    @app.post("/v1/evals/{eval_id}/cancel")
    async def eval_cancel(eval_id: str, request: Request) -> EvalView:
        return await evaluation(request).cancel(eval_id)

    return app
