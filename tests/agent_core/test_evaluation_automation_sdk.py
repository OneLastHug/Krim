"""自动化SDK的本地SQLite、ASGI、授权及原键回归。"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fixtures.session_server import WireFixture
from pydantic import SecretStr
from sqlalchemy import select

from krim_agent.api.boundary import LocalBoundary
from krim_agent.api.evaluation_automation import mount_evaluation_automation
from krim_agent.client import AsyncRunClient
from krim_agent.composition import Application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.automation import EvaluationAutomation
from krim_agent.evaluation.continuous_types import ContinuousRequest, ScheduleSpec, SourceSelection
from krim_agent.evaluation.domain import Budget, CaseSpec, DatasetRef, MetricSpec
from krim_agent.evaluation.hot_types import FeedbackSignal
from krim_agent.evaluation.product_types import (
    FreezeConfirmedRequest,
    ProductConfirmationRequest,
    RunImportRequest,
)
from krim_agent.evaluation.repository import case_candidates
from krim_agent.sdk.evaluation_automation import EvaluationAutomationSDK
from krim_agent.security import AccessPolicy, Credential, Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import observation_outbox as outbox_sql


@asynccontextmanager
async def runtime(root: Path):
    await asyncio.to_thread(root.mkdir, parents=True, exist_ok=True)
    actors = {
        "admin": PrincipalContext("admin", 1, (Membership(workspace_id="alpha", role="admin"),)),
        "alice": PrincipalContext("alice", 1, (Membership(workspace_id="alpha", role="member"),)),
        "viewer": PrincipalContext("viewer", 1, (Membership(workspace_id="alpha", role="viewer"),)),
        "bob": PrincipalContext("bob", 1, (Membership(workspace_id="beta", role="member"),)),
    }
    access = AccessPolicy(
        credentials=tuple(
            Credential(
                principal_id=value.principal_id,
                token=SecretStr("token-" + value.principal_id),
                generation=value.generation,
                memberships=value.memberships,
            )
            for value in actors.values()
        )
    )
    wire = await WireFixture().start()
    context = wire.application(
        root / "runs.sqlite",
        settings=Settings(product_eval_enabled=True),
        access_policy=access,
    )
    app: Application = await context.__aenter__()
    assert app.evaluation_product is not None
    automation = EvaluationAutomation(
        app.evaluation_product, actors.values(), poll_interval=0.01
    )
    try:
        await automation.start()
        yield app, automation, actors, wire
    finally:
        await automation.close()
        await context.__aexit__(None, None, None)
        await wire.close()


async def create_run(
    app: Application, actor: PrincipalContext, workspace: str, key: str
) -> str:
    assert app.sessions is not None
    session = await app.sessions.create(
        actor,
        workspace,
        SessionCreate(request_key=key + "-session", target_key="demo"),
    )
    turn = await app.sessions.submit(
        actor,
        workspace,
        session.id,
        TurnRequest(
            request_key=key + "-turn",
            text="/add 2 3",
            expected_revision=session.revision,
        ),
    )
    ended = await app.sessions.wait(actor, workspace, session.id, turn.id)
    assert ended.run_id is not None
    return ended.run_id


async def outbox_rows(app: Application, workspace: str) -> list[dict[str, Any]]:
    async def read(conn):
        return await outbox_sql.rows(conn, workspace)

    return await app.store.work_read(read)


async def candidate_ids(app: Application, workspace: str) -> list[str]:
    assert app.evaluation_product is not None
    repository = app.evaluation_product.evaluation.repository
    scope = app.evaluation_product._scope(workspace)
    async with repository.lock, repository.engine.begin() as conn:
        values = (
            await conn.execute(
                select(case_candidates.c.candidate_id)
                .where(case_candidates.c.scope == scope)
                .distinct()
            )
        ).scalars().all()
    return list(values)


async def wait_until(predicate) -> None:
    async with asyncio.timeout(8.0):
        while not await predicate():  # noqa: ASYNC110  # 等待真实SQLite持久状态
            await asyncio.sleep(0.01)


async def test_inprocess_sdk_authorizes_feedback_and_keeps_candidates_pending(tmp_path):
    async with runtime(tmp_path) as (app, automation, actors, _wire):
        assert app.evaluation_product is not None
        admin = EvaluationAutomationSDK(
            automation, principal=actors["admin"], workspace="alpha"
        )
        member = EvaluationAutomationSDK(
            automation, principal=actors["alice"], workspace="alpha"
        )
        viewer = EvaluationAutomationSDK(
            automation, principal=actors["viewer"], workspace="alpha"
        )

        initial = await admin.status()
        assert initial["capture"]["enabled"] is False
        assert initial["capture"]["text_capture"] is False
        assert initial["capture"]["argument_capture"] is False
        with pytest.raises(ServiceError) as denied:
            await viewer.set_capture(True)
        assert denied.value.code == "permission_denied"

        enabled = await admin.set_capture(True)
        assert enabled["capture"]["enabled"] is True
        run_id = await create_run(app, actors["alice"], "alpha", "sdk-feedback")
        await wait_until(
            lambda: _has_run_capture(app, "alpha", run_id)
        )
        result = await member.submit_feedback(
            run_id,
            FeedbackSignal(key="sdk-feedback-key", reason="incorrect"),
        )
        assert result == {"revision": 1, "key": "sdk-feedback-key"}
        await wait_until(
            lambda: _has_feedback_candidate(
                app, actors["admin"], "alpha", "sdk-feedback-key"
            )
        )

        identities = await candidate_ids(app, "alpha")
        assert len(identities) == 2
        feedback_candidates = []
        for identity in identities:
            candidate = await app.evaluation_product.candidate(
                actors["admin"], "alpha", identity
            )
            assert candidate["status"] == "pending"
            if candidate.get("signals"):
                feedback_candidates.append(candidate)
        assert len(feedback_candidates) == 1
        assert feedback_candidates[0]["signals"] == [
            {"key": "sdk-feedback-key", "reason": "incorrect", "actor": "alice"}
        ]
        assert "reason" not in result
        disabled = await admin.set_capture(False)
        assert disabled["capture"]["enabled"] is False


async def _has_run_capture(app: Application, workspace: str, run_id: str) -> bool:
    return any(row["run_id"] == run_id for row in await outbox_rows(app, workspace))


async def _has_feedback_candidate(
    app: Application, admin: PrincipalContext, workspace: str, key: str
) -> bool:
    assert app.evaluation_product is not None
    for identity in await candidate_ids(app, workspace):
        candidate = await app.evaluation_product.candidate(admin, workspace, identity)
        if candidate.get("signals") == [
            {"key": key, "reason": "incorrect", "actor": "alice"}
        ]:
            return True
    return False


def _mounted_api(
    app: Application,
    automation: EvaluationAutomation,
    *,
    fault: dict[str, Any] | None = None,
) -> FastAPI:
    api = FastAPI()
    api.state.application = app
    api.add_middleware(LocalBoundary, settings=app.settings)

    @api.middleware("http")
    async def lose_capture_reply(request: Request, call_next):
        response = await call_next(request)
        if (
            fault is not None
            and request.method == "POST"
            and request.url.path.endswith("/capture")
        ):
            fault["capture_posts"] += 1
            if fault["lose_next_success"] and response.status_code < 400:
                fault["lose_next_success"] = False
                return JSONResponse(
                    {"error": {"code": "execution_error"}}, status_code=503
                )
        return response

    @api.exception_handler(ServiceError)
    async def service_error(_request: Request, error: ServiceError):
        return JSONResponse(
            {"error": {"code": error.code, "message": "request failed"}},
            status_code=error.status,
        )

    mount_evaluation_automation(api, lambda _request: automation)
    return api


async def _remote_client(api: FastAPI, token: str) -> AsyncRunClient:
    backend = AsyncRunClient("http://automation.test", api_key=token)
    await backend.http.aclose()
    backend.http = httpx.AsyncClient(
        headers={"Authorization": "Bearer " + token},
        transport=httpx.ASGITransport(app=api),
        base_url="http://automation.test",
    )
    return backend


async def _confirmed_schedule_template(
    app: Application, admin: PrincipalContext, run_id: str
) -> tuple[ScheduleSpec, str]:
    assert app.evaluation_product is not None
    product = app.evaluation_product
    imported = await product.import_runs(
        admin,
        "alpha",
        RunImportRequest(
            request_key="sdk-schedule-import",
            sources=[SourceSelection(run_id=run_id)],
        ),
    )
    candidate_id = imported[0]["candidate_id"]
    draft = await product.candidate(admin, "alpha", candidate_id)
    confirmed = await product.confirm_candidate(
        admin,
        "alpha",
        candidate_id,
        ProductConfirmationRequest(
            request_key="sdk-schedule-confirm",
            expected_revision=draft["revision"],
            expected_hash=draft["hash"],
            case=CaseSpec(
                case_id="manual-context",
                instruction="/add 2 3",
                files={"INPUT.txt": "/add 2 3"},
                verifier="krim-script/1",
                reference={"expected": "5"},
            ),
            context="manual_reconstructed",
            reason="为自动化SDK测试提供合成参考",
        ),
    )
    frozen = await product.freeze_confirmed(
        admin,
        "alpha",
        FreezeConfirmedRequest(
            request_key="sdk-schedule-freeze",
            name="sdk-schedule-suite",
            version="v1",
            confirmations=[confirmed["id"]],
        ),
    )
    target = "krim-script-v2"
    metric_rows = await product.metrics(admin, "alpha", target, mode="continuous")
    template = ContinuousRequest(
        scope="client-scope",
        request_key="sdk-schedule-template",
        mode="execute",
        dataset=DatasetRef.model_validate(frozen["dataset"]),
        dataset_envelope_fp=frozen["dataset_envelope_fp"],
        target_key=target,
        metrics=[MetricSpec.model_validate(row) for row in metric_rows],
        execution=Budget(),
    )
    now = int(time.time())
    spec = ScheduleSpec(
        scope="client-scope",
        schedule_id="sdk-nightly",
        version="v1",
        template=template,
        epoch=now,
        interval_seconds=60,
        max_windows=1,
    )
    return spec, product._scope("alpha")


@pytest.mark.asyncio
async def test_http_sdk_methods_unknown_post_and_original_schedule_key(tmp_path):
    async with runtime(tmp_path) as (app, automation, actors, _wire):
        fault = {"capture_posts": 0, "lose_next_success": True}
        api = _mounted_api(app, automation, fault=fault)
        # SDK中的principal不能覆盖HTTP服务从Authorization解析出的viewer身份。
        viewer_http = await _remote_client(api, "token-viewer")
        forged = EvaluationAutomationSDK(
            viewer_http, principal=actors["admin"], workspace="alpha"
        )
        try:
            with pytest.raises(ServiceError) as denied:
                await forged.set_capture(True)
            assert denied.value.code == "permission_denied"
        finally:
            await viewer_http.close()

        http = await _remote_client(api, "token-admin")
        admin = EvaluationAutomationSDK(http, principal=actors["admin"], workspace="alpha")
        member_http = await _remote_client(api, "token-alice")
        member = EvaluationAutomationSDK(
            member_http, principal=actors["alice"], workspace="alpha"
        )
        try:
            assert (await admin.status())["capture"]["enabled"] is False
            # 路由已提交启用，但HTTP回复未知；SDK必须原样报错且不能再发一次POST。
            with pytest.raises(ServiceError) as unknown:
                await admin.set_capture(True)
            assert unknown.value.status == 503
            assert fault["capture_posts"] == 2
            assert (await admin.status())["capture"]["enabled"] is True

            run_id = await create_run(app, actors["alice"], "alpha", "sdk-http-feedback")
            await wait_until(lambda: _has_run_capture(app, "alpha", run_id))
            feedback = await member.submit_feedback(
                run_id,
                FeedbackSignal(key="http-feedback-key", reason="incorrect"),
            )
            assert feedback == {"revision": 1, "key": "http-feedback-key"}
            await wait_until(
                lambda: _has_feedback_candidate(
                    app, actors["admin"], "alpha", "http-feedback-key"
                )
            )

            spec, scope = await _confirmed_schedule_template(
                app, actors["admin"], run_id
            )
            created = await admin.create_schedule(spec)
            assert created["enabled"] is True and created["schedule_id"] == "sdk-nightly"
            assert await admin.create_schedule(spec) == created
            # 另一个未来计划用于检查稳定排序与分页；不会产生执行任务。
            later_spec = ScheduleSpec.model_validate(
                spec.model_dump(mode="json")
                | {"schedule_id": "sdk-later", "epoch": spec.epoch + 3600}
            )
            later = await admin.create_schedule(later_spec)
            assert later["enabled"] is True
            stored = await app.evaluation_product.evaluation.repository.schedule(
                scope, "sdk-nightly"
            )
            assert stored["spec"]["scope"] == scope
            assert stored["spec"]["template"]["scope"] == scope

            async def has_task() -> bool:
                assert app.evaluation_product is not None
                rows = await app.evaluation_product.list_tasks(actors["admin"], "alpha")
                return any(row["kind"] == "continuous" for row in rows)

            await wait_until(has_task)
            assert app.evaluation_product is not None
            tasks = await app.evaluation_product.list_tasks(actors["admin"], "alpha")
            task = next(row for row in tasks if row["kind"] == "continuous")
            result = await app.evaluation_product.evaluation.kernel.wait(task["id"])
            assert result["status"] == "completed"
            before_reads = {
                row["id"]
                for row in await app.evaluation_product.list_tasks(actors["admin"], "alpha")
            }
            listed = await admin.list_schedules()
            assert [row["schedule_id"] for row in listed] == ["sdk-later", "sdk-nightly"]
            page = await admin.list_schedules(offset=1, limit=1)
            assert page[0]["schedule_id"] == "sdk-nightly"
            detail = await admin.schedule("sdk-nightly")
            assert set(detail) == {
                "schedule_id",
                "version",
                "enabled",
                "revision",
                "observed_through",
                "missed_windows",
                "dataset",
                "target_key",
                "epoch",
                "interval_seconds",
                "max_windows",
            }
            assert detail["dataset"]["name"] == "sdk-schedule-suite"
            assert detail["target_key"] == "krim-script-v2"
            assert "scope" not in detail and "target_descriptor" not in detail
            assert {
                row["id"]
                for row in await app.evaluation_product.list_tasks(actors["admin"], "alpha")
            } == before_reads
            with pytest.raises(ServiceError) as foreign:
                await EvaluationAutomationSDK(
                    automation, principal=actors["bob"], workspace="beta"
                ).schedule("sdk-nightly")
            assert foreign.value.code == "not_found"
            with pytest.raises(ServiceError) as excessive:
                await admin.list_schedules(limit=51)
            assert excessive.value.code == "invalid_input"
            tick = await admin.tick_schedule("sdk-nightly")
            assert tick["id"] == task["id"]
            disabled = await admin.disable_schedule(
                "sdk-nightly", revision=0, key="sdk-disable-nightly"
            )
            assert disabled["enabled"] is False
            assert await admin.disable_schedule(
                "sdk-nightly", revision=0, key="sdk-disable-nightly"
            ) == disabled
        finally:
            await http.close()
            await member_http.close()


@pytest.mark.asyncio
async def test_http_sdk_rejects_malformed_response_shape():
    api = FastAPI()

    @api.get("/v4/workspaces/alpha/eval/automation/status")
    async def malformed_status():
        return {"status": "healthy"}

    backend = await _remote_client(api, "unused")
    sdk = EvaluationAutomationSDK(
        backend,
        principal=PrincipalContext("admin", 1, (Membership(workspace_id="alpha", role="admin"),)),
        workspace="alpha",
    )
    try:
        with pytest.raises(ServiceError) as invalid:
            await sdk.status()
        assert invalid.value.code == "facts_corrupt"
        assert invalid.value.status == 502
    finally:
        await backend.close()
