"""真实SQLite、RunService和评测Kernel的宿主自动化验收。"""

from __future__ import annotations

import asyncio
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

from krim_agent.api.evaluation_automation import mount_evaluation_automation
from krim_agent.composition import Application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.automation import EvaluationAutomation
from krim_agent.evaluation.continuous_types import (
    ContinuousRequest,
    ScheduleSpec,
    SourceSelection,
)
from krim_agent.evaluation.domain import Budget, CaseSpec, DatasetRef, MetricSpec
from krim_agent.evaluation.hot_types import FeedbackSignal
from krim_agent.evaluation.product_types import (
    FreezeConfirmedRequest,
    ProductConfirmationRequest,
    RunImportRequest,
)
from krim_agent.evaluation.repository import case_candidates, eval_schedule_windows
from krim_agent.security import AccessPolicy, Credential, Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import observation_outbox as outbox_sql


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> float:
        return float(self.now)


def actors() -> dict[str, PrincipalContext]:
    return {
        "admin": PrincipalContext("admin", 1, (Membership(workspace_id="alpha", role="admin"),)),
        "alice": PrincipalContext("alice", 1, (Membership(workspace_id="alpha", role="member"),)),
        "viewer": PrincipalContext("viewer", 1, (Membership(workspace_id="alpha", role="viewer"),)),
        "bob": PrincipalContext("bob", 1, (Membership(workspace_id="beta", role="member"),)),
        "beta_admin": PrincipalContext(
            "beta-admin", 1, (Membership(workspace_id="beta", role="admin"),)
        ),
    }


def access_policy(directory: dict[str, PrincipalContext]) -> AccessPolicy:
    return AccessPolicy(
        credentials=tuple(
            Credential(
                principal_id=actor.principal_id,
                token=SecretStr("token-" + actor.principal_id),
                generation=actor.generation,
                memberships=actor.memberships,
            )
            for actor in directory.values()
        )
    )


@asynccontextmanager
async def world(
    root: Path, *, clock: Clock | None = None, poll_interval: float = 0.01
):
    await asyncio.to_thread(root.mkdir, parents=True, exist_ok=True)
    wire = await WireFixture().start()
    directory = actors()
    context = wire.application(
        root / "runs.sqlite",
        settings=Settings(product_eval_enabled=True),
        access_policy=access_policy(directory),
    )
    app: Application = await context.__aenter__()
    assert app.evaluation_product is not None
    automation = EvaluationAutomation(
        app.evaluation_product,
        directory.values(),
        clock=clock or Clock(1_800_000_000),
        poll_interval=poll_interval,
    )
    try:
        await automation.start()
        yield app, automation, directory, wire
    finally:
        await automation.close()
        await context.__aexit__(None, None, None)
        await wire.close()


async def run_for(app: Application, actor: PrincipalContext, workspace: str, key: str) -> str:
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


async def source_rows(app: Application, workspace: str) -> list[dict[str, Any]]:
    async def read(conn):
        return await outbox_sql.rows(conn, workspace)

    return await app.store.work_read(read)


async def wait_until(predicate) -> None:
    async with asyncio.timeout(8.0):
        while not await predicate():  # noqa: ASYNC110  # 等待真实SQLite持久状态
            await asyncio.sleep(0.01)


async def candidate_ids(app: Application, workspace: str) -> list[str]:
    assert app.evaluation_product is not None
    scope = app.evaluation_product._scope(workspace)
    repository = app.evaluation_product.evaluation.repository
    async with repository.lock, repository.engine.begin() as conn:
        values = (
            await conn.execute(
                select(case_candidates.c.candidate_id)
                .where(case_candidates.c.scope == scope)
                .distinct()
            )
        ).scalars().all()
    return list(values)


async def test_capture_feedback_privacy_permissions_and_restart(tmp_path):
    async with world(tmp_path) as (app, automation, directory, _wire):
        assert app.evaluation_product is not None
        product = app.evaluation_product
        admin, alice, viewer, bob = (
            directory["admin"],
            directory["alice"],
            directory["viewer"],
            directory["bob"],
        )

        initial = await automation.status(admin, "alpha")
        assert initial["capture"]["enabled"] is False
        assert initial["capture"]["pending"] == 0
        assert not await source_rows(app, "alpha")
        await run_for(app, alice, "alpha", "before-opt-in")
        assert not await source_rows(app, "alpha")
        with pytest.raises(ServiceError, match="not_found"):
            await automation.status(alice, "beta")
        with pytest.raises(ServiceError, match="permission_denied"):
            await automation.set_capture(viewer, "alpha", True)

        api = FastAPI()

        @api.middleware("http")
        async def inject(request: Request, call_next):
            request.state.principal = admin
            return await call_next(request)

        @api.exception_handler(ServiceError)
        async def service_error(_request: Request, error: ServiceError):
            return JSONResponse({"error": {"code": error.code}}, status_code=error.status)

        mount_evaluation_automation(api, lambda _request: automation)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://automation.test"
        ) as client:
            before = await client.get("/v4/workspaces/alpha/eval/automation/status")
            assert before.status_code == 200 and before.json()["capture"]["enabled"] is False
            enabled = await client.post(
                "/v4/workspaces/alpha/eval/automation/capture", json={"enabled": True}
            )
            assert enabled.status_code == 200
            assert enabled.json()["capture"]["enabled"] is True

        run_id = await run_for(app, alice, "alpha", "captured")
        await wait_until(lambda: _has_capture(app, "alpha", run_id))
        await wait_until(lambda: _has_candidate(app, "alpha"))
        rows = await source_rows(app, "alpha")
        assert len(rows) == 1 and rows[0]["run_id"] == run_id
        record = rows[0]["body"]
        assert record["policy"]["enabled"] is True
        assert record["policy"]["include_text"] is False
        assert record["policy"]["include_arguments"] is False
        assert record["policy"]["artifact_paths"] == []

        ids = await candidate_ids(app, "alpha")
        assert len(ids) == 1
        candidate = await product.candidate(admin, "alpha", ids[0])
        assert candidate["status"] == "pending"
        with pytest.raises(ServiceError, match="permission_denied"):
            await automation.submit_feedback(
                viewer,
                "alpha",
                run_id,
                FeedbackSignal(key="viewer-feedback", reason="incorrect"),
            )
        with pytest.raises(ServiceError, match="not_found"):
            await automation.submit_feedback(
                bob,
                "beta",
                run_id,
                FeedbackSignal(key="cross-workspace", reason="incorrect"),
            )

        feedback = await automation.submit_feedback(
            alice,
            "alpha",
            run_id,
            FeedbackSignal(key="alice-feedback", reason="incorrect"),
        )
        assert feedback == {"revision": 1, "key": "alice-feedback"}
        try:
            await wait_until(lambda: _has_feedback_candidate(app, "alpha"))
        except TimeoutError:
            raise AssertionError(
                {
                    "automation_error": automation._last_error,
                    "status": await automation.status(admin, "alpha"),
                    "candidate_ids": await candidate_ids(app, "alpha"),
                }
            ) from None
        feedback_ids = await candidate_ids(app, "alpha")
        assert len(feedback_ids) == 2
        feedback_id = next(identity for identity in feedback_ids if identity != ids[0])
        candidate = await product.candidate(admin, "alpha", feedback_id)
        assert candidate["status"] == "pending"
        assert candidate.get("signals") == [
            {"key": "alice-feedback", "reason": "incorrect", "actor": "alice"}
        ]
        assert (await product.candidate(admin, "alpha", ids[0]))["status"] == "pending"

        status = await automation.status(admin, "alpha")
        safe_status = repr(status)
        assert "claim_token" not in safe_status
        assert "token-" not in safe_status
        assert "runs.sqlite" not in safe_status
        assert "eval.sqlite" not in safe_status
        assert run_id not in safe_status
        assert status["capture"]["text_capture"] is False
        assert status["capture"]["argument_capture"] is False
        assert status["capture"]["acknowledged"] == 1
        assert "reason" not in status["capture"]

        rows = await source_rows(app, "alpha")
        saved_manifest = rows[0]["delivery"]["manifest"]
        await automation.close()
        restarted = EvaluationAutomation(
            product,
            directory.values(),
            clock=automation.clock,
            poll_interval=0.01,
        )
        await restarted.start()
        await asyncio.sleep(0.05)
        disabled = await restarted.set_capture(admin, "alpha", False)
        assert disabled["capture"]["enabled"] is False
        after_opt_out = await run_for(app, alice, "alpha", "after-opt-out")
        assert not await _has_capture(app, "alpha", after_opt_out)
        await restarted.close()
        assert restarted._task is not None and restarted._task.done()
        assert not product.evaluation.repository.closed
        recovered = await source_rows(app, "alpha")
        assert recovered[0]["delivery"]["manifest"] == saved_manifest
        assert sorted(await candidate_ids(app, "alpha")) == sorted(feedback_ids)
        assert (await product.candidate(admin, "alpha", ids[0]))["status"] == "pending"
        assert (await product.candidate(admin, "alpha", feedback_id))["status"] == "pending"


async def _has_capture(app: Application, workspace: str, run_id: str) -> bool:
    return any(row["run_id"] == run_id for row in await source_rows(app, workspace))


async def _has_candidate(app: Application, workspace: str) -> bool:
    return bool(await candidate_ids(app, workspace))


async def _has_feedback_candidate(app: Application, workspace: str) -> bool:
    assert app.evaluation_product is not None
    for candidate_id in await candidate_ids(app, workspace):
        try:
            candidate = await app.evaluation_product.candidate(
                actors()["admin"], workspace, candidate_id
            )
        except ServiceError:
            continue
        if candidate.get("signals") == [
            {"key": "alice-feedback", "reason": "incorrect", "actor": "alice"}
        ]:
            return True
    return False


async def test_background_schedule_uses_product_scope_and_joins_on_close(tmp_path):
    clock = Clock(1_800_000_000)
    async with world(tmp_path, clock=clock) as (app, automation, directory, _wire):
        assert app.evaluation_product is not None
        product = app.evaluation_product
        admin = directory["admin"]
        owned_task = automation._task
        await asyncio.gather(automation.start(), automation.start())
        assert automation._task is owned_task
        source_run = await run_for(app, admin, "alpha", "schedule-source")
        imported = await product.import_runs(
            admin,
            "alpha",
            RunImportRequest(
                request_key="schedule-import",
                sources=[SourceSelection(run_id=source_run)],
            ),
        )
        candidate_id = imported[0]["candidate_id"]
        draft = await product.candidate(admin, "alpha", candidate_id)
        confirmed = await product.confirm_candidate(
            admin,
            "alpha",
            candidate_id,
            ProductConfirmationRequest(
                request_key="schedule-confirm",
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
                reason="Synthetic reference for the schedule integration test",
            ),
        )
        frozen = await product.freeze_confirmed(
            admin,
            "alpha",
            FreezeConfirmedRequest(
                request_key="schedule-freeze",
                name="scheduled-suite",
                version="v1",
                confirmations=[confirmed["id"]],
            ),
        )
        dataset = frozen["dataset"]
        target = "krim-script-v2"
        metric_rows = await product.metrics(admin, "alpha", target, mode="continuous")
        template = ContinuousRequest(
            scope="client-scope",
            request_key="template-only",
            mode="execute",
            dataset=DatasetRef.model_validate(dataset),
            dataset_envelope_fp=frozen["dataset_envelope_fp"],
            target_key=target,
            metrics=[MetricSpec.model_validate(row) for row in metric_rows],
            execution=Budget(),
        )
        api = FastAPI()

        @api.middleware("http")
        async def inject(request: Request, call_next):
            request.state.principal = admin
            return await call_next(request)

        @api.exception_handler(ServiceError)
        async def service_error(_request: Request, error: ServiceError):
            return JSONResponse({"error": {"code": error.code}}, status_code=error.status)

        mount_evaluation_automation(api, lambda _request: automation)

        async def post(path: str, body: dict[str, Any] | None = None) -> httpx.Response:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api), base_url="http://automation.test"
            ) as client:
                return await client.post(path, json=body)

        spec = ScheduleSpec(
            scope="client-scope",
            schedule_id="nightly",
            version="v1",
            template=template,
            epoch=clock.now,
            interval_seconds=60,
            max_windows=1,
        )
        created_response = await post(
            "/v4/workspaces/alpha/eval/automation/schedules",
            spec.model_dump(mode="json"),
        )
        assert created_response.status_code == 201
        created = created_response.json()
        assert created["enabled"] is True
        scope = product._scope("alpha")
        schedule = await product.evaluation.repository.schedule(scope, "nightly")
        assert schedule["spec"]["scope"] == scope
        assert schedule["spec"]["template"]["scope"] == scope

        async def task_created() -> bool:
            tasks = await product.list_tasks(admin, "alpha")
            return any(item["kind"] == "continuous" for item in tasks)

        await wait_until(task_created)
        tasks = await product.list_tasks(admin, "alpha")
        task = next(item for item in tasks if item["kind"] == "continuous")
        result = await product.evaluation.kernel.wait(task["id"])
        assert result["status"] == "completed"
        async with product.evaluation.repository.engine.begin() as conn:
            window_rows = (
                await conn.execute(
                    select(eval_schedule_windows.c.body).where(
                        eval_schedule_windows.c.scope == scope,
                        eval_schedule_windows.c.schedule_id_version == "nightly/v1",
                    )
                )
            ).scalars().all()
        assert len(window_rows) == 1
        assert window_rows[0]["task_id"] == task["id"]
        retried = await post(
            "/v4/workspaces/alpha/eval/automation/schedules/nightly/tick"
        )
        assert retried.status_code == 202 and retried.json()["id"] == task["id"]

        disabled = await post(
            "/v4/workspaces/alpha/eval/automation/schedules/nightly/disable",
            {"revision": 0, "key": "disable-nightly"},
        )
        assert disabled.status_code == 200 and disabled.json()["enabled"] is False
        task_snapshot = await product.evaluation.repository.snapshot(task["id"])
        assert task_snapshot["request_json"]["scope"] == scope
        assert task_snapshot["request_json"]["schedule"]["window_index"] == 0
        await automation.close()
        assert automation._task is not None and automation._task.done()
        assert not product.evaluation.repository.closed


@pytest.mark.asyncio
async def test_observer_generation_change_fails_closed_after_restart(tmp_path):
    async with world(tmp_path, poll_interval=60) as (app, automation, directory, _wire):
        admin = directory["admin"]
        await automation.set_capture(admin, "alpha", True)
        await asyncio.sleep(0.05)
        await run_for(app, directory["alice"], "alpha", "observer-generation")
        await automation.close()
        assert app.evaluation_product is not None
        replacement = PrincipalContext(
            "admin", 2, (Membership(workspace_id="alpha", role="member"),)
        )
        revised = EvaluationAutomation(
            app.evaluation_product,
            (replacement, *[value for key, value in directory.items() if key != "admin"]),
            poll_interval=0.01,
        )
        await revised.start()
        await asyncio.sleep(0.05)
        status = await revised.status(replacement, "alpha")
        assert status["capture"]["enabled"] is True
        assert status["capture"]["observer_ready"] is False
        assert status["capture"]["pending"] == 1
        await revised.close()


@pytest.mark.asyncio
async def test_schedule_api_mount_uses_getter(tmp_path):
    async with world(tmp_path) as (_app, automation, directory, _wire):
        app = FastAPI()
        admin = directory["admin"]

        @app.middleware("http")
        async def inject(request: Request, call_next):
            request.state.principal = admin
            return await call_next(request)

        @app.exception_handler(ServiceError)
        async def service_error(_request: Request, error: ServiceError):
            return JSONResponse({"error": {"code": error.code}}, status_code=error.status)

        mount_evaluation_automation(app, lambda _request: automation)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://automation.test"
        ) as client:
            response = await client.get("/v4/workspaces/alpha/eval/automation/status")
        assert response.status_code == 200
        assert response.json()["automation"]["running"] is True
