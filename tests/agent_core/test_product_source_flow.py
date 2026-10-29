"""真实来源→人工参考→持久数据集→持续评测，GET 不依赖浏览器缓存。"""

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import event, func, select

from krim_agent.api.evaluations import mount_evaluations
from krim_agent.client import AsyncRunClient
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.evaluation.adapters.krim_source import KrimSourceReader
from krim_agent.evaluation.continuous_types import ContinuousRequest, SourceSelection
from krim_agent.evaluation.domain import CaseSpec, DatasetRef, MetricSpec
from krim_agent.evaluation.product_types import (
    FreezeConfirmedRequest,
    ProductConfirmationRequest,
    RunImportRequest,
)
from krim_agent.evaluation.repository import case_candidates
from krim_agent.sdk import WorkspaceEvaluationSDK
from krim_agent.security import LOCAL, Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest


async def assert_receipt_read_only(application, product, imported):
    api = FastAPI()

    @api.middleware("http")
    async def actor(request: Request, call_next):
        request.state.principal = LOCAL
        return await call_next(request)

    @api.exception_handler(ServiceError)
    async def error_response(_request: Request, error: ServiceError):
        return JSONResponse({"error": {"code": error.code}}, status_code=error.status)

    mount_evaluations(api, lambda _request: product)
    client = AsyncRunClient("http://asgi.test")
    await client.close()
    client.http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://asgi.test"
    )
    engines = [
        product.evaluation.repository.engine.sync_engine,
        application.store.engine.sync_engine,
    ]
    statements = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    for engine in engines:
        event.listen(engine, "before_cursor_execute", capture)
    calls_before = application.provider.calls
    try:
        remote = WorkspaceEvaluationSDK(client)
        for _ in range(2):
            assert (await remote.import_receipt("import"))["candidates"] == imported
        with pytest.raises(ServiceError) as missing:
            await remote.import_receipt("unknown")
        assert missing.value.code == "not_found"
        foreign = PrincipalContext("other", 1, (Membership(workspace_id="other", role="admin"),))
        with pytest.raises(ServiceError) as scoped:
            await product.import_receipt(foreign, "other", "import")
        assert scoped.value.code == "not_found"
        assert statements and all(
            sql.lstrip().upper().startswith(("SELECT", "BEGIN")) for sql in statements
        )
        assert application.provider.calls == calls_before
    finally:
        for engine in engines:
            event.remove(engine, "before_cursor_execute", capture)
        await client.close()


async def test_confirmed_dataset_metadata_survives_new_client(tmp_path):
    async with await open_application(
        tmp_path / "source.sqlite", settings=Settings(product_eval_enabled=True)
    ) as app:
        product = app.evaluation_product
        assert product is not None and app.product_evaluations is not None
        run = await app.service.start(RunRequest(text="/add 2.5 3.5", request_key="source"))
        assert (await app.service.wait(run.id)).status == "completed"
        sdk = WorkspaceEvaluationSDK(product)
        imported = await sdk.import_runs(
            RunImportRequest(request_key="import", sources=[SourceSelection(run_id=run.id)])
        )
        calls_before_receipt = app.provider.calls
        receipt = await sdk.import_receipt("import")
        assert receipt["request_key"] == "import"
        assert receipt["candidates"] == imported
        assert app.provider.calls == calls_before_receipt
        await assert_receipt_read_only(app, product, imported)
        candidate = await sdk.candidate(imported[0]["candidate_id"])
        reader = KrimSourceReader(
            app.store,
            app.sessions,
            LOCAL,
            workspace_id="local",
            scope=product._scope("local"),
            allow_legacy=True,
        )
        source = await reader.read(
            product._scope("local"), SourceSelection(run_id=run.id).model_dump(mode="json")
        )
        reconstructed = source["case"] is None
        if reconstructed:
            assert candidate["eligibility"] == "needs_context"
            case = CaseSpec(
                case_id="manual-context",
                instruction="/add 2.5 3.5",
                files={"INPUT.txt": "/add 2.5 3.5"},
                verifier="krim-script/1",
                reference={"expected": "6"},
            )
        else:
            case = CaseSpec.model_validate(source["case"])
            case = case.model_copy(update={"reference": {"expected": "6"}})
        confirmed = await sdk.confirm_candidate(
            candidate["candidate_id"],
            ProductConfirmationRequest(
                request_key="confirm",
                expected_revision=candidate["revision"],
                expected_hash=candidate["hash"],
                case=case,
                context="manual_reconstructed" if reconstructed else "complete",
                reason="依据算式人工确认 2.5 + 3.5 的参考结果为 6",
            ),
        )
        frozen = await sdk.freeze_confirmed(
            FreezeConfirmedRequest(
                request_key="freeze",
                name="confirmed-source",
                version="v1",
                confirmations=[confirmed["id"]],
            )
        )
        # 新客户端没有此前冻结响应或 localStorage，仍能读取持久 lineage 指纹。
        fresh = WorkspaceEvaluationSDK(product)
        dataset = await fresh.dataset("confirmed-source", "v1")
        assert dataset["dataset_envelope_fp"] == frozen["dataset_envelope_fp"]
        metrics = [MetricSpec.model_validate(row) for row in await fresh.metrics("krim-script-v2")]
        task = await fresh.start_task(
            ContinuousRequest(
                request_key="execute-confirmed",
                mode="execute",
                target_key="krim-script-v2",
                dataset=DatasetRef.model_validate(frozen["dataset"]),
                dataset_envelope_fp=dataset["dataset_envelope_fp"],
                metrics=metrics,
            )
        )
        assert (await app.product_evaluations.kernel.wait(task["id"]))["status"] == "completed"
        finished = await fresh.task(task["id"])
        assert finished["finished_count"] == finished["planned_count"] == 1
        assert finished["report_hash"]
        assert (await fresh.task_by_key("execute-confirmed"))["report_hash"] == finished["report_hash"]
        listed = next(item for item in await fresh.list_tasks() if item["id"] == task["id"])
        assert listed["finished_count"] == 1 and listed["report_hash"] == finished["report_hash"]
        report = await fresh.report_page(task["id"])
        assert report["coverage"]["cell_coverage"] == 1
        assert report["rows"][0]["quality"] == "passed"


async def test_import_receipt_and_same_key_replay_reauthorize_source_owner(tmp_path):
    async with await open_application(
        tmp_path / "owner-source.sqlite", settings=Settings(product_eval_enabled=True)
    ) as app:
        product = app.evaluation_product
        assert product is not None and app.sessions is not None
        alice = PrincipalContext("alice", 1, (Membership(workspace_id="team"),))
        bob = PrincipalContext("bob", 1, (Membership(workspace_id="team"),))
        admin = PrincipalContext(
            "admin", 1, (Membership(workspace_id="team", role="admin"),)
        )
        session = await app.sessions.create(
            alice, "team", SessionCreate(request_key="alice-session")
        )
        turn = await app.sessions.submit(
            alice,
            "team",
            session.id,
            TurnRequest(
                request_key="alice-run",
                text="/add 2.5 3.5",
                expected_revision=0,
            ),
        )
        completed = await app.sessions.wait(alice, "team", session.id, turn.id)
        assert completed.status == "completed" and completed.run_id
        request = RunImportRequest(
            request_key="owner-import",
            sources=[SourceSelection(run_id=completed.run_id)],
        )
        imported = await product.import_runs(alice, "team", request)
        assert len(imported) == 1

        async def candidate_count() -> int:
            async with product.evaluation.repository.engine.begin() as conn:
                return await conn.scalar(
                    select(func.count())
                    .select_from(case_candidates)
                    .where(case_candidates.c.scope == product._scope("team"))
                )

        count_before = await candidate_count()
        calls_before = app.provider.calls
        engines = [
            product.evaluation.repository.engine.sync_engine,
            app.store.engine.sync_engine,
        ]
        statements = []

        def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement)

        for engine in engines:
            event.listen(engine, "before_cursor_execute", capture)
        try:
            with pytest.raises(ServiceError) as denied_get:
                await product.import_receipt(bob, "team", "owner-import")
            assert denied_get.value.code == "not_found"
            with pytest.raises(ServiceError) as denied_replay:
                await product.import_runs(bob, "team", request)
            assert denied_replay.value.code == "not_found"

            owner_receipt = await product.import_receipt(alice, "team", "owner-import")
            admin_receipt = await product.import_receipt(admin, "team", "owner-import")
            assert owner_receipt["candidates"] == admin_receipt["candidates"] == imported
            assert await product.import_runs(admin, "team", request) == imported
        finally:
            for engine in engines:
                event.remove(engine, "before_cursor_execute", capture)

        non_reads = [
            sql
            for sql in statements
            if not sql.lstrip().upper().startswith(("SELECT", "BEGIN"))
        ]
        assert statements and not non_reads, non_reads
        assert app.provider.calls == calls_before
        assert await candidate_count() == count_before
