"""真实 Application/RunService 的评测装配，不替换为假 adapter。"""

import httpx
import pytest

from krim_agent.api import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.domain import CaseSpec, DatasetRef, MetricSpec, TaskSpec
from krim_agent.evaluation.product_types import ProductComparisonRequest, ProductDatasetSpec
from krim_agent.evaluation.repository import Repository
from krim_agent.sdk import WorkspaceEvaluationSDK
from krim_agent.storage.sqlite import SQLiteStore


async def test_product_application_executes_real_krim_versions(tmp_path):
    path = tmp_path / "application.sqlite"
    async with await open_application(path, settings=Settings(product_eval_enabled=True)) as app:
        assert app.product_evaluations is not None and app.evaluation_product is not None
        sdk = WorkspaceEvaluationSDK(app.evaluation_product)
        targets = await sdk.targets()
        assert {row["key"] for row in targets} >= {
            "krim-script-v1",
            "krim-script-v2",
            "krim-script-bad",
        }
        frozen = await sdk.freeze_dataset(
            ProductDatasetSpec(
                name="decimal-check",
                version="v1",
                cases=[
                    CaseSpec(
                        case_id="decimal",
                        instruction="/add 2.5 3.5",
                        files={"INPUT.txt": "two decimals"},
                        verifier="krim-script/1",
                        reference={"expected": "6"},
                    )
                ],
            )
        )
        reports = []
        started = []
        for target in ["krim-script-v2", "krim-script-v1", "krim-script-bad"]:
            metrics = [
                MetricSpec.model_validate(row) for row in await sdk.metrics(target, mode="manual")
            ]
            view = await sdk.start_task(
                TaskSpec(
                    request_key="real-" + target,
                    dataset=DatasetRef(
                        name=frozen["name"],
                        version=frozen["version"],
                        fingerprint=frozen["fingerprint"],
                    ),
                    target_key=target,
                    metrics=metrics,
                )
            )
            assert (await app.product_evaluations.kernel.wait(view["id"]))["status"] == "completed"
            completed = await sdk.task(view["id"])
            assert completed["finished_count"] == completed["planned_count"] == 1
            assert completed["report_hash"]
            started.append(completed)
            reports.append(await sdk.report_page(view["id"]))
        assert reports[0]["rows"][0]["quality"] == "passed"
        assert reports[1]["rows"][0]["quality"] == "failed"
        assert len(app.service.tasks) >= 2
        comparison = await sdk.compare(
            ProductComparisonRequest(
                baseline_id=started[0]["id"],
                candidate_id=started[1]["id"],
                baseline_hash=reports[0]["report_hash"],
                candidate_hash=reports[1]["report_hash"],
            )
        )
        assert comparison["status"] == "invalid"
        approved = await sdk.compare(
            ProductComparisonRequest(
                baseline_id=started[0]["id"],
                candidate_id=started[1]["id"],
                baseline_hash=reports[0]["report_hash"],
                candidate_hash=reports[1]["report_hash"],
                allowed_subjects=[
                    reports[0]["subject_fingerprint"],
                    reports[1]["subject_fingerprint"],
                ],
            )
        )
        assert approved["status"] == "fail"
        changed_tools = await sdk.compare(
            ProductComparisonRequest(
                baseline_id=started[0]["id"],
                candidate_id=started[2]["id"],
                baseline_hash=reports[0]["report_hash"],
                candidate_hash=reports[2]["report_hash"],
                allowed_subjects=[
                    reports[0]["subject_fingerprint"],
                    reports[2]["subject_fingerprint"],
                ],
            )
        )
        assert changed_tools["status"] == "invalid"
        assert changed_tools["reasons"][0]["code"] == "incompatible_conditions"
        product_store = app.product_evaluations.repository
    assert app.store.closed and product_store.closed
    reader = await Repository.open(path.with_name("application.eval.sqlite"))
    try:
        assert reader.healthy
    finally:
        await reader.close()
    second = await SQLiteStore.open(path)
    await second.close()


async def test_http_host_enables_product_surface_by_default(tmp_path):
    api = create_app(Settings(database=tmp_path / "http.sqlite"))
    async with api.router.lifespan_context(api):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://localhost"
        ) as client:
            health = await client.get("/healthz")
            assert health.json()["product_eval_enabled"] is True
            response = await client.get("/v4/workspaces/local/eval/targets")
            assert response.status_code == 200
            assert any(row["key"] == "krim-script-v2" for row in response.json())
    assert api.state.application.store.closed
    assert api.state.application.product_evaluations.repository.closed


async def test_product_database_cannot_alias_run_database(tmp_path):
    path = tmp_path / "same.sqlite"
    with pytest.raises(ServiceError, match="invalid_input"):
        await open_application(
            path, settings=Settings(product_eval_enabled=True, product_eval_database=path)
        )
    assert not path.exists()
