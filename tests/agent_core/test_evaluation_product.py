"""工作区评测产品接缝：隔离、幂等、分页与真实ASGI SDK调用。"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import event, select

from krim_agent.api.evaluations import mount_evaluations
from krim_agent.client import AsyncRunClient
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.app import EvalApplication
from krim_agent.evaluation.domain import (
    Budget,
    CaseSpec,
    CleanupResult,
    DatasetRef,
    ExecutionRef,
    Observation,
    PreparedTrial,
    TaskSpec,
    TrialSpec,
)
from krim_agent.evaluation.product import EvaluationProduct
from krim_agent.evaluation.product_types import (
    ProductComparisonRequest,
    ProductDatasetSpec,
)
from krim_agent.evaluation.repository import Repository, tasks
from krim_agent.sdk.evaluations import EvaluationSDK
from krim_agent.security import Membership, PrincipalContext


class FakeAdapter:
    def __init__(
        self,
        target_key: str = "fake-target",
        execution_schema: str = "local-process/1",
    ) -> None:
        self.target_key = target_key
        self.execution_schema = execution_schema
        self.execute_calls = 0
        self.started = asyncio.Event()

    def descriptor(self) -> dict[str, Any]:
        kind = {
            "native-cli/1": "native-cli",
            "krim-run/1": "krim-run",
        }.get(self.execution_schema, "local-process")
        return {
            "key": self.target_key,
            "version": self.target_key + "/1",
            "kind": kind,
            "execution_conditions": {
                "schema_version": self.execution_schema,
                "python": {
                    "executable": "/fake/python",
                    "realpath": "/fake/python",
                    "sha256": "0" * 64,
                    "prefix": "/fake",
                    "base_prefix": "/fake",
                },
                "env": {"PATH": None, "LANG": None, "LD_LIBRARY_PATH": None},
            },
        }

    def assert_current(self, frozen: dict[str, Any]) -> None:
        if frozen != self.descriptor():
            raise ServiceError("target_binding_changed")

    async def prepare(self, trial: TrialSpec) -> PreparedTrial:
        return PreparedTrial(trial)

    async def execute(self, prepared: PreparedTrial) -> ExecutionRef:
        self.execute_calls += 1
        self.started.set()
        return ExecutionRef(f"fake-exec-{self.execute_calls}")

    async def collect(self, ref: ExecutionRef) -> dict[str, Any]:
        return Observation(
            execution_ref=ref.id,
            exit_code=0,
            stdout=None,
            stderr=None,
            artifacts={},
            process={},
            cleanup="closed",
        ).model_dump(mode="json")

    async def cancel(self, ref: ExecutionRef) -> CleanupResult:
        return CleanupResult(True)

    async def close(self) -> CleanupResult:
        return CleanupResult(True)


async def harness(tmp_path: Path) -> tuple[EvalApplication, EvaluationProduct, FakeAdapter]:
    repository = await Repository.open(tmp_path / "product-eval.sqlite")
    adapter = FakeAdapter()
    repository.register("fake-target", adapter)
    repository.register("fake-krim", FakeAdapter("fake-krim", "krim-run/1"))
    evaluation = EvalApplication(repository, adapter)
    # 目录、数据集和任务测试不导入来源Run；生产装配传入宿主持有的真实服务。
    product = EvaluationProduct(evaluation, cast(Any, None), cast(Any, None))
    return evaluation, product, adapter


def principal(workspace: str, *, role: str = "admin", identity: str = "alice") -> PrincipalContext:
    return PrincipalContext(identity, 1, (Membership(workspace_id=workspace, role=role),))


def dataset_request(name: str, instruction: str) -> ProductDatasetSpec:
    return ProductDatasetSpec(
        name=name,
        version="v1",
        cases=[
            CaseSpec(
                case_id="case1",
                instruction=instruction,
                files={"INPUT.txt": instruction},
            )
        ],
    )


def task_body(
    product: EvaluationProduct,
    dataset: dict[str, Any],
    key: str,
    *,
    forged_scope: str = "client-scope",
) -> dict[str, Any]:
    return TaskSpec(
        scope=forged_scope,
        request_key=key,
        dataset=DatasetRef(
            name=dataset["name"],
            version=dataset["version"],
            fingerprint=dataset["fingerprint"],
        ),
        target_key="fake-target",
        metrics=product.evaluation.repository.metric_registry.specs(),
        execution=Budget(),
    ).model_dump(mode="json")


@pytest.mark.asyncio
async def test_same_dataset_names_are_workspace_isolated_and_guessed_task_ids_are_hidden(tmp_path):
    evaluation, product, _adapter = await harness(tmp_path)
    try:
        alpha, beta = principal("alpha"), principal("beta", identity="bob")
        first = await product.freeze_dataset(alpha, "alpha", dataset_request("suite", "alpha data"))
        second = await product.freeze_dataset(beta, "beta", dataset_request("suite", "beta data"))

        assert first["name"] == second["name"] == "suite"
        assert first["fingerprint"] != second["fingerprint"]
        assert (
            await product.dataset(alpha, "alpha", "suite", "v1")
        )["cases"][0]["instruction"] == "alpha data"
        assert (
            await product.dataset(beta, "beta", "suite", "v1")
        )["cases"][0]["instruction"] == "beta data"

        task = await product.start_task(
            alpha, "alpha", task_body(product, first, "isolated-task")
        )
        assert task["dataset"]["name"] == "suite"
        assert task["kind"] == "manual"
        with pytest.raises(ServiceError) as guessed:
            await product.task(beta, "beta", task["id"])
        assert guessed.value.code == "not_found"
        with pytest.raises(ServiceError) as by_key:
            await product.task_by_key(beta, "beta", "isolated-task")
        assert by_key.value.code == "not_found"

        stored = await product._task(alpha, "alpha", task["id"])
        assert stored["request_key"] == "isolated-task"
        async with evaluation.repository.engine.begin() as conn:
            scopes = (
                await conn.execute(
                    select(
                        tasks.c.scope,
                        tasks.c.body["request_json"]["scope"].as_string(),
                    ).where(tasks.c.id == task["id"])
                )
            ).one()
        assert tuple(scopes) == (product._scope("alpha"), product._scope("alpha"))
        assert product._scope("alpha") != product._scope("beta")
    finally:
        await evaluation.close()


@pytest.mark.asyncio
async def test_task_retry_uses_original_key_and_lists_are_bounded_pages(tmp_path):
    evaluation, product, adapter = await harness(tmp_path)
    try:
        actor = principal("research")
        for index in range(3):
            await product.freeze_dataset(
                actor,
                "research",
                dataset_request(f"set{index}", f"instruction {index}"),
            )
        first_dataset = await product.dataset(actor, "research", "set0", "v1")
        body = task_body(product, first_dataset, "same-original-key")
        first = await product.start_task(actor, "research", body)
        second = await product.start_task(actor, "research", body)
        assert first["id"] == second["id"]
        await evaluation.kernel.wait(first["id"])
        assert adapter.execute_calls == 1

        dataset_page = await product.list_datasets(actor, "research", offset=1, limit=1)
        assert len(dataset_page) == 1 and dataset_page[0]["name"] == "set1"
        for index in range(2):
            dataset = await product.dataset(actor, "research", f"set{index + 1}", "v1")
            task = await product.start_task(
                actor,
                "research",
                task_body(product, dataset, f"task-{index}"),
            )
            await evaluation.kernel.wait(task["id"])
        task_page = await product.list_tasks(actor, "research", offset=1, limit=2)
        assert [item["request_key"] for item in task_page] == ["task-0", "task-1"]
    finally:
        await evaluation.close()


@pytest.mark.asyncio
async def test_viewer_cannot_cancel_workspace_task(tmp_path):
    evaluation, product, _adapter = await harness(tmp_path)
    try:
        owner = principal("team")
        dataset = await product.freeze_dataset(owner, "team", dataset_request("suite", "work"))
        task = await product.start_task(
            owner,
            "team",
            task_body(product, dataset, "task-to-cancel"),
        )
        viewer = principal("team", role="viewer", identity="read-only")
        with pytest.raises(ServiceError) as denied:
            await product.cancel_task(viewer, "team", task["id"])
        assert denied.value.code == "permission_denied"
        # 清理由宿主Kernel执行，测试不把viewer请求传入取消路径。
        await evaluation.kernel.cancel(task["id"])
    finally:
        await evaluation.close()


@pytest.mark.asyncio
async def test_manual_dataset_runs_reports_and_compares(tmp_path):
    evaluation, product, _adapter = await harness(tmp_path)
    try:
        actor = principal("manual-workspace")
        dataset = await product.freeze_dataset(
            actor,
            "manual-workspace",
            ProductDatasetSpec(
                name="manual-suite",
                version="v1",
                cases=[
                    CaseSpec(
                        case_id="basic",
                        instruction="produce the expected result",
                        files={"INPUT.txt": "produce the expected result"},
                        reference={"files": {}, "exit_code": 0},
                    )
                ],
            ),
        )
        started = []
        reports = []
        for key in ["manual-before", "manual-after"]:
            task = await product.start_task(
                actor,
                "manual-workspace",
                task_body(product, dataset, key),
            )
            completed = await evaluation.kernel.wait(task["id"])
            assert completed["status"] == "completed"
            started.append(task)
            reports.append(
                await product.report_page(actor, "manual-workspace", task["id"])
            )

        assert reports[0]["kind"] == "manual"
        assert reports[0]["execution"]["complete"] is True
        assert reports[0]["coverage"]["cell_coverage"] == 1
        comparison = await product.compare(
            actor,
            "manual-workspace",
            ProductComparisonRequest(
                baseline_id=started[0]["id"],
                candidate_id=started[1]["id"],
                baseline_hash=reports[0]["report_hash"],
                candidate_hash=reports[1]["report_hash"],
            ),
        )
        assert comparison["status"] == "pass"
        assert comparison["policy"] == "strict-regression/1"
    finally:
        await evaluation.close()


@pytest.mark.asyncio
async def test_sdk_calls_mounted_asgi_routes(tmp_path):
    evaluation, product, _adapter = await harness(tmp_path)
    app = FastAPI()
    actor = principal("api-workspace")

    @app.middleware("http")
    async def inject_principal(request: Request, call_next):
        request.state.principal = actor
        return await call_next(request)

    @app.exception_handler(ServiceError)
    async def service_error(_request: Request, error: ServiceError) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": error.code, "message": "request failed"}},
            status_code=error.status,
        )

    mount_evaluations(app, lambda _request: product)
    client = AsyncRunClient("http://asgi.test")
    await client.close()
    client.http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://asgi.test",
    )
    sdk = EvaluationSDK(client, principal=actor, workspace="api-workspace")
    try:
        targets = await sdk.targets()
        assert {value["key"] for value in targets} == {"fake-target", "fake-krim"}
        reference_metrics = await sdk.metrics("fake-target", mode="manual")
        krim_metrics = await sdk.metrics("fake-krim", mode="manual")
        continuous_metrics = await sdk.metrics("fake-target")
        assert {value["name"] for value in reference_metrics} == {"artifact_match", "exit_code"}
        assert {value["name"] for value in krim_metrics} == {"krim_result"}
        assert {value["name"] for value in continuous_metrics} >= {
            "artifact_match",
            "exit_code",
            "fact_integrity",
            "context_coverage",
        }
        assert "krim_result" not in {value["name"] for value in continuous_metrics}
        assert "process_constraints" not in {value["name"] for value in continuous_metrics}
        process_metrics = await sdk.metrics("fake-target", include_process=True)
        assert "process_constraints" in {value["name"] for value in process_metrics}

        frozen = await sdk.freeze_dataset(dataset_request("remote-suite", "through ASGI"))
        listing = await sdk.list_datasets()
        assert listing[0]["name"] == "remote-suite"
        fetched = await sdk.dataset("remote-suite", "v1")
        assert fetched["fingerprint"] == frozen["fingerprint"]

        body = task_body(product, frozen, "sdk-original-key")
        created = await sdk.start_task(body)
        queried = await sdk.task_by_key("sdk-original-key")
        assert queried["id"] == created["id"]
        assert (await sdk.list_tasks())[0]["id"] == created["id"]
    finally:
        await client.close()
        await evaluation.close()


@pytest.mark.asyncio
async def test_product_direct_reads_reject_after_close_without_reconnecting(tmp_path):
    evaluation, product, _adapter = await harness(tmp_path)
    owner = principal("closed-workspace")
    dataset = await product.freeze_dataset(
        owner, "closed-workspace", dataset_request("closed-suite", "work")
    )
    task = await product.start_task(
        owner, "closed-workspace", task_body(product, dataset, "closed-task")
    )
    await evaluation.kernel.wait(task["id"])
    await evaluation.close()

    repository = evaluation.repository
    pool = repository.engine.pool
    checkouts = []

    def checkout(*_args: Any) -> None:
        checkouts.append(True)

    event.listen(pool, "checkout", checkout)

    async def assert_closed(operation: Any) -> None:
        with pytest.raises(ServiceError) as closed:
            await operation()
        assert closed.value.code == "closed"

    try:
        await assert_closed(lambda: product.list_datasets(owner, "closed-workspace"))
        await assert_closed(lambda: product.task(owner, "closed-workspace", task["id"]))
        await assert_closed(
            lambda: product.task_by_key(owner, "closed-workspace", "closed-task")
        )
        await assert_closed(lambda: product.list_tasks(owner, "closed-workspace"))
        await assert_closed(
            lambda: product.dataset(owner, "closed-workspace", "closed-suite", "v1")
        )
        assert checkouts == []
    finally:
        event.remove(pool, "checkout", checkout)
