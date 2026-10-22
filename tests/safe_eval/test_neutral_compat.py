"""真实旧API/SDK、本机Provider协议与v1持久事实；全部进入共享kernel。"""

import hashlib
import json
import sys
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "core"))
from fixtures.http_provider_server import HTTPFixture

from krim_agent.api import create_app
from krim_agent.client import AsyncRunClient
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.evaluation.kernel import EvalKernel
from krim_agent.evaluation.reports import validate_report
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.storage.sqlite import SQLiteStore


def manual(name="api"):
    return FreezeRequest.model_validate(
        {
            "name": name,
            "version": "1",
            "cases": [
                {
                    "case_id": "sum",
                    "source_kind": "manual",
                    "input": "/add 2 3",
                    "reason": "固定独立参考",
                    "reference": {
                        "expected": "5",
                        "expected_status": "completed",
                        "expected_code": None,
                        "expected_tools": None,
                        "trajectory": None,
                        "expected_schema": None,
                    },
                }
            ],
        }
    )


def request_for(dataset, target="target-v2", key="compat"):
    return EvalRequest.model_validate(
        {
            "request_key": key,
            "dataset": {k: getattr(dataset, k) for k in ["name", "version", "fingerprint"]},
            "target_key": target,
            "rules": [
                rule("status_code").model_dump(mode="json"),
                rule("exact_match").model_dump(mode="json"),
            ],
            "execution": {
                "repetitions": 1,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        }
    )


@pytest.mark.parametrize("target", ["target-v2", "target-http"])
async def test_real_legacy_http_sdk_cold_identity_and_timeouts(tmp_path, target):
    database = tmp_path / "compat.sqlite"
    async with HTTPFixture(expected_key="SYNTHETIC_SOL") as provider_fixture:
        settings = Settings(database=database, api_key=SecretStr("SYNTHETIC_API"))
        kwargs = {}
        if target == "target-http":
            settings = Settings(
                database=database,
                api_key=SecretStr("SYNTHETIC_API"),
                provider="http",
                max_active_runs=1,
                run_timeout=180,
            )
            kwargs["http_config"] = OpenAICompatibleConfig(
                base_url=provider_fixture.url,
                model="gpt-6.1-sol",
                api_key="SYNTHETIC_SOL",
                reasoning_effort="xhigh",
                read_timeout_seconds=60,
            )
        app = await open_application(database, settings=settings, **kwargs)
        try:
            assert isinstance(app.evaluations.kernel, EvalKernel)
            api = create_app(application=app)
            async with api.router.lifespan_context(api):
                client = AsyncRunClient("http://localhost", "SYNTHETIC_API")
                await client.http.aclose()
                client.http = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=api),
                    base_url="http://localhost",
                    headers={"Authorization": "Bearer SYNTHETIC_API"},
                    trust_env=False,
                )
                try:
                    dataset = await client.freeze(manual())
                    assert await client.freeze(manual()) == dataset
                    request = request_for(dataset, target)
                    assert request.execution.model_dump() == {
                        "repetitions": 1,
                        "concurrency": 1,
                        "item_timeout_seconds": 12,
                        "scorer_timeout_seconds": 1,
                        "batch_timeout_seconds": 600,
                    }
                    view = await client.eval_start(request)
                    result = await client.eval_wait(view.id)
                    assert result.status == "completed"
                    assert (await client.eval_start(request)).id == view.id
                    assert (await client.eval_by_key("compat")).id == view.id
                    with pytest.raises(ServiceError, match="key_conflict"):
                        await client.eval_start(
                            request.model_copy(update={"target_key": "target-v1"})
                        )
                    report = await client.report(view.id)
                    assert report["items"][0]["output"]["text"] == "5"
                    assert report["summary"]["pass_rate"] == 1
                    assert validate_report(report) == report
                    assert report["report_hash"] == fingerprint(
                        {k: v for k, v in report.items() if k != "report_hash"}
                    )
                    snapshot = await app.store.eval_snapshot(view.id)
                    if target == "target-http":
                        assert snapshot["execution"]["effective_timeouts"] == {
                            "schema_version": 1,
                            "source": "trusted-http-host/1",
                            "item_timeout_seconds": 210,
                            "batch_timeout_seconds": 600,
                        }
                        assert len(provider_fixture.requests) == 2
                    else:
                        assert "effective_timeouts" not in snapshot["execution"]
                        assert app.service.bindings[target].loop.provider.calls == 2
                    (tmp_path / "compat-http.json").write_text(
                        json.dumps(
                            {
                                "request": request.model_dump(mode="json"),
                                "response": result.model_dump(mode="json"),
                                "report": report,
                                "effective_execution": snapshot["execution"],
                                "provider_requests": provider_fixture.requests,
                            },
                            indent=2,
                        )
                    )
                finally:
                    await client.close()
        finally:
            await app.close()
        assert app.store.closed and all(t.done() for t in app.evaluations.tasks.values())
        assert all(t.done() for t in app.service.tasks.values())
        if target == "target-http":
            assert (
                app.provider.closed
                and app.provider.active == 0
                and app.provider.streams_closed == 2
            )
        before = hashlib.sha256(database.read_bytes()).hexdigest()
        reader = await SQLiteStore.open(database, readonly=True)
        try:
            cold = await reader.eval_by_key("compat")
            assert cold["id"] == view.id and cold["report"] == report
            assert cold["execution"] == snapshot["execution"]
            assert cold["dataset"] == snapshot["dataset"]
        finally:
            await reader.close()
        assert hashlib.sha256(database.read_bytes()).hexdigest() == before
        assert len(provider_fixture.requests) == (2 if target == "target-http" else 0)


async def test_legacy_neutral_conditions_missing_is_readable_but_not_comparable(tmp_path):
    """兼容旧中立格式向量；不将补出的当前配置当作历史执行条件。"""
    import copy

    from test_neutral_eval import requested

    from krim_agent.evaluation.app import open_eval
    from krim_agent.evaluation.domain import neutral_fingerprint
    from krim_agent.evaluation.reports import gate_reports, validate_neutral_report

    async with await open_eval(tmp_path / "legacy-neutral.sqlite") as app:
        view = await app.run(await requested(app))
        await app.kernel.wait(view["id"])
        current = await app.repository.report(view["id"])
    legacy = copy.deepcopy(current)
    legacy["environment"].pop("execution_conditions")
    legacy["target_descriptor"].pop("execution_conditions")
    legacy.pop("capacity", None)
    legacy["target_fingerprint"] = neutral_fingerprint(legacy["target_descriptor"])
    legacy["report_hash"] = neutral_fingerprint(
        {k: v for k, v in legacy.items() if k != "report_hash"}
    )
    archived = tmp_path / "legacy-neutral-report.json"
    archived.write_text(json.dumps(legacy))
    before = hashlib.sha256(archived.read_bytes()).hexdigest()
    loaded = json.loads(archived.read_text())
    assert validate_neutral_report(loaded) == legacy
    result = gate_reports(loaded, loaded)
    assert result["status"] == "invalid"
    assert result["reasons"] == [{"code": "execution_conditions_missing"}]
    assert "execution_conditions" not in loaded["environment"]
    assert hashlib.sha256(archived.read_bytes()).hexdigest() == before


def test_neutral_json_keeps_legacy_small_canonical_and_hash():
    from krim_agent.contracts import encoded, parse_json
    from krim_agent.evaluation.domain import clone, neutral_encoded, neutral_fingerprint

    for value in [None, True, {"中文": [1, 1.0, False, None, "\u0000"]}]:
        assert neutral_encoded(value) == encoded(value)
        assert neutral_fingerprint(value) == fingerprint(value)
        assert clone(value) == parse_json(encoded(value))
