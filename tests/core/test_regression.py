"""真实版本报告与纯Gate，不用标签伪造评分。"""

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import fingerprint
from krim_agent.evaluation.regression import gate_reports, summarize
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule


def selection(case_id, text, expected):
    return {
        "case_id": case_id,
        "source_kind": "manual",
        "input": text,
        "reason": "人工核算加法",
        "reference": {
            "expected": expected,
            "expected_status": "completed",
            "expected_code": None,
            "expected_tools": [
                {"name": "add", "arguments": None, "executor_started": True, "status": "ok"}
            ],
            "trajectory": {"exact_steps": 2, "tool_names": ["add"]},
            "expected_schema": None,
        },
    }


async def reports(app):
    service = app.evaluations
    revision = await service.freeze(
        FreezeRequest.model_validate(
            {
                "name": "arithmetic",
                "version": "1",
                "cases": [
                    selection("integer", "/add 2 3", "5"),
                    selection("fraction_a", "/add 2.5 0.5", "3"),
                    selection("fraction_b", "/add 1.5 1.5", "3"),
                ],
            }
        )
    )
    values = {}
    for target in ["target-v1", "target-v2", "target-bad"]:
        request = EvalRequest.model_validate(
            {
                "request_key": target,
                "dataset": {k: getattr(revision, k) for k in ["name", "version", "fingerprint"]},
                "target_key": target,
                "rules": [
                    rule(n).model_dump(mode="json")
                    for n in ["status_code", "exact_match", "tool_calls", "trajectory"]
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
        before = app.service.bindings[target].loop.provider.calls
        view = await service.start(request)
        assert (await service.wait(view.id)).status == "completed"
        assert (await service.start(request)).id == view.id
        report = await service.report(view.id)
        assert app.service.bindings[target].loop.provider.calls - before == 6
        assert len({i["run_id"] for i in report["items"]}) == 3
        assert all(i["run_id"] != i["source_run_id"] for i in report["items"])
        assert all(i["output"]["tools"][0]["executor_started"] for i in report["items"])
        values[target] = report
    return values


@pytest.mark.asyncio
async def test_gate_pass_fail_invalid(tmp_path, monkeypatch):
    async with await open_application(tmp_path / "gate.sqlite") as app:
        data = await reports(app)
        assert gate_reports(data["target-v1"], data["target-v2"])["status"] == "pass"
        assert gate_reports(data["target-v1"], data["target-bad"])["status"] == "fail"
        changed = dict(data["target-v2"])
        changed["complete"] = False
        changed["report_hash"] = fingerprint(
            {k: v for k, v in changed.items() if k != "report_hash"}
        )
        assert gate_reports(data["target-v1"], changed)["status"] == "invalid"
        # 不兼容证据由真正的新批次生成，不改标签或预造分数。
        revision = await app.store.dataset("arithmetic", "1")

        async def actual_batch(key, dataset, names, repetitions=1, service=app.evaluations):
            request = EvalRequest.model_validate(
                {
                    "request_key": key,
                    "dataset": {k: getattr(dataset, k) for k in ["name", "version", "fingerprint"]},
                    "target_key": "target-v2",
                    "rules": [rule(n).model_dump(mode="json") for n in names],
                    "execution": {
                        "repetitions": repetitions,
                        "concurrency": 1,
                        "item_timeout_seconds": 12,
                        "scorer_timeout_seconds": 1,
                        "batch_timeout_seconds": 600,
                    },
                }
            )
            started = await service.start(request)
            assert (await service.wait(started.id)).status == "completed"
            return await service.report(started.id)

        names = [r["name"] for r in data["target-v2"]["rules"]]
        changed_rules = await actual_batch("different-rules", revision, ["status_code"])
        changed_repeats = await actual_batch("different-repeats", revision, names, 2)
        next_body = revision.selection().model_dump(mode="json") | {"version": "2"}
        next_revision = await app.evaluations.freeze(FreezeRequest.model_validate(next_body))
        changed_data = await actual_batch("different-dataset", next_revision, names)
        for other in [changed_rules, changed_repeats, changed_data]:
            assert other["complete"] is True
            assert gate_reports(data["target-v2"], other)["status"] == "invalid"
        null_case = selection("null", "/echo null", None)
        null_revision = await app.evaluations.freeze(
            FreezeRequest.model_validate(
                {
                    "name": "null-reference",
                    "version": "1",
                    "cases": [null_case],
                }
            )
        )
        from krim_agent.evaluation import scorers

        original = scorers.score_rule

        async def fail_required(rule, *values):
            if rule.name == "status_code":
                raise ValueError("synthetic required scorer fault")
            return await original(rule, *values)

        # status_code不可省；用真实评分task失败制造零覆盖，而非绕过契约。
        with monkeypatch.context() as patch:
            patch.setattr(scorers, "score_rule", fail_required)
            zero = await actual_batch(
                "zero-coverage", null_revision, ["status_code", "exact_match"]
            )
        assert zero["summary"]["scored_cells"] == 0 and zero["summary"]["mean"] is None
        assert gate_reports(zero, zero)["status"] == "invalid"
        from krim_agent.config import Settings

        async with await open_application(
            tmp_path / "limits.sqlite", settings=Settings(max_steps=3)
        ) as limited:
            same_revision = await limited.evaluations.freeze(revision.selection())
            assert same_revision.fingerprint == revision.fingerprint
            changed_limits = await actual_batch(
                "different-limits", same_revision, names, service=limited.evaluations
            )
        assert changed_limits["complete"] is True
        assert gate_reports(data["target-v2"], changed_limits)["status"] == "invalid"


@pytest.mark.asyncio
async def test_mean_gain_does_not_hide_case_regression(tmp_path):
    async with await open_application(tmp_path / "mean.sqlite") as app:
        data = await reports(app)
        assert data["target-bad"]["summary"]["mean"] > data["target-v1"]["summary"]["mean"]
        result = gate_reports(data["target-v1"], data["target-bad"])
        assert any(
            r["code"] == "passed_to_nonpassed" and r["case_id"] == "integer"
            for r in result["reasons"]
        )


@pytest.mark.asyncio
async def test_summary_recomputed_from_planned_items(tmp_path):
    async with await open_application(tmp_path / "summary.sqlite") as app:
        data = await reports(app)
        report = data["target-v2"]
        summary = summarize(report["items"], report["rules"])
        assert summary == report["summary"]
        assert summary["token_usage"] is None and summary["usage_complete_count"] == 0
        assert summary["planned"] == 3 and summary["scored_cells"] == 12
        wrong = dict(report)
        wrong["summary"] = dict(summary, passed=999)
        wrong["report_hash"] = fingerprint({k: v for k, v in wrong.items() if k != "report_hash"})
        assert gate_reports(report, wrong)["status"] == "invalid"
