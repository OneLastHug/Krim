"""Pure report regression gates over explicitly seeded history fixtures."""

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import fingerprint
from krim_agent.evaluation.regression import gate_reports, summarize
from krim_agent.evaluation.reports import regression_reasons


def selection(case_id: str, text: str, expected: str) -> dict:
    return {
        "case_id": case_id,
        "source_kind": "manual",
        "input": text,
        "reason": "explicit reference fixture",
        "reference": {
            "expected": expected,
            "expected_status": "completed",
            "expected_code": None,
            "expected_tools": None,
            "trajectory": None,
            "expected_schema": None,
        },
    }


@pytest.mark.asyncio
async def test_gate_pass_fail_and_invalid_from_stored_report_fixtures(tmp_path):
    from test_eval_storage import seed_legacy_report

    async with await open_application(tmp_path / "gate.sqlite") as app:
        _request, baseline_id = await seed_legacy_report(app, "baseline-score", score_value=1)
        _request, pass_id = await seed_legacy_report(app, "same-score", score_value=1)
        _request, fail_id = await seed_legacy_report(app, "regressed-score", score_value=0)
        baseline = await app.evaluations.report(baseline_id)
        passed = await app.evaluations.report(pass_id)
        failed = await app.evaluations.report(fail_id)

        assert gate_reports(baseline, passed)["status"] == "pass"
        decision = gate_reports(baseline, failed)
        assert decision["status"] == "fail"
        assert any(reason["code"] == "passed_to_nonpassed" for reason in decision["reasons"])

        incomplete = dict(baseline)
        incomplete["complete"] = False
        incomplete["report_hash"] = fingerprint(
            {key: value for key, value in incomplete.items() if key != "report_hash"}
        )
        assert gate_reports(baseline, incomplete)["status"] == "invalid"

        assert summarize(baseline["items"], baseline["rules"]) == baseline["summary"]
        assert baseline["summary"]["scored_cells"] == 1
        assert baseline["items"][0]["output"] is None



def test_case_regression_is_not_hidden_by_candidate_mean():
    before = {
        "schema_version": 1,
        "items": [
            {"case_id": "stable", "repetition": 0, "quality": "passed",
             "run_id": "baseline-stable", "scores": [{"rule": {"name": "status_code"},
             "status": "scored", "value": 1}]},
            {"case_id": "fragile", "repetition": 0, "quality": "passed",
             "run_id": "baseline-fragile", "scores": [{"rule": {"name": "status_code"},
             "status": "scored", "value": 1}]},
        ],
    }
    after = {
        "schema_version": 1,
        "items": [
            {"case_id": "stable", "repetition": 0, "quality": "passed",
             "run_id": "candidate-stable", "scores": [{"rule": {"name": "status_code"},
             "status": "scored", "value": 1}]},
            {"case_id": "fragile", "repetition": 0, "quality": "failed",
             "run_id": "candidate-fragile", "scores": [{"rule": {"name": "status_code"},
             "status": "scored", "value": 0}]},
        ],
        "summary": {"pass_rate": 0.5},
    }
    reasons = regression_reasons(before, after)
    assert any(
        reason["code"] == "passed_to_nonpassed" and reason["case_id"] == "fragile"
        for reason in reasons
    )
    assert any(reason["code"] == "candidate_not_all_passed" for reason in reasons)
