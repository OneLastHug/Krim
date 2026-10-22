"""原始item汇总与只读Gate；SRC-M2-05，逐样本退化不可被均值掩盖。"""

from __future__ import annotations

from fractions import Fraction
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.contracts import ServiceError, check_json, fingerprint

from .domain import REPORT_BYTES, execution_conditions, neutral_encoded, neutral_fingerprint
from .metrics import metric_identity
from .types import DatasetRevision, EvalOutput, Execution, RuleSpec, Score


def summarize(
    items: list[dict[str, Any]], rules: list[dict[str, Any]], *, neutral: bool = False
) -> dict[
    str,
    JsonValue,
]:
    n, m = len(items), len(rules)
    if not 1 <= n <= 24 or not 1 <= m <= 6:
        raise ServiceError("facts_corrupt")
    metrics: dict[str, JsonValue] = {}
    scored = 0
    total = 0
    skipped = 0
    errors = 0
    complete_items = 0
    usages = []
    for rule in rules:
        scores = []
        for item in items:
            values = item.get("scores")
            if values is not None:
                if len(values) != m:
                    raise ServiceError("facts_corrupt")
                matches = [
                    Score.model_validate(s) for s in values if s["rule"]["name"] == rule["name"]
                ]
                if len(matches) != 1:
                    raise ServiceError("facts_corrupt")
                score = matches[0]
                identity = (
                    metric_identity(rule)
                    if neutral
                    else {
                        "name": rule["name"],
                        "version": rule["version"],
                        "config_fingerprint": fingerprint(rule["config"]),
                    }
                )
                if score.rule != identity:
                    raise ServiceError("facts_corrupt")
                scores.append(score)
        known = [s.value for s in scores if s.status == "scored"]
        count = len(known)
        numerator = sum(cast(int, v) for v in known)
        scored += count
        total += numerator
        skipped += sum(s.status == "skipped" for s in scores)
        errors += sum(s.status == "error" for s in scores)
        metrics[rule["name"]] = {
            "scored": count,
            "planned": n,
            "coverage": float(Fraction(count, n)),
            "mean": float(Fraction(numerator, count)) if count else None,
        }
    for item in items:
        complete_items += int(
            item.get("scores") is not None and all(s["status"] == "scored" for s in item["scores"])
        )
        if item.get("output") is not None:
            raw = item["output"]
            output_data = raw if neutral else EvalOutput.model_validate(raw).model_dump(mode="json")
            if output_data.get("usage_complete") and output_data.get("token_usage") is not None:
                usages.append(output_data["token_usage"])
    passed = sum(i.get("quality") == "passed" for i in items)
    tokens: JsonValue = None
    if len(usages) == n:
        tokens = {
            key: sum(cast(int, u[key]) for u in usages)
            for key in ["input_tokens", "output_tokens", "total_tokens"]
        }
    return {
        "planned": n,
        "scored_cells": scored,
        "planned_cells": n * m,
        "cell_coverage": float(Fraction(scored, n * m)),
        "item_coverage": float(Fraction(complete_items, n)),
        "passed": passed,
        "pass_rate": float(Fraction(passed, n)),
        "mean": float(Fraction(total, scored)) if scored else None,
        "metrics": metrics,
        "skipped": skipped,
        "errors": errors,
        "unexecuted_cells": n * m - scored - skipped - errors,
        "usage_complete_count": len(usages),
        "token_usage": tokens,
    }


def make_report(snapshot: dict[str, Any]) -> dict[str, JsonValue]:
    dataset = DatasetRevision.model_validate(snapshot["dataset"])
    cases = {c.case_id: c for c in dataset.cases}
    items = []
    for item in snapshot["items"]:
        case = cases[item["case_id"]]
        items.append(
            {
                k: item[k]
                for k in [
                    "case_id",
                    "repetition",
                    "run_id",
                    "status",
                    "code",
                    "output",
                    "scores",
                    "quality",
                ]
            }
            | {"source_run_id": case.source.source_run_id}
        )
    report: dict[str, JsonValue] = {
        "schema_version": 1,
        "eval_id": snapshot["id"],
        "revision": snapshot["revision"],
        "complete": snapshot["status"] == "completed"
        and all(i["status"] == "finished" for i in items),
        "status": snapshot["status"],
        "code": snapshot["code"],
        "dataset": dataset.model_dump(mode="json"),
        "dataset_ref": {
            "name": dataset.name,
            "version": dataset.version,
            "fingerprint": dataset.fingerprint,
        },
        "dataset_fingerprint": dataset.fingerprint,
        "target_descriptor": snapshot["target"],
        "target_fingerprint": fingerprint(snapshot["target"]),
        "rules": snapshot["rules"],
        "rules_fingerprint": fingerprint(snapshot["rules"]),
        "execution": snapshot["execution"],
        "execution_fingerprint": fingerprint(snapshot["execution"]),
        "planned_count": snapshot["planned_count"],
        "items": cast(JsonValue, items),
        "summary": summarize(items, snapshot["rules"]),
    }
    report["report_hash"] = fingerprint(report)
    check_json(report, max_bytes=1048576)
    return report


def validate_report(value: dict[str, Any]) -> dict[str, Any]:
    if value.get("schema_version") == "safe-eval/1":
        return validate_neutral_report(value)
    if (
        type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("report_hash")
        != fingerprint({k: v for k, v in value.items() if k != "report_hash"})
    ):
        raise ServiceError("facts_corrupt")
    dataset = DatasetRevision.model_validate(value["dataset"])
    rules = [RuleSpec.model_validate(r) for r in value["rules"]]
    if (
        value["dataset_ref"]
        != {"name": dataset.name, "version": dataset.version, "fingerprint": dataset.fingerprint}
        or value["dataset_fingerprint"] != dataset.fingerprint
    ):
        raise ServiceError("facts_corrupt")
    if (
        not 1 <= len(rules) <= 6
        or "status_code" not in {r.name for r in rules}
        or [r.name for r in rules] != sorted({r.name for r in rules})
    ):
        raise ServiceError("facts_corrupt")
    Execution.model_validate({k: value["execution"][k] for k in Execution.model_fields})
    for key, field in [
        ("target_fingerprint", "target_descriptor"),
        ("rules_fingerprint", "rules"),
        ("execution_fingerprint", "execution"),
    ]:
        if value[key] != fingerprint(value[field]):
            raise ServiceError("facts_corrupt")
    items = value["items"]
    expected = {
        (c.case_id, r) for c in dataset.cases for r in range(value["execution"]["repetitions"])
    }
    if (
        len(items) != len(expected)
        or {(i["case_id"], i["repetition"]) for i in items} != expected
        or value["planned_count"] != len(expected)
    ):
        raise ServiceError("facts_corrupt")
    runs = [i["run_id"] for i in items if i["run_id"] is not None]
    if len(runs) != len(set(runs)):
        raise ServiceError("facts_corrupt")
    from .scorers import classify

    for item in items:
        if item["output"] is not None:
            output = EvalOutput.model_validate(item["output"])
            if output.run_id != item["run_id"]:
                raise ServiceError("facts_corrupt")
        if item["scores"] is not None:
            scores = [Score.model_validate(s) for s in item["scores"]]
            quality = classify(scores, timed_out=item["quality"] == "timed_out")
            if quality != item["quality"]:
                raise ServiceError("facts_corrupt")
    if fingerprint(value["summary"]) != fingerprint(summarize(items, value["rules"])):
        raise ServiceError("facts_corrupt")
    if value["complete"] is not (
        value["status"] == "completed" and all(i["status"] == "finished" for i in items)
    ):
        raise ServiceError("facts_corrupt")
    return value


def gate_reports(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, JsonValue]:
    """纯读两个冻结报告；不访问Store/Provider/scorer。"""
    reasons: list[dict[str, JsonValue]] = []

    def invalid(reason: str) -> dict[str, JsonValue]:
        return {
            "status": "invalid",
            "policy": "strict-regression/1",
            "reasons": [{"code": reason}],
        }

    try:
        before, after = validate_report(baseline), validate_report(candidate)
        for report in [before, after]:
            if (
                not report["complete"]
                or report["status"] != "completed"
                or report["summary"]["cell_coverage"] != 1
            ):
                return invalid("incomplete_coverage")
        if before["schema_version"] != after["schema_version"]:
            return invalid("incompatible_format")
        if before["schema_version"] == "safe-eval/1":
            try:
                for report in [before, after]:
                    execution_conditions(report["environment"].get("execution_conditions"))
            except (ValueError, KeyError, TypeError):
                return invalid("execution_conditions_missing")
        native = False
        if before["schema_version"] == "safe-eval/1":
            first = before["environment"].get("execution_conditions", {})
            second = after["environment"].get("execution_conditions", {})
            native = (
                first.get(
                    "schema_version",
                )
                == "native-cli/1"
                or second.get("schema_version") == "native-cli/1"
            )
            if native:
                if first.get("schema_version") != second.get("schema_version"):
                    return invalid("incompatible_environment")
                for report, condition in [(before, first), (after, second)]:
                    if report["environment"].get("native_admission") is None:
                        return invalid("native_admission_missing")
                    _native_receipt(report)
                    if (
                        neutral_fingerprint(condition["subject"])
                        not in condition["common"]["allowed_subject_fingerprints"]
                    ):
                        return invalid("unapproved_native_subject")
                if first["common"] != second["common"]:
                    return invalid("incompatible_environment")
                if first["admission"] != second["admission"]:
                    return invalid("incompatible_native_admission")
                for field in ["initial_state", "evidence"]:
                    if before["environment"][field] != after["environment"][field]:
                        return invalid("incompatible_environment")
        for key in [
            "dataset_ref",
            "dataset_fingerprint",
            "rules_fingerprint",
            "execution_fingerprint",
        ] + (["environment"] if before["schema_version"] == "safe-eval/1" and not native else []):
            if fingerprint(before[key]) != fingerprint(after[key]):
                return invalid("incompatible_" + key)
    except (ValueError, TypeError, KeyError, ServiceError):
        return invalid("report_invalid")
    old = {(i["case_id"], i["repetition"]): i for i in before["items"]}
    for item in after["items"]:
        prior = old[(item["case_id"], item["repetition"])]
        if prior["quality"] == "passed" and item["quality"] != "passed":
            reasons.append(
                {
                    "code": "passed_to_nonpassed",
                    "case_id": item["case_id"],
                    "repetition": item["repetition"],
                    (
                        "baseline_execution_ref"
                        if before["schema_version"] == "safe-eval/1"
                        else "baseline_run_id"
                    ): prior.get(
                        "execution_ref",
                        prior.get("run_id"),
                    ),
                    (
                        "candidate_execution_ref"
                        if before["schema_version"] == "safe-eval/1"
                        else "candidate_run_id"
                    ): item.get(
                        "execution_ref",
                        item.get("run_id"),
                    ),
                }
            )
        previous = {s["rule"]["name"]: s for s in prior["scores"]}
        for score in item["scores"]:
            if score["value"] < previous[score["rule"]["name"]]["value"]:
                reasons.append(
                    {
                        "code": "score_regression",
                        "case_id": item["case_id"],
                        "repetition": item["repetition"],
                        "rule": score["rule"]["name"],
                        "before": previous[score["rule"]["name"]]["value"],
                        "after": score["value"],
                    }
                )
    if after["summary"]["pass_rate"] != 1:
        reasons.append({"code": "candidate_not_all_passed"})
    return {
        "status": "fail" if reasons else "pass",
        "policy": "strict-regression/1",
        "baseline": {
            "eval_id": before["eval_id"],
            "revision": before["revision"],
            "report_hash": before["report_hash"],
            "target_fingerprint": before["target_fingerprint"],
        },
        "candidate": {
            "eval_id": after["eval_id"],
            "revision": after["revision"],
            "report_hash": after["report_hash"],
            "target_fingerprint": after["target_fingerprint"],
        },
        "reasons": cast(JsonValue, reasons),
    }


def make_neutral_report(snapshot: dict[str, Any]) -> dict[str, Any]:
    report = {
        "schema_version": "safe-eval/1",
        "eval_id": snapshot["id"],
        "revision": snapshot["revision"],
        "status": snapshot["status"],
        "code": snapshot["code"],
        "complete": snapshot["status"] == "completed"
        and all(i["status"] == "finished" for i in snapshot["items"]),
        "dataset": snapshot["dataset"],
        "dataset_ref": snapshot["request_json"]["dataset"],
        "dataset_fingerprint": snapshot["request_json"]["dataset"]["fingerprint"],
        "target_descriptor": snapshot["target"],
        "target_fingerprint": neutral_fingerprint(snapshot["target"]),
        "rules": snapshot["rules"],
        "rules_fingerprint": neutral_fingerprint(snapshot["rules"]),
        "execution": snapshot["execution"],
        "execution_fingerprint": neutral_fingerprint(snapshot["execution"]),
        "environment": snapshot["environment"],
        "planned_count": snapshot["planned_count"],
        "items": snapshot["items"],
        "summary": summarize(
            snapshot["items"],
            snapshot["rules"],
            neutral=True,
        ),
        "cost": None,
        "capacity": snapshot.get("capacity"),
    }
    report["report_hash"] = neutral_fingerprint(report, max_bytes=REPORT_BYTES)
    neutral_encoded(report, max_bytes=REPORT_BYTES)
    return report


def validate_neutral_report(value: dict[str, Any]) -> dict[str, Any]:
    from .domain import Budget, DatasetSpec, MetricSpec

    if value.get("report_hash") != neutral_fingerprint(
        {k: v for k, v in value.items() if k != "report_hash"}, max_bytes=REPORT_BYTES
    ):
        raise ServiceError("facts_corrupt")
    neutral_encoded(value, max_bytes=REPORT_BYTES)
    conditions = value.get("environment", {}).get("execution_conditions")
    if conditions is not None:
        execution_conditions(conditions)
        if value["target_descriptor"].get("execution_conditions") != conditions:
            raise ServiceError("facts_corrupt")
    if (
        isinstance(conditions, dict)
        and conditions.get("schema_version") == "native-cli/1"
        and value.get("environment", {}).get("native_admission") is not None
    ):
        _native_receipt(value)
    dataset = DatasetSpec.model_validate(value["dataset"])
    rules = [MetricSpec.model_validate(r) for r in value["rules"]]
    budget = Budget.model_validate(value["execution"])
    if (
        dataset.ref().model_dump(mode="json") != value["dataset_ref"]
        or dataset.ref().fingerprint != value["dataset_fingerprint"]
    ):
        raise ServiceError("facts_corrupt")
    if not 1 <= len(rules) <= 6 or [r.name for r in rules] != sorted({r.name for r in rules}):
        raise ServiceError("facts_corrupt")
    expected = {(c.case_id, r) for c in dataset.cases for r in range(budget.repetitions)}
    items = value["items"]
    if (
        len(items) != len(expected)
        or {(i["case_id"], i["repetition"]) for i in items} != expected
        or value["planned_count"] != len(expected)
    ):
        raise ServiceError("facts_corrupt")
    refs = [i["execution_ref"] for i in items if i["execution_ref"] is not None]
    if len(refs) != len(set(refs)):
        raise ServiceError("facts_corrupt")
    from .metrics import classify

    for item in items:
        if item["output"] is not None and item["output"]["execution_ref"] != item["execution_ref"]:
            raise ServiceError("facts_corrupt")
        if item["scores"] is not None and item["quality"] != classify(
            [Score.model_validate(s) for s in item["scores"]],
            timed_out=item["quality"] == "timed_out",
        ):
            raise ServiceError("facts_corrupt")
    for key, field in [
        ("target_fingerprint", "target_descriptor"),
        ("rules_fingerprint", "rules"),
        (
            "execution_fingerprint",
            "execution",
        ),
    ]:
        if value[key] != neutral_fingerprint(value[field]):
            raise ServiceError("facts_corrupt")
    if value["summary"] != summarize(items, value["rules"], neutral=True):
        raise ServiceError("facts_corrupt")
    if value["complete"] is not (
        value["status"] == "completed" and all(i["status"] == "finished" for i in items)
    ):
        raise ServiceError("facts_corrupt")
    return value


def _native_receipt(report: dict[str, Any]) -> None:
    """只验已持久数据，不查当前profile或回填缺失历史。"""
    environment = report["environment"]
    if set(
        environment,
    ) != {"initial_state", "evidence", "execution_conditions", "native_admission"}:
        raise ServiceError("facts_corrupt")
    conditions = environment["execution_conditions"]
    admission = conditions["admission"]
    receipt = environment["native_admission"]
    expected = {
        "schema_version",
        "campaign_id",
        "campaign_store_id",
        "store_binding_digest",
        "slot_id",
        "scope",
        "request_key",
        "request_hash",
        "task_id",
        "case_id",
        "repetition",
        "reserved",
    }
    if not isinstance(receipt, dict) or set(receipt) != expected:
        raise ServiceError("facts_corrupt")
    if (
        receipt["schema_version"] != "pn2-slot-reservation/1"
        or receipt["reserved"] is not True
        or type(receipt["repetition"]) is not int
        or receipt["repetition"] != 0
        or receipt["task_id"] != report["eval_id"]
        or receipt["scope"] != "local"
    ):
        raise ServiceError("facts_corrupt")
    for key in ["campaign_id", "campaign_store_id", "store_binding_digest"]:
        if receipt[key] != admission[key]:
            raise ServiceError("facts_corrupt")
    unsigned = {
        key: admission[key]
        for key in [
            "campaign_id",
            "campaign_store_id",
            "store_binding",
            "scope",
            "slot_policy_digest",
        ]
    }
    if (
        neutral_fingerprint(unsigned) != admission["store_binding_digest"]
        or neutral_fingerprint(
            admission["slots_by_target"],
        )
        != admission["slot_policy_digest"]
    ):
        raise ServiceError("facts_corrupt")
    target = report["target_descriptor"]["key"]
    slot = admission["slots_by_target"].get(target, {}).get(receipt["request_key"])
    if slot is None or slot["slot_id"] != receipt["slot_id"]:
        raise ServiceError("facts_corrupt")
    request = {
        "scope": receipt["scope"],
        "request_key": receipt["request_key"],
        "target_key": target,
        "dataset": report["dataset_ref"],
        "metrics": report["rules"],
        "execution": report["execution"],
    }
    if (
        fingerprint(request) != receipt["request_hash"]
        or slot["request_hash"] != receipt["request_hash"]
    ):
        raise ServiceError("facts_corrupt")
    cases = report["dataset"]["cases"]
    if (
        len(cases) != 1
        or receipt["case_id"] != cases[0]["case_id"]
        or report["execution"]["repetitions"] != 1
    ):
        raise ServiceError("facts_corrupt")
