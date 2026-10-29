"""不可变规模报告的纯归约；不读取数据库，不装配目标或执行评分。"""

from __future__ import annotations

from fractions import Fraction
from typing import Any

from krim_agent.contracts import ServiceError

from .continuous_reports import partition
from .continuous_types import ComparisonPolicy
from .domain import Budget, DatasetRef, MetricSpec, clone, neutral_encoded, neutral_fingerprint
from .metrics import classify, metric_identity
from .types import Score


def fold_trial(state: dict[str, Any], row: dict[str, Any], rules: list[dict[str, Any]]) -> None:
    if not state:
        state.update(
            count=0,
            passed=0,
            complete=0,
            scored=0,
            total=0,
            skipped=0,
            errors=0,
            usage_count=0,
            tokens={k: 0 for k in ["input_tokens", "output_tokens", "total_tokens"]},
            metrics={r["name"]: {"scored": 0, "total": 0} for r in rules},
            last=None,
        )
    key = (row["case_id"], row["repetition"])
    if state["last"] is not None and key <= state["last"]:
        raise ServiceError("facts_corrupt")
    state["last"] = key
    values = row.get("scores")
    if values is not None:
        scores = [Score.model_validate(s) for s in values]
        if len(scores) != len(rules) or {s.rule["name"] for s in scores} != {
            r["name"] for r in rules
        }:
            raise ServiceError("facts_corrupt")
        if row["quality"] != classify(scores, timed_out=row["quality"] == "timed_out"):
            raise ServiceError("facts_corrupt")
        for rule in rules:
            score = next(s for s in scores if s.rule["name"] == rule["name"])
            if score.rule != metric_identity(rule):
                raise ServiceError("facts_corrupt")
            if score.status == "scored":
                if type(score.value) is not int:
                    raise ServiceError("facts_corrupt")
                state["scored"] += 1
                state["total"] += score.value
                state["metrics"][rule["name"]]["scored"] += 1
                state["metrics"][rule["name"]]["total"] += score.value
            else:
                state["skipped" if score.status == "skipped" else "errors"] += 1
        state["complete"] += int(all(s.status == "scored" for s in scores))
    state["passed"] += int(row.get("quality") == "passed")
    state["count"] += 1
    usage = row.get("token_usage")
    if row.get("usage_complete") is True and isinstance(usage, dict):
        if any(type(usage.get(k)) is not int or usage[k] < 0 for k in state["tokens"]):
            raise ServiceError("facts_corrupt")
        state["usage_count"] += 1
        for name in state["tokens"]:
            state["tokens"][name] += usage[name]


def _summary(state: dict[str, Any], planned: int, rules: list[dict[str, Any]]) -> dict[str, Any]:
    if state.get("count") != planned or not 1 <= planned <= 300:
        raise ServiceError("facts_corrupt")
    cells = planned * len(rules)
    return {
        "planned": planned,
        "scored_cells": state["scored"],
        "planned_cells": cells,
        "cell_coverage": float(Fraction(state["scored"], cells)),
        "item_coverage": float(Fraction(state["complete"], planned)),
        "passed": state["passed"],
        "pass_rate": float(Fraction(state["passed"], planned)),
        "mean": float(Fraction(state["total"], state["scored"])) if state["scored"] else None,
        "metrics": {
            r["name"]: {
                "scored": state["metrics"][r["name"]]["scored"],
                "planned": planned,
                "coverage": float(Fraction(state["metrics"][r["name"]]["scored"], planned)),
                "mean": float(
                    Fraction(
                        state["metrics"][r["name"]]["total"], state["metrics"][r["name"]]["scored"]
                    )
                )
                if state["metrics"][r["name"]]["scored"]
                else None,
            }
            for r in rules
        },
        "skipped": state["skipped"],
        "errors": state["errors"],
        "unexecuted_cells": cells - state["scored"] - state["skipped"] - state["errors"],
        "usage_complete_count": state["usage_count"],
        "token_usage": clone(state["tokens"]) if state["usage_count"] == planned else None,
    }


def make_manifest(
    header: dict[str, Any], pages: list[dict[str, Any]], state: dict[str, Any]
) -> dict[str, Any]:
    context = header.get("continuous") or {}
    subject, conditions = partition(header["target"], header["environment"])
    value = {
        "schema_version": "safe-eval/scale/1",
        "kind": context.get("mode", "execute"),
        "eval_id": header["id"],
        "revision": header["revision"],
        "request_hash": header["request_hash"],
        "status": header["status"],
        "code": header["code"],
        "complete": header["status"] == "completed",
        "dataset_ref": header["dataset_ref"],
        "dataset_fingerprint": header["dataset_ref"]["fingerprint"],
        "dataset_envelope_fp": context.get("dataset_envelope_fp"),
        "lineage_fingerprint": context.get("lineage_fingerprint"),
        "source_inputs_fingerprint": context.get("inputs_fingerprint"),
        "parent": context.get("parent"),
        "case_index": header["case_index"],
        "target_descriptor": header["target"],
        "target_fingerprint": neutral_fingerprint(header["target"]),
        "subject": subject,
        "subject_fingerprint": neutral_fingerprint(subject),
        "conditions": conditions,
        "conditions_fingerprint": neutral_fingerprint(conditions),
        "rules": header["rules"],
        "rules_fingerprint": neutral_fingerprint(header["rules"]),
        "execution": header["execution"],
        "execution_fingerprint": neutral_fingerprint(header["execution"]),
        "environment": header["environment"],
        "seed_policy": context.get("seed_policy", "unseeded/1"),
        "planned_count": header["planned_count"],
        "plan_fingerprint": header["plan_fingerprint"],
        "pages": pages,
        "summary": _summary(state, header["planned_count"], header["rules"]),
        "cost": None,
        "capacity": header["capacity"],
        "usage_kind": "actual_execution"
        if context.get("mode", "execute") == "execute"
        else "source_execution",
        "rescoring_usage": None,
    }
    if context.get("schema_version") == "continuous-eval/2":
        value.update(
            schema_version="safe-eval/scale/2",
            repository_id=header["repository_id"],
            scope=header["request_json"]["scope"],
            input_format="continuous-input/1",
            inputs_count=context["inputs_count"],
            inputs_index=context["inputs_index"],
            inputs_root=context["inputs_root"],
        )
    value["report_hash"] = neutral_fingerprint(value, max_bytes=262144)
    return validate_manifest(value)


def validate_manifest(value: dict[str, Any]) -> dict[str, Any]:
    try:
        version = value.get("schema_version")
        if version not in {"safe-eval/scale/1", "safe-eval/scale/2"} or value.get(
            "report_hash"
        ) != neutral_fingerprint(
            {k: v for k, v in value.items() if k != "report_hash"}, max_bytes=262144
        ):
            raise ValueError("root_hash")
        if version == "safe-eval/scale/2":
            if (
                value["kind"] not in {"observed_score", "rescore"}
                or value["input_format"] != "continuous-input/1"
                or not isinstance(value["repository_id"], str)
                or not value["repository_id"]
                or not isinstance(value["scope"], str)
                or not value["scope"]
                or type(value["inputs_count"]) is not int
                or not 24 < value["inputs_count"] <= 300
                or value["inputs_count"] != value["planned_count"]
                or type(value["execution"]["repetitions"]) is not int
                or not 1 <= value["execution"]["repetitions"] <= 3
            ):
                raise ValueError("input_identity")
            index = value["inputs_index"]
            matrix = [
                (c, r)
                for c in sorted(value["case_index"])
                for r in range(value["execution"]["repetitions"])
            ]
            if (
                not isinstance(index, dict)
                or len(matrix) != value["inputs_count"]
                or set(index) != {f"{c}:{r}" for c, r in matrix}
                or any(
                    not isinstance(h, str)
                    or len(h) != 64
                    or any(x not in "0123456789abcdef" for x in h)
                    for h in index.values()
                )
            ):
                raise ValueError("input_index")
            neutral_encoded(index, max_bytes=65536)
            binding = {
                "repository_id": value["repository_id"],
                "scope": value["scope"],
                "task_id": value["eval_id"],
                "request_hash": value["request_hash"],
                "dataset_ref": value["dataset_ref"],
            }
            if value["inputs_root"] != neutral_fingerprint(
                {
                    "schema_version": "continuous-input-index/1",
                    "binding": binding,
                    "count": len(matrix),
                    "leaves": [[c, r, index[f"{c}:{r}"]] for c, r in matrix],
                }
            ):
                raise ValueError("input_root")
        elif any(
            k in value for k in ["input_format", "inputs_count", "inputs_index", "inputs_root"]
        ):
            raise ValueError("mixed_input_version")
        if value["kind"] not in {"execute", "observed_score", "rescore"}:
            raise ValueError("kind")
        ref = DatasetRef.model_validate(value["dataset_ref"])
        budget = Budget.model_validate(value["execution"])
        cases = value["case_index"]
        if not isinstance(cases, dict) or not 1 <= len(cases) <= 100:
            raise ValueError("case_index")
        if (
            value["dataset_fingerprint"] != ref.fingerprint
            or value["planned_count"] != len(cases) * budget.repetitions
        ):
            raise ValueError("matrix")
        rules = [MetricSpec.model_validate(r) for r in value["rules"]]
        if not 1 <= len(rules) <= 6 or [r.name for r in rules] != sorted({r.name for r in rules}):
            raise ValueError("rules")
        subject, conditions = partition(value["target_descriptor"], value["environment"])
        if value["subject"] != subject or value["conditions"] != conditions:
            raise ValueError("conditions")
        for field, data in [
            ("target", "target_descriptor"),
            ("subject", "subject"),
            ("conditions", "conditions"),
            ("rules", "rules"),
            ("execution", "execution"),
        ]:
            if value[field + "_fingerprint"] != neutral_fingerprint(value[data]):
                raise ValueError("fingerprint")
        pages = value["pages"]
        if not pages or len(pages) > 19 or sum(p["count"] for p in pages) != value["planned_count"]:
            raise ValueError("pages")
        previous = None
        for i, page in enumerate(pages):
            if page["index"] != i or not 1 <= page["count"] <= 16:
                raise ValueError("page_order")
            first, last = tuple(page["first"]), tuple(page["last"])
            if first > last or (previous is not None and first <= previous):
                raise ValueError("page_keys")
            previous = last
        if value["summary"]["planned"] != value["planned_count"]:
            raise ValueError("denominator")
        if value["complete"] is not (value["status"] == "completed"):
            raise ValueError("complete")
        neutral_encoded(value, max_bytes=262144)
        return value
    except (KeyError, ValueError, TypeError):
        raise ServiceError("facts_corrupt") from None


def validate_page(
    value: dict[str, Any], descriptor: dict[str, Any], root: dict[str, Any]
) -> list[dict[str, Any]]:
    try:
        if (
            value["schema_version"]
            != (
                "safe-eval/scale-page/2"
                if root["schema_version"] == "safe-eval/scale/2"
                else "safe-eval/scale-page/1"
            )
            or value["task_id"] != root["eval_id"]
        ):
            raise ValueError("page_identity")
        if value["revision"] != root["revision"] or value["index"] != descriptor["index"]:
            raise ValueError("page_revision")
        if value["page_hash"] != descriptor["hash"] or value["page_hash"] != neutral_fingerprint(
            {k: v for k, v in value.items() if k != "page_hash"}, max_bytes=2097152
        ):
            raise ValueError("page_hash")
        rows: list[dict[str, Any]] = value["items"]
        if len(rows) != descriptor["count"] or not 1 <= len(rows) <= 16:
            raise ValueError("page_size")
        keys = [(r["case_id"], r["repetition"]) for r in rows]
        if (
            keys != sorted(set(keys))
            or list(keys[0]) != descriptor["first"]
            or list(keys[-1]) != descriptor["last"]
        ):
            raise ValueError("page_matrix")
        if any(
            k[0] not in root["case_index"]
            or type(k[1]) is not int
            or not 0 <= k[1] < root["execution"]["repetitions"]
            for k in keys
        ):
            raise ValueError("page_case")
        if root.get("schema_version") == "safe-eval/scale/2":
            for row in rows:
                digest = row.get("input_hash")
                if (
                    not isinstance(digest, str)
                    or len(digest) != 64
                    or digest != root["inputs_index"].get(f"{row['case_id']}:{row['repetition']}")
                ):
                    raise ValueError("page_input")
        elif any("input_hash" in row for row in rows):
            raise ValueError("mixed_page_version")
        return rows
    except (KeyError, ValueError, TypeError):
        raise ServiceError("facts_corrupt") from None


def compare_case(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if [(r["case_id"], r["repetition"]) for r in before] != [
        (r["case_id"], r["repetition"]) for r in after
    ]:
        raise ServiceError("facts_corrupt")
    reasons = []
    for old, new in zip(before, after, strict=True):
        if old["quality"] == "passed" and new["quality"] != "passed":
            reasons.append(
                {
                    "code": "passed_to_nonpassed",
                    "case_id": new["case_id"],
                    "repetition": new["repetition"],
                }
            )
        previous = {s["rule"]["name"]: s for s in old["scores"]}
        for score in new["scores"]:
            if score["value"] < previous[score["rule"]["name"]]["value"]:
                reasons.append(
                    {
                        "code": "score_regression",
                        "case_id": new["case_id"],
                        "repetition": new["repetition"],
                        "rule": score["rule"]["name"],
                    }
                )
    return reasons


def fold_gate(
    policy: ComparisonPolicy,
    before: dict[str, Any],
    after: dict[str, Any],
    reasons: list[dict[str, Any]],
    before_state: dict[str, Any],
    after_state: dict[str, Any],
) -> dict[str, Any]:
    def invalid(code: str) -> dict[str, Any]:
        return {
            "status": "invalid",
            "policy": "continuous-regression/1",
            "reasons": [{"code": code}],
        }

    try:
        validate_manifest(before)
        validate_manifest(after)
        if (before["eval_id"], after["eval_id"], before["report_hash"], after["report_hash"]) != (
            policy.baseline_id,
            policy.candidate_id,
            policy.baseline_hash,
            policy.candidate_hash,
        ):
            return invalid("report_changed")
        for r, state in [(before, before_state), (after, after_state)]:
            if _summary(state, r["planned_count"], r["rules"]) != r["summary"]:
                return invalid("report_invalid")
            if not r["complete"] or r["summary"]["cell_coverage"] != 1:
                return invalid("incomplete_coverage")
            if r["subject_fingerprint"] not in policy.allowed_subjects:
                return invalid("unapproved_subject")
        for name in [
            "kind",
            "dataset_envelope_fp",
            "case_index",
            "dataset_fingerprint",
            "rules_fingerprint",
            "execution_fingerprint",
            "seed_policy",
            "conditions_fingerprint",
        ]:
            if before[name] != after[name]:
                return invalid("incompatible_" + name)
        if after["summary"]["pass_rate"] != 1:
            reasons = reasons + [{"code": "candidate_not_all_passed"}]
        return {
            "status": "fail" if reasons else "pass",
            "policy": "continuous-regression/1",
            "baseline_hash": before["report_hash"],
            "candidate_hash": after["report_hash"],
            "reasons": reasons,
        }
    except (ServiceError, ValueError, KeyError, TypeError):
        return invalid("report_invalid")
