"""持续任务报告和显式比较；不启动目标或读取实时环境回填旧事实。"""

from __future__ import annotations

from typing import Any

from krim_agent.contracts import ServiceError

from .continuous_types import ComparisonPolicy, ContinuousRequest
from .domain import REPORT_BYTES, clone, execution_conditions, neutral_encoded, neutral_fingerprint


def partition(
    target: dict[str, Any], environment: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    conditions = execution_conditions(target.get("execution_conditions"))
    if conditions != environment.get("execution_conditions"):
        raise ServiceError("facts_corrupt")
    schema = conditions["schema_version"]
    outer = {k: v for k, v in target.items() if k != "execution_conditions"}
    if schema == "local-process/1":
        subject, common = outer, conditions
    elif schema == "native-cli/1":
        if (
            neutral_fingerprint(conditions["subject"])
            not in conditions["common"]["allowed_subject_fingerprints"]
        ):
            raise ServiceError("facts_corrupt")
        subject = {"descriptor": outer, "subject": conditions["subject"]}
        common = {k: v for k, v in conditions.items() if k != "subject"}
    elif schema == "krim-run/1":
        binding = conditions["binding_descriptor"]
        required = {
            "schema_version",
            "key",
            "version",
            "provider",
            "system_prompt",
            "prompt_fingerprint",
            "tools",
            "policy",
            "pre",
            "post",
            "limits",
            "limits_fingerprint",
        }
        optional = {"workspace_profile", "workspace_implementation", "workspace_verification"}
        if (
            not required <= set(binding)
            or set(binding) - required - optional
            or binding["schema_version"] != 2
        ):
            raise ServiceError("facts_corrupt")
        axis = {
            k: binding[k]
            for k in ["key", "version", "provider", "system_prompt", "prompt_fingerprint"]
        }
        subject = {"descriptor": outer, "run_target": axis}
        common = {
            "schema_version": schema,
            "constraints": {k: v for k, v in binding.items() if k not in axis},
            "budget": conditions["budget"],
            "runtime": conditions["public_runtime_conditions"],
        }
    else:
        raise ServiceError("facts_corrupt")
    return clone(subject), clone(
        {
            "execution_conditions": common,
            "environment": {
                k: v
                for k, v in environment.items()
                if k not in {"execution_conditions", "native_admission"}
            },
        }
    )


def make_report(snapshot: dict[str, Any]) -> dict[str, Any]:
    if snapshot.get("layout") == 3:
        root = snapshot.get("report")
        if not isinstance(root, dict):
            raise ServiceError("report_pages_required")
        from .scale_reports import validate_manifest

        checked = validate_manifest(root)
        if checked["eval_id"] != snapshot["id"] or checked["revision"] != snapshot["revision"]:
            raise ServiceError("facts_corrupt")
        return checked
    from .reports import summarize

    request = ContinuousRequest.model_validate(snapshot["request_json"])
    context = snapshot["continuous"]
    subject, conditions = partition(snapshot["target"], snapshot["environment"])
    report = {
        "schema_version": "safe-eval/continuous/1",
        "kind": request.mode,
        "eval_id": snapshot["id"],
        "revision": snapshot["revision"],
        "request_hash": snapshot["request_hash"],
        "status": snapshot["status"],
        "code": snapshot["code"],
        "complete": snapshot["status"] == "completed"
        and all(i["status"] == "finished" for i in snapshot["items"]),
        "dataset": snapshot["dataset"],
        "dataset_ref": request.dataset.model_dump(mode="json"),
        "dataset_fingerprint": request.dataset.fingerprint,
        "dataset_envelope_fp": context["dataset_envelope_fp"],
        "lineage": context["lineage"],
        "target_descriptor": snapshot["target"],
        "target_fingerprint": neutral_fingerprint(snapshot["target"]),
        "subject": subject,
        "subject_fingerprint": neutral_fingerprint(subject),
        "conditions": conditions,
        "conditions_fingerprint": neutral_fingerprint(conditions),
        "rules": snapshot["rules"],
        "rules_fingerprint": neutral_fingerprint(snapshot["rules"]),
        "execution": snapshot["execution"],
        "execution_fingerprint": neutral_fingerprint(snapshot["execution"]),
        "seed_policy": request.seed_policy,
        "environment": snapshot["environment"],
        "planned_count": snapshot["planned_count"],
        "items": snapshot["items"],
        "source_inputs": context["inputs"],
        "parent": context["parent"],
        "summary": summarize(snapshot["items"], snapshot["rules"], neutral=True),
        "cost": None,
        "capacity": snapshot["capacity"],
        "usage_kind": "actual_execution" if request.mode == "execute" else "source_execution",
        "rescoring_usage": None,
    }
    report["report_hash"] = neutral_fingerprint(report, max_bytes=REPORT_BYTES)
    neutral_encoded(report, max_bytes=REPORT_BYTES)
    return report


def validate_report(value: dict[str, Any]) -> dict[str, Any]:
    if value.get("schema_version") in {"safe-eval/scale/1", "safe-eval/scale/2"}:
        from .scale_reports import validate_manifest

        return validate_manifest(value)
    from .reports import validate_neutral_report

    if value.get("schema_version") != "safe-eval/continuous/1" or value.get(
        "report_hash"
    ) != neutral_fingerprint(
        {k: v for k, v in value.items() if k != "report_hash"}, max_bytes=REPORT_BYTES
    ):
        raise ServiceError("facts_corrupt")
    if value["kind"] not in {"execute", "rescore", "observed_score"}:
        raise ServiceError("facts_corrupt")
    # Validate the common immutable report fields through the one existing
    # validator. This temporary view is never persisted or advertised as v1.
    common = dict(value, schema_version="safe-eval/1")
    common["report_hash"] = neutral_fingerprint(
        {k: v for k, v in common.items() if k != "report_hash"}, max_bytes=REPORT_BYTES
    )
    validate_neutral_report(common)
    subject, conditions = partition(value["target_descriptor"], value["environment"])
    if (
        value["subject"] != subject
        or value["subject_fingerprint"] != neutral_fingerprint(subject)
        or value["conditions"] != conditions
        or value["conditions_fingerprint"] != neutral_fingerprint(conditions)
    ):
        raise ServiceError("facts_corrupt")
    if value["dataset_envelope_fp"] != neutral_fingerprint(
        {"dataset_ref": value["dataset_ref"], "lineage": value["lineage"]}
    ):
        raise ServiceError("facts_corrupt")
    links = {c["case_id"]: c for c in value["lineage"]["cases"]}
    for case in value["dataset"]["cases"]:
        if links[case["case_id"]]["case_hash"] != neutral_fingerprint(case):
            raise ServiceError("facts_corrupt")
    if value["kind"] != "execute":
        expected = {f"{i['case_id']}:{i['repetition']}" for i in value["items"]}
        if set(value["source_inputs"]) != expected:
            raise ServiceError("facts_corrupt")
        for item in value["items"]:
            source = value["source_inputs"][f"{item['case_id']}:{item['repetition']}"]
            if source["output_hash"] != neutral_fingerprint(item["output"]):
                raise ServiceError("facts_corrupt")
    elif value["target_descriptor"]["execution_conditions"]["schema_version"] == "native-cli/1":
        raise ServiceError("facts_corrupt")
    return value


def compare(
    policy: ComparisonPolicy, baseline: dict[str, Any], candidate: dict[str, Any]
) -> dict[str, Any]:
    if any(
        value.get("schema_version") in {"safe-eval/scale/1", "safe-eval/scale/2"}
        for value in [baseline, candidate]
    ):
        return {
            "status": "invalid",
            "policy": "continuous-regression/1",
            "reasons": [{"code": "report_pages_required"}],
        }

    def invalid(reason: str) -> dict[str, Any]:
        return {
            "status": "invalid",
            "policy": "continuous-regression/1",
            "reasons": [{"code": reason}],
        }

    try:
        before, after = validate_report(baseline), validate_report(candidate)
        if (before["eval_id"], after["eval_id"], before["report_hash"], after["report_hash"]) != (
            policy.baseline_id,
            policy.candidate_id,
            policy.baseline_hash,
            policy.candidate_hash,
        ):
            return invalid("report_changed")
        for report in [before, after]:
            if not report["complete"] or report["summary"]["cell_coverage"] != 1:
                return invalid("incomplete_coverage")
            if report["subject_fingerprint"] not in policy.allowed_subjects:
                return invalid("unapproved_subject")
        for name in [
            "kind",
            "dataset_envelope_fp",
            "rules_fingerprint",
            "execution_fingerprint",
            "seed_policy",
            "conditions_fingerprint",
        ]:
            if before[name] != after[name]:
                return invalid("incompatible_" + name)
    except (ServiceError, ValueError, KeyError, TypeError):
        return invalid("report_invalid")
    reasons = []
    old = {(i["case_id"], i["repetition"]): i for i in before["items"]}
    for item in after["items"]:
        previous = old[(item["case_id"], item["repetition"])]
        if previous["quality"] == "passed" and item["quality"] != "passed":
            reasons.append(
                {
                    "code": "passed_to_nonpassed",
                    "case_id": item["case_id"],
                    "repetition": item["repetition"],
                }
            )
        scores = {s["rule"]["name"]: s for s in previous["scores"]}
        for score in item["scores"]:
            if score["value"] < scores[score["rule"]["name"]]["value"]:
                reasons.append(
                    {
                        "code": "score_regression",
                        "case_id": item["case_id"],
                        "repetition": item["repetition"],
                        "rule": score["rule"]["name"],
                    }
                )
    if after["summary"]["pass_rate"] != 1:
        reasons.append({"code": "candidate_not_all_passed"})
    return {
        "status": "fail" if reasons else "pass",
        "policy": "continuous-regression/1",
        "baseline_hash": before["report_hash"],
        "candidate_hash": after["report_hash"],
        "reasons": reasons,
    }
