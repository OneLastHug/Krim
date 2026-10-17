"""有限规则；SRC-M2-03/04，缺失、坏事实、质量不同。"""

from __future__ import annotations

import asyncio
from typing import cast

from pydantic import JsonValue

from krim_agent.contracts import fingerprint, parse_json
from krim_agent.tools.output import OutputContract

from .types import EvalOutput, Quality, Reference, RuleSpec, Score


def json_equal(left: JsonValue, right: JsonValue) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(json_equal(v, right[k]) for k, v in left.items())
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            json_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    return left == right


def result(
    rule: RuleSpec,
    status: str,
    value: int | None,
    reason: str | None = None,
    evidence: dict[str, JsonValue] | None = None,
) -> Score:
    return Score.model_validate(
        {
            "rule": {
                "name": rule.name,
                "version": rule.version,
                "config_fingerprint": fingerprint(rule.config),
            },
            "status": status,
            "value": value,
            "reason": reason,
            "evidence": evidence or {},
        }
    )


async def score_rule(rule: RuleSpec, reference: Reference, output: EvalOutput | None) -> Score:
    await asyncio.sleep(0)
    if output is None:
        return result(rule, "skipped", None, "output_missing")
    output = EvalOutput.model_validate(output.model_dump(mode="json"))
    if rule.name == "status_code":
        return result(
            rule,
            "scored",
            int(
                output.status == reference.expected_status
                and output.code == reference.expected_code
            ),
        )
    if rule.name == "exact_match":
        if not isinstance(reference.expected, str):
            return result(rule, "skipped", None, "reference_not_text")
        if output.text is None:
            return result(rule, "skipped", None, "answer_missing")
        return result(rule, "scored", int(output.text == reference.expected))
    if rule.name in {"json_match", "json_schema"}:
        if rule.name == "json_schema" and reference.expected_schema is None:
            return result(rule, "skipped", None, "schema_missing")
        if output.text is None:
            return result(rule, "skipped", None, "answer_missing")
        try:
            value = parse_json(output.text)
        except ValueError:
            return result(rule, "scored", 0, "invalid_json")
        if rule.name == "json_match":
            return result(rule, "scored", int(json_equal(value, reference.expected)))
        try:
            OutputContract(cast(dict[str, JsonValue], reference.expected_schema)).validate(value)
        except ValueError:
            return result(rule, "scored", 0, "schema_mismatch")
        return result(rule, "scored", 1)
    if rule.name == "tool_calls":
        if reference.expected_tools is None:
            return result(rule, "skipped", None, "tool_reference_missing")
        if output.tools is None:
            return result(rule, "skipped", None, "tool_facts_missing")
        if len(output.tools) != len(reference.expected_tools):
            return result(rule, "scored", 0, "tool_count_mismatch")
        for wanted, actual in zip(reference.expected_tools, output.tools, strict=True):
            if wanted.executor_started is not None and actual.executor_started is None:
                return result(rule, "skipped", None, "execution_unknown")
            if wanted.arguments is not None and actual.arguments is None:
                return result(rule, "skipped", None, "arguments_unknown")
            if (
                wanted.name != actual.name
                or wanted.arguments is not None
                and not json_equal(wanted.arguments, actual.arguments)
                or wanted.executor_started is not None
                and wanted.executor_started is not actual.executor_started
                or wanted.status is not None
                and wanted.status != actual.status
            ):
                return result(rule, "scored", 0, "tool_mismatch", {"call_id": actual.call_id})
        return result(rule, "scored", 1)
    spec = reference.trajectory
    if spec is None:
        return result(rule, "skipped", None, "trajectory_reference_missing")
    steps = output.model_steps
    if any(v is not None for v in (spec.exact_steps, spec.min_steps, spec.max_steps)):
        if steps is None:
            return result(rule, "skipped", None, "model_steps_missing")
        if (
            spec.exact_steps is not None
            and steps != spec.exact_steps
            or spec.min_steps is not None
            and steps < spec.min_steps
            or spec.max_steps is not None
            and steps > spec.max_steps
        ):
            return result(rule, "scored", 0, "model_steps_mismatch", {"model_steps": steps})
    if spec.tool_names is not None:
        if output.tools is None:
            return result(rule, "skipped", None, "tool_facts_missing")
        if [t.name for t in output.tools] != spec.tool_names:
            return result(rule, "scored", 0, "tool_order_mismatch")
    return result(rule, "scored", 1)


def classify(scores: list[Score], *, timed_out: bool = False) -> Quality:
    if any(s.status == "error" for s in scores):
        return "error"
    if timed_out:
        return "timed_out"
    if any(s.value == 0 for s in scores if s.status == "scored"):
        return "failed"
    if not scores or any(s.status != "scored" for s in scores):
        return "unscored"
    return "passed"
