"""有限规则；SRC-M2-03/04，缺失、坏事实、质量不同。"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.contracts import fingerprint, parse_json
from krim_agent.tools.output import OutputContract

from .domain import MetricSpec, clone
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


# 普通指标与v1规则共享严格JSON和Score/质量分类，不导入或装配目标。


MetricFunction = Callable[
    [dict[str, Any], dict[str, Any], dict[str, Any]],
    Awaitable[
        tuple[
            str,
            int | None,
            str | None,
        ]
    ],
]


def metric_identity(rule: dict[str, Any]) -> dict[str, str]:
    spec = MetricSpec.model_validate(rule)
    return {
        "name": spec.name,
        "version": spec.version,
        "config_fingerprint": fingerprint(spec.model_dump(mode="json")),
    }


async def artifact_metric(
    case: dict[str, Any],
    output: dict[str, Any],
    config: dict[str, Any],
) -> tuple[str, int | None, str | None]:
    await asyncio.sleep(0)
    reference = case["reference"]
    if output.get("process", {}).get("artifact_error"):
        return "scored", 0, "invalid_artifact"
    if "files" not in reference:
        return "skipped", None, "reference_missing"
    artifacts = output.get("artifacts")
    if artifacts is None:
        return "skipped", None, "artifacts_missing"
    for path, expected in reference["files"].items():
        actual = artifacts.get(path)
        if actual is None:
            return "scored", 0, "artifact_missing"
        text = actual.get("text")
        if path.endswith(".json"):
            try:
                value = parse_json(text)
            except (ValueError, TypeError):
                return "scored", 0, "invalid_artifact"
            if not json_equal(value, expected):
                return "scored", 0, "artifact_mismatch"
        elif text != expected:
            return "scored", 0, "artifact_mismatch"
    if reference.get("exit_code") != 0 and artifacts:
        return "scored", 0, "rejection_created_outputs"
    return "scored", 1, None


async def exit_metric(
    case: dict[str, Any],
    output: dict[str, Any],
    config: dict[str, Any],
) -> tuple[str, int | None, str | None]:
    await asyncio.sleep(0)
    if "exit_code" not in case["reference"] or output.get("exit_code") is None:
        return "skipped", None, "exit_unknown"
    return (
        "scored",
        int(
            type(output["exit_code"]) is int
            and output["exit_code"] == case["reference"]["exit_code"]
        ),
        None,
    )


@dataclass(frozen=True)
class RegisteredMetric:
    spec: MetricSpec
    function: MetricFunction


class MetricRegistry:
    def __init__(self) -> None:
        self._registered: dict[str, RegisteredMetric] = {}
        self.calls = 0
        self._bound: dict[str, RegisteredMetric] = {}
        digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.register(
            MetricSpec(
                name="artifact_match",
                version="1",
                implementation_digest=digest,
                required_evidence=["artifacts"],
            ),
            artifact_metric,
        )
        self.register(
            MetricSpec(
                name="exit_code",
                version="1",
                implementation_digest=digest,
                required_evidence=["exit_code"],
            ),
            exit_metric,
        )

        for name, function, evidence in [
            ("native_proof", native_proof, ["native", "process"]),
            ("repository_checks", repository_checks, ["verification"]),
            ("krim_result", krim_result, ["target_result"]),
        ]:
            self.register(
                MetricSpec(
                    name=name, version="1", implementation_digest=digest, required_evidence=evidence
                ),
                function,
            )

    def register(self, spec: MetricSpec, function: MetricFunction) -> None:
        if spec.name in self._registered:
            raise ValueError("指标重复")
        self._registered[spec.name] = RegisteredMetric(
            MetricSpec.model_validate(clone(spec.model_dump(mode="json"))),
            function,
        )

    def specs(self, group: str = "reference") -> list[MetricSpec]:
        names = {
            "reference": {"artifact_match", "exit_code"},
            "native": {"native_proof", "repository_checks"},
            "krim": {"krim_result"},
        }
        if group not in names:
            raise ValueError("metric_group")
        return [
            r.spec.model_copy(deep=True)
            for key, r in sorted(self._registered.items())
            if key in names[group]
        ]

    def bind(self, rules: list[dict[str, Any]]) -> None:
        for rule in rules:
            registered = self._lookup(rule)
            self._bound[registered.spec.name] = registered

    def _lookup(self, rule: dict[str, Any]) -> RegisteredMetric:
        spec = MetricSpec.model_validate(rule)
        registered = self._registered.get(spec.name)
        if (
            registered is None
            or registered.spec != spec
            or spec.name in self._bound
            and self._bound[spec.name] is not registered
        ):
            raise ValueError("未注册或变化的指标")
        return registered

    def identity(self, rule: dict[str, Any]) -> dict[str, str]:
        return metric_identity(rule)

    def result(
        self,
        rule: dict[str, Any],
        status: str,
        value: int | None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return Score.model_validate(
            dict(
                rule=self.identity(rule),
                status=status,
                value=value,
                reason=reason,
                evidence={},
            )
        ).model_dump(mode="json")

    async def score(
        self,
        rule: dict[str, Any],
        case: dict[str, Any],
        output: dict[str, Any],
    ) -> dict[str, Any]:
        registered = self._lookup(rule)
        if any(
            key not in output or output[key] is None for key in registered.spec.required_evidence
        ):
            return self.result(rule, "skipped", None, "evidence_missing")
        self.calls += 1
        status, value, reason = await registered.function(
            case,
            output,
            clone(registered.spec.config),
        )
        if self._lookup(rule) is not registered:
            raise ValueError("评分绑定变化")
        return self.result(rule, status, value, reason)


async def native_proof(
    case: dict[str, Any], output: dict[str, Any], config: dict[str, Any]
) -> tuple[str, int | None, str | None]:
    import shlex
    from pathlib import Path

    await asyncio.sleep(0)
    native, process = output["native"], output["process"]
    ok = (
        native.get("complete") is True
        and output["cleanup"] == "closed"
        and output["exit_code"] == 0
        and process.get("output_complete") is True
        and not process.get("artifact_error")
        and all(
            output["artifacts"].get(k, {}).get("sha256")
            for k in ["outputs/ledger.json", "outputs/summary.md"]
        )
    )
    tools = [
        t
        for t in native.get("tools", [])
        if t.get("status") == "completed" and not t.get("is_error")
    ]
    ran_tests = False
    for tool in tools:
        if tool.get("kind") != "command" or tool.get("exit_code") not in (0, None):
            continue
        try:
            argv = shlex.split(tool.get("arguments", {}).get("command", ""))
        except (TypeError, ValueError):
            continue
        ran_tests = ran_tests or (
            len(argv) == 7
            and Path(argv[0]).name.startswith("python")
            and argv[1:] == ["-B", "-m", "unittest", "discover", "-s", "tests"]
        )
    ok = ok and ran_tests and any(t.get("kind") == "edit" for t in tools)
    return "scored", int(ok), None if ok else "native_proof_missing"


async def repository_checks(
    case: dict[str, Any], output: dict[str, Any], config: dict[str, Any]
) -> tuple[str, int | None, str | None]:
    await asyncio.sleep(0)
    proof = output["verification"]
    if proof.get("environment_error"):
        return "error", None, "environment_error"
    checks = proof.get("checks", {})
    ok = (
        proof.get("passed") is True
        and proof.get("schema_version") == "native-verifier/1"
        and proof.get("phase") == "final"
        and bool(checks)
        and all(v is True for v in checks.values())
        and output["cleanup"] == "closed"
        and not output["process"].get("artifact_error")
        and all(
            output["artifacts"].get(k, {}).get("sha256")
            for k in ["outputs/ledger.json", "outputs/summary.md"]
        )
    )
    return "scored", int(ok), None if ok else "repository_check_failed"


async def krim_result(
    case: dict[str, Any],
    output: dict[str, Any],
    config: dict[str, Any],
) -> tuple[str, int | None, str | None]:
    await asyncio.sleep(0)
    actual, reference = output["target_result"], case["reference"]
    ok = actual.get("status") == "completed" and actual.get("text") == reference.get("expected")
    return "scored", int(ok), None if ok else "krim_result_mismatch"
