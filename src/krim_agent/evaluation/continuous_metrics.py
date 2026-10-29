"""只读确定性指标；业务硬失败与效率诊断分开。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .domain import MetricSpec, neutral_fingerprint
from .metrics import MetricRegistry


async def fact_integrity(
    case: dict[str, Any], output: dict[str, Any], config: dict[str, Any]
) -> tuple[str, int | None, str | None]:
    context = case.get("_continuous")
    if context is None:
        return "skipped", None, "facts_incomplete"
    if context.get("known_hard_failure"):
        return "scored", 0, "source_hard_failure"
    if context.get("mode") != "execute":
        proof = context.get("source_proof")
        if (
            not isinstance(proof, dict)
            or proof.get("terminal") is not True
            or proof.get("complete") is not True
        ):
            return "skipped", None, "source_incomplete"
    if output.get("cleanup") != "closed":
        return "skipped", None, "facts_incomplete"
    if (output.get("process") or {}).get("artifact_error"):
        return "scored", 0, "artifact_invalid"
    if output.get("process") is not None and output["process"].get("output_complete") is not True:
        return "skipped", None, "output_incomplete"
    if output.get("exit_code") is not None:
        expected = case["reference"].get("exit_code")
        if type(expected) is not int:
            return "skipped", None, "status_reference_missing"
        return (
            "scored",
            int(type(output["exit_code"]) is int and output["exit_code"] == expected),
            None,
        )
    target = output.get("target_result")
    if target is None or target.get("status") in {None, "queued", "running", "interrupted"}:
        return "skipped", None, "target_status_unknown"
    reference = case["reference"]
    ok = target["status"] == reference.get("expected_status", "completed") and target.get(
        "code"
    ) == reference.get("expected_code")
    return "scored", int(ok), None


async def context_coverage(
    case: dict[str, Any], output: dict[str, Any], config: dict[str, Any]
) -> tuple[str, int | None, str | None]:
    context = case.get("_continuous")
    if not isinstance(context, dict) or not isinstance(context.get("lineage_case"), dict):
        return "skipped", None, "context_missing"
    if context["mode"] != "execute" and context.get("source_context_hash") != neutral_fingerprint(
        {k: case[k] for k in ["instruction", "files", "verifier"]}
    ):
        return "skipped", None, "source_context_missing"
    lineage = context["lineage_case"]
    body = {k: v for k, v in case.items() if k != "_continuous"}
    ok = (
        lineage["case_hash"] == neutral_fingerprint(body)
        and lineage["reset"]["version"] == "fresh-files/1"
        and lineage["reset"]["fingerprint"] == neutral_fingerprint(case["files"])
        and lineage["context"] in {"complete", "manual_reconstructed"}
    )
    return "scored", int(ok), None if ok else "context_mismatch"


async def process_constraints(
    case: dict[str, Any], output: dict[str, Any], config: dict[str, Any]
) -> tuple[str, int | None, str | None]:
    expected = case["reference"].get("process_constraints")
    if not isinstance(expected, dict):
        return "skipped", None, "process_reference_missing"
    raw = output.get("native") or output.get("target_result") or {}
    tools = raw.get("tools")
    if not isinstance(tools, list) or not raw.get("complete", raw.get("status") == "completed"):
        return "skipped", None, "tools_unknown"
    completed = []
    unproven = False
    for tool in tools:
        if not isinstance(tool, dict):
            return "skipped", None, "tools_unknown"
        if tool.get("status") in {"completed", "ok"} and tool.get("executor_started", True):
            completed.append(tool)
        elif tool.get("status") == "unknown":
            return "skipped", None, "execution_unknown"
        elif tool.get("status") not in {"denied", "failed", "cancelled"}:
            unproven = True
    if unproven and not completed:
        return "skipped", None, "tools_unproven"
    if "approvals" in expected and raw.get("approvals") is None:
        return "skipped", None, "approvals_unknown"
    if len(completed) < expected.get("min_completed", 0):
        return "scored", 0, "action_missing"
    for tool in completed:
        name = tool.get("name", tool.get("kind"))
        if name in expected.get("forbidden_tools", []) or (
            "allowed_tools" in expected and name not in expected["allowed_tools"]
        ):
            return "scored", 0, "tool_not_allowed"
        for key, value in expected.get("arguments", {}).get(name, {}).items():
            if not isinstance(tool.get("arguments"), dict) or key not in tool["arguments"]:
                return "skipped", None, "arguments_unknown"
            if neutral_fingerprint(tool["arguments"][key]) != neutral_fingerprint(value):
                return "scored", 0, "argument_mismatch"
    if "approvals" in expected and raw["approvals"] != expected["approvals"]:
        return "scored", 0, "approval_mismatch"
    return "scored", 1, None


def register_metrics(registry: MetricRegistry) -> None:
    digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    for name, function in [
        ("fact_integrity", fact_integrity),
        ("context_coverage", context_coverage),
        ("process_constraints", process_constraints),
    ]:
        registry.register(
            MetricSpec(name=name, version="1", implementation_digest=digest), function
        )


def continuous_rules(
    registry: MetricRegistry, group: str = "reference", *, include_process: bool = False
) -> list[MetricSpec]:
    values = registry.specs(group)
    for name in ["fact_integrity", "context_coverage"] + (
        ["process_constraints"] if include_process else []
    ):
        values.append(registry._registered[name].spec.model_copy(deep=True))
    return sorted(values, key=lambda value: value.name)
