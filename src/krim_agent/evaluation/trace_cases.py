"""同snapshot观察、显式选择和参考；SRC-M2-01/04的语义适配，无旧Topic/SDK。"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import cast

from pydantic import JsonValue, ValidationError

from krim_agent.contracts import ServiceError, ToolObservation, fingerprint
from krim_agent.observability import project_trace
from krim_agent.runtime.contracts import RunSnapshot
from krim_agent.runtime.facts import project_facts

from .types import (
    Candidate,
    DatasetRevision,
    EvalOutput,
    EvalTool,
    FreezeRequest,
    FrozenCase,
    ManualSelection,
)


def observe_snapshot(snapshot: RunSnapshot) -> EvalOutput:
    """缺少闭合事实保留null；已提供的矛盾身份不能被缺失掩盖。"""
    try:
        events = snapshot.events
        if [e.seq for e in events] != list(range(len(events))) or any(
            e.run_id != snapshot.run.id for e in events
        ):
            raise ValueError("序号/Run身份损坏")
        if (
            len({e.event_id for e in events}) != len(events)
            or not events
            or events[0].kind != "run_started"
        ):
            raise ValueError("起始事实缺失")
        if events[0].data.get("span_id") != snapshot.run.root_span_id:
            raise ValueError("根Span身份损坏")
        starts: dict[int, str] = {}
        ends: set[int] = set()
        declared: dict[str, tuple[int, str, JsonValue]] = {}
        tools: dict[str, ToolObservation] = {}
        tool_ends: set[str] = set()
        for event in events:
            data = event.data
            if event.kind == "model_start":
                step = data["step_index"]
                if type(step) is not int or step != len(starts):
                    raise ValueError("模型步序号损坏")
                starts[step] = cast(str, data["span_id"])
            elif event.kind == "model_end":
                step = data["step_index"]
                if (
                    type(step) is not int
                    or step not in starts
                    or step in ends
                    or starts[step] != data["span_id"]
                ):
                    raise ValueError("模型闭合损坏")
                ends.add(step)
                calls = data["tool_calls"]
                if not isinstance(calls, list):
                    raise ValueError("调用组损坏")
                for value in calls:
                    if (
                        not isinstance(value, dict)
                        or not isinstance(value.get("id"), str)
                        or not isinstance(value.get("name"), str)
                        or not isinstance(value.get("arguments"), dict)
                    ):
                        raise ValueError("调用形状损坏")
                    key = cast(str, value["id"])
                    if key in declared:
                        raise ValueError("重复调用身份")
                    declared[key] = (step, cast(str, value["name"]), value["arguments"])
            elif event.kind in {"tool_start", "tool_end"}:
                tool = ToolObservation.model_validate(data)
                expected = declared.get(tool.provider_call_id or "")
                if (
                    expected is None
                    or expected[0] != tool.step_index
                    or expected[1] != tool.name
                    or fingerprint(expected[2]) != fingerprint(tool.arguments)
                ):
                    raise ValueError("工具与原模型调用不一致")
                if event.kind == "tool_start":
                    if tool.call_id in tools:
                        raise ValueError("重复工具起点")
                else:
                    before = tools.get(tool.call_id)
                    if (
                        before is None
                        or tool.call_id in tool_ends
                        or (
                            before.span_id,
                            before.name,
                            before.step_index,
                            before.provider_call_id,
                            before.arguments,
                        )
                        != (
                            tool.span_id,
                            tool.name,
                            tool.step_index,
                            tool.provider_call_id,
                            tool.arguments,
                        )
                    ):
                        raise ValueError("工具闭合损坏")
                    tool_ends.add(tool.call_id)
                tools[tool.call_id] = tool
        facts, trace = project_facts(snapshot), project_trace(snapshot)
        models_known = len(starts) == len(ends)
        tools_known = (
            models_known
            and len(tools) == len(declared)
            and len(tool_ends) == len(tools)
            and all(
                t.executor_started is not None and t.result_available is not None
                for t in tools.values()
            )
        )
        if snapshot.run.status == "completed" and not starts:
            raise ValueError("成功Run缺少模型步")
        manifest = [
            {
                "seq": e.seq,
                "event_id": e.event_id,
                "kind": e.kind,
                "at": e.at,
                "data_fingerprint": fingerprint(e.data),
            }
            for e in events
        ]
        digest = fingerprint(
            {
                "run": snapshot.run.model_dump(mode="json"),
                "request": snapshot.request.model_dump(mode="json"),
                "target": snapshot.target,
                "events": manifest,
            }
        )
        complete = trace.complete and models_known and tools_known
        observed = [
            EvalTool.model_validate(
                t.model_dump(mode="json", exclude={"value", "decision"})
                | {
                    "value_fingerprint": fingerprint(t.value)
                    if t.result_available is True
                    else None
                }
            )
            for t in tools.values()
        ]
        return EvalOutput(
            run_id=snapshot.run.id,
            request_key=snapshot.request.request_key,
            trace_id=snapshot.run.trace_id,
            status=snapshot.run.status,
            code=snapshot.run.code,
            snapshot_fingerprint=digest,
            last_seq=len(events) - 1,
            facts_complete=complete,
            text=facts.text if complete and snapshot.run.status == "completed" else None,
            model_steps=facts.model_steps if models_known else None,
            tools=observed if tools_known else None,
            token_usage=facts.token_usage,
            usage_complete=facts.usage_complete,
        )
    except (ValueError, KeyError, TypeError, ValidationError):
        raise ServiceError("facts_corrupt") from None


def _candidate(values: dict[str, JsonValue]) -> Candidate:
    body: dict[str, JsonValue] = {
        "schema_version": 1,
        "filter_version": "self-contained/1",
        **values,
    }
    body["candidate_fingerprint"] = fingerprint(
        {k: v for k, v in body.items() if k != "selected_input"}
    )
    return Candidate.model_validate(body)


def collect_candidate(snapshot: RunSnapshot, include_input: bool = False) -> Candidate:
    output = observe_snapshot(snapshot)
    trace = project_trace(snapshot)
    reasons: list[str] = []
    if snapshot.run.status in {"queued", "running"}:
        eligibility = "ineligible"
        reasons = ["not_terminal"]
    else:
        if snapshot.run.status != "completed":
            reasons.append("source_not_completed")
        if not output.facts_complete:
            reasons.append("incomplete_execution_facts")
        if snapshot.target.get("schema_version") != 2 or any(
            k not in snapshot.target for k in ("provider", "policy", "pre", "post")
        ):
            reasons.append("target_identity_incomplete")
        if len(snapshot.request.text.encode()) > 4096:
            reasons.append("input_capacity")
        if (
            any(e.kind.startswith("approval_") for e in snapshot.events)
            or output.tools is not None
            and any(
                t.name not in {"add", "echo"}
                or t.status != "ok"
                or t.executor_started is not True
                or t.result_available is not True
                for t in output.tools
            )
        ):
            reasons.append("context_dependency")
        eligibility = "needs_context" if reasons else "eligible"
        if not reasons:
            reasons = ["eligible"]
    facts = project_facts(snapshot)
    event_manifest = [
        {
            "seq": e.seq,
            "event_id": e.event_id,
            "kind": e.kind,
            "at": e.at,
            "data_fingerprint": fingerprint(e.data),
        }
        for e in snapshot.events
    ]
    return _candidate(
        {
            "source_kind": "run",
            "source_run_id": snapshot.run.id,
            "eligibility": eligibility,
            "reasons": cast(JsonValue, reasons),
            "input_fingerprint": fingerprint(snapshot.request.text),
            "selected_input": snapshot.request.text
            if include_input and len(snapshot.request.text.encode()) <= 4096
            else None,
            "source_snapshot_fingerprint": output.snapshot_fingerprint,
            "source_target_key": snapshot.target.get("key", snapshot.request.target),
            "source_target_fingerprint": fingerprint(snapshot.target),
            "observed": None
            if eligibility == "ineligible"
            else {
                "status": output.status,
                "code": output.code,
                "text_fingerprint": fingerprint(facts.text),
                "model_steps": output.model_steps,
                "tools_complete": output.tools is not None,
                "token_usage": output.token_usage,
                "usage_complete": output.usage_complete,
            },
            "evidence": {
                "trace_id": snapshot.run.trace_id,
                "root_span_id": snapshot.run.root_span_id,
                "last_seq": output.last_seq,
                "events_fingerprint": fingerprint(event_manifest),
                "facts_fingerprint": fingerprint(facts.model_dump(mode="json", exclude={"events"})),
                "trace_fingerprint": fingerprint(trace.model_dump(mode="json")),
                "complete": output.facts_complete,
            },
        }
    )


def manual_candidate(text: str) -> Candidate:
    return _candidate(
        {
            "source_kind": "manual",
            "source_run_id": None,
            "eligibility": "eligible",
            "reasons": ["manual_input_only"],
            "input_fingerprint": fingerprint(text),
            "selected_input": None,
            "source_snapshot_fingerprint": fingerprint(
                {"kind": "manual", "input_fingerprint": fingerprint(text)}
            ),
            "source_target_key": None,
            "source_target_fingerprint": None,
            "observed": None,
            "evidence": None,
        }
    )


def freeze_revision(
    request: FreezeRequest, snapshots: Mapping[str, RunSnapshot]
) -> DatasetRevision:
    cases = []
    for selection in request.cases:
        if isinstance(selection, ManualSelection):
            text, candidate = selection.input, manual_candidate(selection.input)
        else:
            candidate = collect_candidate(snapshots[selection.source_run_id])
            text = selection.selected_input
            if candidate.eligibility != "eligible":
                raise ServiceError("source_not_eligible")
            if candidate.candidate_fingerprint != selection.candidate_fingerprint:
                raise ServiceError("source_changed")
            if fingerprint(text) != candidate.input_fingerprint:
                raise ServiceError("candidate_input_mismatch")
        cases.append(
            FrozenCase(
                case_id=selection.case_id,
                input=text,
                input_fingerprint=fingerprint(text),
                source=candidate,
                reference=selection.reference,
                reference_fingerprint=fingerprint(selection.reference.model_dump(mode="json")),
                reason=selection.reason,
            )
        )
    content = {
        "schema_version": 1,
        "name": request.name,
        "version": request.version,
        "cases": [c.model_dump(mode="json") for c in cases],
    }
    try:
        return DatasetRevision.model_validate(
            content | {"created_at": time.time(), "fingerprint": fingerprint(content)}
        )
    except ValueError:
        raise ServiceError("input_limit", 422) from None
