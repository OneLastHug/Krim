"""脱敏 Trace 是已提交运行事实的投影，不另起异步 Span 写入器。"""

from typing import cast

from pydantic import Field, JsonValue

from krim_agent.contracts import ERROR_CODES, StrictModel
from krim_agent.runtime.contracts import RunSnapshot


class SpanView(StrictModel):
    run_id: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    kind: str
    start: float
    end: float | None = None
    status: str = "unknown"
    attributes: dict[str, JsonValue] = Field(default_factory=dict)


class TraceView(StrictModel):
    run_id: str
    trace_id: str
    complete: bool
    reasons: list[str]
    spans: list[SpanView]


def project_trace(snapshot: RunSnapshot) -> TraceView:
    run = snapshot.run
    spans: dict[str, SpanView] = {}
    for event in snapshot.events:
        data = event.data
        if event.kind in {"run_started", "model_start", "tool_start"}:
            span_id = cast(str, data["span_id"])
            kind = {"run_started": "run", "model_start": "model", "tool_start": "tool"}[event.kind]
            step = data.get("step_index")
            # 模型请求与工具执行是同一 Run 的子工作；step_index 关联其因果。
            # 工具不作为已经结束的模型网络请求的子区间。
            parent = None if kind == "run" else run.root_span_id
            attributes: dict[str, JsonValue] = {}
            provider = snapshot.target.get("provider")
            if (
                kind == "run"
                and isinstance(provider, dict)
                and provider.get("behavior_version") == "http-sse/1"
            ):
                identity = provider.get("config")
                if isinstance(identity, dict) and isinstance(identity.get("model"), str):
                    attributes["provider"] = "http"
                    attributes["model"] = identity["model"]
                    if identity.get("reasoning_effort") == "xhigh":
                        attributes["reasoning_effort"] = "xhigh"
                    if snapshot.target.get("key") in {"demo", "target-http"}:
                        attributes["target_key"] = snapshot.target["key"]
            if snapshot.work is not None:
                attributes["target_key"] = "target-workspace"
                if kind == "model" and data.get("purpose") in {"task", "summary"}:
                    attributes["purpose"] = data["purpose"]
            if type(step) is int:
                attributes["step_index"] = step
            spans[span_id] = SpanView(
                run_id=run.id,
                trace_id=run.trace_id,
                span_id=span_id,
                parent_span_id=parent,
                kind=kind,
                start=event.at,
                attributes=attributes,
            )
        elif event.kind in {"model_end", "tool_end", "terminal"}:
            span_id = run.root_span_id if event.kind == "terminal" else cast(str, data["span_id"])
            if span_id not in spans:
                continue
            original = spans[span_id]
            status = (
                "ok"
                if event.kind == "model_end" or data.get("status") in {"completed", "ok"}
                else "error"
            )
            if data.get("status") in {"aborted", "cancelled"}:
                status = "cancelled"
            if data.get("status") == "interrupted":
                status = "interrupted"
            attrs = dict(original.attributes)
            if event.kind == "tool_end":
                attrs["executor_started"] = data.get("executor_started")
                attrs["result_available"] = data.get("result_available")
            code = data.get("code")
            if isinstance(code, str) and code in ERROR_CODES:
                attrs["code"] = code
            spans[span_id] = original.model_copy(
                update={"end": event.at, "status": status, "attributes": attrs}
            )
    continuous = [event.seq for event in snapshot.events] == list(range(len(snapshot.events)))
    closed = bool(spans) and all(
        span.end is not None and (span.parent_span_id is None or span.parent_span_id in spans)
        for span in spans.values()
    )
    known = all(
        span.kind != "tool" or span.attributes.get("executor_started") is not None
        for span in spans.values()
    )
    complete = (
        continuous
        and closed
        and known
        and run.status != "interrupted"
        and run.code not in {"event_limit", "output_limit", "storage_unavailable"}
    )
    reasons = [] if complete else ["incomplete_execution_facts"]
    return TraceView(
        run_id=run.id,
        trace_id=run.trace_id,
        complete=complete,
        reasons=reasons,
        spans=list(spans.values()),
    )
