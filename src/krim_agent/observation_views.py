"""只读观测投影；从已提交快照与压缩记录投影元数据，不读正文、不计价。

聚合只覆盖调用方已授权的 run（列表为返回页、详情为单个授权 Turn），
不从初始快照假造实际输入：旧数据缺失的 manifest/计时/usage 如实为 null。
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from typing import Literal, cast

from pydantic import Field, JsonValue

from krim_agent.contracts import RunEvent, RunStatus, ServiceError, StrictModel
from krim_agent.observability import TraceView, project_trace
from krim_agent.runtime.contracts import RunSnapshot
from krim_agent.workspace.types import ArtifactChunk, path_parts

UsageFieldName = Literal[
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_input_tokens",
    "cache_write_input_tokens",
    "reasoning_output_tokens",
]
_USAGE_KEYS: tuple[UsageFieldName, ...] = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_input_tokens",
    "cache_write_input_tokens",
    "reasoning_output_tokens",
)
_TOOL_STATUSES = frozenset({"ok", "denied", "failed", "cancelled", "unknown"})


class UsageView(StrictModel):
    """供应商单条累计用量；子计数是所属总量的内部子集，不再加回总量。"""

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cached_input_tokens: int | None = None
    cache_write_input_tokens: int | None = None
    reasoning_output_tokens: int | None = None


class RequestUsageView(StrictModel):
    usage: UsageView | None = None
    usage_reports: int = 0
    usage_final: bool | None = None


class RequestTiming(StrictModel):
    elapsed_ms: float
    first_content_ms: float | None = None
    first_text_ms: float | None = None
    origin: str | None = None
    source: Literal["model_end"] = "model_end"


class RequestObservation(StrictModel):
    id: str
    step_index: int
    purpose: str | None = None
    status: Literal["ok", "error", "cancelled", "interrupted", "unknown"] = "unknown"
    started_at: float
    ended_at: float | None = None
    duration_ms: float | None = None
    duration_source: Literal["mono", "wall_clock"] | None = None
    timing: RequestTiming | None = None
    tool_calls: int = 0
    usage: RequestUsageView = Field(default_factory=RequestUsageView)
    manifest: dict[str, JsonValue] | None = None
    manifest_status: Literal["present", "missing", "invalid"] = "missing"
    cost: None = None
    cost_status: Literal["unpriced"] = "unpriced"


class ArtifactSource(StrictModel):
    kind: Literal["artifact_chunk"] = "artifact_chunk"
    revision_id: str
    path: str
    offset: int
    total_size: int
    length: int
    sha256: str


class ToolRecord(StrictModel):
    id: str
    call_id: str
    provider_call_id: str | None = None
    step_index: int | None = None
    name: str | None = None
    status: Literal["ok", "denied", "failed", "cancelled", "unknown"] = "unknown"
    code: str | None = None
    decision: str | None = None
    executor_started: bool | None = None
    result_available: bool | None = None
    started_at: float
    ended_at: float | None = None
    duration_ms: float | None = None
    duration_source: Literal["mono", "wall_clock"] | None = None
    source: ArtifactSource | None = None


class CompactionRecord(StrictModel):
    pass_index: int
    status: str
    created_at: float
    finished_at: float | None = None
    summary_step_index: int | None = None
    covered_turns: int | None = None
    covered_turn_ids: list[str] = Field(default_factory=list)
    source_hash: str | None = None
    output_hash: str | None = None
    summary_bytes: int | None = None
    counter_version: str | None = None
    reserved_output_tokens: int | None = None


class CoverageView(StrictModel):
    passes: int = 0
    complete_passes: int = 0
    covered_turns: int | None = None
    history_turns: int | None = None
    first_covered_turn_id: str | None = None
    last_covered_turn_id: str | None = None
    summary_step_index: int | None = None
    manifest_status: Literal["present", "partial", "missing"] = "missing"
    steps_with_usage: int = 0
    steps_missing_usage: int = 0


class RunUsageView(StrictModel):
    totals: UsageView | None = None
    usage_complete: bool = False
    steps_with_usage: int = 0
    steps_missing_usage: int = 0


class ConfigurationView(StrictModel):
    """运行配置标识白名单；不含system_prompt/tools正文、路径、URL或密钥。"""

    key: str | None = None
    version: str | None = None
    provider_behavior_version: str | None = None
    provider_implementation_digest: str | None = None
    model: str | None = None
    reasoning_effort: str | None = None
    thinking: str | None = None
    endpoint_fingerprint: str | None = None
    prompt_fingerprint: str | None = None
    limits_fingerprint: str | None = None
    max_output_tokens: int | None = None


class RunSummary(StrictModel):
    run_id: str
    trace_id: str
    request_key: str
    status: RunStatus
    code: str | None = None
    created_at: float
    finished_at: float | None = None
    duration_ms: float | None = None
    duration_source: Literal["mono", "wall_clock"] | None = None


class TurnObservation(StrictModel):
    session_id: str
    turn_id: str
    run: RunSummary
    trace: TraceView
    configuration: ConfigurationView | None = None
    requests: list[RequestObservation] = Field(default_factory=list)
    tools: list[ToolRecord] = Field(default_factory=list)
    compactions: list[CompactionRecord] = Field(default_factory=list)
    coverage: CoverageView = Field(default_factory=CoverageView)
    usage: RunUsageView = Field(default_factory=RunUsageView)
    cost: None = None
    cost_status: Literal["unpriced"] = "unpriced"


class ObservationItem(StrictModel):
    source_kind: Literal["session_turn"] = "session_turn"
    run_id: str
    trace_id: str
    session_id: str
    turn_id: str
    session_title: str
    target_key: str
    status: str
    created_at: float
    finished_at: float | None = None
    projection_status: Literal["ok", "unavailable"] = "ok"
    projection_code: str | None = None
    duration_ms: float | None = None
    duration_source: Literal["mono", "wall_clock"] | None = None
    model_requests: int | None = None
    tool_calls: int | None = None
    usage: UsageView | None = None
    usage_complete: bool | None = None
    usage_steps: int | None = None
    steps_missing_usage: int | None = None
    compaction_passes: int | None = None
    manifest_requests: int | None = None
    manifest_status: Literal["present", "partial", "missing"] | None = None
    cost: None = None
    cost_status: Literal["unpriced"] = "unpriced"


class ObservationScope(StrictModel):
    version: int = 1
    workspace_id: str
    role: str
    status: RunStatus | None = None
    session_id: str | None = None
    limit: int
    counted: Literal["returned_page"] = "returned_page"


class ObservationPage(StrictModel):
    scope: ObservationScope
    items: list[ObservationItem]
    next_cursor: str | None = None


def _int(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _number(value: object) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) if math.isfinite(float(value)) and value >= 0 else None
    return None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _bool(value: object) -> bool | None:
    return value if type(value) is bool else None


ManifestState = Literal["present", "missing", "invalid"]


def manifest_view(data: object) -> tuple[dict[str, JsonValue] | None, ManifestState]:
    """校验采集结构后原样透传；旧数据缺失为missing，形状损坏为invalid。"""
    if data is None:
        return None, "missing"
    if not isinstance(data, dict):
        return None, "invalid"
    version = data.get("version")
    items = data.get("items")
    if (
        type(version) is not int
        or version != 1
        or not isinstance(data.get("counter_version"), str)
        or type(data.get("input_bytes")) is not int
        or type(data.get("estimated_tokens")) is not int
        or not isinstance(items, list)
        or any(not isinstance(item, dict) for item in items)
    ):
        return None, "invalid"
    return data, "present"


def _usage_view(raw: object) -> UsageView | None:
    if not isinstance(raw, dict):
        return None
    values = {key: _int(raw.get(key)) for key in _USAGE_KEYS}
    if all(value is None for value in values.values()):
        return None
    return UsageView(**values)


def _step_usages(events: Sequence[RunEvent]) -> dict[int, RequestUsageView]:
    """同一step的usage是累计序列；只保留最后一条，不重复计数。"""
    result: dict[int, RequestUsageView] = {}
    for event in events:
        if event.kind != "usage":
            continue
        step = event.data.get("step_index")
        if type(step) is not int:
            continue
        result[step] = RequestUsageView(
            usage=_usage_view(event.data.get("usage")),
            usage_reports=(result[step].usage_reports + 1 if step in result else 1),
            usage_final=_bool(event.data.get("final")),
        )
    return result


def _sum_usage(views: Sequence[UsageView]) -> UsageView | None:
    if not views:
        return None
    totals: dict[str, int | None] = {}
    for key in _USAGE_KEYS:
        values = [getattr(view, key) for view in views if getattr(view, key) is not None]
        totals[key] = sum(values) if values else None
    return UsageView(**totals)


ManifestRollup = Literal["present", "partial", "missing"]


def _manifest_rollup(requests: Sequence[RequestObservation]) -> ManifestRollup:
    if not requests:
        return "missing"
    counted = sum(1 for request in requests if request.manifest_status == "present")
    if counted == 0:
        return "missing"
    return "present" if counted == len(requests) else "partial"


def _timing(data: Mapping[str, JsonValue]) -> RequestTiming | None:
    elapsed = _number(data.get("elapsed_ms"))
    if elapsed is None:
        return None
    return RequestTiming(
        elapsed_ms=elapsed,
        first_content_ms=_number(data.get("first_content_ms")),
        first_text_ms=_number(data.get("first_text_ms")),
        origin=_text(data.get("timing_origin")),
    )


def _span_events(events: Sequence[RunEvent], kind: str) -> dict[str, RunEvent]:
    spans: dict[str, RunEvent] = {}
    for event in events:
        if event.kind != kind:
            continue
        span_id = _text(event.data.get("span_id"))
        if span_id is not None:
            spans[span_id] = event
    return spans


def project_run_summary(snapshot: RunSnapshot) -> RunSummary:
    run = snapshot.run
    started = next(
        (event.at for event in snapshot.events if event.kind == "run_started"), run.created_at
    )
    ended = next(
        (event.at for event in reversed(snapshot.events) if event.kind == "terminal"), None
    )
    if ended is None:
        ended = run.finished_at
    duration = max(0.0, (ended - started) * 1000) if ended is not None else None
    return RunSummary(
        run_id=run.id,
        trace_id=run.trace_id,
        request_key=run.request_key,
        status=run.status,
        code=run.code,
        created_at=run.created_at,
        finished_at=run.finished_at,
        duration_ms=duration,
        duration_source="wall_clock" if duration is not None else None,
    )


def project_requests(
    snapshot: RunSnapshot,
) -> tuple[list[RequestObservation], list[ToolRecord]]:
    """模型请求以span_id为id；工具经step_index关联，同为run的子级。"""
    events = snapshot.events
    starts = _span_events(events, "model_start")
    ends = _span_events(events, "model_end")
    usages = _step_usages(events)
    requests: list[RequestObservation] = []
    for span_id, start in starts.items():
        data = start.data
        step = data.get("step_index")
        manifest, manifest_status = manifest_view(data.get("context_manifest"))
        end = ends.get(span_id)
        timing = _timing(end.data) if end is not None else None
        ended_at = end.at if end is not None else None
        source: Literal["mono", "wall_clock"] | None
        if timing is not None:
            duration, source = timing.elapsed_ms, "mono"
        elif ended_at is not None:
            duration, source = max(0.0, (ended_at - start.at) * 1000), "wall_clock"
        else:
            duration, source = None, None
        tool_calls = end.data.get("tool_calls") if end is not None else None
        requests.append(
            RequestObservation(
                id=span_id,
                step_index=step if type(step) is int else 0,
                purpose=_text(data.get("purpose")),
                status="ok" if end is not None else "unknown",
                started_at=start.at,
                ended_at=ended_at,
                duration_ms=duration,
                duration_source=source,
                timing=timing,
                tool_calls=len(tool_calls) if isinstance(tool_calls, list) else 0,
                usage=(
                    usages.get(step, RequestUsageView())
                    if type(step) is int else RequestUsageView()
                ),
                manifest=manifest,
                manifest_status=manifest_status,
            )
        )
    tools = project_tools(events, workspace=snapshot.work is not None)
    return requests, tools


def _artifact_source(data: Mapping[str, JsonValue], workspace: bool) -> ArtifactSource | None:
    if (not workspace or data.get("name") not in {"workspace_read", "artifact_read"}
            or data.get("status") != "ok" or data.get("result_available") is not True):
        return None
    try:
        chunk = ArtifactChunk.model_validate(data.get("value"))
        path_parts(chunk.path)
    except (ValueError, ServiceError):
        return None
    if chunk.offset + chunk.length > chunk.total_size:
        return None
    # Keep the immutable revision/hash/range, never the encoded file body.
    return ArtifactSource(revision_id=chunk.revision_id, path=chunk.path, offset=chunk.offset,
                          total_size=chunk.total_size, length=chunk.length, sha256=chunk.sha256)


def project_tools(events: Sequence[RunEvent], *, workspace: bool = False) -> list[ToolRecord]:
    starts: dict[str, tuple[RunEvent, dict[str, JsonValue]]] = {}
    ends: dict[str, tuple[RunEvent, dict[str, JsonValue]]] = {}
    order: list[str] = []
    for event in events:
        if event.kind not in {"tool_start", "tool_end"}:
            continue
        call_id = _text(event.data.get("call_id"))
        if call_id is None:
            continue
        store = starts if event.kind == "tool_start" else ends
        if call_id not in store and event.kind == "tool_start":
            order.append(call_id)
        store[call_id] = (event, event.data)
        if call_id not in order:
            order.append(call_id)
    records: list[ToolRecord] = []
    for call_id in order:
        start = starts.get(call_id)
        end = ends.get(call_id)
        first = start or end
        assert first is not None  # order only contains observed starts/ends
        start_event, start_data = first
        end_event, end_data = end or first
        end_status = _text(end_data.get("status"))
        status = end_status if end_status in _TOOL_STATUSES else None
        if status is None:
            start_status = _text(start_data.get("status"))
            status = start_status if start_status in _TOOL_STATUSES else "unknown"
        timing = _timing(end_data)
        started_at = start_event.at if start is not None else end_event.at
        ended_at = end_event.at if end is not None else None
        source: Literal["mono", "wall_clock"] | None
        if timing is not None:
            duration, source = timing.elapsed_ms, "mono"
        elif ended_at is not None and start is not None:
            duration, source = max(0.0, (ended_at - started_at) * 1000), "wall_clock"
        else:
            duration, source = None, None
        records.append(
            ToolRecord(
                id=_text(start_data.get("span_id")) or _text(end_data.get("span_id")) or call_id,
                call_id=call_id,
                provider_call_id=_text(end_data.get("provider_call_id"))
                or _text(start_data.get("provider_call_id")),
                step_index=(_int(end_data.get("step_index"))
                            if _int(end_data.get("step_index")) is not None
                            else _int(start_data.get("step_index"))),
                name=_text(end_data.get("name")) or _text(start_data.get("name")),
                status=cast(Literal["ok", "denied", "failed", "cancelled", "unknown"], status),
                code=_text(end_data.get("code")) or _text(start_data.get("code")),
                decision=_text(end_data.get("decision")) or _text(start_data.get("decision")),
                executor_started=_bool(end_data.get("executor_started"))
                if end_data.get("executor_started") is not None
                else _bool(start_data.get("executor_started")),
                result_available=_bool(end_data.get("result_available"))
                if end_data.get("result_available") is not None
                else _bool(start_data.get("result_available")),
                started_at=started_at,
                ended_at=ended_at,
                duration_ms=duration,
                duration_source=source,
                source=_artifact_source(end_data, workspace) if end is not None else None,
            )
        )
    return records


def project_usage(requests: Sequence[RequestObservation]) -> RunUsageView:
    """totals是已知部分合计；usage_complete要求全部step有final且基础计数齐全。"""
    reported = [request.usage.usage for request in requests if request.usage.usage is not None]
    with_usage = len(reported)
    complete = bool(requests) and with_usage == len(requests) and all(
        request.ended_at is not None
        and request.usage.usage_final is True
        and request.usage.usage is not None
        and request.usage.usage.input_tokens is not None
        and request.usage.usage.output_tokens is not None
        and request.usage.usage.total_tokens is not None
        for request in requests
    )
    return RunUsageView(
        totals=_sum_usage(reported),
        usage_complete=complete,
        steps_with_usage=with_usage,
        steps_missing_usage=len(requests) - with_usage,
    )


def project_compaction_row(row: Mapping[str, object], session_id: str) -> CompactionRecord:
    """读取真实压缩记录；只投影计数与hash，不复制摘要正文。"""
    if row.get("session_id") != session_id:
        raise ServiceError("facts_corrupt")
    refs = row.get("source_refs")
    turn_ids = [
        cast(str, ref["turn_id"])
        for ref in (refs if isinstance(refs, list) else [])
        if isinstance(ref, dict) and type(ref.get("turn_id")) is str
    ]
    output = row.get("output")
    output_hash = _text(row.get("output_hash"))
    summary_bytes: int | None = None
    if isinstance(output, str) and output_hash is not None:
        if hashlib.sha256(output.encode()).hexdigest() != output_hash:
            raise ServiceError("facts_corrupt")
        summary_bytes = len(output.encode())
    policy = row.get("policy")
    counter_version: str | None = None
    reserved: int | None = None
    if isinstance(policy, dict):
        counter_version = _text(policy.get("counter_version"))
        context = policy.get("context")
        if isinstance(context, dict):
            reserved = _int(context.get("reserved_output_tokens"))
    step = _int(row.get("model_step"))
    return CompactionRecord(
        pass_index=cast(int, row["pass_index"]),
        status=cast(str, row["status"]),
        created_at=cast(float, row["created_at"]),
        finished_at=cast(float | None, row.get("finished_at")),
        summary_step_index=step,
        covered_turns=len(turn_ids) if refs else None,
        covered_turn_ids=turn_ids,
        source_hash=_text(row.get("source_hash")),
        output_hash=output_hash,
        summary_bytes=summary_bytes,
        counter_version=counter_version,
        reserved_output_tokens=reserved,
    )


def project_coverage(
    compactions: Sequence[CompactionRecord],
    history_turns: int | None,
    requests: Sequence[RequestObservation],
) -> CoverageView:
    complete = [record for record in compactions if record.status == "complete"]
    latest = complete[-1] if complete else None
    with_usage = sum(1 for request in requests if request.usage.usage is not None)
    covered = latest.covered_turn_ids if latest is not None else []
    return CoverageView(
        passes=len(compactions),
        complete_passes=len(complete),
        covered_turns=latest.covered_turns if latest is not None else None,
        history_turns=history_turns,
        first_covered_turn_id=covered[0] if covered else None,
        last_covered_turn_id=covered[-1] if covered else None,
        summary_step_index=latest.summary_step_index if latest is not None else None,
        manifest_status=_manifest_rollup(requests),
        steps_with_usage=with_usage,
        steps_missing_usage=len(requests) - with_usage,
    )


def project_configuration(target: Mapping[str, JsonValue] | None) -> ConfigurationView | None:
    """只投影安全标量与指纹；旧/v1运行target无provider身份时为null。"""
    if not isinstance(target, dict) or not isinstance(target.get("provider"), dict):
        return None
    provider = cast(dict[str, JsonValue], target["provider"])
    config = provider.get("config")
    config = config if isinstance(config, dict) else {}
    limits = target.get("limits")
    http = limits.get("http") if isinstance(limits, dict) else None
    http = http if isinstance(http, dict) else {}
    return ConfigurationView(
        key=_text(target.get("key")),
        version=_text(target.get("version")),
        provider_behavior_version=_text(provider.get("behavior_version")),
        provider_implementation_digest=_text(provider.get("implementation_digest")),
        model=_text(config.get("model")),
        reasoning_effort=_text(config.get("reasoning_effort")),
        thinking=_text(config.get("thinking")),
        endpoint_fingerprint=_text(config.get("endpoint_fingerprint")),
        prompt_fingerprint=_text(target.get("prompt_fingerprint")),
        limits_fingerprint=_text(target.get("limits_fingerprint")),
        max_output_tokens=_int(http.get("max_output_tokens")),
    )


def project_detail(
    snapshot: RunSnapshot,
    *,
    session_id: str,
    turn_id: str,
    compactions: Sequence[CompactionRecord],
    history_turns: int | None,
) -> TurnObservation:
    requests, tools = project_requests(snapshot)
    return TurnObservation(
        session_id=session_id,
        turn_id=turn_id,
        run=project_run_summary(snapshot),
        trace=project_trace(snapshot),
        configuration=project_configuration(snapshot.target),
        requests=requests,
        tools=tools,
        compactions=list(compactions),
        coverage=project_coverage(compactions, history_turns, requests),
        usage=project_usage(requests),
    )


def project_summary(
    index: Mapping[str, JsonValue],
    snapshot: RunSnapshot | None,
    *,
    compaction_passes: int,
    error: str | None = None,
) -> ObservationItem:
    """列表单项：索引字段照常返回；单run投影失败时聚合字段如实缺失。"""
    item = ObservationItem(
        run_id=cast(str, index["run_id"]),
        trace_id=cast(str, index["trace_id"]),
        session_id=cast(str, index["session_id"]),
        turn_id=cast(str, index["turn_id"]),
        session_title=cast(str, index["session_title"]),
        target_key=cast(str, index["target_key"]),
        status=cast(str, index["status"]),
        created_at=cast(float, index["created_at"]),
        finished_at=cast(float | None, index.get("finished_at")),
    )
    if snapshot is None:
        return item.model_copy(
            update={
                "projection_status": "unavailable",
                "projection_code": error,
                "compaction_passes": compaction_passes or None,
            }
        )
    requests, tools = project_requests(snapshot)
    usage = project_usage(requests)
    started = next(
        (event.at for event in snapshot.events if event.kind == "run_started"),
        snapshot.run.created_at,
    )
    ended = next(
        (event.at for event in reversed(snapshot.events) if event.kind == "terminal"), None
    )
    if ended is None:
        ended = snapshot.run.finished_at
    duration = max(0.0, (ended - started) * 1000) if ended is not None else None
    return item.model_copy(
        update={
            "duration_ms": duration,
            "duration_source": "wall_clock" if duration is not None else None,
            "model_requests": len(requests),
            "tool_calls": len(tools),
            "usage": usage.totals,
            "usage_complete": usage.usage_complete,
            "usage_steps": usage.steps_with_usage,
            "steps_missing_usage": usage.steps_missing_usage,
            "compaction_passes": compaction_passes,
            "manifest_requests": sum(
                1 for request in requests if request.manifest_status == "present"
            ),
            "manifest_status": _manifest_rollup(requests),
        }
    )
