"""Krim Agent公共数据；有限 JSON 规则提取自原 protocol/models、json_data。"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

RunStatus = Literal[
    "queued", "running", "awaiting", "completed", "failed", "aborted", "interrupted"
]
TERMINAL = frozenset({"completed", "failed", "aborted", "interrupted"})
# awaiting（ADR-0015）：停靠等待人工审批——非终态；不挂协程、不占并发名额，
# 答复到达后由停靠恢复点续跑（见 runtime/recovery.py 与 ADR-0015 §3）。
ERROR_CODES = frozenset(
    {
        "account_management_unavailable",
        "accounts_not_initialized",
        "identity_mode_conflict",
        "mcp_unavailable",
        "mcp_invalid_json",
        "mcp_tool_failed",
        "mcp_output_too_large",
        "mcp_discovery_failed",
        "mcp_connection_closed",
        "mcp_tool_not_selected",
        "mcp_queue_full",
        "mcp_timeout",
        "mcp_connect_failed",
        "mcp_discovery_limit",
        "mcp_cursor_invalid",
        "web_invalid_input",
        "web_protocol",
        "web_extract_failed",
        "web_closed",
        "web_config",
        "web_timeout",
        "web_network",
        "web_http_error",
        "web_response_too_large",
        "web_unauthorized",
        "web_payment_required",
        "web_rate_limited",
        "web_usage_limit",
        "web_credits_exhausted",
        "web_redirect",
        "web_output_too_large",
        "summary_protocol",
        "summary_failed",
        "compaction_budget",
        "workspace_contract_required",
        "workspace_unavailable",
        "workspace_busy",
        "artifact_conflict",
        "wrap_reentry",
        "middleware_error",
        "completion_unverified",
        "input_revision_conflict",
        "sandbox_unavailable",
        "sandbox_setup",
        "sandbox_limit",
        "sandbox_cleanup_required",
        "sandbox_cleanup_failed",
        "artifact_invalid",
        "artifact_limit",
        "execution_unknown",
        "context_budget",
        "context_binding_changed",
        "access_policy_invalid",
        "session_closed",
        "queue_timeout",
        "invalid_input",
        "invalid_json",
        "invalid_output",
        "unknown_tool",
        "permission_denied",
        "approval_denied",
        "approval_expired",
        "approval_conflict",
        "approval_unavailable",
        "hook_denied",
        "pre_hook_error",
        "post_hook_error",
        "tool_error",
        "tool_timeout",
        "tool_binding_changed",
        "provider_event",
        "provider_config",
        "provider_request_limit",
        "provider_response_limit",
        "provider_event_limit",
        "provider_protocol",
        "provider_tool_arguments",
        "provider_usage_conflict",
        "provider_incomplete_stream",
        "provider_http_error",
        "provider_redirect",
        "provider_timeout",
        "provider_network",
        "provider_output_limit",
        "duplicate_tool_call",
        "step_limit",
        "tool_limit",
        "input_limit",
        "output_limit",
        "event_limit",
        "no_output",
        "run_timeout",
        "cancelled",
        "interrupted",
        "execution_error",
        "storage_unavailable",
        "run_closed",
        "not_found",
        "not_terminal",
        "key_conflict",
        "capacity",
        "closed",
        "self_wait",
        "invalid_cursor",
        "database_invalid",
        "database_busy",
        "readonly",
        "unauthorized",
        "origin_denied",
        "local_only",
        "body_too_large",
        "source_not_eligible",
        "source_changed",
        "candidate_input_mismatch",
        "revision_conflict",
        "schema_upgrade_required",
        "snapshot_changed",
        "target_binding_changed",
        "facts_corrupt",
    }
)


class ServiceError(Exception):
    def __init__(self, code: str, status: int = 409) -> None:
        self.code = code if code in ERROR_CODES else "execution_error"
        self.status = status
        super().__init__(self.code)


class StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, allow_inf_nan=False, hide_input_in_errors=True
    )


def check_json(value: object, *, max_bytes: int = 65536, max_depth: int = 16) -> None:
    def visit(item: object, depth: int) -> None:
        if depth > max_depth:
            raise ValueError("JSON 深度超限")
        if type(item) in (str, bool, int, float) or item is None:
            if type(item) is int and abs(item) > 9007199254740991:
                raise ValueError("JSON 整数超限")
            return
        if isinstance(item, dict):
            for key, child in item.items():
                if type(key) is not str:
                    raise ValueError("JSON 键必须为字符串")
                visit(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                visit(child, depth + 1)
        else:
            raise ValueError("非 JSON 值")

    visit(value, 0)
    if len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode()) > max_bytes:
        raise ValueError("JSON 字节超限")


def parse_json(raw: str) -> JsonValue:
    def pairs(items: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for key, value in items:
            if key in result:
                raise ValueError("JSON 重复键")
            result[key] = value
        return result

    def constant(_value: str) -> None:
        raise ValueError("JSON 非有限数")

    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    check_json(value, max_bytes=1048576)
    return cast(JsonValue, value)


def encoded(value: object) -> str:
    check_json(value, max_bytes=1048576)
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def fingerprint(value: object) -> str:
    return hashlib.sha256(encoded(value).encode()).hexdigest()


class RunRequest(StrictModel):
    text: str = Field(min_length=1, max_length=32768)
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    target: Literal[
        "demo", "target-v1", "target-v2", "target-bad", "target-http", "target-workspace"
    ] = "demo"

    @field_validator("text")
    @classmethod
    def bounded(cls, value: str) -> str:
        if not value.strip() or len(value.encode()) > 32768:
            raise ValueError("输入为空或字节超限")
        return value


class RunView(StrictModel):
    id: str
    request_key: str
    status: RunStatus
    trace_id: str
    root_span_id: str
    created_at: float
    finished_at: float | None = None
    code: str | None = None
    stop_requested: bool = False


class RunEvent(StrictModel):
    run_id: str
    seq: int = Field(ge=0)
    event_id: str
    kind: str
    data: dict[str, JsonValue]
    at: float = Field(default_factory=time.time)


class ToolObservation(StrictModel):
    call_id: str
    provider_call_id: str | None = None
    arguments: dict[str, JsonValue] | None = None
    span_id: str
    step_index: int
    name: str
    status: Literal["ok", "denied", "failed", "cancelled", "unknown"] = "unknown"
    executor_started: bool | None = None
    result_available: bool | None = None
    value: JsonValue = None
    code: str | None = None
    decision: Literal["allow", "deny", "ask"] = "deny"


class RunFacts(StrictModel):
    run: RunView
    text: str
    model_steps: int
    tools: list[ToolObservation]
    events: list[RunEvent]
    token_usage: dict[str, JsonValue] | None = None
    usage_complete: bool = False


class ApprovalReply(StrictModel):
    request_key: str = Field(min_length=1, max_length=128)
    call_id: str
    arguments_hash: str
    tool_hash: str
    allow: bool


class Message(StrictModel):
    role: Literal["system", "user", "assistant", "tool"]
    content: str
    call_id: str | None = None
    tool_calls: list[dict[str, JsonValue]] = Field(default_factory=list)
