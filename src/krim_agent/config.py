"""Validated internal runtime settings projection.

Service configuration is read only by :mod:`krim_agent.configuration`; this
module deliberately has no environment or file-loading entrypoint.
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

DEFAULT_TOOL_PARALLELISM = 8


def minimum_limit(*values: int | None) -> int | None:
    """Intersect explicit limits without inventing a fallback product quota."""
    declared = [value for value in values if value is not None]
    return min(declared) if declared else None


class Settings(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, hide_input_in_errors=True
    )
    database: Path = Path(".data/krim-agent.db")
    provider: Literal["http"] = "http"
    api_key: SecretStr = Field(default=SecretStr(""), repr=False)
    system_prompt: str = "你是 Krim Agent 的 AI 助手。"
    target_version: str = "agent-v1"
    max_steps: int | None = Field(default=None, ge=1)
    max_tools_per_step: int | None = Field(default=None, ge=1)
    # 执行调度宽度，不限制任务总量。
    read_parallelism: int = Field(default=DEFAULT_TOOL_PARALLELISM, ge=1)
    max_tool_calls: int | None = Field(default=None, ge=1)
    max_active_runs: int | None = Field(default=None, ge=1)
    max_events: int | None = Field(default=None, ge=4)
    max_output_bytes: int | None = Field(default=None, ge=1)
    max_input_bytes: int | None = Field(default=None, ge=1)
    max_request_bytes: int | None = Field(default=None, ge=256)
    run_timeout: float | None = Field(default=None, gt=0)
    model_timeout: float | None = Field(default=None, gt=0)
    tool_timeout: float | None = Field(default=None, gt=0)
    # Parked confirmations have no product countdown. Explicit transport/test
    # windows remain supported without a 24-hour application ceiling.
    approval_timeout: float | None = Field(default=None, gt=0)

    identity_mode: Literal["auto", "managed", "legacy"] = "auto"
    gateway_display_name: str | None = Field(default=None, min_length=1, max_length=64)
    product_eval_enabled: bool = False
    product_eval_database: Path | None = None
    evaluation_automation_enabled: bool = False
    session_max_queued: int | None = Field(default=None, ge=1)
    session_max_open: int | None = Field(default=None, ge=1)
    session_max_turns: int | None = Field(default=None, ge=1)
    session_max_history_bytes: int | None = Field(default=None, ge=1)
    session_queue_timeout: float | None = Field(default=None, gt=0)
    context_max_bytes: int | None = Field(default=None, ge=256)
    context_max_messages: int | None = Field(default=None, ge=2)
    context_reserved_output_tokens: int | None = Field(default=None, ge=1)
    context_counter: Literal["approx-utf8/1"] = "approx-utf8/1"
    history_compaction: bool = True
    development_history_compaction: bool = True

    @field_validator("gateway_display_name")
    @classmethod
    def gateway_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        import unicodedata

        normalized = value.strip()
        if not normalized or any(unicodedata.category(c).startswith("C") for c in normalized):
            raise ValueError("网关显示名无效")
        return normalized

    def request_capacity(self, provider: object | None = None) -> int | None:
        """Intersect explicit transport limits; no default application byte cap."""
        declared = getattr(getattr(provider, "config", None), "max_request_bytes", None)
        candidates = [value for value in (self.max_request_bytes, declared)
                      if isinstance(value, int) and not isinstance(value, bool)]
        return min(candidates) if candidates else None

SESSION_FIELDS = frozenset(
    {
        "identity_mode",
        "gateway_display_name",
        "product_eval_enabled",
        "product_eval_database",
        "evaluation_automation_enabled",
        "session_max_queued",
        "session_max_open",
        "session_max_turns",
        "session_max_history_bytes",
        "session_queue_timeout",
        "context_max_bytes",
        "context_max_messages",
        "context_reserved_output_tokens",
        "context_counter",
        "history_compaction",
        "development_history_compaction",
    }
)
