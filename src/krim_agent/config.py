"""Trusted host runtime configuration; credentials never come from the browser."""

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator


class Settings(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, hide_input_in_errors=True
    )
    database: Path = Path(".data/krim-agent.db")
    provider: Literal["script", "http"] = "script"
    http_mode: Literal["bounded-demo", "agent"] = "bounded-demo"
    api_key: SecretStr = Field(default=SecretStr(""), repr=False)
    system_prompt: str = "你是 Krim Agent 离线演示。"
    target_version: str = "demo-v1"
    max_steps: int | None = Field(default=None, ge=1)
    max_tools_per_step: int = Field(default=4, ge=1)
    read_parallelism: int = Field(default=4, ge=1)  # 执行调度宽度，不限制任务总量
    max_tool_calls: int | None = Field(default=None, ge=1)
    max_active_runs: int | None = Field(default=None, ge=1)
    max_events: int | None = Field(default=None, ge=4)
    max_output_bytes: int | None = Field(default=None, ge=1)
    max_input_bytes: int | None = Field(default=None, ge=1)
    max_request_bytes: int | None = Field(default=None, ge=256)
    run_timeout: float | None = Field(default=None, gt=0)
    model_timeout: float | None = Field(default=None, gt=0)
    tool_timeout: float | None = Field(default=None, gt=0)
    # 上限 86400：无状态停靠的人类尺度批复窗口（ADR-0015 §5）；默认 300 兼容 ADR-0014
    approval_timeout: float = Field(default=300, gt=0, le=86400)

    access_policy_file: Path | None = None
    identity_mode: Literal["auto", "managed", "legacy"] = "auto"
    gateway_display_name: str | None = Field(default=None, min_length=1, max_length=64)
    sandbox_config_file: Path | None = None
    development_config_file: Path | None = None
    workspace_profile_file: Path | None = None
    tool_config_file: Path | None = None
    personal_mcp_key_file: Path | None = None
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

    def request_capacity(self, provider: object | None = None) -> int:
        """Resolve request capacity from a host override and provider declaration.

        Scripted/local fixtures use the default ContextPolicy. HTTP providers
        declare their accepted request size in their trusted configuration.
        """
        from krim_agent.context.types import ContextPolicy

        declared = getattr(getattr(provider, "config", None), "max_request_bytes", None)
        candidates = [value for value in (self.max_request_bytes, declared)
                      if isinstance(value, int) and not isinstance(value, bool)]
        return min(candidates) if candidates else ContextPolicy().max_bytes

    @classmethod
    def from_env(cls) -> "Settings":
        values = {
            name: os.environ["KRIM_AGENT_" + name.upper()]
            for name in cls.model_fields
            if "KRIM_AGENT_" + name.upper() in os.environ
        }
        return cls.model_validate(values)


SESSION_FIELDS = frozenset(
    {
        "access_policy_file",
        "identity_mode",
        "gateway_display_name",
        "workspace_profile_file",
        "sandbox_config_file",
        "development_config_file",
        "tool_config_file",
        "personal_mcp_key_file",
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
