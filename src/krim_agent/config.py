"""可信宿主的有界运行配置；不从浏览器接收供应商凭据。"""

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
    max_steps: int = Field(default=8, ge=1, le=32)
    max_tools_per_step: int = Field(default=4, ge=1, le=16)
    read_parallelism: int = Field(default=4, ge=1, le=16)  # 相邻只读调用并发上限
    max_tool_calls: int = Field(default=32, ge=1, le=128)
    max_active_runs: int = Field(default=4, ge=1, le=16)
    max_events: int = Field(default=2048, ge=4, le=8192)
    max_output_bytes: int = Field(default=262144, ge=1, le=1048576)
    max_input_bytes: int = Field(default=32768, ge=1, le=32768)
    max_request_bytes: int = Field(default=262144, ge=256, le=1048576)
    run_timeout: float = Field(default=30, gt=0, le=300)
    tool_timeout: float = Field(default=5, gt=0, le=30)
    # 上限 86400：无状态停靠的人类尺度批复窗口（ADR-0015 §5）；默认 300 兼容 ADR-0014
    approval_timeout: float = Field(default=300, gt=0, le=86400)

    access_policy_file: Path | None = None
    workspace_profile_file: Path | None = None
    tool_config_file: Path | None = None
    product_eval_enabled: bool = False
    product_eval_database: Path | None = None
    evaluation_automation_enabled: bool = False
    session_max_queued: int = Field(default=8, ge=1, le=16)
    session_max_open: int = Field(default=32, ge=1, le=64)
    session_max_turns: int = Field(default=256, ge=1, le=1024)
    session_max_history_bytes: int = Field(default=4194304, ge=32768, le=16777216)
    session_queue_timeout: float = Field(default=300, gt=0, le=900)
    context_max_bytes: int = Field(default=131072, ge=256, le=1048576)
    context_max_messages: int = Field(default=256, ge=2, le=1024)
    context_reserved_output_tokens: int | None = Field(default=None, ge=1, le=32768)
    context_counter: Literal["approx-utf8/1"] = "approx-utf8/1"

    @field_validator("system_prompt")
    @classmethod
    def prompt_size(cls, value: str) -> str:
        if len(value.encode()) > 16384:
            raise ValueError("系统指令字节超限")
        return value

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
        "workspace_profile_file",
        "tool_config_file",
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
    }
)
