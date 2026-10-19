"""只装配本机Krim Agent的离线配置；不支持旧供应商模式。"""

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
    api_key: SecretStr = Field(default=SecretStr(""), repr=False)
    system_prompt: str = "你是 Krim Agent 离线演示。"
    target_version: str = "demo-v1"
    max_steps: int = Field(default=8, ge=1, le=32)
    max_tools_per_step: int = Field(default=4, ge=1, le=16)
    max_tool_calls: int = Field(default=32, ge=1, le=128)
    max_active_runs: int = Field(default=4, ge=1, le=16)
    max_events: int = Field(default=2048, ge=4, le=8192)
    max_output_bytes: int = Field(default=262144, ge=1, le=1048576)
    max_input_bytes: int = Field(default=32768, ge=1, le=32768)
    max_request_bytes: int = Field(default=262144, ge=256, le=1048576)
    run_timeout: float = Field(default=30, gt=0, le=300)
    tool_timeout: float = Field(default=5, gt=0, le=30)
    approval_timeout: float = Field(default=20, gt=0, le=30)

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
