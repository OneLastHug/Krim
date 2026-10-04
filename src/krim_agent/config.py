"""只读取 KRIM_AGENT_ 配置；默认离线，显式选择 provider 后按配置装配。"""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, model_validator


class Settings(BaseModel):
    database_url: str = "sqlite+aiosqlite:///./.data/krim-agent.db"
    api_key: SecretStr = SecretStr("")
    provider: Literal["fake", "openai-compatible"] = "fake"
    provider_base_url: str | None = None
    provider_model: str | None = None
    provider_api_key: SecretStr = SecretStr("")
    provider_timeout_seconds: float = Field(default=60, gt=0, le=600)
    provider_max_output_tokens: int | None = Field(default=None, ge=1, le=1_000_000)
    max_steps: int = Field(default=8, ge=1, le=32)
    tool_timeout_seconds: float = Field(default=10, gt=0, le=120)
    run_timeout_seconds: float = Field(default=60, gt=0, le=300)
    max_active_runs: int = Field(default=16, ge=1, le=64)
    agent_max_agents: int = Field(default=32, ge=1, le=1024)
    agent_max_concurrent: int = Field(default=8, ge=1, le=64)
    agent_max_recursion_depth: int = Field(default=4, ge=1, le=16)
    agent_max_turns: int = Field(default=8, ge=1, le=128)
    context_strategy: Literal[
        "truncate", "sliding_window", "summarize_then_truncate", "annotated_compact"
    ] = "sliding_window"
    context_policy: Literal["never", "always", "auto"] = "auto"
    context_max_messages: int | None = Field(default=None, ge=2, le=10000)
    context_max_chars: int | None = Field(default=None, ge=128, le=10_000_000)
    context_max_tokens: int | None = Field(default=None, ge=32, le=2_500_000)

    @model_validator(mode="after")
    def validate_provider(self) -> Settings:
        if self.provider == "openai-compatible" and (
            not self.provider_base_url or not self.provider_model
        ):
            raise ValueError("openai-compatible 需要 provider_base_url 和 provider_model")
        return self

    @classmethod
    def from_env(cls) -> Settings:
        values = {
            name: os.environ[f"KRIM_AGENT_{name.upper()}"]
            for name in cls.model_fields
            if f"KRIM_AGENT_{name.upper()}" in os.environ
        }
        return cls.model_validate(values)
