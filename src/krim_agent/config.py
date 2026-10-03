"""只读取 KRIM_AGENT_ 环境变量，不自动联网或加载真实模型。"""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field, SecretStr


class Settings(BaseModel):
    database_url: str = "sqlite+aiosqlite:///./.data/krim-agent.db"
    api_key: SecretStr = SecretStr("")
    max_steps: int = Field(default=8, ge=1, le=32)
    tool_timeout_seconds: float = Field(default=10, gt=0, le=120)
    run_timeout_seconds: float = Field(default=60, gt=0, le=300)
    max_active_runs: int = Field(default=16, ge=1, le=64)
    context_strategy: Literal[
        "truncate", "sliding_window", "summarize_then_truncate", "annotated_compact"
    ] = "sliding_window"
    context_policy: Literal["never", "always", "auto"] = "auto"
    context_max_messages: int | None = Field(default=None, ge=2, le=10000)
    context_max_chars: int | None = Field(default=None, ge=128, le=10_000_000)
    context_max_tokens: int | None = Field(default=None, ge=32, le=2_500_000)

    @classmethod
    def from_env(cls) -> Settings:
        values = {
            name: os.environ[f"KRIM_AGENT_{name.upper()}"]
            for name in cls.model_fields
            if f"KRIM_AGENT_{name.upper()}" in os.environ
        }
        return cls.model_validate(values)
