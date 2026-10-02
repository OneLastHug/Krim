"""只读取 KRIM_AGENT_ 环境变量，不自动联网或加载真实模型。"""

from __future__ import annotations

import os

from pydantic import BaseModel, Field, SecretStr


class Settings(BaseModel):
    database_url: str = "sqlite+aiosqlite:///./.data/krim-agent.db"
    api_key: SecretStr = SecretStr("")
    max_steps: int = Field(default=8, ge=1, le=32)
    tool_timeout_seconds: float = Field(default=10, gt=0, le=120)
    run_timeout_seconds: float = Field(default=60, gt=0, le=300)
    max_active_runs: int = Field(default=16, ge=1, le=64)

    @classmethod
    def from_env(cls) -> Settings:
        values = {
            name: os.environ[f"KRIM_AGENT_{name.upper()}"]
            for name in cls.model_fields
            if f"KRIM_AGENT_{name.upper()}" in os.environ
        }
        return cls.model_validate(values)
