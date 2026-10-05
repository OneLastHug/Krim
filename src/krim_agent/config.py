"""只读取 KRIM_AGENT_ 配置；默认离线，显式选择 provider 后按配置装配。"""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, model_validator

from krim_agent.budgets import UsageBudgetConfig


class Settings(BaseModel):
    database_url: str = "sqlite+aiosqlite:///./.data/krim-agent.db"
    api_key: SecretStr = SecretStr("")
    provider: Literal["fake", "openai-compatible"] = "fake"
    provider_base_url: str | None = None
    provider_model: str | None = None
    provider_api_key: SecretStr = SecretStr("")
    provider_timeout_seconds: float = Field(default=60, gt=0, le=600)
    provider_max_output_tokens: int | None = Field(default=None, ge=1, le=1_000_000)
    provider_include_usage: bool = False
    max_steps: int = Field(default=8, ge=1, le=32)
    tool_timeout_seconds: float = Field(default=10, gt=0, le=120)
    interactive_tools: bool = False
    interaction_timeout_seconds: float = Field(default=120, gt=0, le=300)
    run_timeout_seconds: float = Field(default=60, gt=0, le=300)
    max_active_runs: int = Field(default=16, ge=1, le=64)
    agent_max_agents: int = Field(default=32, ge=1, le=1024)
    agent_max_concurrent: int = Field(default=8, ge=1, le=64)
    agent_max_recursion_depth: int = Field(default=4, ge=1, le=16)
    agent_max_turns: int = Field(default=8, ge=1, le=128)
    usage_budget: UsageBudgetConfig | None = None
    agent_usage_budget: UsageBudgetConfig | None = None
    workflow_usage_budget: UsageBudgetConfig | None = None
    usage_budget_scope: str = Field(default="default", min_length=1, max_length=128)
    context_strategy: Literal[
        "truncate", "sliding_window", "summarize_then_truncate", "annotated_compact"
    ] = "sliding_window"
    context_policy: Literal["never", "always", "auto"] = "auto"
    context_max_messages: int | None = Field(default=None, ge=2, le=10000)
    context_max_chars: int | None = Field(default=None, ge=128, le=10_000_000)
    context_max_tokens: int | None = Field(default=None, ge=32, le=2_500_000)
    context_tokenizer: Literal["approx-v1", "cl100k_base", "o200k_base"] = "approx-v1"
    context_reserved_output_tokens: int = Field(default=0, ge=0, le=1_000_000)
    model_context_window_tokens: int | None = Field(default=None, ge=32, le=2_500_000)
    model_framing_reserve_tokens: int = Field(default=64, ge=0, le=100_000)
    context_compact_mode: Literal["automatic", "confirmation"] = "automatic"
    context_proposal_ttl_seconds: float = Field(default=300, ge=1, le=3600)
    context_notice_messages: int | None = Field(default=None, ge=2, le=10000)
    context_trigger_ratio: float | None = Field(default=None, gt=0, le=1, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_provider(self) -> Settings:
        if self.context_compact_mode == "confirmation" and self.context_strategy not in {
            "summarize_then_truncate",
            "annotated_compact",
        }:
            raise ValueError("压缩确认模式需要摘要策略")
        if self.context_trigger_ratio is not None and self.model_context_window_tokens is None:
            raise ValueError("容量比例触发需要显式模型窗口")
        if any(
            value is not None
            for value in (self.usage_budget, self.agent_usage_budget, self.workflow_usage_budget)
        ):
            if self.context_tokenizer == "approx-v1" or self.context_reserved_output_tokens <= 0:
                raise ValueError("累计预算需要显式 BPE tokenizer 与正数输出预留")
        if self.provider == "openai-compatible" and (
            not self.provider_base_url or not self.provider_model
        ):
            raise ValueError("openai-compatible 需要 provider_base_url 和 provider_model")
        if self.model_context_window_tokens is not None:
            if self.context_tokenizer == "approx-v1":
                raise ValueError("硬模型窗口需要显式 BPE tokenizer，不能按字符近似")
            if self.context_reserved_output_tokens <= 0:
                raise ValueError("硬模型窗口必须显式预留输出 token")
            if (
                self.context_reserved_output_tokens + self.model_framing_reserve_tokens
                >= self.model_context_window_tokens
            ):
                raise ValueError("输出/封装预留必须小于模型窗口")
            if (
                self.provider_max_output_tokens is not None
                and self.provider_max_output_tokens > self.context_reserved_output_tokens
            ):
                raise ValueError("provider 输出上限不能大于上下文输出预留")
        return self

    @classmethod
    def from_env(cls) -> Settings:
        values: dict[str, object] = {
            name: os.environ[f"KRIM_AGENT_{name.upper()}"]
            for name in cls.model_fields
            if f"KRIM_AGENT_{name.upper()}" in os.environ
        }
        for name in ("usage_budget", "agent_usage_budget", "workflow_usage_budget"):
            if name in values:
                values[name] = UsageBudgetConfig.model_validate_json(str(values[name]))
        return cls.model_validate(values)
