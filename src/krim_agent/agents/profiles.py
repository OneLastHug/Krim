"""有限声明式 Agent 配置；资源名称由宿主绑定，不反序列化执行代码。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.budgets import UsageBudgetConfig
from krim_agent.context_engine import ContextRunConfig
from krim_agent.protocol.models import StrictModel


class AgentProfile(StrictModel):
    name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    description: str = Field(min_length=1, max_length=4096)
    provider_key: str = Field(default="runtime", min_length=1, max_length=64)
    tool_names: list[str] = Field(default_factory=list, max_length=128)
    instructions: str = Field(default="", max_length=8192)
    mode: Literal["agent", "ask", "plan", "auto"] = "agent"
    max_turns: int = Field(default=8, ge=1, le=128)
    max_steps: int = Field(default=8, ge=1, le=32)
    timeout_seconds: float = Field(default=60, gt=0, le=300)
    tool_timeout_seconds: float = Field(default=10, gt=0, le=120)
    persistent: bool = False
    context_config: ContextRunConfig = Field(default_factory=ContextRunConfig)
    usage_budget: UsageBudgetConfig | None = None

    @model_validator(mode="after")
    def bounded(self) -> AgentProfile:
        if not self.description.strip() or len(set(self.tool_names)) != len(self.tool_names):
            raise ValueError("描述不能为空，工具名必须唯一")
        if self.usage_budget is not None and (
            self.context_config.tokenizer == "approx-v1"
            or self.context_config.reserved_output_tokens <= 0
        ):
            raise ValueError("预算需要BPE与输出预留")
        return self


class AgentProfileVersion(StrictModel):
    name: str
    revision: int = Field(ge=1)
    enabled: bool
    config: AgentProfile
    config_hash: str


class AgentProfileUpdate(StrictModel):
    expected_revision: int = Field(ge=0)
    config: AgentProfile
    enabled: bool = True
