"""Python SubAgent 生命周期类型；不引入 TypeScript 或商业平台 SDK。"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from typing import Literal
from uuid import uuid4

from pydantic import Field

from krim_agent.budgets import UsageBudgetConfig
from krim_agent.context_engine.types import ContextRunConfig
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelProvider
from krim_agent.tools import ToolRegistry
from krim_agent.tools.core import Policy
from krim_agent.usage import UsageTotals

AgentStatus = Literal[
    "queued", "running", "completed", "failed", "aborted", "interrupted", "destroyed"
]


class AgentError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ExecResult(StrictModel):
    agent_id: str
    run_id: str | None = None
    status: AgentStatus
    text: str = ""
    error: str | None = None
    code: str | None = None
    parent_tool_call_id: str | None = None
    turn: int = Field(ge=0)
    token_usage: UsageTotals | None = None
    usage_complete: bool = False


@dataclass(frozen=True)
class AgentDefinition:
    name: str
    provider: ModelProvider
    tools: ToolRegistry
    max_turns: int = 8
    timeout_seconds: float = 60
    max_steps: int = 8
    persistent: bool = False
    parent_agent_id: str | None = None
    parent_tool_call_id: str | None = None
    agent_id: str = ""
    policy: Policy | None = None
    tool_timeout_seconds: float = 10
    context_config: ContextRunConfig | None = None
    usage_budget: UsageBudgetConfig | None = None

    def __post_init__(self) -> None:
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", self.name) is None:
            raise ValueError("Agent 名称必须为 1–64 字符的字母、数字、下划线或连字符")
        if not 1 <= self.max_turns <= 128:
            raise ValueError("max_turns 必须在 1–128 之间")
        if not 0 < self.timeout_seconds <= 300:
            raise ValueError("timeout_seconds 必须在 0–300 秒之间")
        if not 1 <= self.max_steps <= 32:
            raise ValueError("max_steps 必须在 1–32 之间")
        if not 0 < self.tool_timeout_seconds <= 120:
            raise ValueError("tool_timeout_seconds 必须在 0–120 秒之间")
        if self.context_config is not None:
            if not isinstance(self.context_config, ContextRunConfig):
                raise ValueError("context_config 必须是可信 ContextRunConfig")
            object.__setattr__(
                self,
                "context_config",
                ContextRunConfig.model_validate(self.context_config.model_dump()),
            )
        if self.usage_budget is not None:
            object.__setattr__(
                self,
                "usage_budget",
                UsageBudgetConfig.model_validate(self.usage_budget.model_dump(mode="json")),
            )
            if (
                self.context_config is None
                or self.context_config.tokenizer == "approx-v1"
                or self.context_config.reserved_output_tokens <= 0
            ):
                raise ValueError("Agent 累计预算需要 BPE 上下文与正数输出预留")

    def with_id(self) -> AgentDefinition:
        return replace(self, agent_id=self.agent_id or str(uuid4()))
