"""Python SubAgent 生命周期类型；不引入 TypeScript 或商业平台 SDK。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal
from uuid import uuid4

from pydantic import Field

from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelProvider
from krim_agent.tools import ToolRegistry

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

    def __post_init__(self) -> None:
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", self.name) is None:
            raise ValueError("Agent 名称必须为 1–64 字符的字母、数字、下划线或连字符")
        if not 1 <= self.max_turns <= 128:
            raise ValueError("max_turns 必须在 1–128 之间")
        if not 0 < self.timeout_seconds <= 300:
            raise ValueError("timeout_seconds 必须在 0–300 秒之间")
        if not 1 <= self.max_steps <= 32:
            raise ValueError("max_steps 必须在 1–32 之间")

    def with_id(self) -> AgentDefinition:
        return AgentDefinition(
            name=self.name,
            provider=self.provider,
            tools=self.tools,
            max_turns=self.max_turns,
            timeout_seconds=self.timeout_seconds,
            max_steps=self.max_steps,
            persistent=self.persistent,
            parent_agent_id=self.parent_agent_id,
            parent_tool_call_id=self.parent_tool_call_id,
            agent_id=self.agent_id or str(uuid4()),
        )
