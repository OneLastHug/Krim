"""Model-visible requests contain goals and names, never host authority."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.contracts import StrictModel


class SpawnAgent(StrictModel):
    task: str = Field(min_length=1, max_length=262144)
    name: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,47}$")
    profile: str | None = Field(default=None, max_length=64)
    context: Literal["fresh", "fork"] = "fresh"
    background: str = Field(default="", max_length=262144)
    prefix_end: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def valid_context(self) -> SpawnAgent:
        if not self.task.strip() or self.context == "fresh" and self.prefix_end is not None:
            raise ValueError("task is empty or a fresh context specifies a fork position")
        return self


class SendAgentMessage(StrictModel):
    target: str = Field(min_length=1, max_length=256)
    text: str = Field(min_length=1, max_length=262144)
    wake: Literal["auto", "queue_only"] = "auto"
    new_task: bool = False
    task_id: str | None = Field(default=None, max_length=36)
    send_key: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def valid_task(self) -> SendAgentMessage:
        if not self.text.strip() or self.new_task and self.task_id is not None:
            raise ValueError("message is empty or a new task also specifies an existing task")
        return self


class WaitAgent(StrictModel):
    after_seq: int = Field(default=0, ge=0)
    after_notification_seq: int = Field(default=0, ge=0)
    seconds: float = Field(default=30.0, ge=0, le=300)


class ListAgents(StrictModel):
    after_path: str | None = Field(default=None, max_length=256)
    limit: int = Field(default=64, ge=1, le=256)


class ControlAgent(StrictModel):
    target: str = Field(min_length=1, max_length=256)
    activation_id: str | None = Field(default=None, max_length=36)
    task_id: str | None = Field(default=None, max_length=36)
