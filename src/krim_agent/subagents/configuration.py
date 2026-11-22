"""Subagent configuration models for the trusted ``krim.yaml`` document.

Profiles reference named model routes by name and never carry endpoints or
credentials themselves; the primary route stays the existing single ``model``
section. One-shot process entries only accept host-preconfigured ``argv``,
while their actual execution remains in the existing sandbox modules.
Everything here is parsed once at startup and frozen afterwards.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import StrictModel

SubagentProvider = Literal["sdk_session", "process"]
SubagentWorkspaceMode = Literal["shared_read_only", "isolated_write"]

ROUTE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


def _route_name(value: str | None) -> str | None:
    if value is None or ROUTE_NAME_PATTERN.fullmatch(value):
        return value
    raise ValueError("route names use lowercase letters, digits, '-' or '_'")


class SubagentProfileConfiguration(StrictModel):
    """One declared worker route. Role names carry no permissions.

    ``model`` names the primary route (``default``) or a named ``models``
    entry; ``process`` is set exactly when ``provider`` is ``process``.
    Limits narrow the host execution policy further; ``None`` inherits it.
    """

    model: str = "default"
    provider: SubagentProvider = "sdk_session"
    process: str | None = None
    tools: tuple[str, ...] | None = None
    workspace: SubagentWorkspaceMode = "shared_read_only"
    max_depth: int = Field(default=0, ge=0, le=16)
    context_max_bytes: int | None = Field(default=None, ge=256)
    context_max_messages: int | None = Field(default=None, ge=2)
    max_output_tokens: int | None = Field(default=None, ge=1)
    max_output_bytes: int | None = Field(default=None, ge=1)
    structured_output: bool = False

    @field_validator("model", "process")
    @classmethod
    def _named_route(cls, value: str | None) -> str | None:
        return _route_name(value)

    @field_validator("tools", mode="before")
    @classmethod
    def _tool_sequence(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("tools")
    @classmethod
    def _narrowed_tools(cls, value: tuple[str, ...] | None) -> tuple[str, ...] | None:
        if value is None:
            return None
        if (
            len(value) > 256
            or len(set(value)) != len(value)
            or any(
                not name
                or len(name) > 256
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in name)
                for name in value
            )
        ):
            raise ValueError("invalid narrowed tool names")
        return value

    @model_validator(mode="after")
    def _process_route(self) -> SubagentProfileConfiguration:
        if (self.provider == "process") != (self.process is not None):
            raise ValueError("provider 'process' requires exactly one configured process entry")
        return self


class SubagentProcessConfiguration(StrictModel):
    """Host-preconfigured one-shot command; the model only selects it by name."""

    argv: tuple[str, ...] = Field(min_length=1)

    @field_validator("argv", mode="before")
    @classmethod
    def _argv_sequence(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("argv")
    @classmethod
    def _bounded_argv(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if (
            len(value) > 64
            or sum(len(arg) for arg in value) > 32768
            or any(
                not arg
                or len(arg) > 4096
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in arg)
                for arg in value
            )
        ):
            raise ValueError("invalid process argv")
        return value


class SubagentsConfiguration(StrictModel):
    """Delegation switch plus separate residency, request, process, message,
    event and budget limits. Disabled by default; the numeric defaults are
    tunable starting points for the planned task-set evaluation, not product
    quotas.
    """

    enabled: bool = False
    profiles: dict[str, SubagentProfileConfiguration] = Field(default_factory=dict)
    processes: dict[str, SubagentProcessConfiguration] = Field(default_factory=dict)
    default_profile: str | None = None
    # 驻留成员数；历史成员与结果不占驻留槽，清理中的实例继续占容量。
    max_resident_agents: int = Field(default=8, ge=1)
    # 执行中的子模型请求数；与主请求公平准入，不与驻留合并核算。
    max_model_requests: int = Field(default=4, ge=1)
    # 工具进程数；与驻留、模型请求分开核算。
    max_tool_processes: int = Field(default=16, ge=1)
    max_depth: int = Field(default=2, ge=0, le=16)
    max_message_bytes: int = Field(default=262_144, ge=256)
    max_queued_messages: int = Field(default=128, ge=1)
    max_event_bytes: int = Field(default=65_536, ge=256)
    max_cached_events: int = Field(default=4_096, ge=1)
    # 每棵代理树的累计模型请求预算；None 表示沿用宿主剩余执行预算。
    max_requests_per_tree: int | None = Field(default=None, ge=1)

    @field_validator("default_profile")
    @classmethod
    def _known_profile(cls, value: str | None) -> str | None:
        return _route_name(value)

    @model_validator(mode="after")
    def _consistent_names(self) -> SubagentsConfiguration:
        for section, names in (("profiles", self.profiles), ("processes", self.processes)):
            for name in names:
                if not ROUTE_NAME_PATTERN.fullmatch(name):
                    raise ValueError(f"invalid {section} entry name")
        if self.default_profile is not None and self.default_profile not in self.profiles:
            raise ValueError("default_profile must name a configured profile")
        return self


DEFAULT_WORKER_PROFILE = SubagentProfileConfiguration()
"""Implicit worker route used when a spawn request names no profile."""


def resolve_worker_profile(
    subagents: SubagentsConfiguration, name: str | None
) -> SubagentProfileConfiguration:
    """Resolve a spawn profile request against the frozen configuration.

    Unknown profile names raise ``KeyError``; callers map that to their own
    typed error with spawn context.
    """
    if name is None:
        if subagents.default_profile is not None:
            return subagents.profiles[subagents.default_profile]
        return DEFAULT_WORKER_PROFILE
    return subagents.profiles[name]
