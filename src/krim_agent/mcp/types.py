"""MCP server/工具桥配置；敏感认证与 OAuth 不在本次自动配置范围。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator

from krim_agent.protocol.models import StrictModel


class MCPServerConfig(StrictModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    transport: Literal["stdio", "streamable_http"]
    namespace: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    command: str | None = None
    args: list[str] = Field(default_factory=list, max_length=64)
    cwd: str | None = None
    env: dict[str, str] | None = None
    url: str | None = None
    allowed_hosts: set[str] = Field(default_factory=set)
    headers: dict[str, SecretStr] = Field(default_factory=dict)
    readonly_tools: set[str] = Field(default_factory=set)
    deferred_tools: set[str] = Field(default_factory=set)
    always_load_tools: set[str] = Field(default_factory=set)
    enabled: bool = True
    request_timeout_seconds: float = Field(default=10, gt=0, le=120)
    initialization_timeout_seconds: float = Field(default=120, gt=0, le=600)
    max_pending_requests: int = Field(default=64, ge=1, le=1024)

    @field_validator("name", "namespace")
    @classmethod
    def lower(cls, value: str | None) -> str | None:
        return value.lower() if value is not None else value

    @model_validator(mode="after")
    def validate_transport(self) -> MCPServerConfig:
        if self.transport == "stdio" and (not self.command or self.url is not None):
            raise ValueError("stdio MCP 必须有 command 且不能配置 url")
        if self.transport == "streamable_http" and (not self.url or self.command is not None):
            raise ValueError("HTTP MCP 必须有 url 且不能配置 command")
        return self


class MCPServerStatus(StrictModel):
    name: str
    namespace: str
    status: Literal["disabled", "connecting", "ready", "failed", "closing", "closed"]
    tools: list[str] = Field(default_factory=list)
    error: str | None = None
    error_code: str | None = None


class MCPToolInfo(StrictModel):
    server: str
    remote_name: str
    local_name: str
    description: str
    readonly: bool = False
