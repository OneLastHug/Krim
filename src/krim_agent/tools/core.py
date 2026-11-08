"""不可变工具定义、外部来源与 Hook 快照。"""

from __future__ import annotations

import inspect
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, JsonValue

from krim_agent.contracts import ToolObservation, fingerprint

from .mcp_schema import MCPContract
from .output import OutputContract, WireOutputSchema

type Decision = Literal["allow", "deny", "ask"]


@dataclass(frozen=True)
class Invocation:
    run_id: str
    call_id: str
    span_id: str
    step_index: int
    name: str
    arguments_json: str
    tool_hash: str
    provider_call_id: str | None = None


type Policy = Callable[[Invocation], Awaitable[Decision]]
type PreHook = Callable[[Invocation], Awaitable[str | None]]
type PostHook = Callable[[Invocation, ToolObservation], Awaitable[None]]
type Executor = Callable[[BaseModel], Awaitable[JsonValue]]
type ExternalExecutor = Callable[[Invocation, BaseModel], Awaitable[JsonValue]]


@dataclass(frozen=True)
class ToolSource:
    kind: Literal["http", "mcp"]
    server: str
    remote_name: str
    configuration_digest: str

    def descriptor(self) -> dict[str, JsonValue]:
        if (
            self.kind not in {"http", "mcp"}
            or re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", self.server) is None
            or not self.remote_name
            or len(self.remote_name) > 128
            or any(ord(char) < 32 for char in self.remote_name)
            or re.fullmatch(r"[0-9a-f]{64}", self.configuration_digest) is None
        ):
            raise ValueError("外部工具来源无效")
        return {
            "kind": self.kind,
            "server": self.server,
            "tool": self.remote_name,
            "configuration_digest": self.configuration_digest,
        }


@dataclass(frozen=True)
class ToolSpec:
    name: str
    parameters: type[BaseModel]
    executor: Executor
    output_schema: WireOutputSchema
    version: str
    implementation_digest: str
    risk: str = "read"
    scoped_executor: Callable[[Invocation, BaseModel, object], Awaitable[JsonValue]] | None = None
    capability: str | None = None
    description: str = ""
    source: ToolSource | None = None
    external_executor: ExternalExecutor | None = None
    overridable: bool = False
    binding_target: ToolSpec | None = None

    def __post_init__(self) -> None:
        if re.fullmatch(
            r"[a-z][a-z0-9_]{0,63}", self.name
        ) is None or not inspect.iscoroutinefunction(self.executor):
            raise ValueError("仅接受显式可信异步工具")
        external = self.external_executor is not None
        if external:
            if self.name in {
                "sandbox_project",
                "sandbox_read",
                "sandbox_list",
                "sandbox_search",
                "sandbox_write",
                "sandbox_mkdir",
                "sandbox_remove",
                "sandbox_patch",
                "sandbox_rename",
                "sandbox_command_run",
                "workspace_read",
                "workspace_search",
                "workspace_write",
                "command_run",
                "artifact_read",
                "plan_update",
            }:
                raise ValueError("外部工具不能伪装为私有scoped路由")
            if (
                type(self.source) is not ToolSource
                or not inspect.iscoroutinefunction(self.external_executor)
                or self.scoped_executor is not None
                or self.capability is not None
                or self.risk not in {"read", "write"}
                or self.source.kind == "http"
                and self.risk != "read"
            ):
                raise ValueError("外部工具须登记独立来源与受控执行器")
            self.source.descriptor()
        elif self.source is not None or (self.risk != "read" and self.scoped_executor is None):
            raise ValueError("legacy工具仅接受只读执行器")
        if self.scoped_executor is not None and (
            not inspect.iscoroutinefunction(self.scoped_executor)
            or self.capability is None
            or self.capability
            != {
                "sandbox_project": "read",
                "sandbox_read": "read",
                "sandbox_list": "read",
                "sandbox_search": "read",
                "sandbox_write": "write",
                "sandbox_mkdir": "write",
                "sandbox_remove": "write",
                "sandbox_patch": "write",
                "sandbox_rename": "write",
                "sandbox_command_run": "command",
                "workspace_read": "read",
                "workspace_search": "search",
                "workspace_write": "write",
                "command_run": "command",
                "artifact_read": "artifact",
                "plan_update": "plan",
            }.get(self.name)
            or self.risk
            != {"write": "write", "command": "exec", "plan": "write"}.get(self.capability, "read")
        ):
            raise ValueError("scoped工具须登记精确受控路由和风险")
        if self.scoped_executor is None and self.capability is not None:
            raise ValueError("legacy工具不能登记scoped能力")
        if (
            re.fullmatch(r"[0-9a-f]{64}", self.implementation_digest) is None
            or not self.version
            or len(self.description.encode()) > 8192
            or type(self.overridable) is not bool
        ):
            raise ValueError("工具须有宿主确认的实现摘要/版本")
        if self.binding_target is not None:
            target = self.binding_target
            if (
                not external
                or self.source is None
                or self.source.kind != "mcp"
                or type(target) is not ToolSpec
                or target.binding_target is not None
                or self.parameters is not target.parameters
                or self.executor is not target.executor
                or self.external_executor is not target.external_executor
                or self.source is not target.source
                or self.risk != target.risk
                or fingerprint(self.output_schema) != fingerprint(target.output_schema)
            ):
                raise ValueError("覆盖须绑定原始MCP定义，不能串联或换参数")
        if self.source is not None and self.source.kind == "mcp":
            MCPContract(self.output_schema)
        else:
            OutputContract(self.output_schema)

    def descriptor(self) -> dict[str, JsonValue]:
        value: dict[str, JsonValue] = {
            "name": self.name,
            "version": self.version,
            "implementation_digest": self.implementation_digest,
            "input_schema": self.parameters.model_json_schema(),
            "output_schema": OutputContract(self.output_schema).schema(),
        }
        if self.scoped_executor is not None:
            value.update(risk=self.risk, capability=self.capability, policy_version=(
                "sandbox/1" if self.name.startswith("sandbox_") else "workspace/1"))
        if self.source is not None:
            if type(self.source) is not ToolSource or self.external_executor is None:
                raise ValueError("外部来源绑定已变化")
            value.update(
                risk=self.risk, source=self.source.descriptor(), policy_version="external/1"
            )
        if self.description:
            value["description"] = self.description
        if self.binding_target is not None:
            value["binding"] = {
                "target_name": self.binding_target.name,
                "target_hash": self.binding_target.fingerprint,
            }
        return value

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.descriptor())
