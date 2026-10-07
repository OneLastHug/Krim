"""可信 Agent 目录与模型可调用的 Python Agent 工具适配。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from krim_agent.protocol.models import StrictModel
from krim_agent.tools import ToolRegistry
from krim_agent.tools.core import ToolContext, ToolDefinition, ToolError

from .manager import AgentHandle, AgentManager
from .types import AgentDefinition, AgentError

CatalogRisk = Literal["read", "write"]


class AgentToolInput(StrictModel):
    prompt: str = Field(min_length=1, max_length=8192)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("prompt")
    @classmethod
    def nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Agent prompt 不能只有空白")
        return value


class AgentDescriptor(StrictModel):
    name: str
    description: str
    tool_name: str
    risk: CatalogRisk
    max_turns: int
    max_steps: int
    persistent: bool = False
    exposed_as_tool: bool = True
    profile_revision: int | None = None


@dataclass(frozen=True)
class CatalogEntry:
    definition: AgentDefinition
    description: str
    risk: CatalogRisk
    tool_name: str
    expose_as_tool: bool = True

    def descriptor(self) -> AgentDescriptor:
        return AgentDescriptor(
            name=self.definition.name,
            description=self.description,
            tool_name=self.tool_name,
            risk=self.risk,
            max_turns=self.definition.max_turns,
            max_steps=self.definition.max_steps,
            persistent=self.definition.persistent,
            exposed_as_tool=self.expose_as_tool,
        )


class AgentCatalog:
    """只保存可信 Python 定义；目录数据不接受模型输入，也不序列化 callable。"""

    def __init__(self, manager: AgentManager) -> None:
        self.manager = manager
        self._entries: dict[str, CatalogEntry] = {}

    def register(
        self,
        name: str,
        definition: AgentDefinition,
        *,
        description: str,
        risk: CatalogRisk = "write",
        expose_as_tool: bool = True,
        tool_name: str | None = None,
    ) -> AgentDescriptor:
        if name != definition.name:
            raise ValueError("目录名称必须与 AgentDefinition.name 一致")
        if name in self._entries:
            raise ValueError("Agent 目录名称重复")
        public_name = (
            tool_name
            if tool_name is not None
            else f"agent_{definition.name.lower().replace('-', '_')}"
        )
        entry = CatalogEntry(definition, description, risk, public_name, expose_as_tool)
        descriptor = entry.descriptor()
        if not description.strip() or len(description) > 4096:
            raise ValueError("Agent 描述必须为 1–4096 字符")
        if expose_as_tool and definition.persistent:
            raise ValueError("Agent 工具仅支持工具作用域；persistent 定义可只登记到目录")
        if risk == "read" and any(tool.risk != "read" for tool in definition.tools.definitions):
            raise ValueError("read Agent 不能包含 write 子工具")
        if expose_as_tool and entry.tool_name in self._tool_names():
            raise ValueError("Agent 工具名称重复")
        if expose_as_tool:
            self._tool(entry)
        self._entries[name] = entry
        return descriptor

    def _tool_names(self) -> set[str]:
        return {entry.tool_name for entry in self._entries.values() if entry.expose_as_tool}

    def descriptors(self) -> list[AgentDescriptor]:
        return [entry.descriptor() for entry in self._entries.values()]

    def get(self, name: str) -> CatalogEntry:
        try:
            return self._entries[name]
        except KeyError as exc:
            raise ToolError("agent_not_found", f"未登记 Agent: {name}") from exc

    def install_tools(self, registry: ToolRegistry, *, names: list[str] | None = None) -> list[str]:
        if names is not None and len(names) != len(set(names)):
            raise ValueError("登记的 Agent 名称不能重复")
        selected = [
            self.get(name) for name in (names if names is not None else list(self._entries))
        ]
        definitions = [self._tool(entry) for entry in selected if entry.expose_as_tool]
        existing = {definition.name for definition in registry.definitions}
        conflicts = existing.intersection(definition.name for definition in definitions)
        if conflicts:
            raise ValueError(f"Agent 工具与现有工具冲突: {sorted(conflicts)}")
        registry.register_many(*definitions)
        return [definition.name for definition in definitions]

    def as_tool(self, name: str) -> ToolDefinition:
        entry = self.get(name)
        if not entry.expose_as_tool:
            raise ToolError("agent_not_exposed", "Agent 未声明为模型工具")
        return self._tool(entry)

    def _tool(self, entry: CatalogEntry) -> ToolDefinition:
        if entry.risk == "read" and any(
            tool.risk != "read" for tool in entry.definition.tools.definitions
        ):
            raise ValueError("read Agent 不能包含 write 子工具")

        async def execute(arguments: BaseModel, context: ToolContext) -> dict[str, object]:
            payload = AgentToolInput.model_validate(arguments.model_dump())
            if entry.risk == "read" and any(
                tool.risk != "read" for tool in entry.definition.tools.definitions
            ):
                raise ToolError("agent_risk_changed", "只读 Agent 的子工具风险已变化")
            definition = replace(
                entry.definition,
                agent_id="",
                parent_agent_id=context.agent_id,
                parent_tool_call_id=context.call_id,
                persistent=False,
                tool_timeout_seconds=min(entry.definition.tool_timeout_seconds, context.timeout),
            )
            try:
                handle: AgentHandle = await self.manager.spawn(definition, parent_context=context)
            except AgentError as exc:
                raise ToolError(exc.code, str(exc)) from exc
            try:
                result = await handle.execute(
                    payload.prompt,
                    payload.idempotency_key
                    or hashlib.sha256(
                        f"{context.run_id}:{context.call_id}:{entry.definition.name}".encode()
                    ).hexdigest(),
                )
                if result.status != "completed":
                    raise ToolError(result.code or "agent_failed", result.error or "Agent 执行失败")
                return result.model_dump(mode="json")
            finally:
                await handle.destroy()

        return ToolDefinition(
            name=entry.tool_name,
            description=entry.description,
            parameters=AgentToolInput,
            executor=execute,
            risk=entry.risk,
        )
