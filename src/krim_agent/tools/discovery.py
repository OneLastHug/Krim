"""每次模型运行的工具暴露状态；设计参考 SRC-OSS-016，合约见 ADR-0027。"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from types import MappingProxyType

from pydantic import BaseModel, Field, field_validator

from krim_agent.protocol.models import StrictModel

from .core import Risk, ToolContext, ToolDefinition, ToolResult


class ToolSearchInput(StrictModel):
    query: str = Field(min_length=1, max_length=512)
    limit: int = Field(default=8, ge=1, le=16)

    @field_validator("query")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("工具搜索 query 不能是空白")
        return value


class ToolMatch(StrictModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    description: str = Field(max_length=512)
    risk: Risk


class ToolSearchOutput(StrictModel):
    tools: list[ToolMatch] = Field(max_length=16)


def _terms(value: str) -> set[str]:
    normalized = re.sub(r"([a-z\d])([A-Z])", r"\1 \2", value).casefold()
    return set(re.findall(r"[^\W_]+", normalized, flags=re.UNICODE))


@dataclass
class PreparedToolStep:
    definitions: dict[str, ToolDefinition]
    searches: dict[str, set[str]] = field(default_factory=dict)

    def bindings(self) -> MappingProxyType[str, ToolDefinition]:
        return MappingProxyType(self.definitions)


class ToolDiscovery:
    def __init__(self, definitions: Sequence[ToolDefinition]) -> None:
        self.definitions = {definition.name: definition for definition in definitions}
        self.deferred = {
            definition.name
            for definition in definitions
            if definition.should_defer and not definition.always_load
        }
        if self.deferred and "tool_search" in self.definitions:
            raise ValueError("延迟工具目录不能占用保留名称 tool_search")
        self.loaded = set(self.definitions) - self.deferred

    def prepare(self) -> PreparedToolStep:
        step = PreparedToolStep(
            {
                name: definition
                for name, definition in self.definitions.items()
                if name in self.loaded
            }
        )
        if not self.deferred:
            return step

        async def search(arguments: BaseModel, context: ToolContext) -> ToolSearchOutput:
            payload = ToolSearchInput.model_validate(arguments)
            terms = _terms(payload.query)
            ranked: list[tuple[int, str]] = []
            for name in self.deferred:
                definition = self.definitions[name]
                name_terms = _terms(name)
                description_terms = _terms(definition.description)
                score = sum(
                    2 if term in name_terms else 1
                    for term in terms
                    if term in name_terms or term in description_terms
                )
                if payload.query.casefold() in name.casefold():
                    score += 3
                elif payload.query.casefold() in definition.description.casefold():
                    score += 1
                if score:
                    ranked.append((-score, name))
            ranked.sort()
            matches = [name for _score, name in ranked[: payload.limit]]
            step.searches[context.call_id] = set(matches)
            return ToolSearchOutput(
                tools=[
                    ToolMatch(
                        name=name,
                        description=self.definitions[name].description[:512],
                        risk=self.definitions[name].risk,
                    )
                    for name in matches
                ]
            )

        step.definitions["tool_search"] = ToolDefinition(
            "tool_search",
            "搜索按需工具的名称与描述；找到后从下一模型步骤开始加载",
            ToolSearchInput,
            search,
            "read",
            output_schema=ToolSearchOutput,
        )
        return step

    def consume(
        self, step: PreparedToolStep, call_id: str, name: str, result: ToolResult
    ) -> list[str]:
        candidates = step.searches.pop(call_id, set())
        if name != "tool_search" or result.is_error or not candidates:
            return []
        data = result.data
        if not isinstance(data, dict):
            return []
        entries = data.get("tools")
        if not isinstance(entries, list):
            return []
        accepted: set[str] = set()
        for item in entries:
            if isinstance(item, dict):
                identifier = item.get("name")
                if isinstance(identifier, str) and identifier in candidates:
                    accepted.add(identifier)
        added = sorted(accepted - self.loaded)
        self.loaded.update(accepted)
        return added
