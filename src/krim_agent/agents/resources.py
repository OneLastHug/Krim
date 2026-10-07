"""可信宿主资源版本与生命周期；HTTP 不导入代码、URL 或密钥。"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from typing import Protocol, cast

from pydantic import JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelProvider
from krim_agent.providers.openai_compatible import OpenAICompatibleProvider
from krim_agent.tools import ToolRegistry


class ClosableProvider(ModelProvider, Protocol):
    async def close(self) -> None: ...


class AgentResourceView(StrictModel):
    key: str
    kind: str
    name: str
    version: str | None
    fingerprint: str
    owned: bool = False
    available: bool = True


def provider_fingerprint(provider: ModelProvider) -> str:
    payload: dict[str, JsonValue] = {
        "name": provider.name,
        "class": f"{type(provider).__module__}.{type(provider).__qualname__}",
    }
    if isinstance(provider, OpenAICompatibleProvider):
        # 只纳入配置摘要；密钥轮换不能改变模型定义，原 URL 不进入公开视图。
        config = provider.config.model_dump(mode="json", exclude={"api_key"})
        payload["configuration"] = hashlib.sha256(
            json.dumps(config, sort_keys=True).encode()
        ).hexdigest()
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


class AgentResourceRegistry:
    def __init__(self, providers: dict[str, ModelProvider], tools: ToolRegistry) -> None:
        self.providers, self.tools = providers, tools
        self.provider_versions: dict[str, str] = {}
        self.tool_versions: dict[str, str] = {}
        self._disabled: set[str] = set()
        self._owned: dict[int, ClosableProvider] = {}
        self._closed = False

    @staticmethod
    def _version(value: str) -> str:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) is None:
            raise ValueError("资源版本须为1–128字符的公开字母、数字、点、下划线或连字符")
        return value

    def provider(self, key: str) -> ModelProvider:
        if self._closed or key in self._disabled or key not in self.providers:
            raise KeyError(key)
        return self.providers[key]

    def register_provider(
        self, key: str, provider: ModelProvider, *, version: str, owned: bool = False
    ) -> None:
        if self._closed:
            raise ServiceError("agent_resources_closed", "Agent资源登记已关闭", 503)
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", key) is None:
            raise ValueError("Provider key须为1–64字符的字母、数字、下划线或连字符")
        selected_version = self._version(version)
        if key not in self.providers and len(self.providers) >= 64:
            raise ServiceError("agent_resource_limit", "Provider登记数量已达上限", 429)
        if owned:
            if not inspect.iscoroutinefunction(getattr(provider, "close", None)):
                raise ValueError("owned Provider必须实现异步close")
            if id(provider) not in self._owned and len(self._owned) >= 128:
                raise ServiceError("agent_resource_limit", "拥有的Provider资源已达上限", 429)
            self._owned[id(provider)] = cast(ClosableProvider, provider)
        self.providers[key] = provider
        self.provider_versions[key] = selected_version
        self._disabled.discard(key)

    def register_tool_version(self, name: str, version: str) -> None:
        if self._closed:
            raise ServiceError("agent_resources_closed", "Agent资源登记已关闭", 503)
        if name not in {tool.name for tool in self.tools.definitions}:
            raise ValueError("工具版本只能绑定宿主已登记工具")
        self.tool_versions[name] = self._version(version)

    def disable_provider(self, key: str) -> None:
        if key not in self.providers:
            raise KeyError(key)
        self._disabled.add(key)

    def fingerprint(self, key: str, names: list[str]) -> str:
        provider = self.provider(key)
        # 缺省资源版本保持0021 Profile指纹；工具Schema已由AgentDefinition冻结。
        payload: dict[str, JsonValue] = {
            "key": key,
            "name": provider.name,
            "model": provider.config.model
            if isinstance(provider, OpenAICompatibleProvider)
            else None,
        }
        if key in self.provider_versions:
            payload["provider_version"] = self.provider_versions[key]
        versions = {
            name: self.tool_versions[name] for name in sorted(names) if name in self.tool_versions
        }
        if versions:
            payload["tool_versions"] = cast(dict[str, JsonValue], versions)
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def manifest(self, key: str, names: list[str]) -> dict[str, JsonValue]:
        return {
            "provider_key": key,
            "provider_version": self.provider_versions.get(key),
            "provider_fingerprint": self.provider_signature(key),
            "tool_versions": {name: self.tool_versions.get(name) for name in sorted(names)},
        }

    def provider_signature(self, key: str) -> str:
        base = provider_fingerprint(self.providers[key])
        if key not in self.provider_versions:
            return base
        return hashlib.sha256(
            json.dumps(
                {
                    "configuration": base,
                    "version": self.provider_versions[key],
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()

    def views(self) -> list[AgentResourceView]:
        values = [
            AgentResourceView(
                key=key,
                kind="provider",
                name=provider.name,
                version=self.provider_versions.get(key),
                fingerprint=self.provider_signature(key),
                owned=id(provider) in self._owned,
                available=not self._closed and key not in self._disabled,
            )
            for key, provider in sorted(self.providers.items())
        ]
        values.extend(
            AgentResourceView(
                key=tool.name,
                kind="tool",
                name=tool.name,
                version=self.tool_versions.get(tool.name),
                fingerprint=hashlib.sha256(
                    json.dumps(
                        {
                            "schema": tool.schema().model_dump(mode="json"),
                            "version": self.tool_versions.get(tool.name),
                        },
                        sort_keys=True,
                    ).encode()
                ).hexdigest(),
                available=not self._closed,
            )
            for tool in self.tools.definitions
        )
        return values

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        failed = False
        for provider in self._owned.values():
            try:
                await provider.close()
            except Exception:
                failed = True
        self._owned.clear()
        if failed:
            raise ServiceError("agent_resource_close_failed", "部分Provider资源关闭失败", 503)
