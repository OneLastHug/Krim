"""可信宿主资源版本与生命周期；HTTP 不导入代码、URL 或密钥。"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

from pydantic import JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelProvider
from krim_agent.providers.openai_compatible import OpenAICompatibleProvider
from krim_agent.tools import ToolRegistry
from krim_agent.tools.workspace import workspace_tools
from krim_agent.workspace import Workspace, WorkspaceBinding


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


@dataclass(frozen=True)
class WorkspaceResource:
    binding: WorkspaceBinding
    owned: bool


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
        self.workspaces: dict[str, WorkspaceResource] = {}
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

    @staticmethod
    def _resource_key(value: str, label: str) -> str:
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", value) is None:
            raise ValueError(f"{label}须为1–64字符的字母、数字、下划线或连字符")
        return value

    def register_workspace(
        self,
        key: str,
        root: str | Path | Workspace,
        *,
        version: str,
        owned: bool = False,
        writable: bool = False,
    ) -> WorkspaceBinding:
        """可信宿主登记工作区；HTTP/模型不能调用此入口或提供root。"""
        if self._closed:
            raise ServiceError("agent_resources_closed", "Agent资源登记已关闭", 503)
        selected_key = self._resource_key(key, "Workspace key")
        selected_version = self._version(version)
        if selected_key in self.workspaces:
            raise ServiceError("workspace_key_conflict", "工作区标识已登记，须重启后更换", 409)
        if len(self.workspaces) >= 32:
            raise ServiceError("agent_resource_limit", "Workspace登记数量已达上限", 429)
        for tool in workspace_tools():
            existing = next(
                (item for item in self.tools.definitions if item.name == tool.name), None
            )
            if existing is not None and existing is not tool:
                raise ValueError("工作区工具名称已被其他宿主工具占用")
        workspace = root if isinstance(root, Workspace) else Workspace(root, writable=writable)
        if workspace.closed:
            raise ValueError("不能登记已关闭的工作区")
        if writable and not workspace.writable:
            raise ValueError("已有Workspace未授权写能力")
        binding = WorkspaceBinding(
            key=selected_key,
            workspace=workspace,
            version=selected_version,
            fingerprint=hashlib.sha256(
                json.dumps(
                    {
                        "key": selected_key,
                        "version": selected_version,
                        "workspace": workspace.fingerprint,
                    },
                    sort_keys=True,
                ).encode()
            ).hexdigest(),
        )
        self.workspaces[selected_key] = WorkspaceResource(
            binding=binding,
            owned=owned or not isinstance(root, Workspace),
        )
        for tool in workspace_tools():
            if tool.name not in {item.name for item in self.tools.definitions}:
                self.tools.register(tool)
        return binding

    def workspace(self, key: str) -> WorkspaceBinding:
        if self._closed:
            raise KeyError(key)
        resource = self.workspaces.get(key)
        if resource is None or not resource.binding.available:
            raise KeyError(key)
        return resource.binding

    def disable_workspace(self, key: str) -> None:
        resource = self.workspaces.get(key)
        if resource is None:
            raise KeyError(key)
        resource.binding.revoked.set()

    def fingerprint(self, key: str, names: list[str], *, workspace_key: str | None = None) -> str:
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
        if workspace_key is not None:
            workspace = self.workspace(workspace_key)
            payload["workspace_key"] = workspace.key
            payload["workspace_fingerprint"] = workspace.fingerprint
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def manifest(
        self, key: str, names: list[str], *, workspace_key: str | None = None
    ) -> dict[str, JsonValue]:
        manifest: dict[str, JsonValue] = {
            "provider_key": key,
            "provider_version": self.provider_versions.get(key),
            "provider_fingerprint": self.provider_signature(key),
            "tool_versions": {name: self.tool_versions.get(name) for name in sorted(names)},
        }
        if workspace_key is not None:
            workspace = self.workspace(workspace_key)
            manifest["workspace_key"] = workspace.key
            manifest["workspace_version"] = workspace.version
            manifest["workspace_fingerprint"] = workspace.fingerprint
        return manifest

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
        values.extend(
            AgentResourceView(
                key=resource.binding.key,
                kind="workspace",
                name=f"workspace:{resource.binding.key}",
                version=resource.binding.version,
                fingerprint=resource.binding.fingerprint,
                owned=resource.owned,
                available=not self._closed and resource.binding.available,
            )
            for resource in sorted(self.workspaces.values(), key=lambda item: item.binding.key)
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
        closed_workspaces: set[int] = set()
        for resource in self.workspaces.values():
            resource.binding.revoked.set()
            if resource.owned and id(resource.binding.workspace) not in closed_workspaces:
                closed_workspaces.add(id(resource.binding.workspace))
                try:
                    resource.binding.workspace.close()
                except Exception:
                    failed = True
        self.workspaces.clear()
        if failed:
            raise ServiceError("agent_resource_close_failed", "部分Provider资源关闭失败", 503)
