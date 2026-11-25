"""每会话模型网关运行时：配置域解析、冻结描述符与自定义 provider 生命周期。

GatewayRuntime 把配置域（ModelGatewayService）的解析结果接入 RunService 的
binding 体系：会话创建时按用户选择（或账号默认选择）冻结一份带模型网关
标记的目标描述符；准入与停靠恢复按冻结 revision 重建同一配置的绑定，
工具/宿主配置漂移仍按完整 descriptor 比较拒绝。全局 bindings、已有
provider 与子代理 delegates 不受影响。
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping
from typing import Any, cast

from pydantic import JsonValue

from krim_agent.contracts import ServiceError, encoded, fingerprint, parse_json
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.runtime.service import RunBinding, RunService

from .contracts import GatewaySelection
from .service import ModelGatewayService

_MARKER_KEY = "model_gateway"

# 自定义模型不得绕过宿主对 base provider 的传输/安全上限：这些字段按
# "更严者生效"继承 base 配置（None 表示该维不设应用层硬限，不放大）。
# 模型语义字段（model/base_url/wire_api/api_key/reasoning/thinking/
# max_output_tokens/allow_images）保持按网关声明，不继承。
_INHERITED_BOUNDS = (
    "max_tool_calls",
    "max_request_bytes",
    "max_response_bytes",
    "max_event_bytes",
    "max_events",
    "max_arguments_bytes",
    "connect_timeout_seconds",
    "read_timeout_seconds",
    "write_timeout_seconds",
    "pool_timeout_seconds",
)


def _bounded_config(
    config: OpenAICompatibleConfig, base_config: OpenAICompatibleConfig
) -> OpenAICompatibleConfig:
    values = config.model_dump()
    for name in _INHERITED_BOUNDS:
        bound = getattr(base_config, name)
        value = values[name]
        values[name] = (
            bound if value is None
            else value if bound is None
            else min(value, bound)
        )
    return OpenAICompatibleConfig.model_validate(values)


class GatewayProvider(OpenAICompatibleProvider):
    """在 provider 身份中携带冻结的模型网关标记；不改原 provider 配置类。"""

    def __init__(
        self,
        config: OpenAICompatibleConfig,
        marker: dict[str, JsonValue],
        *,
        max_connections: int | None = None,
    ) -> None:
        super().__init__(config, max_connections=max_connections)
        self._gateway_marker = marker

    def identity(self) -> dict[str, JsonValue]:
        identity = super().identity()
        identity[_MARKER_KEY] = cast(JsonValue, self._gateway_marker)
        return identity


def frozen_gateway_marker(
    target: Mapping[str, JsonValue],
) -> dict[str, JsonValue] | None:
    """读取冻结目标里持久化的每会话模型标记；不存在时返回 None。"""
    provider = target.get("provider")
    if not isinstance(provider, Mapping):
        return None
    config = provider.get("config")
    if not isinstance(config, Mapping):
        return None
    marker = config.get(_MARKER_KEY)
    return dict(marker) if isinstance(marker, Mapping) else None


def _marker_value(
    principal_id: str, revision: int, selection: GatewaySelection
) -> dict[str, JsonValue]:
    """深冻结成纯 JSON 值，保证 descriptor 与 identity 逐字节稳定。"""
    return cast(
        dict[str, JsonValue],
        parse_json(encoded({
            "principal_id": principal_id,
            "revision": revision,
            "selection": selection.model_dump(mode="json"),
        })),
    )


def _marker_parts(
    marker: Mapping[str, Any],
) -> tuple[str, int, GatewaySelection]:
    principal_id = marker.get("principal_id")
    revision = marker.get("revision")
    raw = marker.get("selection")
    if (
        not isinstance(principal_id, str)
        or not principal_id
        or len(principal_id) > 64
        or not principal_id.isascii()
        or type(revision) is not int
        or revision < 1
        or not isinstance(raw, Mapping)
    ):
        raise ServiceError("facts_corrupt")
    try:
        selection = GatewaySelection.model_validate(dict(raw))
    except ValueError:
        raise ServiceError("facts_corrupt") from None
    return principal_id, revision, selection


class GatewayRuntime:
    """配置域到 RunBinding 的桥；自定义 provider 由 close 统一释放。

    runs（RunService）保有执行与存储所有权；本类只做"选择 -> 冻结绑定"的
    解析与缓存，按 descriptor fingerprint 去重，不替换全局 bindings。
    """

    def __init__(self, gateways: ModelGatewayService, runs: RunService) -> None:
        self.gateways = gateways
        self.runs = runs
        self.closed = False
        self._lock = asyncio.Lock()
        self._by_marker: dict[str, RunBinding] = {}
        self._by_descriptor: dict[str, RunBinding] = {}
        self._providers: list[GatewayProvider] = []

    async def close(self) -> None:
        async with self._lock:
            if self.closed:
                return
            self.closed = True
            providers, self._providers = self._providers, []
            self._by_marker.clear()
            self._by_descriptor.clear()
        outcomes = await asyncio.gather(
            *(provider.close() for provider in providers), return_exceptions=True
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome

    async def for_create(
        self,
        principal_id: str,
        target_key: str,
        selection: GatewaySelection | Mapping[str, Any] | None,
    ) -> RunBinding:
        """会话创建选 binding：显式选择或账号默认走网关；无默认保持宿主目标。"""
        base = self.runs.bindings.get(target_key)
        explicit = selection is not None
        if base is None:
            if not self.runs.bindings or explicit:
                raise ServiceError("provider_not_configured", 503)
            raise ServiceError("invalid_input", 422)
        if not explicit:
            try:
                config, chosen, revision = await self.gateways.resolve(
                    principal_id, None, None
                )
            except ServiceError as error:
                if error.code == "provider_not_configured":
                    return base  # 无默认选择：保持原有宿主绑定
                raise
        else:
            config, chosen, revision = await self.gateways.resolve(
                principal_id, selection, None
            )
        if not isinstance(base.loop.provider, OpenAICompatibleProvider):
            raise ServiceError("provider_not_configured", 503)
        return await self._binding(target_key, base, principal_id, revision, chosen, config)

    async def for_target(
        self,
        target_key: str,
        frozen_target: dict[str, JsonValue],
        *,
        owner_id: str | None = None,
    ) -> RunBinding:
        """按冻结目标重建绑定：非自定义返回宿主绑定，自定义按 revision 解析。"""
        if self.closed:
            raise ServiceError("closed", 503)
        marker = frozen_gateway_marker(frozen_target)
        if marker is None:
            binding = self.runs.bindings.get(target_key)
            if binding is None:
                raise ServiceError("provider_not_configured", 503)
            return binding
        principal_id, revision, selection = _marker_parts(marker)
        if owner_id is not None and owner_id != principal_id:
            raise ServiceError("permission_denied", 403)
        expected = fingerprint(frozen_target)
        cached = self._by_descriptor.get(expected)
        if cached is not None:
            return cached
        base = self.runs.bindings.get(target_key)
        if base is None or not isinstance(base.loop.provider, OpenAICompatibleProvider):
            raise ServiceError("provider_not_configured", 503)
        config, chosen, resolved_revision = await self.gateways.resolve(
            principal_id, selection, revision
        )
        if resolved_revision != revision or chosen != selection:
            raise ServiceError("facts_corrupt")
        return await self._binding(
            target_key, base, principal_id, revision, chosen, config, expected=expected,
        )

    async def _binding(
        self,
        target_key: str,
        base: RunBinding,
        principal_id: str,
        revision: int,
        selection: GatewaySelection,
        config: OpenAICompatibleConfig,
        *,
        expected: str | None = None,
    ) -> RunBinding:
        """构造带标记的 provider 与 binding；按 marker/descriptor 指纹缓存。"""
        base_config = cast(OpenAICompatibleProvider, base.loop.provider).config
        config = _bounded_config(config, base_config)
        marker = _marker_value(principal_id, revision, selection)
        marker_key = fingerprint({"target_key": target_key, "marker": marker})
        cached = self._by_marker.get(marker_key)
        if cached is not None:
            if expected is not None and fingerprint(
                parse_json(cached.descriptor_json)
            ) != expected:
                raise ServiceError("target_binding_changed")
            return cached
        provider = GatewayProvider(
            config,
            marker,
            max_connections=getattr(base.loop.provider, "_connections", None),
        )
        loop = copy.copy(base.loop)
        loop.provider = provider
        try:
            binding = RunBinding.freeze(target_key, loop, base.settings)
        except BaseException:
            await provider.close()
            raise
        actual = fingerprint(parse_json(binding.descriptor_json))
        async with self._lock:
            if self.closed:
                await provider.close()
                raise ServiceError("closed", 503)
            existing = self._by_marker.get(marker_key)
            if existing is not None:
                await provider.close()
                if expected is not None and fingerprint(
                    parse_json(existing.descriptor_json)
                ) != expected:
                    raise ServiceError("target_binding_changed")
                return existing
            if expected is not None and actual != expected:
                # 工具/宿主配置漂移：完整 descriptor 不再等于冻结行，宁可拒绝。
                await provider.close()
                raise ServiceError("target_binding_changed")
            self._by_marker[marker_key] = binding
            self._by_descriptor[actual] = binding
            self._providers.append(provider)
        return binding
