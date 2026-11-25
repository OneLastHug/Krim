"""每账号模型网关配置域：CAS 更新、请求幂等、密钥隔离与历史版本解析。

文档与密文分开存放：`model_gateway_documents` 只存当前公开配置，密文放
`secret` JSON；`model_gateway_revisions` 保留同版本快照，使按 revision 重建
的会话仍能解密当时的密钥。明文密钥只在更新事务内短暂存在，写入保险库后
即丢弃；读取视图永不返回密钥值。
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from pydantic import SecretStr, ValidationError
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.account_mcp.contracts import SecretContext, SecretVault
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.storage.model_gateway_models import (
    model_gateway_documents,
    model_gateway_requests,
    model_gateway_revisions,
)
from krim_agent.storage.sessions import read

from .contracts import (
    GatewaySelection,
    GatewaySettingsView,
    GatewayUpdate,
    GatewayView,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import RowMapping

    from krim_agent.storage.sqlite import SQLiteStore

_DOCUMENT_VERSION = 1
_SECRET_VERSION = 1
_PURPOSE = "model-gateway"
_DISCOVERY_MODEL = "discovery"


def _principal(value: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 64 or not value.isascii():
        raise ServiceError("invalid_input", 422)
    return value


def _validate(model: Any, value: object) -> Any:
    try:
        return model.model_validate(value)
    except ValidationError:
        raise ServiceError("invalid_input", 422) from None


def _secret_map(secret: object) -> dict[str, dict[str, object]]:
    if not isinstance(secret, dict) or secret.get("version") != _SECRET_VERSION:
        raise ServiceError("facts_corrupt")
    gateways = secret.get("gateways")
    if gateways is None:
        return {}
    if not isinstance(gateways, dict):
        raise ServiceError("facts_corrupt")
    result: dict[str, dict[str, object]] = {}
    for key, value in gateways.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            raise ServiceError("facts_corrupt")
        result[key] = value
    return result


def _gateway_entry(document: object, gateway_id: str) -> dict[str, object] | None:
    if not isinstance(document, dict) or document.get("version") != _DOCUMENT_VERSION:
        raise ServiceError("facts_corrupt")
    gateways = document.get("gateways")
    if not isinstance(gateways, list):
        raise ServiceError("facts_corrupt")
    return next(
        (
            gateway
            for gateway in gateways
            if isinstance(gateway, dict) and gateway.get("gateway_id") == gateway_id
        ),
        None,
    )


def _model_entry(gateway: dict[str, object], model_id: str) -> dict[str, object] | None:
    models = gateway.get("models")
    if not isinstance(models, list):
        raise ServiceError("facts_corrupt")
    return next(
        (model for model in models if isinstance(model, dict) and model.get("id") == model_id),
        None,
    )


def _env_key_present(gateway: dict[str, object]) -> bool:
    """env 引用当前是否有值；只回存在状态，绝不把值带回视图。"""
    name = gateway.get("api_key_env")
    return isinstance(name, str) and bool(os.environ.get(name))


def _view(revision: int, document: object, secret: object) -> GatewaySettingsView:
    try:
        if not isinstance(document, dict) or document.get("version") != _DOCUMENT_VERSION:
            raise TypeError
        gateways = document.get("gateways")
        if not isinstance(gateways, list):
            raise TypeError
        secret_gateways = _secret_map(secret)
        rendered = [
            GatewayView.model_validate(
                {
                    **entry,
                    "api_key_set": entry.get("gateway_id") in secret_gateways
                    or _env_key_present(entry),
                }
            )
            for entry in gateways
            if isinstance(entry, dict)
        ]
        if len(rendered) != len(gateways):
            raise TypeError
        default = document.get("default_selection")
        return GatewaySettingsView(
            revision=revision,
            gateways=tuple(rendered),
            default_selection=(
                GatewaySelection.model_validate(default) if default is not None else None
            ),
        )
    except (TypeError, ValueError, KeyError):
        raise ServiceError("facts_corrupt") from None


class ModelGatewayService:
    """每账号多网关配置域；主会话负责路由、runtime 与 storage 迁移接线。"""

    def __init__(self, store: SQLiteStore, vault: SecretVault | None = None) -> None:
        self.store = store
        self.vault = vault

    # ------------------------------------------------------------------ 读取

    async def get(self, principal_id: str) -> GatewaySettingsView:
        """返回当前文档视图；从未配置的账号返回 revision 0 的空文档。"""
        principal = _principal(principal_id)

        async def operation(conn: AsyncConnection) -> GatewaySettingsView:
            row = await self._current(conn, principal)
            if row is None:
                return GatewaySettingsView(revision=0)
            return _view(row["revision"], row["document"], row["secret"])

        return await read(self.store, operation)

    # ------------------------------------------------------------------ 更新

    async def update(
        self, principal_id: str, body: GatewayUpdate | Mapping[str, Any]
    ) -> GatewaySettingsView:
        """整份替换当前文档；expected_revision 做 CAS，request_key 做幂等。"""
        principal = _principal(principal_id)
        request = body if isinstance(body, GatewayUpdate) else _validate(GatewayUpdate, body)
        self._validate_gateways(request)
        if self._has_plaintext_key(request) and self.vault is None:
            raise ServiceError("provider_config", 422)
        digest = self._digest(principal, request)

        async def operation(conn: AsyncConnection) -> GatewaySettingsView:
            old = (
                (
                    await conn.execute(
                        select(model_gateway_requests).where(
                            model_gateway_requests.c.principal_id == principal,
                            model_gateway_requests.c.request_key == request.request_key,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if old is not None:
                if old["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                history = await self._history(conn, principal, old["revision"])
                if history is None:
                    raise ServiceError("facts_corrupt")
                return _view(history["revision"], history["document"], history["secret"])
            current = await self._current(conn, principal)
            current_revision = current["revision"] if current is not None else 0
            if current_revision != request.expected_revision:
                raise ServiceError("revision_conflict", 409)
            revision = current_revision + 1
            document, secret = self._materialize(principal, request, current, revision)
            await conn.execute(
                insert(model_gateway_revisions).values(
                    principal_id=principal,
                    revision=revision,
                    document=document,
                    secret=secret,
                )
            )
            if current is None:
                await conn.execute(
                    insert(model_gateway_documents).values(
                        principal_id=principal,
                        revision=revision,
                        document=document,
                        secret=secret,
                    )
                )
            else:
                await conn.execute(
                    update(model_gateway_documents)
                    .where(model_gateway_documents.c.principal_id == principal)
                    .values(revision=revision, document=document, secret=secret)
                )
            await conn.execute(
                insert(model_gateway_requests).values(
                    principal_id=principal,
                    request_key=request.request_key,
                    request_hash=digest,
                    revision=revision,
                )
            )
            return _view(revision, document, secret)

        return await self.store._write(operation)

    # ------------------------------------------------------------------ 解析

    async def resolve(
        self,
        principal_id: str,
        selection: GatewaySelection | Mapping[str, Any] | None,
        revision: int | None,
    ) -> tuple[OpenAICompatibleConfig, GatewaySelection, int]:
        """把选择解析为可运行的传输配置；selection 为空时用文档默认选择。"""
        principal = _principal(principal_id)
        chosen_input = (
            selection
            if selection is None or isinstance(selection, GatewaySelection)
            else _validate(GatewaySelection, selection)
        )
        if revision is not None and (type(revision) is not int or revision < 1):
            raise ServiceError("invalid_input", 422)

        async def operation(
            conn: AsyncConnection,
        ) -> tuple[OpenAICompatibleConfig, GatewaySelection, int]:
            row = await self._snapshot(conn, principal, revision)
            if row is None:
                if revision is None:
                    raise ServiceError("provider_not_configured")
                raise ServiceError("not_found", 404)
            document = row["document"]
            chosen = chosen_input or self._default_selection(document)
            if chosen is None:
                raise ServiceError("provider_not_configured")
            gateway = _gateway_entry(document, chosen.gateway_id)
            if gateway is None:
                raise ServiceError("provider_config", 422)
            model = _model_entry(gateway, chosen.model_id)
            if model is None:
                raise ServiceError("provider_config", 422)
            effort, thinking = self._reasoning(model, chosen.reasoning_level)
            config = self._config(
                principal,
                gateway,
                row["secret"],
                chosen.model_id,
                effort,
                thinking,
                max_output_tokens=model.get("max_output_tokens"),
                allow_images=model.get("allow_images", False),
            )
            return config, chosen, row["revision"]

        return await read(self.store, operation)

    async def gateway_config(
        self, principal_id: str, gateway_id: str, revision: int | None = None
    ) -> OpenAICompatibleConfig:
        """构造某网关的传输配置，供模型发现使用；不依赖已声明的 models 列表。

        以占位模型名 `discovery` 构造（发现只需 base_url、wire_api 与密钥），
        因此网关尚未录入任何模型也可以先探测端点。密钥解析与 resolve 共用。
        """
        principal = _principal(principal_id)
        if not isinstance(gateway_id, str) or not gateway_id:
            raise ServiceError("invalid_input", 422)
        if revision is not None and (type(revision) is not int or revision < 1):
            raise ServiceError("invalid_input", 422)

        async def operation(conn: AsyncConnection) -> OpenAICompatibleConfig:
            row = await self._snapshot(conn, principal, revision)
            if row is None:
                raise ServiceError("not_found", 404)
            gateway = _gateway_entry(row["document"], gateway_id)
            if gateway is None:
                raise ServiceError("not_found", 404)
            return self._config(
                principal,
                gateway,
                row["secret"],
                _DISCOVERY_MODEL,
                None,
                None,
                max_output_tokens=None,
                allow_images=False,
            )

        return await read(self.store, operation)

    # ------------------------------------------------------------------ 内部

    async def _current(
        self, conn: AsyncConnection, principal: str
    ) -> RowMapping | None:
        return (
            (
                await conn.execute(
                    select(model_gateway_documents).where(
                        model_gateway_documents.c.principal_id == principal
                    )
                )
            )
            .mappings()
            .first()
        )

    async def _history(
        self, conn: AsyncConnection, principal: str, revision: int
    ) -> RowMapping | None:
        return (
            (
                await conn.execute(
                    select(model_gateway_revisions).where(
                        model_gateway_revisions.c.principal_id == principal,
                        model_gateway_revisions.c.revision == revision,
                    )
                )
            )
            .mappings()
            .first()
        )

    async def _snapshot(
        self, conn: AsyncConnection, principal: str, revision: int | None
    ) -> RowMapping | None:
        if revision is None:
            return await self._current(conn, principal)
        return await self._history(conn, principal, revision)

    def _default_selection(self, document: object) -> GatewaySelection | None:
        if not isinstance(document, dict) or document.get("version") != _DOCUMENT_VERSION:
            raise ServiceError("facts_corrupt")
        value = document.get("default_selection")
        if value is None:
            return None
        try:
            return GatewaySelection.model_validate(value)
        except ValueError:
            raise ServiceError("facts_corrupt") from None

    def _reasoning(
        self, model: dict[str, object], level: str | None
    ) -> tuple[str | None, str | None]:
        if level is None:
            return None, None
        levels = model.get("reasoning_levels")
        if not isinstance(levels, dict):
            raise ServiceError("facts_corrupt")
        entry = levels.get(level)
        if not isinstance(entry, dict):
            raise ServiceError("provider_config", 422)
        effort = entry.get("effort")
        thinking = entry.get("thinking")
        if effort is not None and not isinstance(effort, str):
            raise ServiceError("facts_corrupt")
        if thinking not in {None, "enabled", "disabled"}:
            raise ServiceError("facts_corrupt")
        return effort, thinking

    def _config(
        self,
        principal: str,
        gateway: dict[str, object],
        secret: object,
        model_id: str,
        effort: str | None,
        thinking: str | None,
        *,
        max_output_tokens: object,
        allow_images: object,
    ) -> OpenAICompatibleConfig:
        payload: dict[str, object] = {
            "base_url": gateway.get("base_url"),
            "model": model_id,
            "wire_api": gateway.get("wire_api"),
            "api_key": self._api_key(principal, gateway, secret),
            "max_output_tokens": max_output_tokens,
            "allow_images": allow_images,
        }
        if effort is not None:
            payload["reasoning_effort"] = effort
        if thinking is not None:
            payload["thinking"] = thinking
        try:
            return OpenAICompatibleConfig.model_validate(payload)
        except ValidationError:
            raise ServiceError("provider_config", 422) from None

    def _api_key(
        self, principal: str, gateway: dict[str, object], secret: object
    ) -> SecretStr:
        gateway_id = gateway.get("gateway_id")
        if not isinstance(gateway_id, str):
            raise ServiceError("facts_corrupt")
        entry = _secret_map(secret).get(gateway_id)
        if entry is not None:
            if self.vault is None:
                raise ServiceError("provider_config", 422)
            credential_revision = entry.get("credential_revision")
            ciphertext = entry.get("ciphertext")
            if type(credential_revision) is not int or not isinstance(ciphertext, str):
                raise ServiceError("facts_corrupt")
            try:
                plaintext = self.vault.decrypt(
                    SecretContext(principal, gateway_id, credential_revision, _PURPOSE),
                    ciphertext,
                )
            except Exception:
                raise ServiceError("provider_config", 422) from None
            if not isinstance(plaintext, bytes):
                raise ServiceError("provider_config", 422)
            try:
                return SecretStr(plaintext.decode("utf-8"))
            except UnicodeDecodeError:
                raise ServiceError("provider_config", 422) from None
        env_name = gateway.get("api_key_env")
        if isinstance(env_name, str):
            value = os.environ.get(env_name)
            if not value:
                raise ServiceError("provider_config", 422)
            return SecretStr(value)
        return SecretStr("")

    def _validate_gateways(self, request: GatewayUpdate) -> None:
        """保存时按模型与其每个推理档位组合验证，避免坏值落库后运行期才失败。"""
        for gateway in request.gateways:
            key = gateway.api_key
            raw = key.get_secret_value() if key is not None else ""
            probes = (
                [(model.id, list(model.reasoning_levels.values())) for model in gateway.models]
                if gateway.models
                else [(_DISCOVERY_MODEL, [])]
            )
            for model_id, levels in probes:
                base: dict[str, object] = {
                    "base_url": gateway.base_url,
                    "model": model_id,
                    "wire_api": gateway.wire_api,
                }
                if raw:
                    base["api_key"] = key
                variants = [base]
                for level in levels:
                    variant = dict(base)
                    if level.effort is not None:
                        variant["reasoning_effort"] = level.effort
                    if level.thinking is not None:
                        variant["thinking"] = level.thinking
                    variants.append(variant)
                for payload in variants:
                    try:
                        OpenAICompatibleConfig.model_validate(payload)
                    except ValidationError:
                        raise ServiceError("invalid_input", 422) from None

    def _has_plaintext_key(self, request: GatewayUpdate) -> bool:
        return any(
            gateway.api_key is not None and gateway.api_key.get_secret_value()
            for gateway in request.gateways
        )

    def _materialize(
        self,
        principal: str,
        request: GatewayUpdate,
        current: RowMapping | None,
        revision: int,
    ) -> tuple[dict[str, object], dict[str, object]]:
        previous = _secret_map(current["secret"]) if current is not None else {}
        gateways: list[dict[str, object]] = []
        stored: dict[str, dict[str, object]] = {}
        for gateway in request.gateways:
            gateways.append(
                {
                    "gateway_id": gateway.gateway_id,
                    "name": gateway.name,
                    "base_url": gateway.base_url,
                    "wire_api": gateway.wire_api,
                    "models": [model.model_dump(mode="json") for model in gateway.models],
                    "api_key_env": gateway.api_key_env,
                }
            )
            raw = (
                gateway.api_key.get_secret_value()
                if gateway.api_key is not None
                else ""
            )
            if gateway.clear_api_key:
                continue
            if raw:
                if self.vault is None:
                    raise ServiceError("provider_config", 422)
                try:
                    ciphertext = self.vault.encrypt(
                        SecretContext(principal, gateway.gateway_id, revision, _PURPOSE),
                        raw.encode("utf-8"),
                    )
                except Exception:
                    raise ServiceError("provider_config", 422) from None
                if not isinstance(ciphertext, str) or not ciphertext:
                    raise ServiceError("provider_config", 422)
                stored[gateway.gateway_id] = {
                    "ciphertext": ciphertext,
                    "credential_revision": revision,
                }
            elif gateway.api_key_env is None:
                carried = previous.get(gateway.gateway_id)
                if carried is not None:
                    stored[gateway.gateway_id] = dict(carried)
        document: dict[str, object] = {
            "version": _DOCUMENT_VERSION,
            "gateways": gateways,
            "default_selection": (
                None
                if request.default_selection is None
                else request.default_selection.model_dump(mode="json")
            ),
        }
        secret: dict[str, object] = {"version": _SECRET_VERSION, "gateways": stored}
        return document, secret

    def _digest(self, principal: str, request: GatewayUpdate) -> str:
        # 台账只落 SHA-256 摘要；库里的密钥是保险库密文，且 Provider 密钥为
        # 高熵串，离线穷举摘要的成本高于直接使用已泄露的密文，故不再引入
        # 额外派生或第二把主密钥。摘要仅用于同 request_key 的内容比对。
        return fingerprint(
            {
                "principal_id": principal,
                "expected_revision": request.expected_revision,
                "gateways": [
                    {
                        "gateway_id": gateway.gateway_id,
                        "name": gateway.name,
                        "base_url": gateway.base_url,
                        "wire_api": gateway.wire_api,
                        "models": [
                            model.model_dump(mode="json") for model in gateway.models
                        ],
                        "api_key": (
                            None
                            if gateway.api_key is None
                            else gateway.api_key.get_secret_value()
                        ),
                        "api_key_env": gateway.api_key_env,
                        "clear_api_key": gateway.clear_api_key,
                    }
                    for gateway in request.gateways
                ],
                "default_selection": (
                    None
                    if request.default_selection is None
                    else request.default_selection.model_dump(mode="json")
                ),
            }
        )
