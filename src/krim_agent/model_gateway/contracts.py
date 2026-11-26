"""模型网关配置域契约：每账号多网关、手工模型目录、默认选择与更新请求。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator

from krim_agent.contracts import StrictModel

GATEWAY_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"
MODEL_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
ENV_NAME_PATTERN = r"^[A-Z_][A-Z0-9_]{0,127}$"


class ReasoningLevel(StrictModel):
    """一个自定义推理档位到请求参数的映射；两项都可为空表示交给供应商默认。"""

    effort: str | None = Field(default=None, max_length=64)
    thinking: Literal["enabled", "disabled"] | None = None
    display_name: str | None = Field(default=None, min_length=1, max_length=32)

    @field_validator("effort")
    @classmethod
    def valid_effort(cls, value: str | None) -> str | None:
        if value is not None and (
            not value or not value.isascii() or any(not 33 <= ord(c) <= 126 for c in value)
        ):
            raise ValueError("无效推理档位值")
        return value


class GatewayModel(StrictModel):
    """手工目录中的一个模型；能力声明只描述预期，不探测端点。"""

    id: str = Field(pattern=MODEL_ID_PATTERN)
    display_name: str = Field(min_length=1, max_length=128)
    allow_images: bool = False
    max_output_tokens: int | None = Field(default=None, ge=1)
    reasoning_levels: dict[str, ReasoningLevel] = Field(default_factory=dict)

    @field_validator("reasoning_levels")
    @classmethod
    def valid_level_names(
        cls, value: dict[str, ReasoningLevel]
    ) -> dict[str, ReasoningLevel]:
        for name in value:
            if (
                not name
                or len(name) > 32
                or not name.isascii()
                or any(not 33 <= ord(c) <= 126 for c in name)
            ):
                raise ValueError("无效推理档位名")
        return value


class GatewayDiscoveryRequest(StrictModel):
    """草稿模型发现请求；不落库不推进版本，密钥语义与 Gateway 保存一致。

    `gateway_id` 缺省表示纯草稿探测；给出时按当前账号读取保存的同ID网关，
    留空 `api_key` 复用已存密钥，新值只覆盖本次探测，`clear_api_key` 清除。
    `api_key_env` 与明文密钥互斥；给出 env 引用时按保存语义接管本次解析。
    """

    base_url: str = Field(min_length=1, max_length=2048)
    wire_api: Literal["chat_completions", "responses"] = "chat_completions"
    gateway_id: str | None = Field(default=None, pattern=GATEWAY_ID_PATTERN)
    expected_revision: int = Field(ge=0)
    api_key: SecretStr | None = Field(default=None, repr=False)
    api_key_env: str | None = Field(default=None, pattern=ENV_NAME_PATTERN)
    clear_api_key: bool = False

    @field_validator("api_key", mode="before")
    @classmethod
    def secret_key(cls, value: object) -> object:
        return SecretStr(value) if isinstance(value, str) else value

    @model_validator(mode="after")
    def consistent(self) -> GatewayDiscoveryRequest:
        key = self.api_key.get_secret_value() if self.api_key is not None else ""
        if key and self.api_key_env is not None:
            raise ValueError("密钥与密钥引用不能同时提供")
        if self.clear_api_key and key:
            raise ValueError("清除与设置密钥不能同时进行")
        return self


class GatewaySelection(StrictModel):
    """一次解析使用的具体网关/模型/档位；runtime 沿用此命名。"""

    gateway_id: str = Field(pattern=GATEWAY_ID_PATTERN)
    model_id: str = Field(pattern=MODEL_ID_PATTERN)
    reasoning_level: str | None = Field(default=None, max_length=32)


class Gateway(StrictModel):
    """更新请求中的一个网关；api_key 只写，空值表示保留已有存储密钥。"""

    gateway_id: str = Field(pattern=GATEWAY_ID_PATTERN)
    name: str = Field(min_length=1, max_length=128)
    base_url: str = Field(min_length=1, max_length=2048)
    wire_api: Literal["chat_completions", "responses"] = "chat_completions"
    models: tuple[GatewayModel, ...] = ()
    api_key: SecretStr | None = Field(default=None, repr=False)
    api_key_env: str | None = Field(default=None, pattern=ENV_NAME_PATTERN)
    clear_api_key: bool = False

    @field_validator("models", mode="before")
    @classmethod
    def tuple_models(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("api_key", mode="before")
    @classmethod
    def secret_key(cls, value: object) -> object:
        return SecretStr(value) if isinstance(value, str) else value

    @model_validator(mode="after")
    def consistent(self) -> Gateway:
        ids = [model.id for model in self.models]
        if len(set(ids)) != len(ids):
            raise ValueError("模型标识重复")
        key = self.api_key.get_secret_value() if self.api_key is not None else ""
        if key and self.api_key_env is not None:
            raise ValueError("密钥与密钥引用不能同时提供")
        if self.clear_api_key and key:
            raise ValueError("清除与设置密钥不能同时进行")
        return self


class GatewayUpdate(StrictModel):
    """整份文档替换；CAS 基线 + 请求键保证重试幂等。"""

    expected_revision: int = Field(ge=0)
    request_key: str = Field(pattern=r"^[!-~]{1,128}$")
    gateways: tuple[Gateway, ...] = ()
    default_selection: GatewaySelection | None = None

    @field_validator("gateways", mode="before")
    @classmethod
    def tuple_gateways(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def consistent(self) -> GatewayUpdate:
        ids = [gateway.gateway_id for gateway in self.gateways]
        if len(set(ids)) != len(ids):
            raise ValueError("网关标识重复")
        selection = self.default_selection
        if selection is not None:
            gateway = next(
                (item for item in self.gateways if item.gateway_id == selection.gateway_id),
                None,
            )
            if gateway is None:
                raise ValueError("默认选择指向不存在的网关")
            model = next(
                (item for item in gateway.models if item.id == selection.model_id), None
            )
            if model is None:
                raise ValueError("默认选择指向不存在的模型")
            if (
                selection.reasoning_level is not None
                and selection.reasoning_level not in model.reasoning_levels
            ):
                raise ValueError("默认选择指向未声明的推理档位")
        return self


class GatewayView(StrictModel):
    """GET 视图中的一个网关；只回 api_key_set，永不回密钥值。"""

    gateway_id: str
    name: str
    base_url: str
    wire_api: Literal["chat_completions", "responses"]
    models: tuple[GatewayModel, ...] = ()
    api_key_set: bool
    api_key_env: str | None = None

    @field_validator("models", mode="before")
    @classmethod
    def tuple_models(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value


class GatewaySettingsView(StrictModel):
    """GET 返回的完整文档形状：版本、网关列表与默认选择。"""

    revision: int = Field(ge=0)
    gateways: tuple[GatewayView, ...] = ()
    default_selection: GatewaySelection | None = None

    @field_validator("gateways", mode="before")
    @classmethod
    def tuple_gateways(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value
