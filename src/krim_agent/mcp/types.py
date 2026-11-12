from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    SecretStr,
    field_validator,
    model_validator,
)

_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")


class MCPServerConfig(BaseModel):
    """可信宿主提供的 MCP 连接配置。配置不会暴露给模型。

    ``stdio`` is a trusted-host execution boundary: ``command`` and ``args``
    are passed to the official MCP SDK as an argv vector, never as a shell
    command. Callers must source this configuration from a trusted host file;
    models and account-scoped request data must not be allowed to construct it.
    The child process runs with the service account's permissions. The caller
    supplies only a small OS runtime environment plus explicitly configured
    values; models and account-scoped requests must not construct this config.
    """

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        allow_inf_nan=False,
        hide_input_in_errors=True,
    )

    name: str
    transport: Literal["stdio", "streamable_http"]
    version: str = "1"
    command: str | None = None
    args: tuple[str, ...] = ()
    env: dict[str, str] | None = Field(default=None, repr=False)
    url: str | None = Field(default=None, repr=False)
    allowed_hosts: frozenset[str] = frozenset()
    headers: dict[str, SecretStr] = Field(default_factory=dict)
    selected_tools: tuple[str, ...] = ()
    readonly_tools: frozenset[str] = frozenset()
    secret_env_names: frozenset[str] = frozenset()
    timeout_seconds: float = Field(default=10.0, gt=0, le=60)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        if not _NAME_RE.fullmatch(value):
            raise ValueError("MCP server name is invalid")
        return value

    @field_validator("version")
    @classmethod
    def _validate_version(cls, value: str) -> str:
        if not value or len(value) > 64 or any(ord(char) < 32 for char in value):
            raise ValueError("MCP server version is invalid")
        return value

    @field_validator("command")
    @classmethod
    def _validate_command(cls, value: str | None) -> str | None:
        if value is not None and (
            not value.strip()
            or len(value) > 4096
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)
        ):
            raise ValueError("MCP stdio command is invalid")
        return value

    @field_validator("args")
    @classmethod
    def _validate_args(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if (
            len(value) > 64
            or sum(len(arg) for arg in value) > 32768
            or any(
                len(arg) > 4096
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in arg)
                for arg in value
            )
        ):
            raise ValueError("MCP stdio arguments are invalid")
        return value

    @field_validator("env")
    @classmethod
    def _copy_env(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is None:
            return None
        copied = dict(value)
        if len(copied) > 64:
            raise ValueError("MCP stdio environment is invalid")
        for key, item in copied.items():
            if (
                not key
                or len(key) > 256
                or len(item) > 8192
                or "=" in key
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in key)
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in item)
            ):
                raise ValueError("MCP stdio environment is invalid")
        return copied

    @field_validator("headers")
    @classmethod
    def _copy_headers(cls, value: dict[str, SecretStr]) -> dict[str, SecretStr]:
        copied = dict(value)
        for key in copied:
            if not key or any(ord(char) < 33 or ord(char) > 126 or char == ":" for char in key):
                raise ValueError("MCP HTTP header name is invalid")
        return copied

    @field_validator("selected_tools")
    @classmethod
    def _validate_selected_tools(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item or "\x00" in item for item in value) or len(set(value)) != len(value):
            raise ValueError("MCP selected tool names are invalid")
        return value

    @field_validator("readonly_tools")
    @classmethod
    def _validate_readonly_tools(cls, value: frozenset[str]) -> frozenset[str]:
        if any(not item or "\x00" in item for item in value):
            raise ValueError("MCP readonly tool names are invalid")
        return value

    @field_validator("allowed_hosts")
    @classmethod
    def _validate_allowed_hosts(cls, value: frozenset[str]) -> frozenset[str]:
        hosts: set[str] = set()
        for host in value:
            normalized = host.rstrip(".").lower()
            if not normalized or "*" in normalized or "/" in normalized or "://" in normalized:
                raise ValueError("MCP allowed host is invalid")
            hosts.add(normalized)
        return frozenset(hosts)

    @model_validator(mode="after")
    def _validate_transport_fields(self) -> MCPServerConfig:
        if self.secret_env_names - set(self.env or {}):
            raise ValueError("MCP credential environment reference is invalid")
        if self.transport == "stdio":
            if self.command is None or self.url is not None or self.headers or self.allowed_hosts:
                raise ValueError("MCP stdio configuration is incomplete or has HTTP fields")
            return self

        if self.command is not None or self.args or self.env is not None:
            raise ValueError("MCP HTTP configuration cannot contain stdio fields")
        if self.url is None or not self.allowed_hosts:
            raise ValueError("MCP HTTP URL and exact allowed-host list are required")
        try:
            parsed = urlsplit(self.url)
            host = parsed.hostname
            port = parsed.port
        except ValueError:
            raise ValueError("MCP HTTP URL is invalid") from None
        if (
            parsed.scheme not in {"https", "http"}
            or host is None
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or parsed.query
            or "?" in self.url
            or "#" in self.url
            or any(char.isspace() for char in self.url)
        ):
            raise ValueError("MCP HTTP URL is invalid")
        normalized_host = host.rstrip(".").lower()
        if normalized_host not in self.allowed_hosts:
            raise ValueError("MCP HTTP host is not allowed")
        if parsed.scheme == "http" and not _is_loopback_host(normalized_host):
            raise ValueError("MCP cleartext HTTP is allowed only for loopback hosts")
        if port is not None and not 1 <= port <= 65535:
            raise ValueError("MCP HTTP port is invalid")
        return self

    @property
    def configuration_digest(self) -> str:
        """Stable public hash; secrets are represented only by their field names."""
        material = {
            "name": self.name,
            "transport": self.transport,
            "version": self.version,
            "command": self.command,
            "args": list(self.args),
            # Environment values are deliberately absent. Even a value that
            # does not look secret today may become one after a host config
            # change; names are sufficient for a stable public binding digest.
            "env_names": sorted(self.env or {}),
            "url": self.url,
            "allowed_hosts": sorted(self.allowed_hosts),
            "header_names": sorted(self.headers),
            "selected_tools": sorted(self.selected_tools),
            "readonly_tools": sorted(self.readonly_tools),
            "timeout_seconds": self.timeout_seconds,
        }
        encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _is_loopback_host(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _encode_json(value: object) -> str:
    def validate(item: object) -> None:
        if item is None or type(item) in (str, bool, int):
            return
        if type(item) is float:
            if not math.isfinite(item):
                raise ValueError("JSON number is not finite")
            return
        if type(item) is list:
            for child in item:
                validate(child)
            return
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise TypeError("JSON object keys must be strings")
                validate(child)
            return
        raise TypeError("Value is not JSON-compatible")

    validate(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True, slots=True)
class MCPTool:
    """A copied, connection-scoped MCP tool description."""

    name: str
    description: str
    input_schema: dict[str, JsonValue]
    output_schema: JsonValue | None
    read_only: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "input_schema", json.loads(_encode_json(self.input_schema)))
        if self.output_schema is not None:
            object.__setattr__(self, "output_schema", json.loads(_encode_json(self.output_schema)))
