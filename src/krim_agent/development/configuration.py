"""Explicit host configuration for the single Docker/runsc development mode."""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.dev_preview.boundary import PreviewOrigins


class DevelopmentListenerConfiguration(StrictModel):
    control_host: Literal["127.0.0.1", "127.0.0.2", "::1"] = "127.0.0.1"
    control_port: int = Field(default=8765, ge=1, le=65535, strict=True)
    preview_host: Literal["127.0.0.1", "127.0.0.2", "::1"] = "127.0.0.1"
    preview_port: int = Field(default=8766, ge=1, le=65535, strict=True)
    control_certificate: Path
    control_private_key: Path
    preview_certificate: Path
    preview_private_key: Path

    @field_validator(
        "control_certificate", "control_private_key",
        "preview_certificate", "preview_private_key",
    )
    @classmethod
    def absolute_tls_path(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("TLS paths must be absolute")
        return value

    @model_validator(mode="after")
    def separate_listeners(self) -> Self:
        if (self.control_host, self.control_port) == (self.preview_host, self.preview_port):
            raise ValueError("control and preview require distinct listeners")
        return self


class DevelopmentBrowserConfiguration(StrictModel):
    image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    ca_path: Path
    ca_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    seccomp_path: Path

    @field_validator("ca_path", "seccomp_path")
    @classmethod
    def absolute(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("browser paths must be absolute")
        return value


class DevelopmentGitConfiguration(StrictModel):
    repository_root: Path
    stage_root: Path
    cache_root: Path

    @field_validator("repository_root", "stage_root", "cache_root")
    @classmethod
    def absolute(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("Git private paths must be absolute")
        return value

    @model_validator(mode="after")
    def private_paths(self) -> Self:
        paths = (self.repository_root, self.stage_root, self.cache_root)
        if any(a.is_relative_to(b) for a in paths for b in paths if a != b):
            raise ValueError("Git private roots must not overlap")
        if len(set(paths)) != 3 or len(os.fsencode(self.cache_root)) > 72:
            raise ValueError("Git cache requires a short, separate private root")
        return self


class DevelopmentPreviewConfiguration(StrictModel):
    control_origin: str
    control_cookie_domain: str
    preview_domain: str
    listener: DevelopmentListenerConfiguration | None = None

    def origins(self) -> PreviewOrigins:
        return PreviewOrigins(
            self.control_origin, self.control_cookie_domain, self.preview_domain,
        )

    @model_validator(mode="after")
    def validate_origins(self) -> Self:
        self.origins()
        return self


class DevelopmentConfiguration(StrictModel):
    schema_version: Literal[1] = 1
    image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    broker_environments_root: Path
    private_state_root: Path
    max_processes: int = Field(default=4, ge=1, le=32)
    cpu_cores: float = Field(default=1.0, ge=0.001, le=64)
    memory_bytes: int = Field(default=1073741824, ge=67108864)
    pids_limit: int = Field(default=256, ge=8, le=65536)
    preview: DevelopmentPreviewConfiguration | None = None
    browser: DevelopmentBrowserConfiguration | None = None
    git: DevelopmentGitConfiguration | None = None
    history_compaction: bool = False
    model_images: bool = False
    max_steps: int = Field(default=8, ge=1, le=32)
    max_tool_calls: int = Field(default=32, ge=1, le=128)

    @model_validator(mode="after")
    def browser_requires_preview(self) -> Self:
        if self.browser is not None and (
            self.preview is None or self.preview.listener is None
            or self.preview.listener.preview_host != "127.0.0.1"
            or self.preview.listener.preview_port != 443
        ):
            raise ValueError("browser requires the dedicated local preview TLS listener")
        return self

    @field_validator("broker_environments_root", "private_state_root")
    @classmethod
    def absolute(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("development host paths must be absolute")
        return value

    @classmethod
    def load(cls, path: Path) -> DevelopmentConfiguration:
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                info = os.fstat(fd)
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077
                    or info.st_size > 65536
                ):
                    raise ValueError("untrusted development configuration")
                data = os.read(fd, 65537)
            finally:
                os.close(fd)
            return cls.model_validate_json(data)
        except (OSError, ValueError):
            raise ServiceError("sandbox_config_invalid", 422) from None
