"""可信宿主提供的沙箱管理配置。"""

import os
import stat
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.sandbox.quota import QuotaPolicy
from krim_agent.sandbox.runtime_profiles import (
    DIGEST_IMAGE,
    IMAGE_ID,
    ProfileProgram,
    ResolvedRuntime,
    RuntimeCatalog,
)


class SandboxConfiguration(StrictModel):
    schema_version: Literal[1] = 1
    max_projects_per_account: int = Field(default=32, ge=1, le=128)
    broker_socket: Path | None = None
    broker_owner_uid: int = Field(default=0, ge=0)
    command_image: str | None = None
    command_purpose: Literal["normal_job"] = "normal_job"
    runtime_manifest: Path | None = None
    allow_dev_runtimes: bool = False
    quotas: QuotaPolicy = Field(default_factory=QuotaPolicy)

    @field_validator("command_image")
    @classmethod
    def fixed_image_reference(cls, value: str | None) -> str | None:
        if value is not None and not (
            IMAGE_ID.fullmatch(value)
            or DIGEST_IMAGE.fullmatch(value)
            and value.startswith("docker.io/library/python@sha256:")
        ):
            raise ValueError("Python命令镜像必须为固定本地ID或官方Python digest")
        return value

    @model_validator(mode="after")
    def valid_profiles(self) -> "SandboxConfiguration":
        if self.broker_socket is not None and not self.broker_socket.is_absolute():
            raise ValueError("Broker连接路径必须绝对")
        if self.command_image is not None and self.broker_socket is None:
            raise ValueError("命令执行必须连接独立Broker")
        if self.runtime_manifest is not None:
            if not self.runtime_manifest.is_absolute() or self.command_image is None:
                raise ValueError("运行时manifest必须为绝对路径并绑定command_image")
            entry = RuntimeCatalog.load(self.runtime_manifest).entry("python", self.command_purpose)
            if entry.availability == "dev_verified" and not self.allow_dev_runtimes:
                raise ValueError("dev运行时必须显式允许")
            if entry.image is not None and entry.image != self.command_image:
                raise ValueError("command_image与python normal_job manifest不一致")
        elif self.allow_dev_runtimes:
            raise ValueError("启用dev运行时必须提供runtime_manifest")
        for name in self.quotas.profiles:
            self.quotas.resolve(name)
        return self

    def resolve_runtime(self, program: ProfileProgram) -> ResolvedRuntime | None:
        if self.runtime_manifest is not None:
            return RuntimeCatalog.load(self.runtime_manifest).resolve(
                program, self.command_purpose, allow_dev=self.allow_dev_runtimes
            )
        if program != "python" or self.command_image is None:
            raise ServiceError("sandbox_backend_unavailable", 503)
        return None

    @classmethod
    def load(cls, path: Path) -> "SandboxConfiguration":
        if os.name != "posix" or not all(
            hasattr(os, flag) for flag in ("O_NOFOLLOW", "O_NONBLOCK")
        ):
            raise ServiceError("sandbox_config_invalid", 422)
        try:
            path = path.absolute()
            if any(not stat.S_ISDIR(parent.lstat().st_mode) for parent in path.parents):
                raise ValueError("配置父目录无效")
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 65536:
                    raise ValueError("配置类型、权限或大小无效")
                data = os.read(fd, 65537)
                if len(data) != info.st_size:
                    raise ValueError("配置已变化")
            finally:
                os.close(fd)
            return cls.model_validate_json(data)
        except (OSError, ValueError, TypeError):
            raise ServiceError("sandbox_config_invalid", 422) from None
