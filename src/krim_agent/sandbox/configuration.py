"""可信宿主提供的沙箱管理配置。"""

import os
import stat
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.sandbox.quota import QuotaPolicy


class SandboxConfiguration(StrictModel):
    schema_version: Literal[1] = 1
    max_projects_per_account: int = Field(default=32, ge=1, le=128)
    broker_socket: Path | None = None
    broker_owner_uid: int = Field(default=0, ge=0)
    command_image: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    quotas: QuotaPolicy = Field(default_factory=QuotaPolicy)

    @model_validator(mode="after")
    def valid_profiles(self) -> "SandboxConfiguration":
        if self.broker_socket is not None and not self.broker_socket.is_absolute():
            raise ValueError("Broker连接路径必须绝对")
        if self.command_image is not None and self.broker_socket is None:
            raise ValueError("命令执行必须连接独立Broker")
        for name in self.quotas.profiles:
            self.quotas.resolve(name)
        return self

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
