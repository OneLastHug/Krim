"""Explicit host configuration for the single Docker/runsc development mode."""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator

from krim_agent.contracts import ServiceError, StrictModel


class DevelopmentConfiguration(StrictModel):
    schema_version: Literal[1] = 1
    image_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    broker_environments_root: Path
    private_state_root: Path
    max_processes: int = Field(default=4, ge=1, le=32)
    cpu_cores: float = Field(default=1.0, ge=0.001, le=64)
    memory_bytes: int = Field(default=1073741824, ge=67108864)
    pids_limit: int = Field(default=256, ge=8, le=65536)

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
