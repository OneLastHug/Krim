"""命令执行与有界产物；此处为可信Python合约，不接受宿主路径输入。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import ConfigDict, Field, model_validator

from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace import Workspace

ExecutionStatus = Literal[
    "completed", "failed", "timed_out", "cancelled", "output_limit", "setup_failed", "uncertain"
]


class CommandSpec(StrictModel):
    argv: list[str] = Field(min_length=1, max_length=128)
    cwd: str = Field(default=".", min_length=1, max_length=512)

    @model_validator(mode="after")
    def bounded(self) -> CommandSpec:
        if not self.argv[0] or any("\0" in arg or len(arg) > 8192 for arg in self.argv):
            raise ValueError("命令参数为空或超过边界")
        Workspace._parts(self.cwd)
        check_json(self.model_dump(mode="json"), max_bytes=32768)
        return self


class SandboxLimits(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    wall_seconds: float = Field(default=30, ge=0.1, le=300)
    startup_seconds: float = Field(default=10, ge=1, le=60)
    cpu_seconds: int = Field(default=10, ge=1, le=300)
    memory_bytes: int = Field(default=268435456, ge=67108864, le=2147483648)
    process_count: int = Field(default=32, ge=8, le=128)
    open_files: int = Field(default=128, ge=32, le=1024)
    file_bytes: int = Field(default=1048576, ge=1024, le=8388608)
    workspace_bytes: int = Field(default=33554432, ge=1048576, le=268435456)
    temporary_bytes: int = Field(default=8388608, ge=1048576, le=67108864)
    output_bytes: int = Field(default=65536, ge=1024, le=1048576)
    artifact_bytes: int = Field(default=16777216, ge=1024, le=134217728)
    artifact_files: int = Field(default=256, ge=1, le=4096)

    @model_validator(mode="after")
    def allocation(self) -> SandboxLimits:
        if self.artifact_bytes > self.workspace_bytes or self.file_bytes > self.workspace_bytes:
            raise ValueError("文件与产物上限不能大于工作区空间")
        return self


@dataclass(frozen=True)
class SandboxFile:
    data: bytes
    mode: int = 0o644


@dataclass(frozen=True)
class ExecutionArtifact:
    path: str
    data: bytes
    mode: int


@dataclass(frozen=True)
class ExecutionResult:
    execution_id: str
    status: ExecutionStatus
    dispatched: bool
    started: bool
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    output_truncated: bool
    artifacts: tuple[ExecutionArtifact, ...]
    artifacts_complete: bool
    descendants_stopped: int
    error_code: str | None
    elapsed_seconds: float
