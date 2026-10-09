"""持久命令的请求、资源身份与公开元数据；文件字节另行读取。"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import ConfigDict, Field

from krim_agent.protocol.models import StrictModel

from .models import CommandSpec, ExecutionStatus, SandboxLimits


class CommandRequest(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    command: CommandSpec


class CommandResource(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    key: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    version: str = Field(default="1", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class CommandFile(StrictModel):
    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_size: int = Field(ge=0)
    mode: int = Field(ge=0, le=0o777)


class CommandSnapshot(StrictModel):
    request: CommandRequest
    resource: CommandResource
    limits: SandboxLimits
    directories: list[str]
    inputs: list[CommandFile]
    run_id: str | None = Field(default=None, max_length=36)
    call_id: str | None = Field(default=None, min_length=1, max_length=128)


CommandPhase = Literal["preparing", "dispatched", "running", "finished", "interrupted"]


class CommandView(StrictModel):
    command_id: str
    scope_id: str
    topic_id: str
    request: CommandRequest
    resource: CommandResource
    run_id: str | None
    call_id: str | None
    phase: CommandPhase
    revision: int
    cancel_requested: bool
    dispatched: bool
    started: bool
    outcome: ExecutionStatus | None
    exit_code: int | None
    output_truncated: bool
    stdout_bytes: int
    stderr_bytes: int
    input_files: int
    artifact_files: int
    artifacts_complete: bool
    processes_reaped: bool
    error_code: str | None
    created_at: datetime
    updated_at: datetime
    interrupted_at: datetime | None

    @property
    def terminal(self) -> bool:
        return self.phase in {"finished", "interrupted"}


@dataclass(frozen=True)
class CommandReservation:
    view: CommandView
    token: str | None
