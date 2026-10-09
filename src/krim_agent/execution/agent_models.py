"""Agent命令工具的有限请求与预览，私有完整字节不放进工具结果。"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace

from .models import ExecutionStatus
from .records import CommandFile, CommandPhase, CommandRequest


class AgentCommandRequest(CommandRequest):
    input_paths: list[str] = Field(default_factory=list, max_length=32)
    directories: list[str] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def paths(self) -> AgentCommandRequest:
        try:
            for values in (self.input_paths, self.directories):
                if len(values) != len(set(values)) or any(
                    not Workspace._parts(path) for path in values
                ):
                    raise ValueError("命令输入/目录须为不重复的工作区相对路径")
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self


class CommandLookup(StrictModel):
    command_id: str | None = Field(default=None, min_length=1, max_length=36)
    request_key: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def target(self) -> CommandLookup:
        if (self.command_id is None) == (self.request_key is None):
            raise ValueError("须指定command_id或request_key中的一个")
        return self


class CommandRead(CommandLookup):
    source: Literal["stdout", "stderr", "artifact"] = "stdout"
    path: str | None = Field(default=None, min_length=1, max_length=512)
    offset: int = Field(default=0, ge=0, le=8388608)
    limit: int = Field(default=2048, ge=1, le=4096)

    @model_validator(mode="after")
    def file_source(self) -> CommandRead:
        if (self.source == "artifact") != (self.path is not None):
            raise ValueError("只有读取产物时才须指定path")
        try:
            if self.path is not None and not Workspace._parts(self.path):
                raise ValueError("产物路径须为相对文件")
        except ServiceError as error:
            raise ValueError(str(error)) from error
        return self


class CommandSummary(StrictModel):
    command_id: str
    revision: int
    phase: CommandPhase
    outcome: ExecutionStatus | None
    dispatched: bool
    started: bool
    processes_reaped: bool
    exit_code: int | None
    error_code: str | None
    stdout_text: str
    stderr_text: str
    stdout_base64: str
    stderr_base64: str
    stdout_bytes: int
    stderr_bytes: int
    output_truncated: bool
    input_files: int
    artifact_files: int
    artifacts: list[CommandFile] = Field(max_length=32)
    artifacts_truncated: bool
    artifacts_complete: bool


class CommandSlice(StrictModel):
    command_id: str
    source: Literal["stdout", "stderr", "artifact"]
    path: str | None
    offset: int
    next_offset: int
    byte_size: int
    data_base64: str
    text: str
    truncated: bool
