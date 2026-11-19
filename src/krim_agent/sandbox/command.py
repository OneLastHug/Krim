"""新沙箱命令协议，不放宽旧 workspace/1 的 Command 类型。"""

from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import StrictModel
from krim_agent.sandbox.xfs import internal_id


class SandboxCommand(StrictModel):
    project_id: str | None = None
    expected_generation: int = Field(ge=0)
    program: Literal["python", "node"] = "python"
    argv: list[str] = Field(min_length=1, max_length=32)
    wall_seconds: int = Field(default=20, ge=1, le=60)

    @field_validator("project_id")
    @classmethod
    def valid_project(cls, value: str | None) -> str | None:
        return internal_id(value) if value is not None else None

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if (
            any("\0" in item for item in self.argv)
            or sum(len(item.encode("utf-8")) for item in self.argv) > 8192
        ):
            raise ValueError("命令参数无效")
        if self.program == "node":
            entry = self.argv[0]
            if entry in {"-e", "--eval"}:
                if len(self.argv) < 2:
                    raise ValueError("Node eval必须带脚本")
            elif (
                entry.startswith(("-", "/"))
                or "\\" in entry
                or any(segment == ".." for segment in entry.split("/"))
            ):
                raise ValueError("Node仅接受eval或工作目录内脚本路径")
        return self
