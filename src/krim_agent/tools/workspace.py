"""六个可信scoped路由；模型只提供严格数据，不能登记宿主能力。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, JsonValue, model_validator

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.pipeline import Operations
from krim_agent.tools.registry import ToolRegistry
from krim_agent.workspace.types import path_parts

Revision = Annotated[str,
     Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Read(StrictModel):
    revision_id: Revision
    path: str
    offset: int = Field(default=0, ge=0)
    length: int = Field(default=2048, ge=1, le=2048)

    @model_validator(mode="after")
    def bounded(self) -> Read:
        path_parts(self.path)
        return self


class Search(StrictModel):
    revision_id: Revision
    query: str = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def bounded(self) -> Search:
        if len(self.query.encode()) > 256:
            raise ValueError("字面查询过长")
        return self


class Write(StrictModel):
    expected_revision: int = Field(ge=0)
    path: str
    expected_file_hash: Digest | None
    offset: int = Field(ge=0)
    delete_length: int = Field(ge=0, le=1048576)
    replacement: str

    @model_validator(mode="after")
    def bounded(self) -> Write:
        path_parts(self.path)
        if len(self.replacement.encode()) > 2048:
            raise ValueError("替换内容过长")
        return self


class Command(StrictModel):
    expected_revision: int = Field(ge=0)
    program: Literal["python"]
    argv: list[str] = Field(min_length=1, max_length=32)
    cwd: str = "."

    @model_validator(mode="after")
    def bounded(self) -> Command:
        if any(
            "\0" in arg for arg in self.argv) or sum(len(arg.encode()) for arg in self.argv) > 8192:
            raise ValueError("命令参数无效")
        if self.cwd != ".":
            path_parts(self.cwd)
        return self


class Step(StrictModel):
    id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,128}$")
    text: str = Field(min_length=1, max_length=1024)
    status: Literal["pending", "running", "completed", "failed"]


class PlanUpdate(StrictModel):
    expected_revision: int = Field(ge=0)
    steps: list[Step] = Field(max_length=8)

    @model_validator(mode="after")
    def bounded(self) -> PlanUpdate:
        if len({step.id for step in self.steps}) != len(self.steps):
            raise ValueError("计划步骤重复")
        return self


async def unavailable(parameters: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


def registry(operations: Operations) -> ToolRegistry:
    digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    definitions: list[tuple[str, type[BaseModel], Literal["read", "write", "exec"], str]] = [
        ("workspace_read", Read, "read", "read"),
        ("workspace_search", Search, "read", "search"),
        ("workspace_write", Write, "write", "write"),
        ("command_run", Command, "exec", "command"),
        ("artifact_read", Read, "read", "artifact"),
        ("plan_update", PlanUpdate, "write", "plan"),
    ]
    return ToolRegistry(tuple(ToolSpec(name, model, unavailable, {"type": "object"},
        "workspace/1", digest, risk, operations.execute, capability)
        for name, model, risk, capability in definitions))
