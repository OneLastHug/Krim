"""完整目录成果的确定性依赖计划；只规划顺序，不执行宿主文件动作。"""

from __future__ import annotations

from collections.abc import Iterable
from itertools import islice
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace

DependencyKind = Literal["directory", "file"]
DependencyOperation = Literal["create", "delete"]


class DirectoryDependencyEntry(StrictModel):
    """来自已核验manifest的目录或普通文件相对路径。"""

    path: str = Field(min_length=1, max_length=512)
    kind: DependencyKind
    mode: int | None = Field(default=None, ge=0, le=0o777)

    @model_validator(mode="after")
    def valid_path_and_mode(self) -> DirectoryDependencyEntry:
        try:
            parts = Workspace._parts(self.path)
        except ServiceError:
            raise ValueError("目录成果路径无效") from None
        if not parts or "/".join(parts) != self.path:
            raise ValueError("目录成果路径必须为规范相对路径")
        if (self.kind == "directory") != (self.mode is not None):
            raise ValueError("目录须携带普通mode，文件不能携带mode")
        return self


class DirectoryDependencyStep(DirectoryDependencyEntry):
    depth: int = Field(ge=1, le=16)
    depends_on: list[str] = Field(max_length=16383)

    @model_validator(mode="after")
    def actual_depth(self) -> DirectoryDependencyStep:
        if self.depth != len(self.path.split("/")):
            raise ValueError("目录步骤深度与路径不符")
        return self


class DirectoryDependencyPlan(StrictModel):
    """可审计的拓扑顺序；消费者仍须逐项走原提案/Writer账本。"""

    operation: DependencyOperation
    steps: list[DirectoryDependencyStep] = Field(min_length=1, max_length=16384)

    @model_validator(mode="after")
    def acyclic_and_unique(self) -> DirectoryDependencyPlan:
        paths = [step.path for step in self.steps]
        if len(paths) != len(set(paths)):
            raise ValueError("目录依赖计划包含重复路径")
        by_path = {step.path: step for step in self.steps}
        children: dict[str, list[str]] = {path: [] for path in paths}
        for step in self.steps:
            parent = step.path.rpartition("/")[0]
            if parent:
                if parent not in by_path or by_path[parent].kind != "directory":
                    raise ValueError("目录计划缺少父目录闭包")
                children[parent].append(step.path)
        positions = {step.path: index for index, step in enumerate(self.steps)}
        for step in self.steps:
            parent = step.path.rpartition("/")[0]
            expected = (
                ([parent] if parent else [])
                if self.operation == "create"
                else sorted(children[step.path])
            )
            if step.depends_on != expected:
                raise ValueError("目录计划必须保留全部直接依赖")
            if any(positions[value] >= positions[step.path] for value in step.depends_on):
                raise ValueError("目录计划步骤不能先于其依赖")
        return self


def build_directory_dependency_plan(
    entries: Iterable[DirectoryDependencyEntry],
    *,
    operation: DependencyOperation = "create",
    max_entries: int = 16384,
) -> DirectoryDependencyPlan:
    """从完整manifest生成父目录闭包和稳定拓扑顺序。"""
    if type(max_entries) is not int or not 1 <= max_entries <= 16384:
        raise ValueError("目录依赖计划容量无效")
    if operation not in {"create", "delete"}:
        raise ValueError("目录依赖动作无效")
    raw = list(islice(entries, max_entries + 1))
    if not 1 <= len(raw) <= max_entries:
        raise ServiceError("execution_directory_dependency_limit", "目录依赖条目超过上限", 409)
    # 调用方可以修改Pydantic对象；规划时复制并重验，不能借model_copy跳过路径门禁。
    values = [DirectoryDependencyEntry.model_validate(entry.model_dump()) for entry in raw]
    by_path: dict[str, DirectoryDependencyEntry] = {}
    for entry in values:
        if entry.path in by_path:
            raise ServiceError("execution_directory_dependency_conflict", "目录成果路径重复", 409)
        by_path[entry.path] = entry
    directories = {entry.path for entry in values if entry.kind == "directory"}
    for entry in values:
        parts = entry.path.split("/")
        parents = ["/".join(parts[:index]) for index in range(1, len(parts))]
        if any(parent not in directories for parent in parents):
            raise ServiceError(
                "execution_directory_dependency_parent", "完整目录成果缺少父目录闭包", 409
            )
    parent_of: dict[str, str | None] = {}
    for entry in values:
        parts = entry.path.split("/")
        parent_of[entry.path] = "/".join(parts[:-1]) or None
    children: dict[str, list[str]] = {entry.path: [] for entry in values}
    for path, parent in parent_of.items():
        if parent is not None:
            children[parent].append(path)
    depth = {entry.path: len(entry.path.split("/")) for entry in values}
    if operation == "create":
        ordered = sorted(
            values,
            key=lambda entry: (
                depth[entry.path],
                0 if entry.kind == "directory" else 1,
                entry.path,
            ),
        )
        dependencies: dict[str, list[str]] = {}
        for entry in values:
            parent = parent_of[entry.path]
            dependencies[entry.path] = [parent] if parent is not None else []
    else:
        ordered = sorted(
            values,
            key=lambda entry: (
                -depth[entry.path],
                0 if entry.kind == "file" else 1,
                entry.path,
            ),
        )
        dependencies = {entry.path: sorted(children[entry.path]) for entry in values}
    return DirectoryDependencyPlan(
        operation=operation,
        steps=[
            DirectoryDependencyStep(
                path=entry.path,
                kind=entry.kind,
                mode=entry.mode,
                depth=depth[entry.path],
                depends_on=dependencies[entry.path],
            )
            for entry in ordered
        ],
    )
