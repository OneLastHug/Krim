"""目录树持久计划公开事实；内部根准备材料和执行凭据不进入请求。"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from krim_agent.execution.directory_publication import CommandDirectoryProposalRequest
from krim_agent.execution.publication import CommandProposalRequest
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel, check_json
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import WorkspaceAttemptView, WorkspaceChangeAction
from krim_agent.workspace_directory import DirectoryAttemptView, DirectoryChangeAction

TreePlanStatus = Literal[
    "planned", "running", "done", "partial", "failed", "uncertain", "cancelled", "interrupted"
]
TreeProducerStatus = Literal["planned", "running", "finished", "cancelled", "interrupted"]
TreeSource = Literal["file", "directory"]
TreePhase = Literal["preview", "change"]
TreeStepStatus = Literal["pending", "running", "done", "rejected", "uncertain", "skipped"]


class TreePlanRequest(StrictModel):
    command_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_path: str = Field(min_length=1, max_length=512)
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$")
    accept_failed: bool = False
    max_entries: int = Field(default=1024, ge=1, le=16384)

    @model_validator(mode="after")
    def canonical_root(self) -> "TreePlanRequest":
        try:
            parts = Workspace._parts(self.root_path)
        except ServiceError:
            raise ValueError("目录树根须为授权工作区规范相对路径") from None
        if not parts or "/".join(parts) != self.root_path:
            raise ValueError("目录树根须为非空规范相对目录")
        check_json(self.model_dump(mode="json"), max_bytes=16384)
        return self


class TreePlanView(StrictModel):
    plan_id: str
    scope_id: str
    command_id: str
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_path: str
    workspace_key: str
    resource_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_key: str
    snapshot_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: TreePlanStatus
    producer_status: TreeProducerStatus
    step_count: int = Field(ge=1, le=16384)
    done_count: int = Field(ge=0, le=16384)
    stop_reason: str | None
    created_at: datetime
    updated_at: datetime


class TreePlanStepView(StrictModel):
    position: int = Field(ge=0, lt=16384)
    parent_position: int | None = Field(default=None, ge=0, lt=16384)
    source: TreeSource
    path: str
    mode: int = Field(ge=0, le=0o777)
    sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    byte_size: int | None = Field(default=None, ge=0)
    status: TreeStepStatus
    error_code: str | None = None
    proposal_id: str | None = None
    operation: WorkspaceAttemptView | DirectoryAttemptView | None = None

    @model_validator(mode="after")
    def source_metadata(self) -> "TreePlanStepView":
        parts = Workspace._parts(self.path)
        if not parts or "/".join(parts) != self.path:
            raise ValueError("目录树步骤路径无效")
        if self.parent_position is not None and self.parent_position >= self.position:
            raise ValueError("目录树步骤须依赖更早父项")
        if self.source == "directory":
            if self.sha256 is not None or self.byte_size is not None or self.mode & 0o700 != 0o700:
                raise ValueError("目录树目录步骤元数据无效")
        elif self.sha256 is None or self.byte_size is None:
            raise ValueError("目录树文件步骤须有原SHA和字节数")
        if self.operation is None and self.status in {"running", "done", "uncertain"}:
            raise ValueError("目录树步骤没有原operation不能自报效果")
        if self.operation is not None and (
            isinstance(self.operation, DirectoryAttemptView) != (self.source == "directory")
            or self.operation.proposal_id != self.proposal_id
            or self.operation.status != self.status
            or self.operation.action != "apply"
            or self.operation.expected_revision != 0
            or self.status == "running"
            and self.operation.receipt is not None
            or self.status in {"done", "rejected"}
            and self.operation.receipt is None
            or self.status == "uncertain"
            and self.operation.receipt is None
            and not isinstance(self.operation, DirectoryAttemptView)
            or self.operation.receipt is not None
            and (
                self.operation.receipt.status != self.status
                or self.operation.receipt.path != self.path
            )
        ):
            raise ValueError("目录树回程未保留原来源与状态")
        return self


class TreePlanStepPage(StrictModel):
    plan_id: str
    scope_id: str
    items: list[TreePlanStepView] = Field(max_length=32)
    next_after: int | None = Field(default=None, ge=0, lt=16384)


class TreePlanQuery(StrictModel):
    kind: Literal["plan", "by_key", "steps"]
    plan_id: str | None = Field(
        default=None, min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
    )
    request_key: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f]+$"
    )
    after: int | None = Field(default=None, ge=0, lt=16384)
    limit: int = Field(default=32, ge=1, le=32)

    @model_validator(mode="after")
    def selection(self) -> "TreePlanQuery":
        if self.kind == "by_key":
            if self.request_key is None or self.plan_id is not None:
                raise ValueError("目录树原键查询须只提供request_key")
        elif self.plan_id is None or self.request_key is not None:
            raise ValueError("目录树计划/步骤查询须只提供plan_id")
        if self.kind != "steps" and (self.after is not None or self.limit != 32):
            raise ValueError("目录树单项查询不接受步骤分页参数")
        return self


class TreePlanQueryResult(StrictModel):
    kind: Literal["plan", "by_key", "steps"]
    plan: TreePlanView | None = None
    steps: TreePlanStepPage | None = None

    @model_validator(mode="after")
    def one_result(self) -> "TreePlanQueryResult":
        if self.kind == "steps" and (self.steps is None or self.plan is not None):
            raise ValueError("目录树步骤回程须只提供原步骤页")
        if self.kind != "steps" and self.steps is not None:
            raise ValueError("目录树单项回程不能携带步骤页")
        if self.kind == "plan" and self.plan is None:
            raise ValueError("目录树按ID回程缺少原计划")
        check_json(self.model_dump(mode="json"))
        return self


class TreePlanStartRequest(StrictModel):
    plan_id: str = Field(min_length=1, max_length=36, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$")


@dataclass(frozen=True)
class TreePlanModelSource:
    run_id: str
    call_id: str


@dataclass(frozen=True)
class TreePlanFence:
    plan_id: str
    position: int
    source: TreeSource
    phase: TreePhase
    token: str
    model_source: TreePlanModelSource | None = None


@dataclass(frozen=True)
class TreePlanExecution:
    view: TreePlanView
    token: str
    model_source: TreePlanModelSource | None = None


@dataclass(frozen=True)
class TreePlanDispatch:
    fence: TreePlanFence
    name: str
    request: (
        CommandProposalRequest
        | CommandDirectoryProposalRequest
        | WorkspaceChangeAction
        | DirectoryChangeAction
    )
