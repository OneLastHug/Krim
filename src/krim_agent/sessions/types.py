"""v2会话/Turn/控制合约；不改变v1 Run协议。"""

from typing import Literal

from pydantic import Field, JsonValue, field_validator, model_validator

from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import Message, StrictModel
from krim_agent.sandbox.tasks import SandboxCompletion

TargetKey = Literal[
    "demo", "target-v1", "target-v2", "target-bad", "target-http", "target-workspace",
    "target-development"
]
Key = str


class SessionCreate(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    title: str = Field(default="新对话", min_length=1, max_length=128)
    target_key: TargetKey = "demo"
    context_policy: ContextPolicy | None = None


class TurnRequest(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    text: str = Field(min_length=1, max_length=32768)
    expected_revision: int = Field(ge=0)
    completion: SandboxCompletion | None = None

    @field_validator("text")
    @classmethod
    def bounded(cls, text: str) -> str:
        if not text.strip() or len(text.encode()) > 32768:
            raise ValueError("输入为空或超限")
        return text


class ControlRequest(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    expected_revision: int = Field(ge=0)
    title: str | None = Field(default=None, min_length=1, max_length=128)


class SessionLifecycleRequest(StrictModel):
    request_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    expected_revision: int = Field(ge=0)


class SessionView(StrictModel):
    id: str
    workspace_id: str
    owner_id: str
    create_key: str
    title: str
    target_key: TargetKey
    target_fingerprint: str
    context_policy: ContextPolicy
    revision: int
    status: Literal["open", "closed"]
    dispatch_paused: bool
    created_at: float
    closed_at: float | None
    archived_at: float | None = None
    deleted_at: float | None = None
    lifecycle_revision: int = Field(default=0, ge=0)


class TurnView(StrictModel):
    id: str
    session_id: str
    workspace_id: str
    owner_id: str
    ordinal: int
    request_key: str
    text: str
    accepted_revision: int
    status: Literal["queued", "running", "completed", "failed", "cancelled", "interrupted"]
    run_id: str | None
    context_fingerprint: str | None
    transcript: list[Message] | None
    code: str | None
    created_at: float
    started_at: float | None
    finished_at: float | None


class TraceIndexItem(StrictModel):
    source_kind: Literal["session_turn"]
    run_id: str
    trace_id: str
    session_id: str
    turn_id: str
    session_title: str
    target_key: TargetKey
    status: Literal["queued", "running", "completed", "failed", "aborted", "interrupted"]
    created_at: float
    finished_at: float | None


class TraceIndexPage(StrictModel):
    items: list[TraceIndexItem]
    next_cursor: str | None


class ControlView(StrictModel):
    id: str
    session_id: str
    request_key: str
    kind: str
    status: Literal["pending", "completed"]
    result: dict[str, JsonValue] | None


# 侧栏呈现（schema 17）：仅当前主体的展示层布局，不改变会话归属、生命周期
# 或执行父子语义。group_id 为 null 表示默认"会话"分组；groups/sessions 数组
# 顺序即手动排序；parent 链（嵌套层数，含顶层节点）最多 32 层，前端渲染同限。
SIDEBAR_MAX_PARENT_DEPTH = 32


class SidebarGroup(StrictModel):
    id: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]{1,128}$")
    title: str = Field(min_length=1, max_length=256)


class SidebarSessionEntry(StrictModel):
    session_id: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]{1,128}$")
    group_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[!-~]{1,128}$"
    )
    parent_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[!-~]{1,128}$"
    )
    pinned: bool = False


class SessionSidebarDocument(StrictModel):
    revision: int = Field(ge=0)
    groups: tuple[SidebarGroup, ...]
    sessions: tuple[SidebarSessionEntry, ...]


class SessionSidebarWrite(StrictModel):
    groups: tuple[SidebarGroup, ...] = Field(default=(), max_length=100)
    sessions: tuple[SidebarSessionEntry, ...] = Field(default=(), max_length=10000)
    expected_revision: int = Field(ge=0)

    @model_validator(mode="after")
    def structure(self) -> "SessionSidebarWrite":
        ids = [group.id for group in self.groups]
        if len(set(ids)) != len(ids):
            raise ValueError("重复分组ID")
        known = set(ids)
        entries: dict[str, SidebarSessionEntry] = {}
        for entry in self.sessions:
            if entry.session_id in entries:
                raise ValueError("重复会话条目")
            entries[entry.session_id] = entry
        for entry in self.sessions:
            if entry.group_id is not None and entry.group_id not in known:
                raise ValueError("分组不存在")
            if entry.parent_id is not None and entry.parent_id not in entries:
                raise ValueError("父会话不存在")
        depths: dict[str, int] = {}
        for start in self.sessions:
            chain: list[str] = []
            current = start
            while current.parent_id is not None and current.session_id not in depths:
                if current.session_id in chain:
                    raise ValueError("父会话成环")
                chain.append(current.session_id)
                parent = entries[current.parent_id]
                if parent.group_id != current.group_id:
                    raise ValueError("父会话分组不一致")
                current = parent
            if current.parent_id is None:
                base = 1
            else:
                base = depths[current.session_id]
            depth = base
            for session_id in reversed(chain):
                depth += 1
                if depth > SIDEBAR_MAX_PARENT_DEPTH:
                    raise ValueError("父会话层级超限")
                depths[session_id] = depth
        return self
