"""子代理域公共类型：状态字面量、准入输入模型；输出一律 JSON 兼容 dict。

身份约定（plan §4.1）：tree_id 是一项根协作工作的代理树；agent_id 是子会话
稳定身份，跨续聊/驻留/重启不变；activation_id 是一次驻留实例，冷恢复产生
新值；task_id 关联一次明确委派；run_id 复用现有 RunStore 执行。
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue

from krim_agent.contracts import RunRequest, StrictModel

TreeStatus = Literal["open", "closed"]
MemberStatus = Literal["active", "closed"]
MemberRole = Literal["root", "agent"]
ActivationStatus = Literal["starting", "resident", "closing", "released"]
TaskStatus = Literal[
    "accepted",
    "running",
    "waiting_input",
    "awaiting_approval",
    "completed",
    "aborted",
    "error",
    "max_tokens",
    "refusal",
    "interrupted",
]
NotificationStatus = Literal["pending", "attached", "consumed", "suppressed"]
MessageKind = Literal["task", "message"]
WakeMode = Literal["auto", "queue_only"]

# 驻留容量口径：starting/resident/closing 都占用名额；released 不占。
# cleanup 失败的实例停在 closing，继续计入容量，不能被当成可复用（plan §5.2）。
LIVE_ACTIVATION_STATUS: frozenset[str] = frozenset({"starting", "resident", "closing"})
ACTIVE_TASK_STATUS: frozenset[str] = frozenset(
    {"accepted", "running", "waiting_input", "awaiting_approval"}
)
TERMINAL_TASK_STATUS: frozenset[str] = frozenset(
    {"completed", "aborted", "error", "max_tokens", "refusal", "interrupted"}
)
STOP_REASONS: frozenset[str] = TERMINAL_TASK_STATUS

KEY_PATTERN = r"^[A-Za-z0-9_.:-]+$"
PATH_PATTERN = r"^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*$"

MAX_PATH_LENGTH = 256
MAX_KEY_LENGTH = 128


class TreeScope(StrictModel):
    """ensure_tree 的宿主范围：根用户会话/turn/run、归属与项目、幂等键。"""

    session_id: str
    turn_id: str | None = None
    run_id: str | None = None
    workspace_id: str
    owner_id: str
    project_id: str | None = None
    request_key: str = Field(min_length=1, max_length=MAX_KEY_LENGTH, pattern=KEY_PATTERN)


class ReserveAgent(StrictModel):
    """预留名字与驻留容量的启动请求；provider 准备失败可整体回滚。"""

    tree_id: str
    parent_id: str | None = None
    path: str = Field(min_length=1, max_length=MAX_PATH_LENGTH, pattern=PATH_PATTERN)
    role: MemberRole = "agent"
    provider: str
    profile: str
    provider_version: str
    profile_version: str
    workdir: str | None = None
    # max_depth/resident_limit 是准入策略，不参与幂等身份哈希。
    max_depth: int | None = Field(default=None, ge=0)
    resident_limit: int | None = Field(default=None, ge=0)
    request_key: str = Field(min_length=1, max_length=MAX_KEY_LENGTH, pattern=KEY_PATTERN)


class AcceptSpawn(StrictModel):
    """持久接受初始任务：建任务、发首条消息、绑定当前任务。"""

    agent_id: str
    activation_id: str
    input: dict[str, JsonValue]
    parent_run_id: str | None = None


class SendMessage(StrictModel):
    """同树消息：send_key 承担重放幂等；kind=task 且不带 task_id 时开新任务。"""

    tree_id: str
    target_id: str
    sender_id: str | None = None
    kind: MessageKind = "message"
    wake: WakeMode = "auto"
    body: dict[str, JsonValue]
    send_key: str = Field(min_length=1, max_length=MAX_KEY_LENGTH, pattern=KEY_PATTERN)
    task_id: str | None = None


class StartTask(StrictModel):
    """为已接受任务创建生产 run：同一事务插入 run 与 child_run ownership 映射。"""

    agent_id: str
    task_id: str
    activation_id: str
    request: RunRequest
    target: dict[str, JsonValue]
    owner_turn_id: str | None = None


class SettleResult(StrictModel):
    """单次结算：同事务写结果与待通知记录；重放按内容哈希返回原结果。"""

    task_id: str
    run_id: str | None = None
    stop_reason: Literal[
        "completed", "aborted", "error", "max_tokens", "refusal", "interrupted"
    ]
    code: str | None = None
    output: dict[str, JsonValue]
    diagnostics: dict[str, JsonValue] | None = None
    expected_task_revision: int | None = None


class CommitInput(StrictModel):
    """父/子输入边界：checkpoint、收件箱与通知消费游标同一事务推进。"""

    agent_id: str
    payload: dict[str, JsonValue]
    message_ids: list[str] = Field(default_factory=list)
    notification_ids: list[str] = Field(default_factory=list)
    run_id: str | None = None
    expected_inbox_cursor: int | None = None
    expected_notify_cursor: int | None = None


class HistoryFrame(StrictModel):
    """成员独立历史快照；save_history 按版本 CAS 覆盖。"""

    payload: dict[str, JsonValue]
    expected_version: int | None = None
