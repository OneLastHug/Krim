"""子代理持久化表（schema 20 追加）：树、成员、驻留、任务、消息、结果、通知。

复用核心 metadata；树绑定根用户会话/项目/根任务，成员携带稳定 agent_id 与
名字路径，child_run ownership 映射把每条生产 run 接回宿主会话与 turn。
Team 任务板表随后另行追加，不在此文件。
"""

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
)

from .models import metadata

subagent_trees = Table(
    "subagent_trees", metadata,
    Column("id", String(36), primary_key=True),
    Column("session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("root_turn_id", String(36), ForeignKey("turns.id")),
    Column("root_run_id", String(36), ForeignKey("runs.id")),
    Column("workspace_id", String(64), nullable=False),
    Column("owner_id", String(64), nullable=False),
    Column("project_id", String(36), ForeignKey("sandbox_projects.project_id")),
    Column("request_key", String(128), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("input_version", Integer, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("close_reason", String(64)),
    Column("notify_cursor", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("closed_at", Float),
    UniqueConstraint("session_id", "request_key"),
    CheckConstraint("input_version >= 1"),
    CheckConstraint("revision >= 1"),
    CheckConstraint("notify_cursor >= 0"),
    CheckConstraint("status IN ('open', 'closed')"),
)

# 根成员（role='root'）可以存在但驻留准入只统计 role='agent' 的实例。
subagent_members = Table(
    "subagent_members", metadata,
    Column("id", String(36), primary_key=True),
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), nullable=False),
    Column("parent_id", String(36), ForeignKey("subagent_members.id")),
    Column("path", String(256), nullable=False),
    Column("role", String(8), nullable=False),
    Column("provider", String(64), nullable=False),
    Column("profile", String(64), nullable=False),
    Column("provider_version", String(64), nullable=False),
    Column("profile_version", String(64), nullable=False),
    Column("workdir", String),
    Column("depth", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("current_task_id", String(36), ForeignKey("subagent_tasks.id")),
    Column("inbox_cursor", Integer, nullable=False),
    Column("notify_cursor", Integer, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("request_key", String(128), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("closed_at", Float),
    UniqueConstraint("tree_id", "path"),
    UniqueConstraint("tree_id", "request_key"),
    CheckConstraint("depth >= 0"),
    CheckConstraint("inbox_cursor >= 0"),
    CheckConstraint("notify_cursor >= 0"),
    CheckConstraint("revision >= 1"),
    CheckConstraint("status IN ('active', 'closed')"),
    CheckConstraint("role IN ('root', 'agent')"),
)

# instance 是成员内单调递增的驻留实例版本：冷恢复产生新值，旧句柄的 CAS 在
# 新实例存在后必然失败；清理失败停在 closing，继续占用容量（plan §5.2）。
subagent_activations = Table(
    "subagent_activations", metadata,
    Column("id", String(36), primary_key=True),
    Column("agent_id", String(36), ForeignKey("subagent_members.id"), nullable=False),
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), nullable=False),
    Column("instance", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("provider_version", String(64), nullable=False),
    Column("cleanup_code", String(64)),
    Column("created_at", Float, nullable=False),
    Column("resident_at", Float),
    Column("released_at", Float),
    UniqueConstraint("agent_id", "instance"),
    CheckConstraint("instance >= 1"),
    CheckConstraint(
        "status IN ('starting', 'resident', 'closing', 'released')"
    ),
)

# 任务：accepted → running ↔ waiting_input / awaiting_approval → 终态；
# 终态只能经 settle_result 同事务落结果与通知，set_task_status 只走活跃态。
subagent_tasks = Table(
    "subagent_tasks", metadata,
    Column("id", String(36), primary_key=True),
    Column("agent_id", String(36), ForeignKey("subagent_members.id"), nullable=False),
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), nullable=False),
    Column("ordinal", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("run_id", String(36), ForeignKey("runs.id")),
    Column("parent_run_id", String(36), ForeignKey("runs.id")),
    Column("source_version", Integer, nullable=False),
    Column("provider_version", String(64), nullable=False),
    Column("profile_version", String(64), nullable=False),
    Column("input", JSON, nullable=False),
    Column("input_hash", String(64), nullable=False),
    Column("request_key", String(128), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("input_closed", Boolean, nullable=False, default=False),
    Column("created_at", Float, nullable=False),
    Column("finished_at", Float),
    UniqueConstraint("agent_id", "ordinal"),
    UniqueConstraint("tree_id", "request_key"),
    CheckConstraint("ordinal >= 1"),
    CheckConstraint("revision >= 1"),
    CheckConstraint(
        "status IN ('accepted', 'running', 'waiting_input', 'awaiting_approval',"
        " 'completed', 'aborted', 'error', 'max_tokens', 'refusal', 'interrupted')"
    ),
)

# child_run ownership：每条生产 run 恰好属于一个子代理任务；宿主用
# owner_session_id/owner_turn_id 把重启后停靠的 run 纳入权限与批准路由。
subagent_child_runs = Table(
    "subagent_child_runs", metadata,
    Column("run_id", String(36), ForeignKey("runs.id"), primary_key=True),
    Column("task_id", String(36), ForeignKey("subagent_tasks.id"), nullable=False),
    Column("agent_id", String(36), ForeignKey("subagent_members.id"), nullable=False),
    Column("activation_id", String(36), ForeignKey("subagent_activations.id"), nullable=False),
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), nullable=False),
    Column("owner_session_id", String(36), ForeignKey("sessions.id"), nullable=False),
    Column("owner_turn_id", String(36), ForeignKey("turns.id")),
    Column("source_version", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
)
Index("ix_subagent_child_runs_task", subagent_child_runs.c.task_id)
Index("ix_subagent_child_runs_owner", subagent_child_runs.c.owner_session_id)

# 收件箱：目标内 seq 连续分配；同一 send_key 重放返回原回执。消费靠成员
# inbox_cursor 与输入 checkpoint 关联，不重复附入同一内容。
subagent_messages = Table(
    "subagent_messages", metadata,
    Column("id", String(36), primary_key=True),
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), nullable=False),
    Column("target_id", String(36), ForeignKey("subagent_members.id"), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("sender_id", String(36), ForeignKey("subagent_members.id")),
    Column("task_id", String(36), ForeignKey("subagent_tasks.id")),
    Column("kind", String(16), nullable=False),
    Column("wake", String(16), nullable=False),
    Column("body", JSON, nullable=False),
    Column("body_hash", String(64), nullable=False),
    Column("send_key", String(128), nullable=False),
    Column("send_key_hash", String(64), nullable=False),
    Column("checkpoint_id", String(36), ForeignKey("subagent_input_checkpoints.id")),
    Column("accepted_at", Float, nullable=False),
    UniqueConstraint("target_id", "seq"),
    UniqueConstraint("target_id", "send_key_hash"),
    CheckConstraint("seq >= 1"),
    CheckConstraint("kind IN ('task', 'message')"),
    CheckConstraint("wake IN ('auto', 'queue_only')"),
)
Index("ix_subagent_messages_task", subagent_messages.c.task_id)

# 结果：每任务至多一条（单次结算）；内容哈希承担重放等值校验。
subagent_results = Table(
    "subagent_results", metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", String(36), ForeignKey("subagent_tasks.id"), nullable=False),
    Column("run_id", String(36), ForeignKey("runs.id")),
    Column("stop_reason", String(16), nullable=False),
    Column("code", String(64)),
    Column("output", JSON, nullable=False),
    Column("diagnostics", JSON),
    Column("content_hash", String(64), nullable=False),
    Column("settled_at", Float, nullable=False),
    UniqueConstraint("task_id"),
)

# 完成通知：pending → attached（附入父输入 checkpoint）→ consumed（父 step
# 提交）；suppressed 保留结果与原因。consumer_id 为空表示宿主消费（根成员
# 结果）；seq 按树分配，游标存于消费方（成员 notify_cursor / 树 notify_cursor）。
subagent_notifications = Table(
    "subagent_notifications", metadata,
    Column("id", String(36), primary_key=True),
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), nullable=False),
    Column("result_id", String(36), ForeignKey("subagent_results.id"), nullable=False),
    Column("task_id", String(36), ForeignKey("subagent_tasks.id"), nullable=False),
    Column("producer_id", String(36), ForeignKey("subagent_members.id"), nullable=False),
    Column("consumer_id", String(36), ForeignKey("subagent_members.id")),
    Column("seq", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("checkpoint_id", String(36), ForeignKey("subagent_input_checkpoints.id")),
    Column("suppress_reason", String(64)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    UniqueConstraint("tree_id", "seq"),
    CheckConstraint("seq >= 1"),
    CheckConstraint("status IN ('pending', 'attached', 'consumed', 'suppressed')"),
)
Index("ix_subagent_notifications_consumer", subagent_notifications.c.consumer_id, "status")

# 父/子输入 checkpoint：追加保存；收件箱消息与完成通知在 commit_input 同一
# 事务附入 checkpoint 并推进游标，避免模型重放输入时重复附入。
subagent_input_checkpoints = Table(
    "subagent_input_checkpoints", metadata,
    Column("id", String(36), primary_key=True),
    Column("agent_id", String(36), ForeignKey("subagent_members.id"), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("run_id", String(36), ForeignKey("runs.id")),
    Column("payload", JSON, nullable=False),
    Column("fingerprint", String(64), nullable=False),
    Column("created_at", Float, nullable=False),
    UniqueConstraint("agent_id", "seq"),
    CheckConstraint("seq >= 1"),
)

# 成员独立历史：一行一份最新快照，版本 CAS 覆盖；历史成员与结果不占驻留。
subagent_history = Table(
    "subagent_history", metadata,
    Column("agent_id", String(36), ForeignKey("subagent_members.id"), primary_key=True),
    Column("version", Integer, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("fingerprint", String(64), nullable=False),
    Column("updated_at", Float, nullable=False),
    CheckConstraint("version >= 1"),
)

SUBAGENT_TABLES = (
    subagent_trees,
    subagent_members,
    subagent_activations,
    subagent_tasks,
    subagent_child_runs,
    subagent_messages,
    subagent_results,
    subagent_notifications,
    subagent_input_checkpoints,
    subagent_history,
)
SUBAGENT_TABLE_NAMES = frozenset(table.name for table in SUBAGENT_TABLES)
