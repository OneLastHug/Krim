"""Team 任务板表（schema 20 追加，与 subagent_models 同批合并导入）。

一个 subagent_trees 协作树维护一份持久任务板：工作项含目标、状态、依赖、
负责人、来源 task/run、产物与检查证据；revision 是板面 CAS 版本，
ownership_version 在每次归属变化时递增，保证转交后旧 worker 的旧版本
完成被拒。依赖与证据独立成表，用外键/唯一键承担完整性与幂等。

宿主通过 SQLiteStore.work_write/work_read 调用 TeamService（subagents/team.py），
本文件只定义表结构；TEAM_TABLES/TEAM_TABLE_NAMES 供 sqlite.py 的
_upgrade/create_all 与 schema20 集合推导导入。
"""

from sqlalchemy import (
    JSON,
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
from .subagent_models import subagent_members, subagent_trees  # noqa: F401  外键目标注册

# 板面状态：open（待领取）→ claimed（已归属）→ in_progress（已开始）→
# completed/failed/cancelled；blocked 表示存在 failed/cancelled 依赖的明确阻塞。
subagent_team_tasks = Table(
    "subagent_team_tasks", metadata,
    Column("id", String(36), primary_key=True),
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), nullable=False),
    Column("ordinal", Integer, nullable=False),
    Column("goal", String, nullable=False),
    Column("status", String(16), nullable=False),
    Column("owner_id", String(36), ForeignKey("subagent_members.id")),
    Column("created_by", String(36), ForeignKey("subagent_members.id"), nullable=False),
    Column("source_task_id", String(36), ForeignKey("subagent_tasks.id")),
    Column("source_run_id", String(36), ForeignKey("runs.id")),
    Column("revision", Integer, nullable=False),
    Column("ownership_version", Integer, nullable=False),
    Column("request_key", String(128), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    Column("completed_at", Float),
    UniqueConstraint("tree_id", "ordinal"),
    UniqueConstraint("tree_id", "request_key"),
    CheckConstraint("ordinal >= 1"),
    CheckConstraint("revision >= 1"),
    CheckConstraint("ownership_version >= 1"),
    CheckConstraint(
        "status IN ('open', 'claimed', 'in_progress', 'blocked',"
        " 'completed', 'failed', 'cancelled')"
    ),
)
Index("ix_subagent_team_tasks_tree_status", subagent_team_tasks.c.tree_id,
      subagent_team_tasks.c.status)
Index("ix_subagent_team_tasks_owner", subagent_team_tasks.c.owner_id)

# 依赖边：task 等 depends_on 完成；只允许同树任务，禁止自环（环由服务层
# DFS 在 create/update 事务内拒绝）。同树约束在事务内校验（跨树拒）。
subagent_team_task_deps = Table(
    "subagent_team_task_deps", metadata,
    Column("task_id", String(36), ForeignKey("subagent_team_tasks.id"), primary_key=True),
    Column("depends_on_id", String(36), ForeignKey("subagent_team_tasks.id"), primary_key=True),
    Column("created_at", Float, nullable=False),
    CheckConstraint("task_id != depends_on_id"),
)
Index("ix_subagent_team_task_deps_depends_on", subagent_team_task_deps.c.depends_on_id)

# 产物与检查证据：只追加，归属转交不删除；ownership_version/revision 记录
# 附着时的归属与板面版本，旧 worker 的产出在历史中保留归属轨迹。
subagent_team_task_evidence = Table(
    "subagent_team_task_evidence", metadata,
    Column("id", String(36), primary_key=True),
    Column("task_id", String(36), ForeignKey("subagent_team_tasks.id"), nullable=False),
    Column("agent_id", String(36), ForeignKey("subagent_members.id"), nullable=False),
    Column("kind", String(16), nullable=False),
    Column("body", JSON, nullable=False),
    Column("request_key", String(128), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("ownership_version", Integer, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
    UniqueConstraint("task_id", "request_key"),
    CheckConstraint("kind IN ('artifact', 'check')"),
    CheckConstraint("ownership_version >= 1"),
    CheckConstraint("revision >= 1"),
)
Index("ix_subagent_team_task_evidence_task", subagent_team_task_evidence.c.task_id)

TEAM_TABLES = (
    subagent_team_tasks,
    subagent_team_task_deps,
    subagent_team_task_evidence,
)
TEAM_TABLE_NAMES = frozenset(table.name for table in TEAM_TABLES)
