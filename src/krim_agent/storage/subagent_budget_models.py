"""子代理请求预算表（计划 8.3，schema 20+ 追加）：树预算、子租约与计费台账。

只计真实模型请求次数，不伪造 usage/token；原始用量仍由既有 run 事件唯一记账。
表接入全局 schema（sqlite.py 阶梯与 metadata.create_all）由主会话统一完成；
本文件只定义表与导出 BUDGET_TABLES/BUDGET_TABLE_NAMES。

核算模型：
- budgets.spent 是全树已计费请求数（根池 + 各租约的 charge 之和）。
- 活跃租约的 request_limit 仍占调用方池位（outstanding）；release 后未用
  部分自动回流，spent 不回流。
- 有限树/有限租约下的子租约划拨必然有界（reserve 即时校验），因此
  remaining = limit - spent - 活跃子租约 grant 之和。
"""

from sqlalchemy import (
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
)

from .models import metadata

# 每棵树一条预算行；request_limit 为 None 表示不限额（真正继承 unlimited），
# spent 允许超过 request_limit（旧根已花费可能超过新设定的交集上限）。
subagent_budgets = Table(
    "subagent_budgets", metadata,
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), primary_key=True),
    Column("request_limit", Integer),
    Column("spent", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    CheckConstraint("spent >= 0"),
    CheckConstraint("request_limit IS NULL OR request_limit >= 0"),
)

# 子租约：从根池（parent_key 为空）或活跃父租约划拨；key 由调用方固化进
# task.input，重放同键同意图（requested/parent_key 原值）返回原租约。
# requested 是调用方原始意图（None=默认划拨），request_limit 是已解析的
# 划拨额（默认规则落地后的值，None=不限额）；复合外键保证父子同树。
subagent_budget_leases = Table(
    "subagent_budget_leases", metadata,
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), primary_key=True),
    Column("key", String(128), primary_key=True),
    Column("parent_key", String(128)),
    Column("request_limit", Integer),
    Column("requested", Integer),
    Column("spent", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("released_at", Float),
    ForeignKeyConstraint(
        ["parent_key", "tree_id"],
        ["subagent_budget_leases.key", "subagent_budget_leases.tree_id"],
    ),
    CheckConstraint("spent >= 0"),
    CheckConstraint("request_limit IS NULL OR request_limit >= 1"),
    CheckConstraint("status IN ('active', 'released')"),
)
Index(
    "ix_subagent_budget_leases_parent",
    subagent_budget_leases.c.tree_id,
    subagent_budget_leases.c.parent_key,
)

# 计费台账：每次真实模型请求一行，attempt_id 是调用方预先固定的稳定身份；
# 同一 run/step 可多次计费（重新推理是新的真实请求），只按 attempt_id 幂等。
subagent_budget_charges = Table(
    "subagent_budget_charges", metadata,
    Column("attempt_id", String(36), primary_key=True),
    Column("tree_id", String(36), ForeignKey("subagent_trees.id"), nullable=False),
    Column("run_id", String(36), ForeignKey("runs.id"), nullable=False),
    Column("lease_key", String(128)),
    Column("step_index", Integer, nullable=False),
    Column("created_at", Float, nullable=False),
    ForeignKeyConstraint(
        ["tree_id", "lease_key"],
        ["subagent_budget_leases.tree_id", "subagent_budget_leases.key"],
    ),
    CheckConstraint("step_index >= 0"),
)
Index("ix_subagent_budget_charges_tree", subagent_budget_charges.c.tree_id)

BUDGET_TABLES = (
    subagent_budgets,
    subagent_budget_leases,
    subagent_budget_charges,
)
BUDGET_TABLE_NAMES = frozenset(table.name for table in BUDGET_TABLES)
