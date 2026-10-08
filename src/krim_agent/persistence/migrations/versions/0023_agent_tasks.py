"""Agent session plan/todo 持久账本与幂等操作。"""

import sqlalchemy as sa
from alembic import op

revision = "0023_agent_tasks"
down_revision = "0022_agent_sessions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_task_heads",
        sa.Column("scope_id", sa.String(80), primary_key=True),
        sa.Column("kind", sa.String(16), primary_key=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("kind IN ('plan','todo')", name="ck_agent_task_head_kind"),
        sa.CheckConstraint("revision >= 0", name="ck_agent_task_revision"),
    )
    op.create_table(
        "agent_task_items",
        sa.Column("scope_id", sa.String(80), primary_key=True),
        sa.Column("kind", sa.String(16), primary_key=True),
        sa.Column("item_id", sa.String(128), primary_key=True),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("content", sa.String(200), nullable=False),
        sa.Column("completed", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("kind IN ('plan','todo')", name="ck_agent_task_item_kind"),
        sa.CheckConstraint("position >= 0", name="ck_agent_task_position"),
    )
    op.create_index("ix_agent_task_items_scope_kind", "agent_task_items", ["scope_id", "kind"])
    op.create_table(
        "agent_task_operations",
        sa.Column("scope_id", sa.String(80), primary_key=True),
        sa.Column("kind", sa.String(16), primary_key=True),
        sa.Column("request_key", sa.String(128), primary_key=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("result", sa.JSON(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(16), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("call_id", sa.String(128), nullable=True),
        sa.Column("tool_request_hash", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    for table in ("agent_task_heads", "agent_task_items", "agent_task_operations"):
        if op.get_bind().execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one():
            raise RuntimeError("Agent计划/待办或历史非空，拒绝有损降级")
    op.drop_table("agent_task_operations")
    op.drop_index("ix_agent_task_items_scope_kind", table_name="agent_task_items")
    op.drop_table("agent_task_items")
    op.drop_table("agent_task_heads")
