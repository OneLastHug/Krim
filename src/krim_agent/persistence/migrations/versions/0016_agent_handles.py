"""持久 Agent 句柄目录；不序列化 provider、工具 callable、policy 或 task。"""

import sqlalchemy as sa
from alembic import op

revision = "0016_agent_handles"
down_revision = "0015_attachments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_handles",
        sa.Column("agent_id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("persistent", sa.Boolean(), nullable=False),
        sa.Column("turn", sa.Integer(), nullable=False),
        sa.Column("current_run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("definition_hash", sa.String(64), nullable=False),
        sa.Column("parent_agent_id", sa.String(36), nullable=True),
        sa.Column("parent_tool_call_id", sa.String(128), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("turn >= 0", name="ck_agent_turn"),
        sa.CheckConstraint("revision >= 0", name="ck_agent_handle_revision"),
        sa.CheckConstraint(
            "(owner_id IS NULL AND lease_expires_at IS NULL) OR "
            "(owner_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_agent_handle_lease",
        ),
    )
    op.create_index("ix_agent_handles_topic_id", "agent_handles", ["topic_id"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT COUNT(*) FROM agent_handles")).scalar():
        raise RuntimeError("已有持久 Agent 句柄，回退会丢失句柄目录；请先导出")
    op.drop_index("ix_agent_handles_topic_id", table_name="agent_handles")
    op.drop_table("agent_handles")
