"""按话题活动分支持久化 Context Engine 摘要。"""

import sqlalchemy as sa
from alembic import op

revision = "0008_context_summaries"
down_revision = "0007_workflow_requests"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "context_summaries",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("topic_id", sa.String(36), nullable=False),
        sa.Column("branch_key", sa.String(64), nullable=False),
        sa.Column("source_until_message_id", sa.String(36), nullable=True),
        sa.Column("source_message_ids", sa.JSON(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("strategy", sa.String(32), nullable=False),
        sa.Column("policy", sa.String(16), nullable=False),
        sa.Column("never_update", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["topic_id"], ["topics.id"]),
        sa.UniqueConstraint("topic_id", "branch_key"),
    )
    op.create_index("ix_context_summaries_topic_id", "context_summaries", ["topic_id"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT 1 FROM context_summaries LIMIT 1")).first():
        raise RuntimeError("已有 Context 摘要，回退会丢失压缩状态；请先备份导出")
    op.drop_table("context_summaries")
