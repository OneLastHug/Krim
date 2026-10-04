"""持久工具审批与问题请求，不保存可重放的执行句柄。"""

import sqlalchemy as sa
from alembic import op

revision = "0009_user_interactions"
down_revision = "0008_context_summaries"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "interaction_requests",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("call_id", sa.String(128), nullable=False),
        sa.Column("tool_name", sa.String(64), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("arguments_hash", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("response", sa.JSON(), nullable=True),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("run_id", "call_id", "kind", "arguments_hash"),
    )
    op.create_index("ix_interaction_requests_run_id", "interaction_requests", ["run_id"])
    op.create_index("ix_interaction_requests_topic_id", "interaction_requests", ["topic_id"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT 1 FROM interaction_requests LIMIT 1")).first():
        raise RuntimeError("已有用户交互历史，降级会丢失审批/答案；请先备份导出")
    op.drop_table("interaction_requests")
