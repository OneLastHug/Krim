"""持久压缩提案与 exact summary 确认；保留过期/取消与完成审计。"""

import sqlalchemy as sa
from alembic import op

revision = "0018_context_proposals"
down_revision = "0017_usage_budgets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "context_compact_proposals",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("step", sa.Integer(), nullable=True),
        sa.Column("branch_key", sa.String(64), nullable=False),
        sa.Column("snapshot_hash", sa.String(64), nullable=False),
        sa.Column("prior_summary_hash", sa.String(64), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("revision >= 0", name="ck_context_proposal_revision"),
    )
    op.create_index("ix_context_compact_proposals_topic", "context_compact_proposals", ["topic_id"])
    op.create_index("ix_context_compact_proposals_run", "context_compact_proposals", ["run_id"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT COUNT(*) FROM context_compact_proposals")).scalar():
        raise RuntimeError("已有 Context 摘要压缩提案，降级会丢失确认/来源审计；请先导出")
    op.drop_index("ix_context_compact_proposals_run", table_name="context_compact_proposals")
    op.drop_index("ix_context_compact_proposals_topic", table_name="context_compact_proposals")
    op.drop_table("context_compact_proposals")
