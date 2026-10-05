"""宿主资源快照目录与执行账本；不复用 Workflow checkpoint 格式。"""

import sqlalchemy as sa
from alembic import op

revision = "0014_host_checkpoints"
down_revision = "0013_change_lists"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "host_checkpoint_topics",
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), primary_key=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("active_operation_id", sa.String(36), nullable=True),
        sa.CheckConstraint("revision >= 0", name="ck_host_checkpoint_revision"),
    )
    op.create_table(
        "host_checkpoints",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("title", sa.String(120), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("snapshot_ref", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_host_checkpoints_topic_id", "host_checkpoints", ["topic_id"])
    op.create_table(
        "host_checkpoint_operations",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column(
            "checkpoint_id", sa.String(36), sa.ForeignKey("host_checkpoints.id"), nullable=False
        ),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("execution", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("receipt", sa.JSON(), nullable=True),
        sa.Column("receipt_history", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("topic_id", "idempotency_key"),
    )
    op.create_index(
        "ix_host_checkpoint_operations_topic_id", "host_checkpoint_operations", ["topic_id"]
    )


def downgrade() -> None:
    for table in ("host_checkpoint_topics", "host_checkpoints", "host_checkpoint_operations"):
        if op.get_bind().execute(sa.text(f"SELECT COUNT(*) FROM {table}")).scalar():
            raise RuntimeError("已有宿主快照或执行账本，回退会丢失恢复/审计数据；请先导出")
    op.drop_index("ix_host_checkpoint_operations_topic_id", table_name="host_checkpoint_operations")
    op.drop_table("host_checkpoint_operations")
    op.drop_index("ix_host_checkpoints_topic_id", table_name="host_checkpoints")
    op.drop_table("host_checkpoints")
    op.drop_table("host_checkpoint_topics")
