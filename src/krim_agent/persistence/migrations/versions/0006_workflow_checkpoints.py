"""Workflow 根/子运行与有序事件的一致检查点。"""

import sqlalchemy as sa
from alembic import op

revision = "0006_workflow_checkpoints"
down_revision = "0005_oauth_tokens"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflow_checkpoints",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("scope", sa.String(128), nullable=False),
        sa.Column("graph_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("run_id"),
        sa.CheckConstraint("revision >= 0", name="ck_workflow_revision"),
        sa.CheckConstraint(
            "(owner_id IS NULL AND lease_expires_at IS NULL) OR "
            "(owner_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_workflow_lease",
        ),
    )
    op.create_index("ix_workflow_checkpoints_scope", "workflow_checkpoints", ["scope"])
    op.create_index("ix_workflow_checkpoints_status", "workflow_checkpoints", ["status"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT 1 FROM workflow_checkpoints LIMIT 1")).first():
        raise RuntimeError("已有 Workflow 检查点，回退会丢失恢复状态；请先备份导出")
    op.drop_table("workflow_checkpoints")
