"""Workflow API 的作用域幂等请求索引。"""

import sqlalchemy as sa
from alembic import op

revision = "0007_workflow_requests"
down_revision = "0006_workflow_checkpoints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflow_requests",
        sa.Column("scope", sa.String(128), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("scope", "idempotency_key"),
        sa.ForeignKeyConstraint(["run_id"], ["workflow_checkpoints.run_id"]),
        sa.UniqueConstraint("run_id"),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT 1 FROM workflow_requests LIMIT 1")).first():
        raise RuntimeError("已有 Workflow 请求索引，回退会丢失幂等边界；请先备份导出")
    op.drop_table("workflow_requests")
