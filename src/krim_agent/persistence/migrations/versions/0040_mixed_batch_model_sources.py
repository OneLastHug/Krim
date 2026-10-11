"""0040：模型混合批次来源附表，保留0039父子表及host历史。"""

import sqlalchemy as sa
from alembic import op

revision = "0040_mixed_batch_model_sources"
down_revision = "0039_mixed_workspace_batches"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "mixed_workspace_batch_model_sources",
        sa.Column(
            "batch_id",
            sa.String(36),
            sa.ForeignKey("mixed_workspace_batches.batch_id"),
            primary_key=True,
        ),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("call_id", sa.String(128), nullable=False),
        sa.UniqueConstraint("run_id", "call_id", name="uq_mixed_batch_model_call"),
        sa.CheckConstraint("length(call_id) BETWEEN 1 AND 128", name="ck_mixed_batch_model_call"),
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM mixed_workspace_batch_model_sources"))
        .scalar_one()
    ):
        raise RuntimeError("模型混合批次来源非空，拒绝有损降级")
    op.drop_table("mixed_workspace_batch_model_sources")
