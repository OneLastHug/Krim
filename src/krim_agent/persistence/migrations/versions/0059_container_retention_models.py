"""0059：回收当前模型来源；ALTER扩展，不重建旧GC表/JSON/外键。"""

import sqlalchemy as sa
from alembic import op

revision = "0059_container_retention_models"
down_revision = "0058_container_retention"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workspace_container_retentions",
        sa.Column("model_sources_metadata", sa.JSON(none_as_null=True), nullable=True),
    )
    op.create_table(
        "workspace_container_retention_model_sources",
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), primary_key=True),
        sa.Column("call_id", sa.String(128), primary_key=True),
        sa.Column(
            "operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_container_retentions.operation_id"),
            nullable=False,
        ),
        sa.Column("phase", sa.String(8), nullable=False),
        sa.Column("epoch", sa.Integer(), nullable=False),
        sa.UniqueConstraint("operation_id", "epoch", name="uq_container_retention_model_epoch"),
        sa.CheckConstraint(
            "phase IN ('remove','verify') AND length(call_id) BETWEEN 1 AND 128 "
            "AND ((phase = 'remove' AND epoch = 0) "
            "OR (phase = 'verify' AND epoch >= 1 AND epoch <= 128))",
            name="ck_container_retention_model_source",
        ),
    )


def downgrade() -> None:
    sql = op.get_bind()
    if (
        sql.execute(
            sa.text("SELECT 1 FROM workspace_container_retention_model_sources LIMIT 1")
        ).first()
        or sql.execute(
            sa.text(
                "SELECT 1 FROM workspace_container_retentions "
                "WHERE model_sources_metadata IS NOT NULL LIMIT 1"
            )
        ).first()
    ):
        raise RuntimeError("模型回收不能降级丢失原Run/call来源或标记")
    op.drop_table("workspace_container_retention_model_sources")
    op.drop_column("workspace_container_retentions", "model_sources_metadata")
