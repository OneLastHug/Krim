"""0050：模型原生捕获父调用来源，保留旧宿主格式与循环FK材料。"""

import sqlalchemy as sa
from alembic import op

revision = "0050_native_checkpoint_models"
down_revision = "0049_checkpoint_restore_bytes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        op.execute(
            sa.text(
                "ALTER TABLE native_workspace_checkpoints ADD COLUMN model_bound "
                "BOOLEAN NOT NULL DEFAULT 0 CONSTRAINT ck_native_checkpoint_model_bound "
                "CHECK (model_bound IN (0,1))"
            )
        )
    else:
        op.add_column(
            "native_workspace_checkpoints",
            sa.Column("model_bound", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        )
        op.create_check_constraint(
            "ck_native_checkpoint_model_bound",
            "native_workspace_checkpoints",
            "model_bound IN (0,1)",
        )
    op.create_table(
        "native_checkpoint_model_sources",
        sa.Column(
            "checkpoint_id",
            sa.String(36),
            sa.ForeignKey("native_workspace_checkpoints.checkpoint_id"),
            primary_key=True,
        ),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("call_id", sa.String(128), nullable=False),
        sa.UniqueConstraint("run_id", "call_id", name="uq_native_checkpoint_model_call"),
        sa.CheckConstraint("length(call_id) BETWEEN 1 AND 128", name="ck_native_checkpoint_call"),
    )


def downgrade() -> None:
    sql = op.get_bind()
    if sql.execute(sa.text("SELECT 1 FROM native_checkpoint_model_sources LIMIT 1")).first() or (
        sql.execute(
            sa.text("SELECT 1 FROM native_workspace_checkpoints WHERE model_bound != 0 LIMIT 1")
        ).first()
    ):
        raise RuntimeError("模型检查点不能降级丢失原Run/call来源")
    op.drop_table("native_checkpoint_model_sources")
    if sql.dialect.name != "sqlite":
        op.drop_constraint(
            "ck_native_checkpoint_model_bound", "native_workspace_checkpoints", type_="check"
        )
    op.drop_column("native_workspace_checkpoints", "model_bound")
