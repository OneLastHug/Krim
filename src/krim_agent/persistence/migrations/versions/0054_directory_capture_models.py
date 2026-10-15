"""0054：原模型目录捕获来源；Host原行/摘要/材料保持。"""

import sqlalchemy as sa
from alembic import op

revision = "0054_directory_capture_models"
down_revision = "0053_directory_captures"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        op.execute(
            sa.text(
                "ALTER TABLE workspace_directory_captures ADD COLUMN model_bound "
                "BOOLEAN NOT NULL DEFAULT 0 CONSTRAINT ck_directory_capture_model_bound "
                "CHECK (model_bound IN (0,1))"
            )
        )
    else:
        op.add_column(
            "workspace_directory_captures",
            sa.Column("model_bound", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        )
        op.create_check_constraint(
            "ck_directory_capture_model_bound",
            "workspace_directory_captures",
            "model_bound IN (0,1)",
        )
    op.create_table(
        "workspace_directory_capture_model_sources",
        sa.Column(
            "checkpoint_id",
            sa.String(36),
            sa.ForeignKey("workspace_directory_captures.checkpoint_id"),
            primary_key=True,
        ),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("call_id", sa.String(128), nullable=False),
        sa.UniqueConstraint("run_id", "call_id", name="uq_directory_capture_model_call"),
        sa.CheckConstraint("length(call_id) BETWEEN 1 AND 128", name="ck_directory_capture_call"),
    )


def downgrade() -> None:
    sql = op.get_bind()
    if (
        sql.execute(
            sa.text("SELECT 1 FROM workspace_directory_capture_model_sources LIMIT 1")
        ).first()
        or sql.execute(
            sa.text("SELECT 1 FROM workspace_directory_captures WHERE model_bound != 0 LIMIT 1")
        ).first()
    ):
        raise RuntimeError("模型目录检查点不能降级丢失原Run/call来源")
    op.drop_table("workspace_directory_capture_model_sources")
    if sql.dialect.name != "sqlite":
        op.drop_constraint(
            "ck_directory_capture_model_bound", "workspace_directory_captures", type_="check"
        )
    op.drop_column("workspace_directory_captures", "model_bound")
