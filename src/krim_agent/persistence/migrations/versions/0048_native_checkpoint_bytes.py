"""0048：原生捕获格式冻结；旧文本材料/请求hash不重写。"""

import sqlalchemy as sa
from alembic import op

revision = "0048_native_checkpoint_bytes"
down_revision = "0047_generated_tree_root_undos"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        # 只追加列，保留原checkpoint/restore相互引用与已有私有Blob。
        op.execute(
            sa.text(
                "ALTER TABLE native_workspace_checkpoints ADD COLUMN capture_format "
                "VARCHAR(8) NOT NULL DEFAULT 'text' CONSTRAINT "
                "ck_native_checkpoint_capture_format CHECK (capture_format IN ('text','bytes'))"
            )
        )
    else:
        op.add_column(
            "native_workspace_checkpoints",
            sa.Column("capture_format", sa.String(8), nullable=False, server_default="text"),
        )
        op.create_check_constraint(
            "ck_native_checkpoint_capture_format",
            "native_workspace_checkpoints",
            "capture_format IN ('text','bytes')",
        )


def downgrade() -> None:
    connection = op.get_bind()
    if connection.execute(
        sa.text("SELECT 1 FROM native_workspace_checkpoints WHERE capture_format != 'text' LIMIT 1")
    ).first():
        raise RuntimeError("原生字节检查点不能降级丢失格式与来源")
    if connection.dialect.name == "sqlite":
        op.execute(sa.text("ALTER TABLE native_workspace_checkpoints DROP COLUMN capture_format"))
    else:
        op.drop_constraint(
            "ck_native_checkpoint_capture_format", "native_workspace_checkpoints", type_="check"
        )
        op.drop_column("native_workspace_checkpoints", "capture_format")
