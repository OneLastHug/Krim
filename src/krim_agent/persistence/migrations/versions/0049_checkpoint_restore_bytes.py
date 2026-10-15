"""0049：冻结恢复前捕获格式，保留旧文本恢复JSON/hash/材料。"""

import sqlalchemy as sa
from alembic import op

revision = "0049_checkpoint_restore_bytes"
down_revision = "0048_native_checkpoint_bytes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        op.execute(
            sa.text(
                "ALTER TABLE workspace_restore_plans ADD COLUMN baseline_capture_format "
                "VARCHAR(8) NOT NULL DEFAULT 'text' CONSTRAINT "
                "ck_restore_baseline_format CHECK (baseline_capture_format IN ('text','bytes'))"
            )
        )
    else:
        op.add_column(
            "workspace_restore_plans",
            sa.Column(
                "baseline_capture_format", sa.String(8), nullable=False, server_default="text"
            ),
        )
        op.create_check_constraint(
            "ck_restore_baseline_format",
            "workspace_restore_plans",
            "baseline_capture_format IN ('text','bytes')",
        )


def downgrade() -> None:
    connection = op.get_bind()
    if connection.execute(
        sa.text(
            "SELECT 1 FROM workspace_restore_plans WHERE baseline_capture_format != 'text' LIMIT 1"
        )
    ).first():
        raise RuntimeError("字节恢复前基线不能降级丢失冻结格式")
    if connection.dialect.name == "sqlite":
        op.execute(
            sa.text("ALTER TABLE workspace_restore_plans DROP COLUMN baseline_capture_format")
        )
    else:
        op.drop_constraint("ck_restore_baseline_format", "workspace_restore_plans", type_="check")
        op.drop_column("workspace_restore_plans", "baseline_capture_format")
