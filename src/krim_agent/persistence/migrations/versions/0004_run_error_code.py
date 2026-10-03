"""持久错误码供 Python SDK 和缺失终帧修复保持同一真实终态。"""

import sqlalchemy as sa
from alembic import op

revision = "0004_run_error_code"
down_revision = "0003_stream_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("error_code", sa.String(64), nullable=True))


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT 1 FROM runs WHERE error_code IS NOT NULL LIMIT 1"))
        .first()
    ):
        raise RuntimeError("已有运行错误码，回退会丢失诊断与终帧恢复信息；请先备份导出")
    op.drop_column("runs", "error_code")
