"""将 SSE 业务帧保存到 SQLite，使运行重启后可以按序恢复。"""

import sqlalchemy as sa
from alembic import op

revision = "0003_stream_events"
down_revision = "0002_message_tree"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run_events",
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), primary_key=True),
        sa.Column("seq", sa.Integer(), primary_key=True),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("data", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_run_events_run_id", "run_events", ["run_id"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT 1 FROM run_events LIMIT 1")).first():
        raise RuntimeError("已有持久流事件，回退会丢失续传数据；请先备份并导出")
    op.drop_index("ix_run_events_run_id", table_name="run_events")
    op.drop_table("run_events")
