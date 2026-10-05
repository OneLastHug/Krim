"""默认无记录的可选脱敏追踪，不删除既有观测历史。"""

import sqlalchemy as sa
from alembic import op

revision = "0019_trace_spans"
down_revision = "0018_context_proposals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "trace_spans",
        sa.Column("span_id", sa.String(16), primary_key=True),
        sa.Column("trace_id", sa.String(32), nullable=False),
        sa.Column("owner_kind", sa.String(16), nullable=False),
        sa.Column("owner_ref", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index("ix_trace_spans_trace_id", "trace_spans", ["trace_id"])
    op.create_index("ix_trace_spans_owner_ref", "trace_spans", ["owner_ref"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT COUNT(*) FROM trace_spans")).scalar():
        raise RuntimeError("已有追踪记录，降级会丢失观测历史；请先导出")
    op.drop_index("ix_trace_spans_owner_ref", table_name="trace_spans")
    op.drop_index("ix_trace_spans_trace_id", table_name="trace_spans")
    op.drop_table("trace_spans")
