"""评估批次、样本检查点和独立进度事件；非空历史禁止降级删除。"""

import sqlalchemy as sa
from alembic import op

revision = "0020_evaluation_jobs"
down_revision = "0019_trace_spans"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "eval_jobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("scope", sa.String(64), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("snapshot", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("control", sa.String(16), nullable=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("epoch", sa.Integer(), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("report", sa.JSON(), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("scope", "request_key"),
        sa.CheckConstraint(
            "epoch >= 0 AND revision >= 0 AND event_count >= 0", name="ck_eval_job_counts"
        ),
        sa.CheckConstraint(
            "(owner_id IS NULL AND lease_expires_at IS NULL) OR "
            "(owner_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_eval_job_lease",
        ),
    )
    op.create_index("ix_eval_jobs_scope", "eval_jobs", ["scope"])
    op.create_index("ix_eval_jobs_status", "eval_jobs", ["status"])
    op.create_table(
        "eval_items",
        sa.Column("job_id", sa.String(36), sa.ForeignKey("eval_jobs.id"), primary_key=True),
        sa.Column("ordinal", sa.Integer(), primary_key=True),
        sa.Column("case_id", sa.String(128), nullable=False),
        sa.Column("repetition", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("bindings", sa.JSON(), nullable=False),
        sa.Column("output", sa.JSON(), nullable=True),
        sa.Column("scores", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("history", sa.JSON(), nullable=False),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("duration_ms", sa.Float(), nullable=False),
        sa.Column("scoring_ms", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_eval_items_phase", "eval_items", ["phase"])
    op.create_table(
        "eval_events",
        sa.Column("job_id", sa.String(36), sa.ForeignKey("eval_jobs.id"), primary_key=True),
        sa.Column("seq", sa.Integer(), primary_key=True),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT COUNT(*) FROM eval_jobs")).scalar():
        raise RuntimeError("已有评估任务，降级会丢失执行和评分历史；请先导出")
    op.drop_table("eval_events")
    op.drop_index("ix_eval_items_phase", table_name="eval_items")
    op.drop_table("eval_items")
    op.drop_index("ix_eval_jobs_status", table_name="eval_jobs")
    op.drop_index("ix_eval_jobs_scope", table_name="eval_jobs")
    op.drop_table("eval_jobs")
