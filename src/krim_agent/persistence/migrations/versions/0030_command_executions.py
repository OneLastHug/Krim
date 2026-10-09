"""隔离命令请求、私有输入/产物与不可重放派发记录。"""

import sqlalchemy as sa
from alembic import op

revision = "0030_command_executions"
down_revision = "0029_workspace_restore_plans"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "command_ledger_guard",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("revision", sa.Integer(), nullable=False),
    )
    op.create_table(
        "command_executions",
        sa.Column("command_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("snapshot", sa.JSON(), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("call_id", sa.String(128), nullable=True),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("sandbox_id", sa.String(36), nullable=True),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False),
        sa.Column("dispatched", sa.Boolean(), nullable=False),
        sa.Column("started", sa.Boolean(), nullable=False),
        sa.Column("stdout", sa.LargeBinary(), nullable=False),
        sa.Column("stderr", sa.LargeBinary(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("reserved_bytes", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("interrupted_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("scope_id", "request_key"),
        sa.UniqueConstraint("run_id", "call_id"),
        sa.CheckConstraint(
            "phase IN ('preparing','dispatched','running','finished','interrupted')",
            name="ck_command_phase",
        ),
        sa.CheckConstraint("revision >= 0 AND reserved_bytes >= 0", name="ck_command_counts"),
        sa.CheckConstraint(
            "(phase IN ('preparing','dispatched','running') AND owner_id IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR (phase IN ('finished','interrupted') "
            "AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_command_owner",
        ),
        sa.CheckConstraint(
            "(run_id IS NULL AND call_id IS NULL) OR (run_id IS NOT NULL AND call_id IS NOT NULL)",
            name="ck_command_call",
        ),
    )
    op.create_index("ix_command_executions_scope_id", "command_executions", ["scope_id"])
    op.create_table(
        "command_files",
        sa.Column(
            "command_id",
            sa.String(36),
            sa.ForeignKey("command_executions.command_id"),
            primary_key=True,
        ),
        sa.Column("kind", sa.String(8), primary_key=True),
        sa.Column("path", sa.String(512), primary_key=True),
        sa.Column("mode", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=False),
    )
    op.create_table(
        "command_events",
        sa.Column(
            "command_id",
            sa.String(36),
            sa.ForeignKey("command_executions.command_id"),
            primary_key=True,
        ),
        sa.Column("sequence", sa.Integer(), primary_key=True),
        sa.Column("kind", sa.String(24), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM command_executions")).scalar_one():
        raise RuntimeError("命令记录非空，拒绝丢失派发事实与私有成果的有损降级")
    op.drop_table("command_events")
    op.drop_table("command_files")
    op.drop_index("ix_command_executions_scope_id", table_name="command_executions")
    op.drop_table("command_executions")
    op.drop_table("command_ledger_guard")
