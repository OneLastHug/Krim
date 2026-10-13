"""0045：原plan唯一完整实际树来源封存；不改旧已发布材料。"""

import sqlalchemy as sa
from alembic import op

revision = "0045_completed_tree_seals"
down_revision = "0044_tree_exchange_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_tree_completed_seals",
        sa.Column(
            "plan_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_plans.plan_id"),
            primary_key=True,
        ),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column(
            "command_id",
            sa.String(36),
            sa.ForeignKey("command_executions.command_id"),
            nullable=False,
        ),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("snapshot_revision", sa.String(64), nullable=False),
        sa.Column("tree_fingerprint", sa.String(64), nullable=False),
        sa.Column("metadata", sa.JSON(none_as_null=True), nullable=False),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("proof", sa.JSON(none_as_null=True), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("call_id", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("run_id", "call_id", name="uq_tree_seal_model_call"),
        sa.CheckConstraint(
            "(run_id IS NULL AND call_id IS NULL) OR (run_id IS NOT NULL AND call_id IS NOT NULL)",
            name="ck_tree_seal_run_call",
        ),
        sa.CheckConstraint(
            "call_id IS NULL OR length(call_id) BETWEEN 1 AND 128", name="ck_tree_seal_call"
        ),
    )
    op.create_index(
        "ix_workspace_tree_completed_seals_scope_id", "workspace_tree_completed_seals", ["scope_id"]
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM workspace_tree_completed_seals"))
        .scalar_one()
    ):
        raise RuntimeError("完整树来源封存非空，拒绝有损降级")
    op.drop_table("workspace_tree_completed_seals")
