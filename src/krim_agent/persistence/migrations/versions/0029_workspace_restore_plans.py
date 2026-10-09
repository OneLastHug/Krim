"""原生检查点条件恢复计划与原文件提案/操作关联。"""

import sqlalchemy as sa
from alembic import op

revision = "0029_workspace_restore_plans"
down_revision = "0028_native_workspace_checkpoints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_restore_plans",
        sa.Column("plan_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("workspace_fingerprint", sa.String(64), nullable=False),
        sa.Column(
            "checkpoint_id",
            sa.String(36),
            sa.ForeignKey("native_workspace_checkpoints.checkpoint_id"),
            nullable=False,
        ),
        sa.Column(
            "baseline_checkpoint_id",
            sa.String(36),
            sa.ForeignKey("native_workspace_checkpoints.checkpoint_id"),
            nullable=True,
        ),
        sa.Column("baseline_key", sa.String(128), nullable=False, unique=True),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("execute_key", sa.String(128), nullable=True),
        sa.Column("execute_hash", sa.String(64), nullable=True),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
        sa.UniqueConstraint("scope_id", "execute_key"),
        sa.CheckConstraint(
            "phase IN ('preparing','ready','executing','finished','failed',"
            "'cancelled','interrupted','expired')",
            name="ck_restore_phase",
        ),
        sa.CheckConstraint(
            "(phase IN ('preparing','executing') AND owner_id IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR (phase NOT IN ('preparing','executing') "
            "AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_restore_owner",
        ),
        sa.CheckConstraint(
            "revision >= 0 AND revision <= 9007199254740991", name="ck_restore_revision"
        ),
    )
    op.create_index("ix_workspace_restore_plans_scope_id", "workspace_restore_plans", ["scope_id"])
    op.create_table(
        "workspace_restore_items",
        sa.Column(
            "plan_id",
            sa.String(36),
            sa.ForeignKey("workspace_restore_plans.plan_id"),
            primary_key=True,
        ),
        sa.Column("position", sa.Integer(), primary_key=True),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_proposals.proposal_id"),
            nullable=True,
            unique=True,
        ),
        sa.Column(
            "operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_attempts.operation_id"),
            nullable=True,
            unique=True,
        ),
        sa.Column("expected_revision", sa.Integer(), nullable=False),
        sa.Column("file_key", sa.String(64), nullable=True),
        sa.Column("effect_sequence", sa.Integer(), nullable=True),
        sa.Column("request_key", sa.String(128), nullable=False, unique=True),
        sa.Column("admission_open", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("current_mode", sa.Integer(), nullable=True),
        sa.Column("target_mode", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.UniqueConstraint("plan_id", "path"),
        sa.CheckConstraint("position >= 0 AND position < 32", name="ck_restore_position"),
        sa.CheckConstraint(
            "status IN ('pending','rejected','skipped')", name="ck_restore_item_status"
        ),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM workspace_restore_plans")).scalar_one():
        raise RuntimeError("恢复计划非空，拒绝丢失提案来源与执行资格的有损降级")
    op.drop_table("workspace_restore_items")
    op.drop_index("ix_workspace_restore_plans_scope_id", table_name="workspace_restore_plans")
    op.drop_table("workspace_restore_plans")
