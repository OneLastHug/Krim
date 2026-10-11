"""0041：原命令目录树的独立父/步骤冻结账本，保留旧表及外键。"""

import sqlalchemy as sa
from alembic import op

revision = "0041_workspace_tree_plans"
down_revision = "0040_mixed_batch_model_sources"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_tree_plans",
        sa.Column("plan_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column(
            "command_id",
            sa.String(36),
            sa.ForeignKey("command_executions.command_id"),
            nullable=False,
        ),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("root_path", sa.String(512), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("snapshot_revision", sa.String(64), nullable=False),
        sa.Column("frozen", sa.JSON(), nullable=False),
        sa.Column("metadata_bytes", sa.Integer(), nullable=False),
        sa.Column("step_count", sa.Integer(), nullable=False),
        sa.Column("producer_status", sa.String(16), nullable=False),
        sa.Column("created_owner_id", sa.String(36), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("stop_reason", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
        sa.CheckConstraint(
            "producer_status IN ('planned','running','finished','cancelled','interrupted')",
            name="ck_tree_plan_status",
        ),
        sa.CheckConstraint("step_count >= 1 AND step_count <= 16384", name="ck_tree_step_count"),
        sa.CheckConstraint("metadata_bytes >= 0", name="ck_tree_metadata_bytes"),
    )
    op.create_index("ix_workspace_tree_plans_scope_id", "workspace_tree_plans", ["scope_id"])
    op.create_table(
        "workspace_tree_steps",
        sa.Column(
            "plan_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_plans.plan_id"),
            primary_key=True,
        ),
        sa.Column("position", sa.Integer(), primary_key=True),
        sa.Column("parent_position", sa.Integer(), nullable=True),
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column("mode", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=True),
        sa.Column("byte_size", sa.Integer(), nullable=True),
        sa.Column(
            "file_proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_proposals.proposal_id"),
            nullable=True,
            unique=True,
        ),
        sa.Column(
            "directory_proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_directory_proposals.proposal_id"),
            nullable=True,
            unique=True,
        ),
        sa.Column("expected_revision", sa.Integer(), nullable=True),
        sa.Column(
            "file_operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_attempts.operation_id"),
            nullable=True,
            unique=True,
        ),
        sa.Column(
            "directory_operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_directory_attempts.operation_id"),
            nullable=True,
            unique=True,
        ),
        sa.Column("admission_open", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.UniqueConstraint("plan_id", "path"),
        sa.ForeignKeyConstraint(
            ["plan_id", "parent_position"],
            ["workspace_tree_steps.plan_id", "workspace_tree_steps.position"],
        ),
        sa.CheckConstraint("position >= 0 AND position < 16384", name="ck_tree_position"),
        sa.CheckConstraint(
            "(position=0 AND source='directory' AND parent_position IS NULL) OR "
            "(position>0 AND parent_position IS NOT NULL "
            "AND parent_position>=0 AND parent_position<position)",
            name="ck_tree_parent_position",
        ),
        sa.CheckConstraint("mode >= 0 AND mode <= 511", name="ck_tree_mode"),
        sa.CheckConstraint(
            "status IN ('pending','rejected','skipped')", name="ck_tree_step_status"
        ),
        sa.CheckConstraint(
            "(source='directory' AND sha256 IS NULL AND byte_size IS NULL "
            "AND file_proposal_id IS NULL AND file_operation_id IS NULL) OR "
            "(source='file' AND sha256 IS NOT NULL AND byte_size IS NOT NULL AND byte_size>=0 "
            "AND directory_proposal_id IS NULL AND directory_operation_id IS NULL)",
            name="ck_tree_source_references",
        ),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM workspace_tree_plans")).scalar_one():
        raise RuntimeError("目录树计划非空，拒绝有损降级")
    op.drop_table("workspace_tree_steps")
    op.drop_index("ix_workspace_tree_plans_scope_id", table_name="workspace_tree_plans")
    op.drop_table("workspace_tree_plans")
