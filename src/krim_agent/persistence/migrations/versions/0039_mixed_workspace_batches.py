"""0039：混合文件/目录提案批次父子冻结账本。"""

import sqlalchemy as sa
from alembic import op

revision = "0039_mixed_workspace_batches"
down_revision = "0038_directory_move_target_lock"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "mixed_workspace_batches",
        sa.Column("batch_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("snapshot_revision", sa.String(64), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("producer_status", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("stop_reason", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
        sa.CheckConstraint("action IN ('apply','keep','undo')", name="ck_mixed_batch_action"),
        sa.CheckConstraint(
            "producer_status IN ('running','finished','cancelled','interrupted')",
            name="ck_mixed_batch_producer_status",
        ),
    )
    op.create_index("ix_mixed_workspace_batches_scope_id", "mixed_workspace_batches", ["scope_id"])
    op.create_table(
        "mixed_workspace_batch_children",
        sa.Column(
            "batch_id",
            sa.String(36),
            sa.ForeignKey("mixed_workspace_batches.batch_id"),
            primary_key=True,
        ),
        sa.Column("position", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("proposal_id", sa.String(36), nullable=False),
        sa.Column(
            "file_proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_proposals.proposal_id"),
            nullable=True,
        ),
        sa.Column(
            "directory_proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_directory_proposals.proposal_id"),
            nullable=True,
        ),
        sa.Column("expected_revision", sa.Integer(), nullable=False),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column("target_path", sa.String(512), nullable=True),
        sa.Column("physical_keys", sa.JSON(), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False, unique=True),
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
        sa.UniqueConstraint("batch_id", "source", "proposal_id"),
        sa.CheckConstraint("position >= 0 AND position < 32", name="ck_mixed_batch_position"),
        sa.CheckConstraint(
            "status IN ('pending','rejected','skipped')", name="ck_mixed_batch_child_status"
        ),
        sa.CheckConstraint(
            "(source='file' AND file_proposal_id=proposal_id "
            "AND file_proposal_id IS NOT NULL AND directory_proposal_id IS NULL "
            "AND directory_operation_id IS NULL AND target_path IS NULL) OR "
            "(source='directory' AND directory_proposal_id=proposal_id "
            "AND directory_proposal_id IS NOT NULL AND file_proposal_id IS NULL "
            "AND file_operation_id IS NULL)",
            name="ck_mixed_batch_source_references",
        ),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM mixed_workspace_batches")).scalar_one():
        raise RuntimeError("混合批次账本非空，拒绝有损降级")
    op.drop_table("mixed_workspace_batch_children")
    op.drop_index("ix_mixed_workspace_batches_scope_id", table_name="mixed_workspace_batches")
    op.drop_table("mixed_workspace_batches")
