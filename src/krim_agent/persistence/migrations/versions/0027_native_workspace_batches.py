"""持久保存原生批次编排，只引用原文件操作事实。"""

import sqlalchemy as sa
from alembic import op

revision = "0027_native_workspace_batches"
down_revision = "0026_workspace_effect_chain"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "native_workspace_batches",
        sa.Column("batch_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("snapshot_revision", sa.String(64), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("producer_status", sa.String(16), nullable=False),
        sa.Column("stop_reason", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
        sa.CheckConstraint("action IN ('keep','undo')", name="ck_native_batch_action"),
        sa.CheckConstraint(
            "producer_status IN ('running','finished','cancelled','interrupted')",
            name="ck_native_batch_producer_status",
        ),
    )
    op.create_index(
        "ix_native_workspace_batches_scope_id", "native_workspace_batches", ["scope_id"]
    )
    op.create_table(
        "native_workspace_batch_children",
        sa.Column(
            "batch_id",
            sa.String(36),
            sa.ForeignKey("native_workspace_batches.batch_id"),
            primary_key=True,
        ),
        sa.Column("position", sa.Integer(), primary_key=True),
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_proposals.proposal_id"),
            nullable=False,
        ),
        sa.Column("expected_revision", sa.Integer(), nullable=False),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column("file_key", sa.String(64), nullable=False),
        sa.Column("apply_sequence", sa.Integer(), nullable=True),
        sa.Column("request_key", sa.String(128), nullable=False, unique=True),
        sa.Column(
            "operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_attempts.operation_id"),
            nullable=True,
            unique=True,
        ),
        sa.Column("admission_open", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.UniqueConstraint("batch_id", "proposal_id"),
        sa.CheckConstraint("position >= 0 AND position < 32", name="ck_native_batch_position"),
        sa.CheckConstraint(
            "status IN ('pending','rejected','skipped')", name="ck_native_batch_child_status"
        ),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM native_workspace_batches")).scalar_one():
        raise RuntimeError("原生修改批次账本非空，拒绝丢失编排与操作关联的有损降级")
    op.drop_table("native_workspace_batch_children")
    op.drop_index("ix_native_workspace_batches_scope_id", table_name="native_workspace_batches")
    op.drop_table("native_workspace_batches")
