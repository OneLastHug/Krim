"""原生文件检查点的冻结请求和有界私有快照。"""

import sqlalchemy as sa
from alembic import op

revision = "0028_native_workspace_checkpoints"
down_revision = "0027_native_workspace_batches"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "native_workspace_checkpoints",
        sa.Column("checkpoint_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("workspace_fingerprint", sa.String(64), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("title", sa.String(120), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
        sa.CheckConstraint(
            "status IN ('capturing','available','failed','cancelled','interrupted')",
            name="ck_native_checkpoint_status",
        ),
        sa.CheckConstraint(
            "(status = 'capturing' AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status != 'capturing' AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_native_checkpoint_owner",
        ),
    )
    op.create_index(
        "ix_native_workspace_checkpoints_scope_id", "native_workspace_checkpoints", ["scope_id"]
    )
    op.create_table(
        "native_workspace_checkpoint_files",
        sa.Column(
            "checkpoint_id",
            sa.String(36),
            sa.ForeignKey("native_workspace_checkpoints.checkpoint_id"),
            primary_key=True,
        ),
        sa.Column("position", sa.Integer(), primary_key=True),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column("captured", sa.Boolean(), nullable=False),
        sa.Column("capture_metadata", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("content", sa.LargeBinary(), nullable=True),
        sa.UniqueConstraint("checkpoint_id", "path"),
        sa.CheckConstraint("position >= 0 AND position < 32", name="ck_native_checkpoint_position"),
        sa.CheckConstraint(
            "(captured = 1 AND capture_metadata IS NOT NULL) "
            "OR (captured = 0 AND capture_metadata IS NULL AND content IS NULL)",
            name="ck_native_checkpoint_capture",
        ),
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM native_workspace_checkpoints"))
        .scalar_one()
    ):
        raise RuntimeError("原生文件检查点非空，拒绝丢失冻结快照的有损降级")
    op.drop_table("native_workspace_checkpoint_files")
    op.drop_index(
        "ix_native_workspace_checkpoints_scope_id", table_name="native_workspace_checkpoints"
    )
    op.drop_table("native_workspace_checkpoints")
