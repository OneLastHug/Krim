"""0052：独立原生目录完整材料，不改旧文件检查点或生成来源。"""

import sqlalchemy as sa
from alembic import op

revision = "0052_directory_checkpoints"
down_revision = "0051_checkpoint_restore_models"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_directory_checkpoints",
        sa.Column("checkpoint_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("title", sa.String(120), nullable=False),
        sa.Column("root_path", sa.String(512), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("workspace_fingerprint", sa.String(64), nullable=False),
        sa.Column("snapshot_fingerprint", sa.String(64), nullable=False),
        sa.Column("metadata", sa.JSON(none_as_null=True), nullable=False),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_directory_checkpoint_key"),
        sa.CheckConstraint(
            "length(root_path) BETWEEN 1 AND 512 AND length(title) BETWEEN 1 AND 120 "
            "AND length(request_key) BETWEEN 1 AND 128",
            name="ck_directory_checkpoint_input",
        ),
        sa.CheckConstraint(
            "length(request_hash) = 64 AND length(resource_fingerprint) = 64 "
            "AND length(workspace_fingerprint) = 64 AND length(snapshot_fingerprint) = 64",
            name="ck_directory_checkpoint_hashes",
        ),
        sa.CheckConstraint(
            "length(payload) <= 33554432 AND length(metadata) <= 2097152",
            name="ck_directory_checkpoint_material",
        ),
    )
    op.create_index(
        "ix_workspace_directory_checkpoints_scope_id",
        "workspace_directory_checkpoints",
        ["scope_id"],
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM workspace_directory_checkpoints"))
        .scalar_one()
    ):
        raise RuntimeError("原生目录检查点材料非空，拒绝有损降级")
    op.drop_table("workspace_directory_checkpoints")
