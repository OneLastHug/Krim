"""0053：原生目录捕获任务与完整材料的持久关联。"""

import sqlalchemy as sa
from alembic import op

revision = "0053_directory_captures"
down_revision = "0052_directory_checkpoints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_directory_captures",
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
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column(
            "material_id",
            sa.String(36),
            sa.ForeignKey("workspace_directory_checkpoints.checkpoint_id"),
            nullable=True,
        ),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_directory_capture_key"),
        sa.UniqueConstraint("material_id", name="uq_directory_capture_material"),
        sa.CheckConstraint(
            "status IN ('capturing','available','failed','cancelled','interrupted')",
            name="ck_directory_capture_status",
        ),
        sa.CheckConstraint(
            "(status = 'capturing' AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status != 'capturing' AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_directory_capture_owner",
        ),
        sa.CheckConstraint(
            "status != 'available' OR material_id IS NOT NULL",
            name="ck_directory_capture_available",
        ),
        sa.CheckConstraint(
            "(status IN ('capturing','available') AND error_code IS NULL) OR "
            "(status IN ('failed','cancelled','interrupted') AND error_code IS NOT NULL)",
            name="ck_directory_capture_error",
        ),
        sa.CheckConstraint(
            "length(root_path) BETWEEN 1 AND 512 AND length(title) BETWEEN 1 AND 120 "
            "AND length(request_key) BETWEEN 1 AND 128",
            name="ck_directory_capture_input",
        ),
        sa.CheckConstraint(
            "length(request_hash) = 64 AND length(resource_fingerprint) = 64 "
            "AND length(workspace_fingerprint) = 64 AND length(token) = 64",
            name="ck_directory_capture_hashes",
        ),
    )
    op.create_index(
        "ix_workspace_directory_captures_scope_id", "workspace_directory_captures", ["scope_id"]
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM workspace_directory_captures"))
        .scalar_one()
    ):
        raise RuntimeError("原生目录捕获任务非空，拒绝有损降级")
    op.drop_table("workspace_directory_captures")
