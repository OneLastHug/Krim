"""受限、话题隔离的二进制附件 Blob 与幂等指纹。"""

import sqlalchemy as sa
from alembic import op

revision = "0015_attachments"
down_revision = "0014_host_checkpoints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "attachments",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("filename", sa.String(120), nullable=False),
        sa.Column("media_type", sa.String(120), nullable=False),
        sa.Column("byte_size", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("topic_id", "idempotency_key"),
        sa.CheckConstraint("byte_size > 0 AND byte_size <= 2097152", name="ck_attachment_size"),
    )
    op.create_index("ix_attachments_topic_id", "attachments", ["topic_id"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT COUNT(*) FROM attachments")).scalar():
        raise RuntimeError("已有二进制附件，回退会丢失资源；请先导出")
    op.drop_index("ix_attachments_topic_id", table_name="attachments")
    op.drop_table("attachments")
