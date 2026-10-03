"""为 OAuth 令牌增加应用层加密密文表。"""

import sqlalchemy as sa
from alembic import op

revision = "0005_oauth_tokens"
down_revision = "0004_run_error_code"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "oauth_tokens",
        sa.Column("scope_hash", sa.String(64), nullable=False),
        sa.Column("subject", sa.String(256), nullable=False),
        sa.Column("server", sa.String(32), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("scope_hash", "subject"),
        sa.CheckConstraint("revision >= 0", name="ck_oauth_revision"),
        sa.CheckConstraint(
            "(lease_owner IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_oauth_lease",
        ),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT 1 FROM oauth_tokens LIMIT 1")).first():
        raise RuntimeError("已有 OAuth 密文，回退会丢失令牌；请先备份导出")
    op.drop_table("oauth_tokens")
