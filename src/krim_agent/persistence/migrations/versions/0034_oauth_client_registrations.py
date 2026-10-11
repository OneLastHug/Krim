"""OAuth DCR客户端声明的密文/绑定/短租约。"""

import sqlalchemy as sa
from alembic import op

revision = "0034_oauth_client_registrations"
down_revision = "0033_mcp_resource_profiles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "oauth_client_registrations",
        sa.Column("binding_hash", sa.String(64), primary_key=True),
        sa.Column("subject", sa.String(256), nullable=False),
        sa.Column("server", sa.String(32), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("revision >= 0", name="ck_oauth_client_revision"),
        sa.CheckConstraint(
            "(lease_owner IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_oauth_client_lease",
        ),
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM oauth_client_registrations"))
        .scalar_one()
    ):
        raise RuntimeError("OAuth DCR客户端非空，拒绝丢失注册身份的有损降级")
    op.drop_table("oauth_client_registrations")
