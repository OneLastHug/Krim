"""PKCE 授权状态的加密存储、绑定与单次消费终态。"""

import sqlalchemy as sa
from alembic import op

revision = "0032_oauth_states"
down_revision = "0031_command_artifact_proposals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "oauth_states",
        sa.Column("state_hash", sa.String(64), primary_key=True),
        sa.Column("subject", sa.String(256), nullable=False),
        sa.Column("scope_hash", sa.String(64), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("ended_at", sa.Float(), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending','consumed','cancelled','expired','invalidated')",
            name="ck_oauth_state_status",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND ciphertext IS NOT NULL AND ended_at IS NULL) OR "
            "(status != 'pending' AND ciphertext IS NULL AND ended_at IS NOT NULL)",
            name="ck_oauth_state_payload",
        ),
        sa.CheckConstraint("expires_at > created_at", name="ck_oauth_state_time"),
    )
    op.create_index("ix_oauth_states_subject", "oauth_states", ["subject"])
    op.create_index("ix_oauth_states_expires_at", "oauth_states", ["expires_at"])


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM oauth_states")).scalar_one():
        raise RuntimeError("OAuth 状态表非空，拒绝丢失授权与单次消费事实的有损降级")
    op.drop_index("ix_oauth_states_expires_at", table_name="oauth_states")
    op.drop_index("ix_oauth_states_subject", table_name="oauth_states")
    op.drop_table("oauth_states")
