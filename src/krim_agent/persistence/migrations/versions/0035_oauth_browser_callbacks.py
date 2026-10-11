"""主体/资源/浏览器绑定的OAuth授权intent与单次回程。"""

import sqlalchemy as sa
from alembic import op

revision = "0035_oauth_browser_callbacks"
down_revision = "0034_oauth_client_registrations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "oauth_browser_callbacks",
        sa.Column("attempt_id", sa.String(36), primary_key=True),
        sa.Column("state_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("scope", sa.String(64), nullable=False),
        sa.Column("name", sa.String(32), nullable=False),
        sa.Column("subject", sa.String(256), nullable=False),
        sa.Column("resource_hash", sa.String(64), nullable=False),
        sa.Column("browser_hash", sa.String(64), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=True),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "phase IN ('pending','exchanging','complete','failed','cancelled','expired')",
            name="ck_oauth_browser_phase",
        ),
        sa.CheckConstraint("expires_at > created_at", name="ck_oauth_browser_ttl"),
        sa.CheckConstraint(
            "(phase IN ('pending','exchanging') AND ciphertext IS NOT NULL) OR "
            "(phase NOT IN ('pending','exchanging') AND ciphertext IS NULL)",
            name="ck_oauth_browser_intent",
        ),
    )
    op.create_index(
        "ix_oauth_browser_callbacks_expires_at", "oauth_browser_callbacks", ["expires_at"]
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM oauth_browser_callbacks")).scalar_one():
        raise RuntimeError("OAuth浏览器回程非空，拒绝丢失授权码的有损降级")
    op.drop_index("ix_oauth_browser_callbacks_expires_at", table_name="oauth_browser_callbacks")
    op.drop_table("oauth_browser_callbacks")
