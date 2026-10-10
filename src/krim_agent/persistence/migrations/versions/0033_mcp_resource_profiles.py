"""可信MCP资源的持久引用选择；不保存命令/路径/URL或凭证。"""

import sqlalchemy as sa
from alembic import op

revision = "0033_mcp_resource_profiles"
down_revision = "0032_oauth_states"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "mcp_resource_profiles",
        sa.Column("scope", sa.String(64), primary_key=True),
        sa.Column("name", sa.String(32), primary_key=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("resource_version", sa.String(128), nullable=False),
        sa.Column("resource_hash", sa.String(64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("public_metadata", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "revision >= 1 AND revision <= 9007199254740991", name="ck_mcp_profile_revision"
        ),
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM mcp_resource_profiles")).scalar_one():
        raise RuntimeError("MCP引用选择非空，拒绝丢失版本与启用事实的有损降级")
    op.drop_table("mcp_resource_profiles")
