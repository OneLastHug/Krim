"""版本化 Agent 声明与实例版本绑定；历史非空禁止有损回退。"""

import sqlalchemy as sa
from alembic import op

revision = "0021_agent_profiles"
down_revision = "0020_evaluation_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_profile_heads",
        sa.Column("name", sa.String(64), primary_key=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.CheckConstraint("revision >= 1", name="ck_agent_profile_head_revision"),
    )
    op.create_table(
        "agent_profile_versions",
        sa.Column("name", sa.String(64), primary_key=True),
        sa.Column("revision", sa.Integer(), primary_key=True),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.CheckConstraint("revision >= 1", name="ck_agent_profile_version_revision"),
    )
    op.add_column("agent_handles", sa.Column("profile_revision", sa.Integer(), nullable=True))


def downgrade() -> None:
    count = (
        op.get_bind().execute(sa.text("SELECT count(*) FROM agent_profile_versions")).scalar_one()
    )
    bound = (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM agent_handles WHERE profile_revision IS NOT NULL"))
        .scalar_one()
    )
    if count or bound:
        raise RuntimeError("Agent配置版本/实例绑定非空，拒绝有损降级")
    op.drop_column("agent_handles", "profile_revision")
    op.drop_table("agent_profile_versions")
    op.drop_table("agent_profile_heads")
