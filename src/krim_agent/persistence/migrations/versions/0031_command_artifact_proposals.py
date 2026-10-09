"""命令产物与条件文件提案的不可丢失来源关联。"""

import sqlalchemy as sa
from alembic import op

revision = "0031_command_artifact_proposals"
down_revision = "0030_command_executions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "command_artifact_proposals",
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_proposals.proposal_id"),
            primary_key=True,
        ),
        sa.Column(
            "command_id",
            sa.String(36),
            sa.ForeignKey("command_executions.command_id"),
            nullable=False,
        ),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column("file_key", sa.String(64), nullable=False),
        sa.Column("effect_sequence", sa.Integer(), nullable=True),
        sa.Column("ttl_seconds", sa.Float(), nullable=False),
        sa.Column("accept_failed", sa.Integer(), nullable=False),
        sa.CheckConstraint("accept_failed IN (0,1)", name="ck_command_proposal_accept"),
        sa.CheckConstraint(
            "ttl_seconds > 0 AND ttl_seconds <= 86400", name="ck_command_proposal_ttl"
        ),
        sa.CheckConstraint(
            "effect_sequence IS NULL OR effect_sequence > 0", name="ck_command_proposal_effect"
        ),
    )
    op.create_index(
        "ix_command_artifact_proposals_command_id", "command_artifact_proposals", ["command_id"]
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM command_artifact_proposals"))
        .scalar_one()
    ):
        raise RuntimeError("命令产物提案非空，拒绝丢失来源与版本约束的有损降级")
    op.drop_index(
        "ix_command_artifact_proposals_command_id", table_name="command_artifact_proposals"
    )
    op.drop_table("command_artifact_proposals")
