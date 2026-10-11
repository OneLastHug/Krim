"""0037：原命令目录manifest到目录条件提案的持久来源。"""

import sqlalchemy as sa
from alembic import op

revision = "0037_command_directory_sources"
down_revision = "0036_workspace_directory_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "command_directory_proposals",
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_directory_proposals.proposal_id"),
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
        sa.Column("mode", sa.Integer(), nullable=False),
        sa.Column("entry_key", sa.String(64), nullable=False),
        sa.Column("ttl_seconds", sa.Float(), nullable=False),
        sa.Column("accept_failed", sa.Integer(), nullable=False),
        sa.CheckConstraint("mode >= 0 AND mode <= 511", name="ck_command_directory_mode"),
        sa.CheckConstraint("accept_failed IN (0,1)", name="ck_command_directory_accept"),
        sa.CheckConstraint(
            "ttl_seconds > 0 AND ttl_seconds <= 86400", name="ck_command_directory_ttl"
        ),
    )
    op.create_index(
        "ix_command_directory_proposals_command_id", "command_directory_proposals", ["command_id"]
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM command_directory_proposals"))
        .scalar_one()
    ):
        raise RuntimeError("命令目录来源非空，拒绝丢失原来源约束的有损降级")
    op.drop_index(
        "ix_command_directory_proposals_command_id", table_name="command_directory_proposals"
    )
    op.drop_table("command_directory_proposals")
