"""0036：目录条件事务独立提案/attempt/lock账本。"""

import sqlalchemy as sa
from alembic import op

revision = "0036_workspace_directory_ledger"
down_revision = "0035_oauth_browser_callbacks"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_directory_proposals",
        sa.Column("proposal_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False, index=True),
        sa.Column(
            "topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False, index=True
        ),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column("prepared", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("active_operation_id", sa.String(36), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
    )
    op.create_table(
        "workspace_directory_attempts",
        sa.Column("operation_id", sa.String(36), primary_key=True),
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_directory_proposals.proposal_id"),
            nullable=False,
            index=True,
        ),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("expected_revision", sa.Integer(), nullable=False),
        sa.Column("previous_status", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("prepared", sa.JSON(), nullable=False),
        sa.Column("observed_after", sa.JSON(), nullable=True),
        sa.Column("receipt", sa.JSON(), nullable=True),
        sa.Column("recovery", sa.JSON(), nullable=True),
        sa.Column("cleanup_pending", sa.Boolean(), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("call_id", sa.String(128), nullable=True),
        sa.Column("receipt_history", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
    )
    op.create_table(
        "workspace_directory_locks",
        sa.Column("lock_key", sa.String(64), primary_key=True),
        sa.Column(
            "operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_directory_attempts.operation_id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("directory_key", sa.String(64), nullable=True, index=True),
    )


def downgrade() -> None:
    bind = op.get_bind()
    for table in (
        "workspace_directory_locks",
        "workspace_directory_attempts",
        "workspace_directory_proposals",
    ):
        if bind.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one():
            raise RuntimeError("目录条件事务账本非空，拒绝有损降级")
    op.drop_table("workspace_directory_locks")
    op.drop_table("workspace_directory_attempts")
    op.drop_table("workspace_directory_proposals")
