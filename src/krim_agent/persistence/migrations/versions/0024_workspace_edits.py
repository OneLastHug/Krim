"""持久文件修改提案、操作租约、私有快照与不确定文件锁。"""

import sqlalchemy as sa
from alembic import op

revision = "0024_workspace_edits"
down_revision = "0023_agent_tasks"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_ledger_guards",
        sa.Column("guard_id", sa.String(16), primary_key=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_table(
        "workspace_edit_proposals",
        sa.Column("proposal_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column("prepared", sa.JSON(), nullable=False),
        sa.Column("preview", sa.JSON(), nullable=False),
        sa.Column("before_content", sa.LargeBinary(), nullable=True),
        sa.Column("after_content", sa.LargeBinary(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("active_operation_id", sa.String(36), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
        sa.CheckConstraint("revision >= 0", name="ck_workspace_proposal_revision"),
        sa.CheckConstraint(
            "status IN ('proposed','applying','applied','undoing','undone','keeping',"
            "'kept','rejected','uncertain')",
            name="ck_workspace_proposal_status",
        ),
    )
    op.create_index(
        "ix_workspace_edit_proposals_scope_id", "workspace_edit_proposals", ["scope_id"]
    )
    op.create_index(
        "ix_workspace_edit_proposals_topic_id", "workspace_edit_proposals", ["topic_id"]
    )
    op.create_table(
        "workspace_edit_attempts",
        sa.Column("operation_id", sa.String(36), primary_key=True),
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_proposals.proposal_id"),
            nullable=False,
        ),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("expected_revision", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("prepared", sa.JSON(), nullable=False),
        sa.Column("before_content", sa.LargeBinary(), nullable=True),
        sa.Column("after_content", sa.LargeBinary(), nullable=True),
        sa.Column("receipt", sa.JSON(), nullable=True),
        sa.Column("recovery", sa.JSON(), nullable=True),
        sa.Column("cleanup_pending", sa.Boolean(), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("call_id", sa.String(128), nullable=True),
        sa.Column("receipt_history", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key"),
        sa.CheckConstraint("action IN ('apply','undo','keep')", name="ck_workspace_attempt_action"),
        sa.CheckConstraint(
            "status IN ('running','done','rejected','uncertain')",
            name="ck_workspace_attempt_status",
        ),
        sa.CheckConstraint("expected_revision >= 0", name="ck_workspace_attempt_revision"),
        sa.CheckConstraint(
            "(owner_id IS NULL AND lease_expires_at IS NULL) OR "
            "(owner_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_workspace_attempt_owner",
        ),
    )
    op.create_index(
        "ix_workspace_edit_attempts_proposal_id", "workspace_edit_attempts", ["proposal_id"]
    )
    op.create_table(
        "workspace_file_locks",
        sa.Column("file_key", sa.String(64), primary_key=True),
        sa.Column(
            "operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_attempts.operation_id"),
            nullable=False,
            unique=True,
        ),
    )


def downgrade() -> None:
    for table in ("workspace_edit_proposals", "workspace_edit_attempts", "workspace_file_locks"):
        if op.get_bind().execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one():
            raise RuntimeError("文件修改提案、快照或操作账本非空，拒绝有损降级")
    op.drop_table("workspace_file_locks")
    op.drop_index("ix_workspace_edit_attempts_proposal_id", table_name="workspace_edit_attempts")
    op.drop_table("workspace_edit_attempts")
    op.drop_index("ix_workspace_edit_proposals_topic_id", table_name="workspace_edit_proposals")
    op.drop_index("ix_workspace_edit_proposals_scope_id", table_name="workspace_edit_proposals")
    op.drop_table("workspace_edit_proposals")
    op.drop_table("workspace_ledger_guards")
