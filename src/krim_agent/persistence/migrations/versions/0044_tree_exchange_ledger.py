"""0044：双完整树交换原proposal/attempt/命名空间锁/模型来源；原历史保持。"""

import sqlalchemy as sa
from alembic import op

revision = "0044_tree_exchange_ledger"
down_revision = "0043_tree_move_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_tree_exchange_proposals",
        sa.Column("proposal_id", sa.String(36), primary_key=True),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("request", sa.JSON(), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("left_path", sa.String(512), nullable=False),
        sa.Column("right_path", sa.String(512), nullable=False),
        sa.Column("prepared", sa.JSON(), nullable=False),
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("call_id", sa.String(128), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column(
            "active_operation_id",
            sa.String(36),
            sa.ForeignKey(
                "workspace_tree_exchange_attempts.operation_id",
                name="fk_tree_exchange_active_operation",
                use_alter=True,
            ),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_tree_exchange_proposal_key"),
        sa.CheckConstraint(
            "revision >= 0 AND revision <= 9007199254740991", name="ck_tree_exchange_revision"
        ),
        sa.CheckConstraint(
            "(run_id IS NULL AND call_id IS NULL) OR (run_id IS NOT NULL AND call_id IS NOT NULL)",
            name="ck_tree_exchange_proposal_source",
        ),
        sa.CheckConstraint(
            "status IN ('proposed','applying','applied','undoing','undone','keeping',"
            "'kept','rejected','uncertain','cancelled')",
            name="ck_tree_exchange_proposal_status",
        ),
    )
    op.create_index(
        "ix_workspace_tree_exchange_proposals_scope_id",
        "workspace_tree_exchange_proposals",
        ["scope_id"],
    )
    op.create_index(
        "ix_workspace_tree_exchange_proposals_topic_id",
        "workspace_tree_exchange_proposals",
        ["topic_id"],
    )
    op.create_table(
        "workspace_tree_exchange_attempts",
        sa.Column("operation_id", sa.String(36), primary_key=True),
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_exchange_proposals.proposal_id"),
            nullable=False,
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
        sa.Column("payload", sa.LargeBinary(), nullable=False),
        sa.Column("after_prepared", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("after_payload", sa.LargeBinary(), nullable=True),
        sa.Column("receipt", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("receipt_history", sa.JSON(), nullable=False),
        sa.Column("intent_recorded", sa.Boolean(), nullable=False),
        sa.Column("cancelled", sa.Boolean(), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.Column("call_id", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_tree_exchange_attempt_key"),
        sa.UniqueConstraint("operation_id", "proposal_id", name="uq_tree_exchange_attempt_parent"),
        sa.CheckConstraint("action IN ('apply','undo','keep')", name="ck_tree_exchange_action"),
        sa.CheckConstraint(
            "status IN ('running','done','rejected','uncertain')",
            name="ck_tree_exchange_attempt_status",
        ),
        sa.CheckConstraint(
            "expected_revision >= 0 AND expected_revision <= 9007199254740989",
            name="ck_tree_exchange_expected_revision",
        ),
        sa.CheckConstraint(
            "intent_recorded IN (false,true) AND cancelled IN (false,true)",
            name="ck_tree_exchange_flags",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL) OR "
            "(status != 'running' AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_tree_exchange_owner",
        ),
        sa.CheckConstraint(
            "(run_id IS NULL AND call_id IS NULL) OR (run_id IS NOT NULL AND call_id IS NOT NULL)",
            name="ck_tree_exchange_run_call",
        ),
        sa.CheckConstraint(
            "(after_prepared IS NULL AND after_payload IS NULL) OR "
            "(after_prepared IS NOT NULL AND after_payload IS NOT NULL)",
            name="ck_tree_exchange_after",
        ),
        sa.CheckConstraint(
            "status NOT IN ('done','rejected') OR receipt IS NOT NULL",
            name="ck_tree_exchange_receipt",
        ),
        sa.CheckConstraint(
            "(receipt IS NULL AND after_prepared IS NULL AND after_payload IS NULL) OR "
            "(receipt IS NOT NULL AND after_prepared IS NOT NULL AND after_payload IS NOT NULL)",
            name="ck_tree_exchange_effect",
        ),
    )
    op.create_index(
        "ix_workspace_tree_exchange_attempts_proposal_id",
        "workspace_tree_exchange_attempts",
        ["proposal_id"],
    )
    op.create_table(
        "workspace_tree_exchange_locks",
        sa.Column(
            "operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_exchange_attempts.operation_id"),
            primary_key=True,
        ),
        sa.Column("kind", sa.String(16), primary_key=True),
        sa.Column("lock_key", sa.String(64), primary_key=True),
        sa.UniqueConstraint("kind", "lock_key", name="uq_tree_exchange_namespace_key"),
        sa.CheckConstraint("kind IN ('entry','directory')", name="ck_tree_exchange_lock_kind"),
    )
    op.create_table(
        "workspace_tree_exchange_model_sources",
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), primary_key=True),
        sa.Column("call_id", sa.String(128), primary_key=True),
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_exchange_proposals.proposal_id"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("operation_id", sa.String(36), nullable=True),
        sa.ForeignKeyConstraint(
            ["operation_id", "proposal_id"],
            [
                "workspace_tree_exchange_attempts.operation_id",
                "workspace_tree_exchange_attempts.proposal_id",
            ],
        ),
        sa.UniqueConstraint("operation_id", name="uq_tree_exchange_model_operation"),
        sa.CheckConstraint(
            "kind IN ('preview','apply','undo','keep')", name="ck_tree_exchange_model_kind"
        ),
        sa.CheckConstraint("length(call_id) BETWEEN 1 AND 128", name="ck_tree_exchange_model_call"),
        sa.CheckConstraint(
            "(kind = 'preview' AND operation_id IS NULL) OR "
            "(kind != 'preview' AND operation_id IS NOT NULL)",
            name="ck_tree_exchange_model_operation",
        ),
    )

    op.create_index(
        "uq_tree_exchange_model_preview_parent",
        "workspace_tree_exchange_model_sources",
        ["proposal_id"],
        unique=True,
        sqlite_where=sa.text("kind = 'preview'"),
        postgresql_where=sa.text("kind = 'preview'"),
    )
    if op.get_bind().dialect.name != "sqlite":
        op.create_foreign_key(
            "fk_tree_exchange_active_operation",
            "workspace_tree_exchange_proposals",
            "workspace_tree_exchange_attempts",
            ["active_operation_id"],
            ["operation_id"],
        )


def downgrade() -> None:
    names = (
        "workspace_tree_exchange_model_sources",
        "workspace_tree_exchange_locks",
        "workspace_tree_exchange_attempts",
        "workspace_tree_exchange_proposals",
    )
    for name in names:
        if op.get_bind().execute(sa.text(f"SELECT count(*) FROM {name}")).scalar_one():
            raise RuntimeError("双完整树交换账本非空，拒绝有损降级")
    if op.get_bind().dialect.name != "sqlite":
        op.drop_constraint(
            "fk_tree_exchange_active_operation",
            "workspace_tree_exchange_proposals",
            type_="foreignkey",
        )
    for name in names:
        op.drop_table(name)
