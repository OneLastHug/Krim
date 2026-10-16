"""0058：独立空事务容器回收账本/围栏；原恢复回执和材料不改。"""

import sqlalchemy as sa
from alembic import op

revision = "0058_container_retention"
down_revision = "0057_missing_directory_rollbacks"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_container_retentions",
        sa.Column("operation_id", sa.String(36), primary_key=True),
        sa.Column(
            "source_plan_id",
            sa.String(36),
            sa.ForeignKey("workspace_missing_directory_restore_plans.plan_id"),
            nullable=False,
        ),
        sa.Column("source_operation_id", sa.String(36), nullable=False),
        sa.Column(
            "active_source_id",
            sa.String(36),
            sa.ForeignKey("workspace_missing_directory_restore_plans.plan_id"),
            nullable=True,
        ),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("source_metadata", sa.JSON(), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("request_metadata", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("epoch", sa.Integer(), nullable=False),
        sa.Column("active_kind", sa.String(8), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("intent_recorded", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("cancel_requested", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("receipt_metadata", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("verification_count", sa.Integer(), nullable=False),
        sa.Column("verification_metadata", sa.JSON(), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_container_retention_key"),
        sa.UniqueConstraint("active_source_id", name="uq_container_retention_source"),
        sa.ForeignKeyConstraint(
            ["source_operation_id", "source_plan_id"],
            [
                "workspace_missing_directory_restore_attempts.operation_id",
                "workspace_missing_directory_restore_attempts.plan_id",
            ],
        ),
        sa.CheckConstraint(
            "status IN ('running','done','rejected','unknown','cancelled','interrupted') "
            "AND active_kind IN ('remove','verify')",
            name="ck_container_retention_status",
        ),
        sa.CheckConstraint(
            "(status IN ('running','done','unknown') AND active_source_id IS NOT NULL "
            "AND active_source_id = source_plan_id) "
            "OR (status IN ('rejected','cancelled','interrupted') AND active_source_id IS NULL)",
            name="ck_container_retention_source_slot",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status != 'running' AND owner_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_container_retention_owner",
        ),
        sa.CheckConstraint(
            "intent_recorded IN (FALSE,TRUE) AND cancel_requested IN (FALSE,TRUE) "
            "AND (status != 'unknown' OR intent_recorded = TRUE) "
            "AND (status NOT IN ('cancelled','interrupted') OR intent_recorded = FALSE) "
            "AND (status != 'cancelled' OR cancel_requested = TRUE) "
            "AND (status NOT IN ('done','unknown') OR receipt_metadata IS NOT NULL)",
            name="ck_container_retention_effect",
        ),
        sa.CheckConstraint(
            "length(source_hash) = 64 AND length(request_hash) = 64 AND length(token) = 64 "
            "AND length(request_key) BETWEEN 1 AND 128 "
            "AND revision >= 0 AND revision <= 9007199254740991 "
            "AND epoch >= 0 AND epoch <= 9007199254740991 "
            "AND verification_count >= 0 AND verification_count <= 128 "
            "AND (active_kind != 'verify' OR verification_count > 0)",
            name="ck_container_retention_bounds",
        ),
    )
    op.create_index(
        "ix_workspace_container_retentions_scope_id", "workspace_container_retentions", ["scope_id"]
    )
    op.create_table(
        "workspace_container_retention_locks",
        sa.Column(
            "operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_container_retentions.operation_id"),
            primary_key=True,
        ),
        sa.Column("kind", sa.String(16), primary_key=True),
        sa.Column("lock_key", sa.String(64), primary_key=True),
        sa.UniqueConstraint("kind", "lock_key", name="uq_container_retention_physical_lock"),
        sa.CheckConstraint(
            "kind IN ('entry','directory')", name="ck_container_retention_lock_kind"
        ),
        sa.CheckConstraint("length(lock_key) = 64", name="ck_container_retention_lock_key"),
    )


def downgrade() -> None:
    connection = op.get_bind()
    if (
        connection.execute(
            sa.text("SELECT COUNT(*) FROM workspace_container_retentions")
        ).scalar_one()
        or connection.execute(
            sa.text("SELECT COUNT(*) FROM workspace_container_retention_locks")
        ).scalar_one()
    ):
        raise RuntimeError("容器回收记录或围栏非空，禁止有损降级")
    op.drop_table("workspace_container_retention_locks")
    op.drop_index(
        "ix_workspace_container_retentions_scope_id", table_name="workspace_container_retentions"
    )
    op.drop_table("workspace_container_retentions")
