"""0056：缺失恢复独立计划/原执行/命名空间锁/来源；旧0055保持。"""

import sqlalchemy as sa
from alembic import op

revision = "0056_missing_directory_restores"
down_revision = "0055_directory_restore_plans"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_missing_directory_restore_plans",
        sa.Column("plan_id", sa.String(length=36), nullable=False),
        sa.Column("scope_id", sa.String(length=80), nullable=False),
        sa.Column("topic_id", sa.String(length=36), nullable=False),
        sa.Column("request_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("ttl_seconds", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("target_kind", sa.String(length=16), nullable=False),
        sa.Column("target_checkpoint_id", sa.String(length=36), nullable=True),
        sa.Column("target_baseline_plan_id", sa.String(length=36), nullable=True),
        sa.Column("target_material_id", sa.String(length=36), nullable=False),
        sa.Column("target_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("target_summary", sa.JSON(), nullable=False),
        sa.Column("root_path", sa.String(length=512), nullable=False),
        sa.Column("workspace_key", sa.String(length=64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("workspace_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("phase", sa.String(length=16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("owner_id", sa.String(length=36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(length=64), nullable=False),
        sa.Column("stage_token", sa.String(length=32), nullable=False),
        sa.Column("absence_metadata", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("absence_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("absence_hash", sa.String(length=64), nullable=True),
        sa.Column("observation_owner_id", sa.String(length=36), nullable=True),
        sa.Column("stage_metadata", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("prepared_metadata", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("prepared_payload", sa.LargeBinary(), nullable=True),
        sa.Column("execute_key", sa.String(length=128), nullable=True),
        sa.Column("execute_hash", sa.String(length=64), nullable=True),
        sa.Column("preview_model_bound", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("execute_model_bound", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("cancel_requested", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("cleanup_pending", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("staging_reserved_bytes", sa.Integer(), server_default="0", nullable=False),
        sa.Column("staging_reserved_entries", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(phase = 'preparing' AND owner_id IS NOT NULL AND lease_expires_at IS NOT "
            "NULL) OR (phase != 'preparing' AND owner_id IS NULL AND lease_expires_at I"
            "S NULL)",
            name="ck_missing_restore_owner",
        ),
        sa.CheckConstraint(
            "phase != 'finished' OR prepared_metadata IS NOT NULL",
            name="ck_missing_restore_finished",
        ),
        sa.CheckConstraint(
            "phase IN ('preparing','ready','executing','finished','failed','cancelled',"
            "'interrupted','expired','uncertain')",
            name="ck_missing_restore_phase",
        ),
        sa.CheckConstraint(
            "phase NOT IN ('executing','finished','uncertain') OR execute_key IS NOT NULL",
            name="ck_missing_restore_execute",
        ),
        sa.CheckConstraint(
            "phase NOT IN ('ready','executing','finished','uncertain') OR absence_metad"
            "ata IS NOT NULL",
            name="ck_missing_restore_ready",
        ),
        sa.CheckConstraint(
            "target_kind IN ('checkpoint','baseline') AND ((target_kind = 'checkpoint' "
            "AND target_checkpoint_id IS NOT NULL AND target_baseline_plan_id IS NULL) "
            "OR (target_kind = 'baseline' AND target_checkpoint_id IS NULL AND target_b"
            "aseline_plan_id IS NOT NULL))",
            name="ck_missing_restore_target",
        ),
        sa.CheckConstraint(
            "(absence_metadata IS NULL AND absence_fingerprint IS NULL AND absence_hash"
            " IS NULL AND observation_owner_id IS NULL) OR (absence_metadata IS NOT NUL"
            "L AND absence_fingerprint IS NOT NULL AND absence_hash IS NOT NULL AND obs"
            "ervation_owner_id IS NOT NULL)",
            name="ck_missing_restore_absence",
        ),
        sa.CheckConstraint(
            "(execute_key IS NULL) = (execute_hash IS NULL)", name="ck_missing_restore_execute_hash"
        ),
        sa.CheckConstraint(
            "(prepared_metadata IS NULL AND prepared_payload IS NULL) OR (prepared_meta"
            "data IS NOT NULL AND prepared_payload IS NOT NULL AND absence_metadata IS "
            "NOT NULL AND stage_metadata IS NOT NULL)",
            name="ck_missing_restore_prepared",
        ),
        sa.CheckConstraint(
            "length(root_path) BETWEEN 1 AND 512 AND length(request_key) BETWEEN 1 AND "
            "128 AND length(workspace_key) BETWEEN 1 AND 64",
            name="ck_missing_restore_input",
        ),
        sa.CheckConstraint(
            "preview_model_bound IN (FALSE,TRUE) AND execute_model_bound IN (FALSE,TRUE"
            ") AND cancel_requested IN (FALSE,TRUE) AND cleanup_pending IN (FALSE,TRUE)",
            name="ck_missing_restore_flags",
        ),
        sa.CheckConstraint(
            "revision >= 0 AND revision <= 9007199254740991 AND expires_at > 0 AND ttl_"
            "seconds > 0 AND ttl_seconds <= 86400 AND length(token) = 64 AND length(sta"
            "ge_token) = 32 AND length(request_hash) = 64 AND length(resource_fingerpri"
            "nt) = 64 AND length(workspace_fingerprint) = 64 AND length(target_fingerpr"
            "int) = 64 AND (absence_fingerprint IS NULL OR length(absence_fingerprint) "
            "= 64) AND (absence_hash IS NULL OR length(absence_hash) = 64) AND staging_"
            "reserved_bytes >= 0 AND staging_reserved_bytes <= 33554432 AND staging_res"
            "erved_entries >= 0 AND staging_reserved_entries <= 1025",
            name="ck_missing_restore_bounds",
        ),
        sa.ForeignKeyConstraint(
            ["target_baseline_plan_id"],
            ["workspace_directory_restore_plans.plan_id"],
        ),
        sa.ForeignKeyConstraint(
            ["target_checkpoint_id"],
            ["workspace_directory_captures.checkpoint_id"],
        ),
        sa.ForeignKeyConstraint(
            ["target_material_id"],
            ["workspace_directory_checkpoints.checkpoint_id"],
        ),
        sa.ForeignKeyConstraint(
            ["topic_id"],
            ["topics.id"],
        ),
        sa.PrimaryKeyConstraint("plan_id"),
        sa.UniqueConstraint("scope_id", "execute_key", name="uq_missing_restore_execute_key"),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_missing_restore_preview_key"),
    )
    op.create_index(
        op.f("ix_workspace_missing_directory_restore_plans_scope_id"),
        "workspace_missing_directory_restore_plans",
        ["scope_id"],
        unique=False,
    )
    op.create_table(
        "workspace_missing_directory_restore_attempts",
        sa.Column("operation_id", sa.String(length=36), nullable=False),
        sa.Column("plan_id", sa.String(length=36), nullable=False),
        sa.Column("scope_id", sa.String(length=80), nullable=False),
        sa.Column("request_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("expected_revision", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("token", sa.String(length=64), nullable=False),
        sa.Column("owner_id", sa.String(length=36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("intent_recorded", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("cancelled", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("effect_metadata", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("verification_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("verification_model_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("verify_model_bound", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("verification_run_id", sa.String(length=36), nullable=True),
        sa.Column("verification_call_id", sa.String(length=128), nullable=True),
        sa.Column("verification_hash", sa.String(length=64), nullable=True),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(status = 'running' AND owner_id IS NOT NULL AND lease_expires_at IS NOT N"
            "ULL) OR (status != 'running' AND owner_id IS NULL AND lease_expires_at IS "
            "NULL)",
            name="ck_missing_restore_operation_owner",
        ),
        sa.CheckConstraint(
            "status != 'done' OR (intent_recorded = TRUE AND effect_metadata IS NOT NULL)",
            name="ck_missing_restore_done_effect",
        ),
        sa.CheckConstraint(
            "status != 'uncertain' OR intent_recorded = TRUE",
            name="ck_missing_restore_uncertain_intent",
        ),
        sa.CheckConstraint(
            "status IN ('running','done','rejected','uncertain','cancelled','interrupte"
            "d','expired')",
            name="ck_missing_restore_operation_status",
        ),
        sa.CheckConstraint(
            "(verify_model_bound = TRUE AND verification_run_id IS NOT NULL AND verific"
            "ation_call_id IS NOT NULL) OR (verify_model_bound = FALSE AND verification"
            "_run_id IS NULL AND verification_call_id IS NULL)",
            name="ck_missing_restore_verify_source",
        ),
        sa.CheckConstraint(
            "verification_count >= 0 AND verification_model_count >= 0 AND verification"
            "_model_count <= verification_count AND intent_recorded IN (FALSE,TRUE) AND"
            " verify_model_bound IN (FALSE,TRUE) AND cancelled IN (FALSE,TRUE) AND leng"
            "th(token) = 64 AND length(request_hash) = 64 AND expected_revision >= 1 AN"
            "D expected_revision <= 9007199254740991",
            name="ck_missing_restore_operation_bounds",
        ),
        sa.ForeignKeyConstraint(
            ["plan_id"],
            ["workspace_missing_directory_restore_plans.plan_id"],
        ),
        sa.ForeignKeyConstraint(
            ["verification_run_id"],
            ["runs.id"],
        ),
        sa.PrimaryKeyConstraint("operation_id"),
        sa.UniqueConstraint("operation_id", "plan_id", name="uq_missing_restore_operation_parent"),
        sa.UniqueConstraint("plan_id", name="uq_missing_restore_single_execution"),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_missing_restore_operation_key"),
    )
    op.create_index(
        op.f("ix_workspace_missing_directory_restore_attempts_scope_id"),
        "workspace_missing_directory_restore_attempts",
        ["scope_id"],
        unique=False,
    )
    op.create_table(
        "workspace_missing_directory_restore_locks",
        sa.Column("plan_id", sa.String(length=36), nullable=False),
        sa.Column("operation_id", sa.String(length=36), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("lock_key", sa.String(length=64), nullable=False),
        sa.CheckConstraint("kind IN ('entry','directory')", name="ck_missing_restore_lock_kind"),
        sa.ForeignKeyConstraint(
            ["operation_id", "plan_id"],
            [
                "workspace_missing_directory_restore_attempts.operation_id",
                "workspace_missing_directory_restore_attempts.plan_id",
            ],
        ),
        sa.ForeignKeyConstraint(
            ["plan_id"],
            ["workspace_missing_directory_restore_plans.plan_id"],
        ),
        sa.PrimaryKeyConstraint("plan_id", "kind", "lock_key"),
        sa.UniqueConstraint("kind", "lock_key", name="uq_missing_restore_physical_lock"),
    )
    op.create_table(
        "workspace_missing_directory_restore_model_sources",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("call_id", sa.String(length=128), nullable=False),
        sa.Column("plan_id", sa.String(length=36), nullable=False),
        sa.Column("phase", sa.String(length=16), nullable=False),
        sa.Column("operation_id", sa.String(length=36), nullable=True),
        sa.CheckConstraint(
            "phase IN ('preview','execute','verify') AND length(call_id) BETWEEN 1 AND "
            "128 AND ((phase = 'preview' AND operation_id IS NULL) OR (phase != 'previe"
            "w' AND operation_id IS NOT NULL))",
            name="ck_missing_restore_model_source",
        ),
        sa.ForeignKeyConstraint(
            ["operation_id", "plan_id"],
            [
                "workspace_missing_directory_restore_attempts.operation_id",
                "workspace_missing_directory_restore_attempts.plan_id",
            ],
        ),
        sa.ForeignKeyConstraint(
            ["plan_id"],
            ["workspace_missing_directory_restore_plans.plan_id"],
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["runs.id"],
        ),
        sa.PrimaryKeyConstraint("run_id", "call_id"),
    )
    op.create_index(
        "uq_missing_restore_original_source",
        "workspace_missing_directory_restore_model_sources",
        ["plan_id", "phase"],
        unique=True,
        sqlite_where=sa.text("phase IN ('preview','execute')"),
        postgresql_where=sa.text("phase IN ('preview','execute')"),
    )


def downgrade() -> None:
    connection = op.get_bind()
    for name in (
        "workspace_missing_directory_restore_model_sources",
        "workspace_missing_directory_restore_locks",
        "workspace_missing_directory_restore_attempts",
        "workspace_missing_directory_restore_plans",
    ):
        if connection.execute(sa.text("SELECT COUNT(*) FROM " + name)).scalar_one():
            raise RuntimeError("缺失目录恢复账本非空，禁止有损降级")
    op.drop_index(
        "uq_missing_restore_original_source",
        table_name="workspace_missing_directory_restore_model_sources",
        sqlite_where=sa.text("phase IN ('preview','execute')"),
        postgresql_where=sa.text("phase IN ('preview','execute')"),
    )
    op.drop_table("workspace_missing_directory_restore_model_sources")
    op.drop_table("workspace_missing_directory_restore_locks")
    op.drop_index(
        op.f("ix_workspace_missing_directory_restore_attempts_scope_id"),
        table_name="workspace_missing_directory_restore_attempts",
    )
    op.drop_table("workspace_missing_directory_restore_attempts")
    op.drop_index(
        op.f("ix_workspace_missing_directory_restore_plans_scope_id"),
        table_name="workspace_missing_directory_restore_plans",
    )
    op.drop_table("workspace_missing_directory_restore_plans")
