"""0055：目录恢复独立计划/原执行/物理锁与阶段来源，不重建旧材料。"""

import sqlalchemy as sa
from alembic import op

revision = "0055_directory_restore_plans"
down_revision = "0054_directory_capture_models"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_directory_restore_plans",
        sa.Column("plan_id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("topic_id", sa.String(36), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("ttl_seconds", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("target_kind", sa.String(16), nullable=False),
        sa.Column("target_checkpoint_id", sa.String(36), nullable=True),
        sa.Column("target_baseline_plan_id", sa.String(36), nullable=True),
        sa.Column("target_material_id", sa.String(36), nullable=False),
        sa.Column("target_fingerprint", sa.String(64), nullable=False),
        sa.Column("target_summary", sa.JSON(), nullable=False),
        sa.Column("baseline_material_id", sa.String(36), nullable=True),
        sa.Column("baseline_fingerprint", sa.String(64), nullable=True),
        sa.Column("baseline_summary", sa.JSON(), nullable=True),
        sa.Column("baseline_hash", sa.String(64), nullable=True),
        sa.Column("root_path", sa.String(512), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("workspace_fingerprint", sa.String(64), nullable=False),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("preparation_status", sa.String(16), nullable=False),
        sa.Column("preparation_error_code", sa.String(128), nullable=True),
        sa.Column("preparation_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("stage_token", sa.String(32), nullable=False),
        sa.Column("stage_metadata", sa.JSON(), nullable=True),
        sa.Column("prepared_metadata", sa.JSON(), nullable=True),
        sa.Column("prepared_payload", sa.LargeBinary(), nullable=True),
        sa.Column("execute_key", sa.String(128), nullable=True),
        sa.Column("execute_hash", sa.String(64), nullable=True),
        sa.Column("preview_model_bound", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("execute_model_bound", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("cleanup_pending", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("staging_reserved_bytes", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("staging_reserved_entries", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            (
                "(baseline_material_id IS NULL AND baseline_fingerprint IS NULL AND bas"
                "eline_summary IS NULL AND baseline_hash IS NULL) OR (baseline_material"
                "_id IS NOT NULL AND baseline_fingerprint IS NOT NULL AND baseline_summ"
                "ary IS NOT NULL AND baseline_hash IS NOT NULL)"
            ),
            name="ck_directory_restore_baseline_descriptor",
        ),
        sa.CheckConstraint(
            (
                "revision >= 0 AND revision <= 9007199254740991 AND expires_at > 0 AND "
                "ttl_seconds > 0 AND ttl_seconds <= 86400 AND length(token) = 64 AND le"
                "ngth(stage_token) = 32 AND length(request_hash) = 64 AND length(resour"
                "ce_fingerprint) = 64 AND length(workspace_fingerprint) = 64 AND length"
                "(target_fingerprint) = 64 AND (baseline_fingerprint IS NULL OR length("
                "baseline_fingerprint) = 64) AND (baseline_hash IS NULL OR length(basel"
                "ine_hash) = 64) AND staging_reserved_bytes >= 0 AND staging_reserved_b"
                "ytes <= 33554432 AND staging_reserved_entries >= 0 AND staging_reserve"
                "d_entries <= 1025"
            ),
            name="ck_directory_restore_bounds",
        ),
        sa.CheckConstraint(
            "phase NOT IN ('executing','finished','uncertain') OR execute_key IS NOT NULL",
            name="ck_directory_restore_execute",
        ),
        sa.CheckConstraint(
            "(execute_key IS NULL) = (execute_hash IS NULL)",
            name="ck_directory_restore_execute_hash",
        ),
        sa.CheckConstraint(
            (
                "preview_model_bound IN (FALSE,TRUE) AND execute_model_bound IN (FALSE,"
                "TRUE) AND cancel_requested IN (FALSE,TRUE) AND cleanup_pending IN (FAL"
                "SE,TRUE)"
            ),
            name="ck_directory_restore_flags",
        ),
        sa.CheckConstraint(
            (
                "length(root_path) BETWEEN 1 AND 512 AND length(request_key) BETWEEN 1 "
                "AND 128 AND length(workspace_key) BETWEEN 1 AND 64"
            ),
            name="ck_directory_restore_input",
        ),
        sa.CheckConstraint(
            (
                "(phase = 'preparing' AND owner_id IS NOT NULL AND lease_expires_at IS "
                "NOT NULL) OR (phase != 'preparing' AND owner_id IS NULL AND lease_expi"
                "res_at IS NULL)"
            ),
            name="ck_directory_restore_owner",
        ),
        sa.CheckConstraint(
            (
                "phase IN ('preparing','ready','executing','finished','failed','cancell"
                "ed','interrupted','expired','uncertain')"
            ),
            name="ck_directory_restore_phase",
        ),
        sa.CheckConstraint(
            (
                "preparation_status IN ('running','done','rejected','cancelled','interr"
                "upted','expired')"
            ),
            name="ck_directory_restore_preparation_status",
        ),
        sa.CheckConstraint(
            (
                "(prepared_metadata IS NULL AND prepared_payload IS NULL) OR (prepared_"
                "metadata IS NOT NULL AND prepared_payload IS NOT NULL AND baseline_mat"
                "erial_id IS NOT NULL AND stage_metadata IS NOT NULL)"
            ),
            name="ck_directory_restore_prepared",
        ),
        sa.CheckConstraint(
            "phase NOT IN ('finished','uncertain') OR prepared_metadata IS NOT NULL",
            name="ck_directory_restore_published",
        ),
        sa.CheckConstraint(
            (
                "phase NOT IN ('ready','executing','finished','uncertain') OR (baseline"
                "_material_id IS NOT NULL AND preparation_status = 'done')"
            ),
            name="ck_directory_restore_ready",
        ),
        sa.CheckConstraint(
            (
                "target_kind IN ('checkpoint','baseline') AND ((target_kind = 'checkpoi"
                "nt' AND target_checkpoint_id IS NOT NULL AND target_baseline_plan_id I"
                "S NULL) OR (target_kind = 'baseline' AND target_checkpoint_id IS NULL "
                "AND target_baseline_plan_id IS NOT NULL AND target_baseline_plan_id !="
                " plan_id))"
            ),
            name="ck_directory_restore_target",
        ),
        sa.ForeignKeyConstraint(["topic_id"], ["topics.id"]),
        sa.ForeignKeyConstraint(
            ["target_checkpoint_id"], ["workspace_directory_captures.checkpoint_id"]
        ),
        sa.ForeignKeyConstraint(
            ["baseline_material_id"], ["workspace_directory_checkpoints.checkpoint_id"]
        ),
        sa.ForeignKeyConstraint(
            ["target_material_id"], ["workspace_directory_checkpoints.checkpoint_id"]
        ),
        sa.ForeignKeyConstraint(
            ["target_baseline_plan_id"], ["workspace_directory_restore_plans.plan_id"]
        ),
        sa.UniqueConstraint("baseline_material_id", name="uq_directory_restore_baseline"),
        sa.UniqueConstraint("scope_id", "execute_key", name="uq_directory_restore_execute_key"),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_directory_restore_preview_key"),
    )
    op.create_index(
        "ix_workspace_directory_restore_plans_scope_id",
        "workspace_directory_restore_plans",
        ["scope_id"],
        unique=False,
    )
    op.create_table(
        "workspace_directory_restore_attempts",
        sa.Column("operation_id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("plan_id", sa.String(36), nullable=False),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("expected_revision", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.Float(), nullable=True),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("intent_recorded", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("effect_metadata", sa.JSON(), nullable=True),
        sa.Column("verification_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("verification_model_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("verify_model_bound", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("verification_run_id", sa.String(36), nullable=True),
        sa.Column("verification_call_id", sa.String(128), nullable=True),
        sa.Column("verification_hash", sa.String(64), nullable=True),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            (
                "(verification_run_id IS NULL) = (verification_call_id IS NULL) AND len"
                "gth(token) = 64 AND expected_revision >= 0 AND expected_revision <= 90"
                "07199254740991 AND length(request_hash) = 64"
            ),
            name="ck_directory_restore_operation_bounds",
        ),
        sa.CheckConstraint(
            "status != 'done' OR effect_metadata IS NOT NULL",
            name="ck_directory_restore_operation_done",
        ),
        sa.CheckConstraint(
            (
                "(status IN ('running','done') AND error_code IS NULL) OR (status NOT I"
                "N ('running','done') AND error_code IS NOT NULL)"
            ),
            name="ck_directory_restore_operation_error",
        ),
        sa.CheckConstraint(
            (
                "mode IN ('execute','verify') AND intent_recorded IN (FALSE,TRUE) AND v"
                "erify_model_bound IN (FALSE,TRUE) AND verification_count >= 0 AND veri"
                "fication_model_count >= 0 AND verification_model_count <= verification"
                "_count"
            ),
            name="ck_directory_restore_operation_flags",
        ),
        sa.CheckConstraint(
            "status NOT IN ('done','uncertain') OR intent_recorded = TRUE",
            name="ck_directory_restore_operation_intent",
        ),
        sa.CheckConstraint(
            (
                "(status = 'running' AND owner_id IS NOT NULL AND lease_expires_at IS N"
                "OT NULL) OR (status != 'running' AND owner_id IS NULL AND lease_expire"
                "s_at IS NULL)"
            ),
            name="ck_directory_restore_operation_owner",
        ),
        sa.CheckConstraint(
            (
                "status IN ('running','done','rejected','uncertain','cancelled','interr"
                "upted','expired')"
            ),
            name="ck_directory_restore_operation_status",
        ),
        sa.CheckConstraint(
            "mode != 'verify' OR intent_recorded = TRUE",
            name="ck_directory_restore_verification_intent",
        ),
        sa.ForeignKeyConstraint(["verification_run_id"], ["runs.id"]),
        sa.ForeignKeyConstraint(["plan_id"], ["workspace_directory_restore_plans.plan_id"]),
        sa.UniqueConstraint("scope_id", "request_key", name="uq_directory_restore_operation_key"),
        sa.UniqueConstraint(
            "operation_id", "plan_id", name="uq_directory_restore_operation_parent"
        ),
        sa.UniqueConstraint("plan_id", name="uq_directory_restore_single_execution"),
    )
    op.create_index(
        "ix_workspace_directory_restore_attempts_scope_id",
        "workspace_directory_restore_attempts",
        ["scope_id"],
        unique=False,
    )
    op.create_table(
        "workspace_directory_restore_locks",
        sa.Column("plan_id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("operation_id", sa.String(36), nullable=False),
        sa.Column("kind", sa.String(16), primary_key=True, nullable=False),
        sa.Column("lock_key", sa.String(64), primary_key=True, nullable=False),
        sa.CheckConstraint("kind IN ('entry','directory')", name="ck_directory_restore_lock_kind"),
        sa.ForeignKeyConstraint(["plan_id"], ["workspace_directory_restore_plans.plan_id"]),
        sa.ForeignKeyConstraint(
            ["operation_id", "plan_id"],
            [
                "workspace_directory_restore_attempts.operation_id",
                "workspace_directory_restore_attempts.plan_id",
            ],
        ),
        sa.UniqueConstraint("kind", "lock_key", name="uq_directory_restore_physical_lock"),
    )
    op.create_table(
        "workspace_directory_restore_model_sources",
        sa.Column("run_id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("call_id", sa.String(128), primary_key=True, nullable=False),
        sa.Column("plan_id", sa.String(36), nullable=False),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("operation_id", sa.String(36), nullable=True),
        sa.CheckConstraint(
            (
                "phase IN ('preview','execute','verify') AND length(call_id) BETWEEN 1 "
                "AND 128 AND ((phase = 'preview' AND operation_id IS NULL) OR (phase !="
                " 'preview' AND operation_id IS NOT NULL))"
            ),
            name="ck_directory_restore_model_source",
        ),
        sa.ForeignKeyConstraint(
            ["operation_id", "plan_id"],
            [
                "workspace_directory_restore_attempts.operation_id",
                "workspace_directory_restore_attempts.plan_id",
            ],
        ),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.ForeignKeyConstraint(["plan_id"], ["workspace_directory_restore_plans.plan_id"]),
    )
    op.create_index(
        "uq_directory_restore_original_source",
        "workspace_directory_restore_model_sources",
        ["plan_id", "phase"],
        unique=True,
        sqlite_where=sa.text("phase IN ('preview','execute')"),
        postgresql_where=sa.text("phase IN ('preview','execute')"),
    )


def downgrade() -> None:
    sql = op.get_bind()
    for name in [
        "workspace_directory_restore_plans",
        "workspace_directory_restore_attempts",
        "workspace_directory_restore_locks",
        "workspace_directory_restore_model_sources",
    ]:
        if sql.execute(sa.text(f"SELECT count(*) FROM {name}")).scalar_one():
            raise RuntimeError("目录恢复计划非空，拒绝丢失原来源、私有备份或unknown操作")
    op.drop_table("workspace_directory_restore_model_sources")
    op.drop_table("workspace_directory_restore_locks")
    op.drop_table("workspace_directory_restore_attempts")
    op.drop_table("workspace_directory_restore_plans")
