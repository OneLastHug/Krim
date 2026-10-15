"""0051：恢复阶段原模型来源与唯一基线父关系，不重建旧循环FK/BLOB。"""

import sqlalchemy as sa
from alembic import op

revision = "0051_checkpoint_restore_models"
down_revision = "0050_native_checkpoint_models"
branch_labels = None
depends_on = None


def upgrade() -> None:
    sqlite = op.get_bind().dialect.name == "sqlite"
    for phase in ("preview", "execute"):
        name = phase + "_model_bound"
        if sqlite:
            op.execute(
                sa.text(
                    f"ALTER TABLE workspace_restore_plans ADD COLUMN {name} "
                    f"BOOLEAN NOT NULL DEFAULT 0 CONSTRAINT ck_restore_{phase}_model "
                    f"CHECK ({name} IN (0,1))"
                )
            )
        else:
            op.add_column(
                "workspace_restore_plans",
                sa.Column(name, sa.Boolean(), nullable=False, server_default=sa.text("0")),
            )
            op.create_check_constraint(
                f"ck_restore_{phase}_model", "workspace_restore_plans", f"{name} IN (0,1)"
            )
    op.create_table(
        "workspace_restore_model_sources",
        sa.Column(
            "plan_id",
            sa.String(36),
            sa.ForeignKey("workspace_restore_plans.plan_id"),
            primary_key=True,
        ),
        sa.Column("phase", sa.String(8), primary_key=True),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("call_id", sa.String(128), nullable=False),
        sa.UniqueConstraint("run_id", "call_id", name="uq_restore_model_call"),
        sa.CheckConstraint("phase IN ('preview','execute')", name="ck_restore_model_phase"),
        sa.CheckConstraint("length(call_id) BETWEEN 1 AND 128", name="ck_restore_model_call"),
    )
    if sqlite:
        op.execute(
            sa.text(
                "ALTER TABLE native_checkpoint_model_sources ADD COLUMN restore_plan_id "
                "VARCHAR(36) REFERENCES workspace_restore_plans(plan_id)"
            )
        )
    else:
        op.add_column(
            "native_checkpoint_model_sources", sa.Column("restore_plan_id", sa.String(36))
        )
        op.create_foreign_key(
            "fk_native_checkpoint_restore_plan",
            "native_checkpoint_model_sources",
            "workspace_restore_plans",
            ["restore_plan_id"],
            ["plan_id"],
        )
    op.create_index(
        "uq_native_checkpoint_restore_baseline",
        "native_checkpoint_model_sources",
        ["restore_plan_id"],
        unique=True,
    )


def downgrade() -> None:
    sql = op.get_bind()
    if any(
        sql.execute(sa.text(query)).first()
        for query in (
            "SELECT 1 FROM workspace_restore_model_sources LIMIT 1",
            "SELECT 1 FROM workspace_restore_plans "
            "WHERE preview_model_bound != 0 OR execute_model_bound != 0 LIMIT 1",
            "SELECT 1 FROM native_checkpoint_model_sources "
            "WHERE restore_plan_id IS NOT NULL LIMIT 1",
        )
    ):
        raise RuntimeError("模型恢复不能降级丢失阶段来源或基线父关系")
    op.drop_index("uq_native_checkpoint_restore_baseline", "native_checkpoint_model_sources")
    if sql.dialect.name != "sqlite":
        op.drop_constraint(
            "fk_native_checkpoint_restore_plan",
            "native_checkpoint_model_sources",
            type_="foreignkey",
        )
    op.drop_column("native_checkpoint_model_sources", "restore_plan_id")
    op.drop_table("workspace_restore_model_sources")
    for phase in ("execute", "preview"):
        if sql.dialect.name != "sqlite":
            op.drop_constraint(
                f"ck_restore_{phase}_model", "workspace_restore_plans", type_="check"
            )
        op.drop_column("workspace_restore_plans", phase + "_model_bound")
