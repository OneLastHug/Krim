"""0057：原缺失基线回退来源；保留旧Move表/JSON/hash/BLOB。"""

import sqlalchemy as sa
from alembic import op

revision = "0057_missing_directory_rollbacks"
down_revision = "0056_missing_directory_restores"
branch_labels = None
depends_on = None

ORIGIN_CHECK = (
    "(missing_restore_plan_id IS NULL AND missing_restore_hash IS NULL) OR "
    "(missing_restore_plan_id IS NOT NULL AND missing_restore_hash IS NOT NULL "
    "AND length(missing_restore_hash) = 64)"
)


def upgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        op.execute(
            sa.text(
                "ALTER TABLE workspace_tree_move_proposals ADD COLUMN missing_restore_plan_id "
                "VARCHAR(36) CONSTRAINT fk_move_missing_plan "
                "REFERENCES workspace_missing_directory_restore_plans(plan_id)"
            )
        )
        op.execute(
            sa.text(
                "ALTER TABLE workspace_tree_move_proposals ADD COLUMN missing_restore_hash "
                "VARCHAR(64) CONSTRAINT ck_tree_move_missing_origin CHECK (" + ORIGIN_CHECK + ")"
            )
        )
    else:
        op.add_column(
            "workspace_tree_move_proposals",
            sa.Column(
                "missing_restore_plan_id",
                sa.String(36),
                sa.ForeignKey(
                    "workspace_missing_directory_restore_plans.plan_id", name="fk_move_missing_plan"
                ),
                nullable=True,
            ),
        )
        op.add_column(
            "workspace_tree_move_proposals",
            sa.Column("missing_restore_hash", sa.String(64), nullable=True),
        )
        op.create_check_constraint(
            "ck_tree_move_missing_origin", "workspace_tree_move_proposals", ORIGIN_CHECK
        )
    op.create_table(
        "workspace_missing_directory_rollbacks",
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_move_proposals.proposal_id"),
            primary_key=True,
        ),
        sa.Column(
            "source_plan_id",
            sa.String(36),
            sa.ForeignKey("workspace_missing_directory_restore_plans.plan_id"),
            nullable=False,
        ),
        sa.Column("source_operation_id", sa.String(36), nullable=False),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("source_metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_operation_id", "source_plan_id"],
            [
                "workspace_missing_directory_restore_attempts.operation_id",
                "workspace_missing_directory_restore_attempts.plan_id",
            ],
        ),
        sa.CheckConstraint("length(source_hash) = 64", name="ck_missing_rollback_source_hash"),
    )
    op.create_index(
        "ix_workspace_missing_directory_rollbacks_source_plan_id",
        "workspace_missing_directory_rollbacks",
        ["source_plan_id"],
    )


def downgrade() -> None:
    connection = op.get_bind()
    if (
        connection.execute(
            sa.text("SELECT COUNT(*) FROM workspace_missing_directory_rollbacks")
        ).scalar_one()
        or connection.execute(
            sa.text(
                "SELECT COUNT(*) FROM workspace_tree_move_proposals "
                "WHERE missing_restore_plan_id IS NOT NULL OR missing_restore_hash IS NOT NULL"
            )
        ).scalar_one()
    ):
        raise RuntimeError("缺失基线回退来源/标记非空，禁止有损降级")
    op.drop_index(
        "ix_workspace_missing_directory_rollbacks_source_plan_id",
        table_name="workspace_missing_directory_rollbacks",
    )
    op.drop_table("workspace_missing_directory_rollbacks")
    if connection.dialect.name == "sqlite":
        op.execute(
            sa.text("ALTER TABLE workspace_tree_move_proposals DROP COLUMN missing_restore_hash")
        )
        op.execute(
            sa.text("ALTER TABLE workspace_tree_move_proposals DROP COLUMN missing_restore_plan_id")
        )
    else:
        op.drop_constraint(
            "ck_tree_move_missing_origin", "workspace_tree_move_proposals", type_="check"
        )
        op.drop_constraint(
            "fk_move_missing_plan", "workspace_tree_move_proposals", type_="foreignkey"
        )
        op.drop_column("workspace_tree_move_proposals", "missing_restore_hash")
        op.drop_column("workspace_tree_move_proposals", "missing_restore_plan_id")
