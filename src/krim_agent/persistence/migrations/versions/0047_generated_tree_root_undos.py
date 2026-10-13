"""0047：生成根撤销原seal/restore来源，旧Move请求/hash/私有材料保持。"""

import sqlalchemy as sa
from alembic import op

revision = "0047_generated_tree_root_undos"
down_revision = "0046_tree_exchange_origins"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        # SQLite原active-operation循环外键不重建/删除原表；仅追加可空列。
        op.execute(
            sa.text(
                "ALTER TABLE workspace_tree_move_proposals ADD COLUMN generated_plan_id "
                "VARCHAR(36) CONSTRAINT fk_move_generated_plan "
                "REFERENCES workspace_tree_completed_seals(plan_id)"
            )
        )
    else:
        op.add_column(
            "workspace_tree_move_proposals",
            sa.Column(
                "generated_plan_id",
                sa.String(36),
                sa.ForeignKey(
                    "workspace_tree_completed_seals.plan_id", name="fk_move_generated_plan"
                ),
                nullable=True,
            ),
        )
    op.create_table(
        "workspace_generated_tree_undos",
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_move_proposals.proposal_id"),
            primary_key=True,
        ),
        sa.Column(
            "source_plan_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_completed_seals.plan_id"),
            nullable=False,
        ),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("workspace_key", sa.String(64), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("tree_fingerprint", sa.String(64), nullable=False),
        sa.Column("approved_fingerprint", sa.String(64), nullable=False),
        sa.Column("proof_hash", sa.String(64), nullable=False),
        sa.Column(
            "previous_restore_operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_move_attempts.operation_id"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("length(tree_fingerprint) = 64", name="ck_generated_undo_tree_hash"),
        sa.CheckConstraint(
            "length(approved_fingerprint) = 64", name="ck_generated_undo_approved_hash"
        ),
        sa.CheckConstraint("length(proof_hash) = 64", name="ck_generated_undo_proof_hash"),
    )
    op.create_index(
        "ix_workspace_generated_tree_undos_source_plan_id",
        "workspace_generated_tree_undos",
        ["source_plan_id"],
    )


def downgrade() -> None:
    connection = op.get_bind()
    if (
        connection.execute(
            sa.text("SELECT count(*) FROM workspace_generated_tree_undos")
        ).scalar_one()
        or connection.execute(
            sa.text(
                "SELECT count(*) FROM workspace_tree_move_proposals "
                "WHERE generated_plan_id IS NOT NULL"
            )
        ).scalar_one()
    ):
        raise RuntimeError("原生成根撤销标记/来源非空，拒绝有损降级")
    op.drop_table("workspace_generated_tree_undos")
    if connection.dialect.name == "sqlite":
        op.execute(
            sa.text("ALTER TABLE workspace_tree_move_proposals DROP COLUMN generated_plan_id")
        )
    else:
        op.drop_constraint(
            "fk_move_generated_plan", "workspace_tree_move_proposals", type_="foreignkey"
        )
        op.drop_column("workspace_tree_move_proposals", "generated_plan_id")
