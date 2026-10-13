"""0046：原交换proposal与完成树封存同事务来源；旧0044材料不改。"""

import sqlalchemy as sa
from alembic import op

revision = "0046_tree_exchange_origins"
down_revision = "0045_completed_tree_seals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_tree_exchange_origins",
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_exchange_proposals.proposal_id"),
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
        sa.Column("proof_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_workspace_tree_exchange_origins_source_plan_id",
        "workspace_tree_exchange_origins",
        ["source_plan_id"],
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM workspace_tree_exchange_origins"))
        .scalar_one()
    ):
        raise RuntimeError("交换生成树来源关联非空，拒绝有损降级")
    op.drop_table("workspace_tree_exchange_origins")
