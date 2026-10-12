"""0042：目录树模型准备/启动原调用附表；保留0041及host历史。"""

import sqlalchemy as sa
from alembic import op

revision = "0042_workspace_tree_model_sources"
down_revision = "0041_workspace_tree_plans"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workspace_tree_plan_model_sources",
        sa.Column(
            "plan_id",
            sa.String(36),
            sa.ForeignKey("workspace_tree_plans.plan_id"),
            primary_key=True,
        ),
        sa.Column("kind", sa.String(16), primary_key=True),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("call_id", sa.String(128), nullable=False),
        sa.UniqueConstraint("run_id", "call_id", name="uq_tree_model_call"),
        sa.CheckConstraint("kind IN ('prepare','start')", name="ck_tree_model_kind"),
        sa.CheckConstraint("length(call_id) BETWEEN 1 AND 128", name="ck_tree_model_call"),
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM workspace_tree_plan_model_sources"))
        .scalar_one()
    ):
        raise RuntimeError("模型目录树来源非空，拒绝有损降级")
    op.drop_table("workspace_tree_plan_model_sources")
