"""累计预算 account/request/allocation/binding；保留未知请求和历史账本。"""

import sqlalchemy as sa
from alembic import op

revision = "0017_usage_budgets"
down_revision = "0016_agent_handles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "usage_budget_accounts",
        sa.Column("account_id", sa.String(160), primary_key=True),
        sa.Column("owner_kind", sa.String(16), nullable=False),
        sa.Column("owner_id", sa.String(128), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("spent_tokens", sa.BigInteger(), nullable=False),
        sa.Column("spent_cost_micros", sa.BigInteger(), nullable=False),
        sa.Column("closed", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("owner_kind", "owner_id"),
        sa.CheckConstraint(
            "revision >= 0 AND spent_tokens >= 0 AND spent_cost_micros >= 0",
            name="ck_usage_budget_account",
        ),
    )
    op.create_table(
        "usage_budget_requests",
        sa.Column("request_id", sa.String(128), primary_key=True),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("step", sa.Integer(), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("input_tokens", sa.BigInteger(), nullable=False),
        sa.Column("output_reserve", sa.BigInteger(), nullable=False),
        sa.Column("reported_usage", sa.JSON(), nullable=True),
        sa.Column("source", sa.String(24), nullable=True),
        sa.Column("receipt", sa.String(256), nullable=True),
        sa.Column("overdrawn", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("run_id", "step"),
        sa.CheckConstraint(
            "step >= 0 AND step <= 31 AND revision >= 0 "
            "AND input_tokens >= 0 AND output_reserve > 0",
            name="ck_usage_budget_request",
        ),
    )
    op.create_index("ix_usage_budget_requests_run_id", "usage_budget_requests", ["run_id"])
    op.create_table(
        "usage_budget_allocations",
        sa.Column(
            "request_id",
            sa.String(128),
            sa.ForeignKey("usage_budget_requests.request_id"),
            primary_key=True,
        ),
        sa.Column(
            "account_id",
            sa.String(160),
            sa.ForeignKey("usage_budget_accounts.account_id"),
            primary_key=True,
        ),
        sa.Column("reserved_tokens", sa.BigInteger(), nullable=False),
        sa.Column("reserved_cost_micros", sa.BigInteger(), nullable=True),
        sa.Column("charged_tokens", sa.BigInteger(), nullable=True),
        sa.Column("charged_cost_micros", sa.BigInteger(), nullable=True),
        sa.Column("price", sa.JSON(), nullable=True),
        sa.CheckConstraint("reserved_tokens >= 0", name="ck_usage_budget_allocation"),
    )
    op.create_table(
        "usage_budget_bindings",
        sa.Column("owner_kind", sa.String(16), primary_key=True),
        sa.Column("owner_id", sa.String(128), primary_key=True),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("parents", sa.JSON(), nullable=False),
        sa.Column("account_ids", sa.JSON(), nullable=False),
    )


def downgrade() -> None:
    for table in ("usage_budget_requests", "usage_budget_accounts", "usage_budget_bindings"):
        if op.get_bind().execute(sa.text(f"SELECT COUNT(*) FROM {table}")).scalar():
            raise RuntimeError("已有累计预算账本，回退会丢失预留/结算/绑定；请先导出")
    op.drop_table("usage_budget_bindings")
    op.drop_table("usage_budget_allocations")
    op.drop_index("ix_usage_budget_requests_run_id", table_name="usage_budget_requests")
    op.drop_table("usage_budget_requests")
    op.drop_table("usage_budget_accounts")
