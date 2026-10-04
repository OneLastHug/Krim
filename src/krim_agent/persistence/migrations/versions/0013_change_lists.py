"""持久清单、幂等执行及回执历史；等待外部宿主时不持有事务。"""

import sqlalchemy as sa
from alembic import op

revision = "0013_change_lists"
down_revision = "0012_skills"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "change_lists",
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), primary_key=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("items", sa.JSON(), nullable=False),
        sa.Column("active_operation_id", sa.String(128), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("revision >= 0", name="ck_change_revision"),
    )
    op.create_table(
        "change_operations",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("execution", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("receipt", sa.JSON(), nullable=True),
        sa.Column("receipt_history", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("topic_id", "idempotency_key"),
    )
    op.create_index("ix_change_operations_topic_id", "change_operations", ["topic_id"])


def downgrade() -> None:
    connection = op.get_bind()
    if (
        connection.execute(sa.text("SELECT COUNT(*) FROM change_lists")).scalar()
        or connection.execute(sa.text("SELECT COUNT(*) FROM change_operations")).scalar()
    ):
        raise RuntimeError("已有变更清单或执行账本，回退会丢失审计；请先导出")
    op.drop_index("ix_change_operations_topic_id", table_name="change_operations")
    op.drop_table("change_operations")
    op.drop_table("change_lists")
