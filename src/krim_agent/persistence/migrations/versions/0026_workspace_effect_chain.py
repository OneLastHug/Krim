"""保存真实文件效果顺序及保留见证，不猜测旧账本的应用顺序。"""

import sqlalchemy as sa
from alembic import op

revision = "0026_workspace_effect_chain"
down_revision = "0025_workspace_publication_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 原地新增列，不重建被逻辑文件锁引用的操作表；历史见证保持SQL NULL。
    op.add_column("workspace_edit_attempts", sa.Column("keep_witness", sa.JSON(), nullable=True))
    op.create_table(
        "workspace_file_effects",
        sa.Column("effect_sequence", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "operation_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_attempts.operation_id"),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "proposal_id",
            sa.String(36),
            sa.ForeignKey("workspace_edit_proposals.proposal_id"),
            nullable=False,
        ),
        sa.Column("scope_id", sa.String(80), nullable=False),
        sa.Column("resource_fingerprint", sa.String(64), nullable=False),
        sa.Column("file_key", sa.String(64), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("observed_after", sa.JSON(), nullable=True),
        sa.CheckConstraint(
            "effect_sequence >= 1 AND effect_sequence <= 9007199254740991",
            name="ck_workspace_effect_sequence",
        ),
        sa.CheckConstraint("action IN ('apply','undo')", name="ck_workspace_effect_action"),
        sqlite_autoincrement=True,
    )
    op.create_index("ix_workspace_file_effects_file_key", "workspace_file_effects", ["file_key"])


def downgrade() -> None:
    connection = op.get_bind()
    if connection.execute(sa.text("SELECT count(*) FROM workspace_file_effects")).scalar_one():
        raise RuntimeError("文件修改效果账本非空，拒绝丢失真实顺序的有损降级")
    if connection.execute(
        sa.text("SELECT count(*) FROM workspace_edit_attempts WHERE keep_witness IS NOT NULL")
    ).scalar_one():
        raise RuntimeError("文件修改保留见证非空，拒绝有损降级")
    op.drop_index("ix_workspace_file_effects_file_key", table_name="workspace_file_effects")
    op.drop_table("workspace_file_effects")
    op.drop_column("workspace_edit_attempts", "keep_witness")
