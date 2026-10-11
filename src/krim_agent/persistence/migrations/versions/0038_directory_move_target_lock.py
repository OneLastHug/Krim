"""0038：0036目录锁增加移动目标entry的物理锁。"""

import sqlalchemy as sa
from alembic import op

revision = "0038_directory_move_target_lock"
down_revision = "0037_command_directory_sources"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workspace_directory_locks", sa.Column("target_lock_key", sa.String(64), nullable=True)
    )
    op.create_index(
        "ix_workspace_directory_locks_target_lock_key",
        "workspace_directory_locks",
        ["target_lock_key"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.execute(
        sa.text("SELECT count(*) FROM workspace_directory_locks WHERE target_lock_key IS NOT NULL")
    ).scalar_one():
        raise RuntimeError("目录移动目标锁非空，拒绝有损降级")
    op.drop_index(
        "ix_workspace_directory_locks_target_lock_key", table_name="workspace_directory_locks"
    )
    op.drop_column("workspace_directory_locks", "target_lock_key")
