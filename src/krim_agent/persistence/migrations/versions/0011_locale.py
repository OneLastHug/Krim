"""为话题保存规范化的 Locale。"""

import sqlalchemy as sa
from alembic import op

revision = "0011_locale"
down_revision = "0010_prompt_context"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "topics", sa.Column("locale", sa.String(16), nullable=False, server_default="zh-CN")
    )


def downgrade() -> None:
    if (
        op.get_bind()
        .execute(sa.text("SELECT 1 FROM topics WHERE locale != 'zh-CN' LIMIT 1"))
        .first()
    ):
        raise RuntimeError("已有非默认 Locale，回退会丢失区域设置；请先导出")
    op.drop_column("topics", "locale")
