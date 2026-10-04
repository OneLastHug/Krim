"""话题 PromptContext 目录；已发送消息在 details 中保存独立快照。"""

import json

import sqlalchemy as sa
from alembic import op

revision = "0010_prompt_context"
down_revision = "0009_user_interactions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "topics", sa.Column("prompt_context", sa.JSON(), nullable=False, server_default="{}")
    )


def downgrade() -> None:
    for (value,) in op.get_bind().execute(sa.text("SELECT prompt_context FROM topics")):
        data = json.loads(value) if isinstance(value, str) else value
        if isinstance(data, dict) and (data.get("mentions") or data.get("context_tags")):
            raise RuntimeError("已有引用/背景标签目录，降级会丢失上下文；请先备份导出")
    op.drop_column("topics", "prompt_context")
