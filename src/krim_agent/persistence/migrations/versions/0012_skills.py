"""为话题持久保存明确注入的技能目录。"""

import json

import sqlalchemy as sa
from alembic import op

revision = "0012_skills"
down_revision = "0011_locale"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("topics", sa.Column("skills", sa.JSON(), nullable=False, server_default="{}"))


def downgrade() -> None:
    for (raw,) in op.get_bind().execute(sa.text("SELECT skills FROM topics")):
        value = json.loads(raw) if isinstance(raw, str) else raw
        if value not in ({}, {"skills": []}):
            raise RuntimeError("已有 Skills 目录，回退会丢失技能；请先导出")
    op.drop_column("topics", "skills")
