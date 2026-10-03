"""为旧线性历史回填父链；原 ID、内容、sequence、运行和审计保持原值。"""

import json

import sqlalchemy as sa
from alembic import op

revision = "0002_message_tree"
down_revision = "0001_m1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("action", sa.String(16), nullable=False, server_default="send"))
    op.add_column("runs", sa.Column("source_message_id", sa.String(36), nullable=True))
    op.add_column(
        "topics", sa.Column("active_branches", sa.JSON(), nullable=False, server_default="{}")
    )
    with op.batch_alter_table("messages") as batch:
        batch.add_column(sa.Column("parent_id", sa.String(36), nullable=True))
        batch.create_foreign_key("fk_messages_parent", "messages", ["parent_id"], ["id"])
        batch.create_index("ix_messages_parent_id", ["parent_id"])
    connection = op.get_bind()
    rows = connection.execute(
        sa.text("SELECT id, topic_id FROM messages ORDER BY topic_id, sequence")
    ).fetchall()
    previous: dict[str, str] = {}
    choices: dict[str, dict[str, str]] = {}
    for message_id, topic_id in rows:
        parent_id = previous.get(topic_id)
        connection.execute(
            sa.text("UPDATE messages SET parent_id=:parent WHERE id=:id"),
            {"parent": parent_id, "id": message_id},
        )
        choices.setdefault(topic_id, {})[parent_id or "__root__"] = message_id
        previous[topic_id] = message_id
    for topic_id, branches in choices.items():
        connection.execute(
            sa.text("UPDATE topics SET active_branches=:branches WHERE id=:id"),
            {"branches": json.dumps(branches), "id": topic_id},
        )


def downgrade() -> None:
    branched = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT 1 FROM messages GROUP BY topic_id, parent_id HAVING count(*) > 1 LIMIT 1"
            )
        )
        .first()
    )
    if branched:
        raise RuntimeError("已有消息分支，回退会丢失父链；请先备份并导出分支")
    # 重建被引用表期间延迟外键裁决，事务提交前重新校验完整结构。
    if op.get_bind().dialect.name == "sqlite":
        op.execute("PRAGMA defer_foreign_keys=ON")
    with op.batch_alter_table("messages") as batch:
        batch.drop_constraint("fk_messages_parent", type_="foreignkey")
        batch.drop_index("ix_messages_parent_id")
        batch.drop_column("parent_id")
    # 这三个字段没有约束或索引，原生 DROP COLUMN 保留被引用表的身份。
    op.drop_column("topics", "active_branches")
    op.drop_column("runs", "source_message_id")
    op.drop_column("runs", "action")
    if op.get_bind().dialect.name == "sqlite":
        if op.get_bind().exec_driver_sql("PRAGMA foreign_key_check").first():
            raise RuntimeError("迁移回退后的外键不完整，事务必须回滚")
