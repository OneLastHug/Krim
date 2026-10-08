"""持久保存修改动作来源状态，拒绝撤销时保留既有用户决定。"""

import sqlalchemy as sa
from alembic import op

revision = "0025_workspace_publication_state"
down_revision = "0024_workspace_edits"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            "SELECT operation_id, proposal_id, action, status, expected_revision "
            "FROM workspace_edit_attempts ORDER BY proposal_id, expected_revision, operation_id"
        )
    ).mappings()
    previous_proposal: str | None = None
    previous_revision = -1
    state = "proposed"
    sources: list[dict[str, str]] = []
    allowed = {"apply": {"proposed"}, "undo": {"applied", "kept"}, "keep": {"applied"}}
    for row in rows:
        if row["proposal_id"] != previous_proposal:
            state, previous_revision = "proposed", -1
            previous_proposal = row["proposal_id"]
        if (
            row["expected_revision"] <= previous_revision
            or row["action"] not in allowed
            or state not in allowed[row["action"]]
        ):
            raise RuntimeError("文件修改操作历史无法推导来源状态，拒绝不可靠升级")
        sources.append({"operation_id": row["operation_id"], "previous_status": state})
        previous_revision = row["expected_revision"]
        if row["status"] == "done":
            state = {"apply": "applied", "undo": "undone", "keep": "kept"}[row["action"]]
        elif row["status"] == "rejected":
            # 必须重放0024实际规则：旧kept后的撤销拒绝当时已写成applied。
            # 只补充动作来源，不改写既有提案终态和历史回执。
            state = {"apply": "rejected", "undo": "applied", "keep": "applied"}[row["action"]]
        elif row["status"] == "uncertain":
            state = "uncertain"
        elif row["status"] == "running":
            state = {"apply": "applying", "undo": "undoing", "keep": "keeping"}[row["action"]]
        else:
            raise RuntimeError("文件修改操作状态无效，拒绝不可靠升级")
    # 原地追加避免重建被文件锁外键引用的表；默认仅用于追加旧行，随后全量回填。
    op.add_column(
        "workspace_edit_attempts",
        sa.Column(
            "previous_status",
            sa.String(16),
            sa.CheckConstraint(
                "previous_status IN ('proposed','applied','kept')",
                name="ck_workspace_attempt_previous_status",
            ),
            nullable=False,
            server_default="proposed",
        ),
    )
    if sources:
        connection.execute(
            sa.text(
                "UPDATE workspace_edit_attempts SET previous_status=:previous_status "
                "WHERE operation_id=:operation_id"
            ),
            sources,
        )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM workspace_edit_attempts")).scalar_one():
        raise RuntimeError("文件修改操作账本非空，拒绝丢失来源状态的有损降级")
    op.drop_column("workspace_edit_attempts", "previous_status")
