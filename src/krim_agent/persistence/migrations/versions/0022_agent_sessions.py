"""Agent 会话与原子轮次；保留旧持久实例身份和 Run。"""

import sqlalchemy as sa
from alembic import op

revision = "0022_agent_sessions"
down_revision = "0021_agent_profiles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_sessions",
        sa.Column("session_id", sa.String(36), primary_key=True),
        sa.Column("agent_id", sa.String(36), nullable=False, unique=True),
        sa.Column(
            "topic_id", sa.String(36), sa.ForeignKey("topics.id"), nullable=False, unique=True
        ),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("persistent", sa.Boolean(), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=True),
        sa.Column("definition_hash", sa.String(64), nullable=False),
        sa.Column("parent_session_id", sa.String(36), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("owner_id", sa.String(36), nullable=True),
        sa.Column("expires_at", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("closed_at", sa.DateTime(), nullable=True),
        sa.Column("permissions", sa.JSON(), nullable=False),
        sa.Column("turn", sa.Integer(), nullable=False),
        sa.Column("current_run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=True),
        sa.CheckConstraint("revision >= 0 AND turn >= 0", name="ck_agent_session_revision_turn"),
        sa.CheckConstraint(
            "status IN ('open','closed','expired','destroyed')", name="ck_agent_session_status"
        ),
    )
    op.create_index("ix_agent_sessions_parent_session_id", "agent_sessions", ["parent_session_id"])
    op.create_table(
        "agent_session_turns",
        sa.Column(
            "session_id",
            sa.String(36),
            sa.ForeignKey("agent_sessions.session_id"),
            primary_key=True,
        ),
        sa.Column("turn", sa.Integer(), primary_key=True),
        sa.Column("agent_id", sa.String(36), nullable=False),
        sa.Column("run_id", sa.String(36), sa.ForeignKey("runs.id"), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("turn >= 1", name="ck_agent_session_turn"),
    )
    bind = op.get_bind()
    bind.execute(
        sa.text(
            "INSERT INTO agent_sessions "
            "(session_id,agent_id,topic_id,name,persistent,profile_revision,definition_hash,"
            "parent_session_id,status,revision,owner_id,permissions,turn,current_run_id,"
            "created_at) "
            "SELECT agent_id,agent_id,topic_id,name,persistent,profile_revision,definition_hash,"
            "parent_agent_id,'open',0,owner_id,'{}',turn,current_run_id,updated_at "
            "FROM agent_handles"
        )
    )
    bind.execute(
        sa.text(
            "INSERT INTO agent_session_turns (session_id,turn,agent_id,run_id,created_at) "
            "SELECT s.session_id,ROW_NUMBER() OVER (PARTITION BY s.session_id "
            "ORDER BY r.created_at,r.id),s.agent_id,r.id,r.created_at "
            "FROM agent_sessions s JOIN runs r ON r.topic_id=s.topic_id"
        )
    )
    bind.execute(
        sa.text(
            "UPDATE agent_sessions SET turn=MAX(turn,(SELECT COUNT(*) FROM agent_session_turns t "
            "WHERE t.session_id=agent_sessions.session_id)), current_run_id=COALESCE("
            "(SELECT run_id FROM agent_session_turns t WHERE "
            "t.session_id=agent_sessions.session_id "
            "ORDER BY turn DESC LIMIT 1),current_run_id)"
        )
    )


def downgrade() -> None:
    if op.get_bind().execute(sa.text("SELECT count(*) FROM agent_sessions")).scalar_one():
        raise RuntimeError("Agent会话/轮次账本非空，拒绝有损降级")
    op.drop_table("agent_session_turns")
    op.drop_index("ix_agent_sessions_parent_session_id", table_name="agent_sessions")
    op.drop_table("agent_sessions")
