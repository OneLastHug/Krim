"""Agent 会话账本；会话检查和 Run 创建使用同一个事务。"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import cast

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.agents.sessions import AgentSessionView, AgentTurnView, SessionStatus
from krim_agent.protocol.domain import RunView, ServiceError
from krim_agent.protocol.models import utc_now

from .database import AgentHandleRow, AgentSessionRow, AgentSessionTurnRow, Database, RunRow


def effective_status(row: AgentSessionRow) -> SessionStatus:
    if row.status in {"open", "closed"} and row.expires_at is not None:
        if row.expires_at <= time.time():
            return "expired"
    return cast(SessionStatus, row.status)


def session_view(row: AgentSessionRow) -> AgentSessionView:
    return AgentSessionView(
        session_id=row.session_id,
        agent_id=row.agent_id,
        topic_id=row.topic_id,
        name=row.name,
        persistent=row.persistent,
        profile_revision=row.profile_revision,
        definition_hash=row.definition_hash,
        parent_session_id=row.parent_session_id,
        status=effective_status(row),
        revision=row.revision,
        owner_id=row.owner_id,
        expires_at=row.expires_at,
        created_at=row.created_at,
        closed_at=row.closed_at,
        permissions=row.permissions,
        turn=row.turn,
        current_run_id=row.current_run_id,
    )


async def assert_session_owner(
    session: AsyncSession, row: AgentSessionRow, owner_id: str | None
) -> None:
    if owner_id is None or row.owner_id != owner_id:
        raise ServiceError("agent_session_owner", "Agent 会话未由当前进程接管", 409)
    if row.persistent:
        handle = await session.get(AgentHandleRow, row.agent_id)
        if (
            handle is None
            or handle.owner_id != owner_id
            or (handle.lease_expires_at or 0) <= time.time()
        ):
            raise ServiceError("agent_lease_lost", "Agent 会话的持久租约已失效", 409)


async def assert_session_open(session: AsyncSession, row: AgentSessionRow) -> None:
    status = effective_status(row)
    if status != "open":
        raise ServiceError(f"agent_session_{status}", "Agent 会话已关闭、过期或销毁", 410)
    parent = row.parent_session_id
    depth = 0
    while parent is not None:
        depth += 1
        ancestor = await session.get(AgentSessionRow, parent)
        if ancestor is None or effective_status(ancestor) != "open" or depth > 16:
            raise ServiceError("agent_parent_session_closed", "父 Agent 会话不可执行", 409)
        parent = ancestor.parent_session_id


async def guard_agent_topic(
    session: AsyncSession,
    topic_id: str,
    session_id: str | None,
    agent_id: str | None,
    owner_id: str | None,
) -> AgentSessionRow | None:
    row = await session.scalar(select(AgentSessionRow).where(AgentSessionRow.topic_id == topic_id))
    if row is None:
        if session_id is not None:
            raise ServiceError("agent_session_topic", "Agent 会话与话题不匹配", 403)
        return None
    if row.session_id != session_id or row.agent_id != agent_id:
        raise ServiceError("agent_topic_managed", "Agent 专属话题须通过 Agent 会话执行", 403)
    await assert_session_owner(session, row, owner_id)
    return row


class AgentSessionStore:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(
        self,
        *,
        session_id: str,
        agent_id: str,
        topic_id: str,
        name: str,
        persistent: bool,
        profile_revision: int | None,
        definition_hash: str,
        parent_session_id: str | None,
        owner_id: str,
        expires_at: float | None,
        permissions: Mapping[str, JsonValue],
    ) -> AgentSessionView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = AgentSessionRow(
                session_id=session_id,
                agent_id=agent_id,
                topic_id=topic_id,
                name=name,
                persistent=persistent,
                profile_revision=profile_revision,
                definition_hash=definition_hash,
                parent_session_id=parent_session_id,
                owner_id=owner_id,
                expires_at=expires_at,
                permissions=dict(permissions),
                status="open",
                revision=0,
                created_at=utc_now(),
                turn=0,
            )
            session.add(row)
            await session.flush()
            return session_view(row)

    async def get(self, session_id: str) -> AgentSessionView:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.get(AgentSessionRow, session_id)
            if row is None:
                raise ServiceError("agent_session_not_found", "Agent 会话不存在", 404)
            return session_view(row)

    async def for_agent(self, agent_id: str) -> AgentSessionView:
        async with self.database.write_lock, self.database.sessions() as session:
            row = await session.scalar(
                select(AgentSessionRow).where(AgentSessionRow.agent_id == agent_id)
            )
            if row is None:
                raise ServiceError("agent_session_not_found", "Agent 会话不存在", 404)
            return session_view(row)

    async def attach(
        self,
        session_id: str,
        owner_id: str,
        definition_hash: str,
        permissions: Mapping[str, JsonValue],
    ) -> AgentSessionView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, session_id)
            if row is None or row.status == "destroyed" or row.definition_hash != definition_hash:
                raise ServiceError("agent_session_definition", "Agent 会话定义不可恢复", 409)
            # 持久 handle 的 claim 已完成；会话 owner 只能由该租约持有者接管。
            handle = await session.get(AgentHandleRow, row.agent_id)
            if handle is None or handle.owner_id != owner_id:
                raise ServiceError("agent_session_owner", "Agent 会话缺少持久租约", 409)
            row.owner_id = owner_id
            row.revision += 1
            if not row.permissions:
                row.permissions = dict(permissions)
            return session_view(row)

    async def transition(
        self, session_id: str, owner_id: str, expected_revision: int, status: SessionStatus
    ) -> AgentSessionView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, session_id)
            if row is None:
                raise ServiceError("agent_session_not_found", "Agent 会话不存在", 404)
            await assert_session_owner(session, row, owner_id)
            if row.revision != expected_revision:
                raise ServiceError("agent_session_conflict", "Agent 会话版本已变化", 409)
            previous = effective_status(row)
            if previous == "destroyed" or (status == "open" and previous == "expired"):
                raise ServiceError(f"agent_session_{previous}", "Agent 会话不可重开", 410)
            if status == "open":
                row.status = "open"
                await assert_session_open(session, row)
            else:
                row.status = "expired" if previous == "expired" and status == "closed" else status
            row.revision += 1
            row.closed_at = None if status == "open" else utc_now()
            return session_view(row)

    async def release(self, session_id: str, owner_id: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, session_id)
            if row is not None and row.owner_id == owner_id:
                row.owner_id = None
                row.revision += 1

    async def turns(self, session_id: str) -> list[AgentTurnView]:
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.execute(
                select(AgentSessionTurnRow, RunRow.status)
                .join(RunRow, AgentSessionTurnRow.run_id == RunRow.id)
                .where(AgentSessionTurnRow.session_id == session_id)
                .order_by(AgentSessionTurnRow.turn)
            )
            return [
                AgentTurnView(
                    session_id=row.session_id,
                    agent_id=row.agent_id,
                    run_id=row.run_id,
                    turn=row.turn,
                    status=status,
                    created_at=row.created_at,
                )
                for row, status in rows
            ]

    async def assert_run(self, session_id: str, run_id: str) -> None:
        async with self.database.write_lock, self.database.sessions() as session:
            found = await session.scalar(
                select(AgentSessionTurnRow.run_id).where(
                    AgentSessionTurnRow.session_id == session_id,
                    AgentSessionTurnRow.run_id == run_id,
                )
            )
            if found is None:
                raise ServiceError("agent_run_mismatch", "Run 不属于该 Agent 会话", 403)

    async def find_turn(self, session_id: str, key: str) -> tuple[RunView, int] | None:
        async with self.database.write_lock, self.database.sessions() as session:
            found = (
                await session.execute(
                    select(RunRow, AgentSessionTurnRow.turn)
                    .join(AgentSessionTurnRow, RunRow.id == AgentSessionTurnRow.run_id)
                    .where(
                        AgentSessionTurnRow.session_id == session_id,
                        RunRow.idempotency_key == key,
                    )
                )
            ).first()
            if found is None:
                return None
            row, turn = found
            return RunView.model_validate(row), turn
