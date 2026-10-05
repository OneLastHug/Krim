"""持久 Agent inventory 与 owner/revision CAS；不保存运行时 callable 或 secret。"""

from __future__ import annotations

import time
from dataclasses import dataclass

from sqlalchemy import delete, exists, or_, select, update

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import utc_now

from .database import AgentHandleRow, Database, RunRow, TopicRow


@dataclass(frozen=True)
class StoredAgentHandle:
    agent_id: str
    name: str
    topic_id: str
    persistent: bool
    turn: int
    current_run_id: str | None
    status: str
    definition_hash: str
    revision: int
    owner_id: str | None
    lease_expires_at: float | None
    parent_agent_id: str | None
    parent_tool_call_id: str | None


class AgentHandleStore:
    def __init__(self, database: Database, *, lease_seconds: float = 30) -> None:
        if not 0.2 <= lease_seconds <= 600:
            raise ValueError("Agent 租约须在 0.2–600 秒")
        self.database = database
        self.lease_seconds = lease_seconds

    @staticmethod
    def _stored(row: AgentHandleRow) -> StoredAgentHandle:
        return StoredAgentHandle(
            agent_id=row.agent_id,
            name=row.name,
            topic_id=row.topic_id,
            persistent=row.persistent,
            turn=row.turn,
            current_run_id=row.current_run_id,
            status=row.status,
            definition_hash=row.definition_hash,
            revision=row.revision,
            owner_id=row.owner_id,
            lease_expires_at=row.lease_expires_at,
            parent_agent_id=row.parent_agent_id,
            parent_tool_call_id=row.parent_tool_call_id,
        )

    async def list(self) -> list[StoredAgentHandle]:
        async with self.database.write_lock, self.database.sessions() as session:
            rows = await session.scalars(select(AgentHandleRow).order_by(AgentHandleRow.agent_id))
            return [self._stored(row) for row in rows]

    async def renew(self, agent_id: str, owner_id: str, revision: int) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            now = time.time()
            identifier = await session.scalar(
                update(AgentHandleRow)
                .where(
                    AgentHandleRow.agent_id == agent_id,
                    AgentHandleRow.owner_id == owner_id,
                    AgentHandleRow.revision == revision,
                    AgentHandleRow.lease_expires_at > now,
                )
                .values(lease_expires_at=now + self.lease_seconds, updated_at=utc_now())
                .returning(AgentHandleRow.agent_id)
            )
            if identifier is None:
                raise ServiceError("agent_lease_lost", "Agent 所有权、版本或租约已变化", 409)

    async def create(self, value: StoredAgentHandle, owner_id: str) -> StoredAgentHandle:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            if await session.get(TopicRow, value.topic_id) is None:
                raise ServiceError("topic_not_found", "Agent topic 不存在", 404)
            if await session.get(AgentHandleRow, value.agent_id) is not None:
                raise ServiceError("agent_id_conflict", "Agent ID 已存在", 409)
            if value.current_run_id is not None:
                run = await session.get(RunRow, value.current_run_id)
                if run is None or run.topic_id != value.topic_id:
                    raise ServiceError("agent_run_mismatch", "Agent run 不属于当前 topic", 409)
            row = AgentHandleRow(
                agent_id=value.agent_id,
                name=value.name,
                topic_id=value.topic_id,
                persistent=value.persistent,
                turn=value.turn,
                current_run_id=value.current_run_id,
                status=value.status,
                owner_id=owner_id,
                revision=0,
                lease_expires_at=time.time() + self.lease_seconds,
                definition_hash=value.definition_hash,
                parent_agent_id=value.parent_agent_id,
                parent_tool_call_id=value.parent_tool_call_id,
                updated_at=utc_now(),
            )
            session.add(row)
            await session.flush()
            return self._stored(row)

    async def claim(
        self, agent_id: str, owner_id: str, expected_revision: int
    ) -> StoredAgentHandle:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            now = time.time()
            row = await session.scalar(
                update(AgentHandleRow)
                .where(
                    AgentHandleRow.agent_id == agent_id,
                    AgentHandleRow.revision == expected_revision,
                    or_(
                        AgentHandleRow.owner_id.is_(None),
                        AgentHandleRow.owner_id == owner_id,
                        AgentHandleRow.lease_expires_at <= now,
                    ),
                )
                .values(
                    owner_id=owner_id,
                    lease_expires_at=now + self.lease_seconds,
                    revision=AgentHandleRow.revision + 1,
                    updated_at=utc_now(),
                )
                .returning(AgentHandleRow)
            )
            if row is None:
                raise ServiceError("agent_busy", "Agent 已由另一进程接管或版本变化", 409)
            return self._stored(row)

    async def save(
        self,
        agent_id: str,
        owner_id: str,
        revision: int,
        *,
        turn: int,
        current_run_id: str | None,
        status: str,
        release: bool = False,
    ) -> StoredAgentHandle:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            now = time.time()
            statement = update(AgentHandleRow).where(
                AgentHandleRow.agent_id == agent_id,
                AgentHandleRow.owner_id == owner_id,
                AgentHandleRow.revision == revision,
                AgentHandleRow.lease_expires_at > now,
                AgentHandleRow.turn <= turn,
            )
            if current_run_id is not None:
                statement = statement.where(
                    exists(
                        select(RunRow.id).where(
                            RunRow.id == current_run_id, RunRow.topic_id == AgentHandleRow.topic_id
                        )
                    )
                )
            row = await session.scalar(
                statement.values(
                    turn=turn,
                    current_run_id=current_run_id,
                    status=status,
                    revision=AgentHandleRow.revision + 1,
                    updated_at=utc_now(),
                    owner_id=None if release else owner_id,
                    lease_expires_at=None if release else now + self.lease_seconds,
                ).returning(AgentHandleRow)
            )
            if row is None:
                raise ServiceError(
                    "agent_lease_lost", "Agent 所有权、版本、租约或运行归属已变化", 409
                )
            return self._stored(row)

    async def delete(self, agent_id: str, owner_id: str, revision: int) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            identifier = await session.scalar(
                delete(AgentHandleRow)
                .where(
                    AgentHandleRow.agent_id == agent_id,
                    AgentHandleRow.owner_id == owner_id,
                    AgentHandleRow.revision == revision,
                    AgentHandleRow.lease_expires_at > time.time(),
                )
                .returning(AgentHandleRow.agent_id)
            )
            if identifier is None:
                raise ServiceError("agent_lease_lost", "Agent 所有权、版本或租约已变化", 409)
