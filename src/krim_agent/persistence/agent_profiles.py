"""不可改写的 Agent 版本与当前指针；更新使用 SQLite CAS。"""

from __future__ import annotations

import hashlib
import json

from pydantic import JsonValue
from sqlalchemy import JSON, Boolean, Integer, String, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Mapped, mapped_column

from krim_agent.agents.profiles import AgentProfile, AgentProfileUpdate, AgentProfileVersion
from krim_agent.protocol.domain import ServiceError

from .database import Base, Database


class AgentProfileHeadRow(Base):
    __tablename__ = "agent_profile_heads"
    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer)
    enabled: Mapped[bool] = mapped_column(Boolean)


class AgentProfileVersionRow(Base):
    __tablename__ = "agent_profile_versions"
    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    config: Mapped[dict[str, JsonValue]] = mapped_column(JSON)
    config_hash: Mapped[str] = mapped_column(String(64))
    enabled: Mapped[bool] = mapped_column(Boolean)


class AgentProfileStore:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def put(self, request: AgentProfileUpdate) -> AgentProfileVersion:
        config = AgentProfile.model_validate(request.config.model_dump())
        value = config.model_dump(mode="json")
        digest = hashlib.sha256(
            json.dumps(value, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        async with self.database.write_lock, self.database.sessions.begin() as session:
            if request.expected_revision == 0:
                created = await session.scalar(
                    insert(AgentProfileHeadRow)
                    .values(name=config.name, revision=1, enabled=request.enabled)
                    .on_conflict_do_nothing()
                    .returning(AgentProfileHeadRow.name)
                )
            else:
                created = await session.scalar(
                    update(AgentProfileHeadRow)
                    .where(
                        AgentProfileHeadRow.name == config.name,
                        AgentProfileHeadRow.revision == request.expected_revision,
                    )
                    .values(revision=request.expected_revision + 1, enabled=request.enabled)
                    .returning(AgentProfileHeadRow.name)
                )
            if created is None:
                raise ServiceError("agent_profile_conflict", "Agent配置版本已变化", 409)
            revision = request.expected_revision + 1
            session.add(
                AgentProfileVersionRow(
                    name=config.name,
                    revision=revision,
                    config=value,
                    config_hash=digest,
                    enabled=request.enabled,
                )
            )
            return AgentProfileVersion(
                name=config.name,
                revision=revision,
                enabled=request.enabled,
                config=config,
                config_hash=digest,
            )

    async def get(self, name: str, revision: int | None = None) -> AgentProfileVersion:
        async with self.database.write_lock, self.database.sessions() as session:
            if revision is None:
                head = await session.get(AgentProfileHeadRow, name)
                if head is None:
                    raise ServiceError("agent_profile_not_found", "Agent配置不存在", 404)
                revision = head.revision
            row = await session.get(AgentProfileVersionRow, (name, revision))
            if row is None:
                raise ServiceError("agent_profile_not_found", "Agent配置版本不存在", 404)
            return AgentProfileVersion(
                name=name,
                revision=revision,
                enabled=row.enabled,
                config=AgentProfile.model_validate(row.config),
                config_hash=row.config_hash,
            )

    async def list(self) -> list[AgentProfileVersion]:
        async with self.database.write_lock, self.database.sessions() as session:
            names = list(
                await session.scalars(
                    select(AgentProfileHeadRow.name).order_by(AgentProfileHeadRow.name)
                )
            )
        return [await self.get(name) for name in names]
