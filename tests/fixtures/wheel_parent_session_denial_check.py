"""项目外安装包：实际父子级联、原拒绝原因与终态优先级。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.agent_sessions import assert_session_open
from krim_agent.persistence.database import AgentSessionRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.tools import ToolRegistry


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__
    with TemporaryDirectory(prefix="krim-parent-denial-") as folder:
        database = Database(f"sqlite+aiosqlite:///{Path(folder) / 'state.sqlite'}")
        await database.initialize()
        manager = AgentManager(KrimRepository(database))
        try:
            parent = await manager.spawn(
                AgentDefinition(name="parent", provider=FakeProvider(delay=0), tools=ToolRegistry())
            )
            child = await manager.spawn(
                AgentDefinition(
                    name="child",
                    provider=FakeProvider(delay=0),
                    tools=ToolRegistry(),
                    parent_agent_id=parent.agent_id,
                )
            )
            await parent.close_session((await parent.session()).revision)
            assert (await child.session()).status == "closed"
            async with database.write_lock, database.sessions.begin() as session:
                row = await session.get(AgentSessionRow, child.session_id)
                for status, expected in (
                    ("closed", "agent_parent_session_closed"),
                    ("expired", "agent_session_expired"),
                    ("destroyed", "agent_session_destroyed"),
                ):
                    row.status = status
                    await session.flush()
                    try:
                        await assert_session_open(session, row)
                    except ServiceError as error:
                        assert error.code == expected
                    else:
                        raise AssertionError("关闭会话不应通过守卫")
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "parent_denial": True,
                        "terminal_priority": True,
                    }
                )
            )
        finally:
            await manager.close()
            await database.close()


if __name__ == "__main__":
    asyncio.run(main())
