"""父子会话关闭时的稳定拒绝原因；不通过放宽权限或吞掉终态错误验证。"""

import asyncio

import pytest

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.persistence.agent_sessions import assert_session_open
from krim_agent.persistence.database import AgentSessionRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider, ModelText
from krim_agent.tools import ToolRegistry


class WaitingProvider:
    name = "session-denial-wait"

    def __init__(self) -> None:
        self.entered, self.closed = asyncio.Event(), asyncio.Event()

    async def stream(self, _messages, _tools):
        self.entered.set()
        try:
            yield ModelText(text="partial")
            await asyncio.Event().wait()
        finally:
            self.closed.set()


@pytest.mark.parametrize(
    "own_status,parent_status,expected",
    [
        ("open", "closed", "agent_parent_session_closed"),
        ("closed", "closed", "agent_parent_session_closed"),
        ("closed", "expired", "agent_parent_session_closed"),
        ("closed", "open", "agent_session_closed"),
        ("expired", "closed", "agent_session_expired"),
        ("destroyed", "closed", "agent_session_destroyed"),
    ],
)
async def test_saved_parent_and_child_states_use_parent_boundary(
    repository, own_status, parent_status, expected
):
    manager = AgentManager(repository)
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
        async with repository.database.write_lock, repository.database.sessions.begin() as session:
            own = await session.get(AgentSessionRow, child.session_id)
            ancestor = await session.get(AgentSessionRow, parent.session_id)
            own.status, ancestor.status = own_status, parent_status
            await session.flush()
            with pytest.raises(ServiceError) as error:
                await assert_session_open(session, own)
            assert error.value.code == expected
    finally:
        await manager.close()


async def test_actual_parent_cascade_keeps_parent_denial_and_reaps_child(repository):
    provider = WaitingProvider()
    manager = AgentManager(repository)
    try:
        parent = await manager.spawn(
            AgentDefinition(name="parent", provider=FakeProvider(delay=0), tools=ToolRegistry())
        )
        child = await manager.spawn(
            AgentDefinition(
                name="child",
                provider=provider,
                tools=ToolRegistry(),
                parent_agent_id=parent.agent_id,
            )
        )
        task = asyncio.create_task(child.execute("wait"))
        await asyncio.wait_for(provider.entered.wait(), 3)
        await parent.close_session((await parent.session()).revision)
        result = await asyncio.wait_for(task, 3)
        assert result.status == "aborted" and provider.closed.is_set()
        assert (await child.session()).status == "closed"
        async with repository.database.write_lock, repository.database.sessions() as session:
            row = await session.get(AgentSessionRow, child.session_id)
            with pytest.raises(ServiceError) as error:
                await assert_session_open(session, row)
            assert error.value.code == "agent_parent_session_closed"
    finally:
        await manager.close()
