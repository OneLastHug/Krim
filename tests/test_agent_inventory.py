import asyncio
import json
import sys

import pytest
from sqlalchemy import update

from krim_agent.persistence.agents import AgentHandleStore, StoredAgentHandle
from krim_agent.persistence.database import AgentHandleRow
from krim_agent.protocol.domain import ServiceError


async def test_inventory_owner_revision_and_release_are_durable(repository):
    topic = await repository.create_topic("inventory")
    store = AgentHandleStore(repository.database)
    value = StoredAgentHandle(
        agent_id="agent-id",
        name="default",
        topic_id=topic.id,
        persistent=True,
        turn=0,
        current_run_id=None,
        status="created",
        definition_hash="0" * 64,
        revision=0,
        owner_id=None,
        lease_expires_at=None,
        parent_agent_id=None,
        parent_tool_call_id=None,
    )
    stored = await store.create(value, "owner-one")
    assert stored.owner_id == "owner-one" and stored.revision == 0
    with pytest.raises(ServiceError) as busy:
        await store.claim(value.agent_id, "owner-two", 0)
    assert busy.value.code == "agent_busy"
    released = await store.save(
        value.agent_id, "owner-one", 0, turn=1, current_run_id=None, status="detached", release=True
    )
    claimed = await store.claim(value.agent_id, "owner-two", released.revision)
    assert claimed.turn == 1 and claimed.owner_id == "owner-two"
    with pytest.raises(ServiceError) as stale:
        await store.save(
            value.agent_id,
            "owner-one",
            released.revision,
            turn=2,
            current_run_id=None,
            status="running",
        )
    assert stale.value.code == "agent_lease_lost"
    await store.delete(value.agent_id, "owner-two", claimed.revision)
    assert not await store.list()


async def test_expired_inventory_owner_cannot_save_delete_or_renew(repository):
    topic = await repository.create_topic("expired")
    store = AgentHandleStore(repository.database)
    value = StoredAgentHandle(
        agent_id="expired",
        name="default",
        topic_id=topic.id,
        persistent=True,
        turn=0,
        current_run_id=None,
        status="created",
        definition_hash="0" * 64,
        revision=0,
        owner_id=None,
        lease_expires_at=None,
        parent_agent_id=None,
        parent_tool_call_id=None,
    )
    await store.create(value, "old")
    async with repository.database.sessions.begin() as session:
        await session.execute(
            update(AgentHandleRow)
            .where(AgentHandleRow.agent_id == "expired")
            .values(lease_expires_at=0)
        )
    for action in [
        lambda: store.save("expired", "old", 0, turn=0, current_run_id=None, status="running"),
        lambda: store.delete("expired", "old", 0),
        lambda: store.renew("expired", "old", 0),
    ]:
        with pytest.raises(ServiceError) as lost:
            await action()
        assert lost.value.code == "agent_lease_lost"
    claimed = await store.claim("expired", "new", 0)
    assert claimed.owner_id == "new" and claimed.revision == 1


async def test_two_real_processes_restore_same_persistent_agent_and_resume_new_run(tmp_path):
    path = tmp_path / "process-agents.sqlite"

    async def worker(mode, agent_id=None):
        arguments = [sys.executable, "tests/fixtures/persistent_agent_worker.py", str(path), mode]
        if agent_id is not None:
            arguments.append(agent_id)
        process = await asyncio.create_subprocess_exec(
            *arguments, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        output, errors = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, errors.decode()
        return json.loads(output)

    first = await worker("write")
    restored = await worker("resume", first["agent_id"])
    assert first["agent_id"] == restored["agent_id"] and first["topic_id"] == restored["topic_id"]
    assert first["run_id"] != restored["run_id"] and first["turn"] == 1 and restored["turn"] == 2
    assert "first" in first["text"] and "second" in restored["text"]
