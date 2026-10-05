import pytest

from krim_agent.persistence.agents import AgentHandleStore, StoredAgentHandle
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
