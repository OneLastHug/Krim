"""当前目录一次SQL快照；展示不能替代实际执行的最新资格。"""

import pytest
from sqlalchemy import delete, event

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence.agent_profiles import AgentProfileStore, AgentProfileVersionRow
from krim_agent.protocol.domain import ServiceError


@pytest.mark.parametrize("count", [1, 48])
async def test_catalog_current_profiles_use_one_read_independent_of_directory_size(runtime, count):
    service = AgentService(runtime, runtime.settings)
    database = runtime.repository.database
    for index in range(count):
        await service.profiles.put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name=f"snapshot_{index:02d}", description="目录快照", tool_names=["add"]
                ),
            )
        )
    reads = []

    def observe(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            reads.append(statement)

    event.listen(database.engine.sync_engine, "before_cursor_execute", observe)
    try:
        actual = [item for item in await service.catalog_all() if item.profile_revision is not None]
        assert [item.name for item in actual] == [f"snapshot_{index:02d}" for index in range(count)]
        assert all(item.available and item.error_code is None for item in actual)
        # 限制真实数据库往返次数，不能让目录大小转成逐项多次连接。
        assert len(reads) == 1, (count, len(reads))
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", observe)
        await service.close()


async def test_profile_list_keeps_current_revision_sorted_disabled_and_historical_versions(
    repository,
):
    store = AgentProfileStore(repository.database)
    first = await store.put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(name="zeta", description="旧配置", instructions="旧指令"),
        )
    )
    alpha = await store.put(
        AgentProfileUpdate(
            expected_revision=0, config=AgentProfile(name="alpha", description="排序前项")
        )
    )
    changed = await store.put(
        AgentProfileUpdate(
            expected_revision=1,
            enabled=False,
            config=first.config.model_copy(update={"description": "停用当前版本"}),
        )
    )
    assert await store.list() == [alpha, changed]
    assert await store.get("zeta", 1) == first
    third = await store.put(
        AgentProfileUpdate(
            expected_revision=2,
            config=first.config.model_copy(update={"instructions": "新指令"}),
        )
    )
    assert await store.list() == [alpha, third]


async def test_profile_list_refuses_missing_current_version_instead_of_dropping_head(repository):
    store = AgentProfileStore(repository.database)
    await store.put(
        AgentProfileUpdate(
            expected_revision=0, config=AgentProfile(name="broken", description="损坏快照")
        )
    )
    async with repository.database.sessions.begin() as sql:
        await sql.execute(
            delete(AgentProfileVersionRow).where(AgentProfileVersionRow.name == "broken")
        )
    with pytest.raises(ServiceError) as missing:
        await store.list()
    assert missing.value.code == "agent_profile_not_found" and missing.value.status == 404


async def test_catalog_available_snapshot_cannot_authorize_historical_spawn_after_head_disabled(
    runtime,
):
    service = AgentService(runtime, runtime.settings)
    try:
        first = await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(name="gated", description="当前资格", tool_names=["add"]),
            )
        )
        shown = next(item for item in await service.catalog_all() if item.name == "gated")
        assert shown.available and shown.profile_revision == 1
        await service.profile_put(
            AgentProfileUpdate(expected_revision=1, config=first.config, enabled=False)
        )
        assert all(item.name != "gated" for item in await service.catalog_all())
        with pytest.raises(ServiceError) as denied:
            await service.spawn("gated", profile_revision=shown.profile_revision)
        assert denied.value.code == "agent_profile_disabled"
        assert not service.manager._handles
    finally:
        await service.close()
