"""原真实离线模型完成Missing后，可信回收保持原Run/帧/来源与turn，不重放。"""

import asyncio

import pytest
from sqlalchemy import func, select, update
from test_directory_restore_execution import bridge
from test_model_missing_directory_restores import Provider, seed_target, setup

from krim_agent.persistence.database import AgentSessionRow
from krim_agent.persistence.database import WorkspaceContainerRetentionRow as Retention
from krim_agent.persistence.database import WorkspaceMissingDirectoryRestoreModelSourceRow as Source
from krim_agent.persistence.workspace_container_retentions import WorkspaceContainerRetentionStore
from krim_agent.persistence.workspace_directory_captures import WorkspaceDirectoryCaptureStore
from krim_agent.persistence.workspace_directory_checkpoints import WorkspaceDirectoryCheckpointStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_missing_directory_restores import (
    WorkspaceMissingDirectoryRestoreStore,
)
from krim_agent.workspace_container_retention import ContainerRetentionRequest
from krim_agent.workspace_empty_container_retention import WorkspaceEmptyContainerRetention


async def test_retention_of_original_model_done_preserves_source_run_frames_and_actor_turn(
    runtime, tmp_path, monkeypatch
):
    provider = Provider()
    service, agent, binding = await setup(
        runtime, tmp_path, provider, persistent=True, round_timeout=20
    )
    try:
        provider.target_id = await seed_target(agent, tmp_path)
        finished = await agent.execute(
            "原模型正常完成Missing恢复", idempotency_key="retention-model-source"
        )
        assert finished.status == "completed" and provider.finished.phase == "finished"
        original = provider.finished
        scope = "session:" + agent.session_id
        repository = runtime.repository
        messages = (
            await repository.message_tree(original.scope_id.removeprefix("topic:"))
            if original.scope_id.startswith("topic:")
            else await repository.message_tree((await service.get(agent.agent_id)).topic_id)
        )
        events = await repository.list_events(finished.run_id)
        async with repository.database.sessions() as sql:
            sources = [
                (row.phase, row.run_id, row.call_id)
                for row in await sql.scalars(
                    select(Source).where(Source.plan_id == original.plan_id).order_by(Source.phase)
                )
            ]
        assert {row[1] for row in sources} == {finished.run_id}
        assert {row[2] for row in sources} == {"original-missing-preview", "original-missing-start"}
        calls = provider.calls

        def forbidden(*_args, **_kwargs):
            raise AssertionError("回收/原键不得重放原模型")

        monkeypatch.setattr(provider, "stream", forbidden)
        store = WorkspaceContainerRetentionStore(service.missing_directory_restore_store)
        async with repository.database.sessions() as sql:
            source = await store.sources.read(
                sql,
                scope,
                original.plan_id,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=binding.workspace.fingerprint,
            )
        assert source.metadata["execute_source"] == {
            "run_id": finished.run_id,
            "call_id": "original-missing-start",
        }
        core = WorkspaceEmptyContainerRetention(binding.workspace)
        observed = core.inspect(source.stage, authorize=lambda: None)
        request = ContainerRetentionRequest(
            source_plan_id=original.plan_id,
            expected_source_hash=source.view.source_hash,
            expected_container_fingerprint=observed.fingerprint,
            request_key="model-source-container-retention",
        )
        view, _ = await store.reserve(scope, binding, request, qualify=lambda: None)
        fence = await store.execution(scope, view.operation_id)
        loop = asyncio.get_running_loop()
        authorize = bridge(
            loop, lambda: store.assert_execution(scope, fence, binding, qualify=lambda: None)
        )
        record = bridge(
            loop,
            lambda value: store.record_intent(scope, fence, binding, value, qualify=lambda: None),
        )
        effect = await asyncio.to_thread(
            core.remove,
            source.stage,
            expected_fingerprint=observed.fingerprint,
            confirmed=True,
            authorize=authorize,
            record_intent=record,
        )
        assert effect.status == "removed"
        result = await store.settle(scope, fence, binding, effect, qualify=lambda: None)
        assert result.status == "done" and await store.by_key(scope, request.request_key) == result
        assert (
            await service.missing_directory_restore_store.get(scope, original.plan_id) == original
        )
        assert await repository.message_tree(messages.topic.id) == messages
        assert await repository.list_events(finished.run_id) == events
        async with repository.database.sessions() as sql:
            assert [
                (row.phase, row.run_id, row.call_id)
                for row in await sql.scalars(
                    select(Source).where(Source.plan_id == original.plan_id).order_by(Source.phase)
                )
            ] == sources
        assert provider.calls == calls and (await service.get(agent.agent_id)).turn == 1
        assert (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["owner", "parent-closed"])
async def test_current_actor_owner_and_parent_open_are_required_before_new_retention(
    runtime, tmp_path, fault
):
    provider = Provider()
    service, agent, binding = await setup(
        runtime, tmp_path, provider, persistent=True, round_timeout=20
    )
    try:
        provider.target_id = await seed_target(agent, tmp_path)
        final = await agent.execute(
            "完成原缺失恢复供资格验证", idempotency_key="retention-live-source"
        )
        assert final.status == "completed"
        original = provider.finished
        scope = "session:" + agent.session_id
        good = WorkspaceContainerRetentionStore(service.missing_directory_restore_store)
        async with runtime.repository.database.sessions() as sql:
            source = await good.sources.read(
                sql,
                scope,
                original.plan_id,
                workspace_key=binding.key,
                resource_fingerprint=binding.fingerprint,
                workspace_fingerprint=binding.workspace.fingerprint,
            )
        observed = WorkspaceEmptyContainerRetention(binding.workspace).inspect(
            source.stage, authorize=lambda: None
        )
        request = ContainerRetentionRequest(
            source_plan_id=original.plan_id,
            expected_source_hash=source.view.source_hash,
            expected_container_fingerprint=observed.fingerprint,
            request_key="current-owner-required",
        )
        if fault == "owner":
            edits = WorkspaceEditStore(runtime.repository.database, owner_id="different-owner")
            store = WorkspaceContainerRetentionStore(
                WorkspaceMissingDirectoryRestoreStore(
                    WorkspaceDirectoryCaptureStore(WorkspaceDirectoryCheckpointStore(edits))
                )
            )
        else:
            parent = await service.spawn("default")
            async with runtime.repository.database.sessions() as sql, sql.begin():
                await sql.execute(
                    update(AgentSessionRow)
                    .where(AgentSessionRow.session_id == agent.session_id)
                    .values(parent_session_id=parent.session_id)
                )
            await service.close_session(parent.agent_id, parent.session.revision)
            store = good
        from krim_agent.protocol.domain import ServiceError

        with pytest.raises(ServiceError):
            await store.reserve(scope, binding, request, qualify=lambda: None)
        async with runtime.repository.database.sessions() as sql:
            assert await sql.scalar(select(func.count()).select_from(Retention)) == 0
        assert (
            await service.missing_directory_restore_store.get(scope, original.plan_id) == original
        )
    finally:
        await service.close()
