"""目录树冻结：原来源、根父身份、SQL原子性、并发与冷读取。"""

import asyncio
import shutil
import sys
from dataclasses import replace

import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import event, func, select, text, update
from sqlalchemy.exc import IntegrityError
from test_command_directory_dependency_source import counts, prepare

from krim_agent.persistence.database import Database, WorkspaceTreePlanRow, WorkspaceTreeStepRow
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace_directory import ArtifactDirectorySpec, WorkspaceDirectoryWriter
from krim_agent.workspace_tree import TreePlanRequest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实隔离来源与根目标准备",
)


async def tree_setup(runtime, root, *, code=None, root_path="tree"):
    service, agent, result, source_store = await prepare(runtime, root, code=code)
    binding = service._handle(agent.agent_id).definition.workspace
    assert binding is not None
    scope = "session:" + agent.session_id
    source = await source_store.plan(scope, source_store_request(result.command_id))
    selected = next(item for item in source.plan.steps if item.path == root_path)
    prepared = WorkspaceDirectoryWriter(binding.require()).prepare_artifact(
        ArtifactDirectorySpec(
            path=root_path,
            operation="create",
            mode=selected.mode,
            command_id=result.command_id,
            source_hash=source.source_hash,
        )
    )
    store = WorkspaceTreePlanStore(service.workspace_edits, service.manager.directory_changes.store)
    request = TreePlanRequest(
        command_id=result.command_id,
        source_hash=source.source_hash,
        root_path=root_path,
        request_key="tree-plan",
    )
    return service, agent, scope, binding, prepared, store, request


def source_store_request(command_id):
    from krim_agent.execution.directory_publication import CommandDirectoryDependencyRequest

    return CommandDirectoryDependencyRequest(command_id=command_id)


async def tree_count(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (WorkspaceTreePlanRow, WorkspaceTreeStepRow)
        ]


async def test_reserve_root_source_steps_atomic_same_key_and_cancel_cold_facts(runtime, tmp_path):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        first, created = await store.reserve(scope, binding, request, prepared)
        assert created and first.status == "planned" and first.step_count == 3
        assert first.command_id == request.command_id and first.source_hash == request.source_hash
        assert first.root_path == "tree" and first.done_count == 0
        page = await store.steps(scope, first.plan_id, limit=2)
        assert [item.path for item in page.items] == ["tree", "tree/sub"]
        assert [item.parent_position for item in page.items] == [None, 0]
        assert page.next_after == 1
        tail = await store.steps(scope, first.plan_id, after=page.next_after, limit=2)
        assert tail.items[0].parent_position == 1 and tail.items[0].path == "tree/sub/data.bin"
        assert tail.items[0].byte_size == 2 and tail.next_after is None
        again, created = await store.reserve(scope, binding, request, None)
        assert not created and again == first
        assert await tree_count(store.database) == [1, 3]
        assert await counts(store.database) == [0] * 4 and list(tmp_path.iterdir()) == []
        assert (
            "root_prepared" not in first.model_dump_json()
            and "token" not in first.model_dump_json()
        )
        cancelled = await store.cancel(scope, first.plan_id)
        assert cancelled.status == "cancelled"
        assert all(
            item.status == "skipped" for item in (await store.steps(scope, first.plan_id)).items
        )
        assert (await store.reserve(scope, binding, request, None))[0] == cancelled
        assert (
            await tree_count(store.database) == [1, 3] and await counts(store.database) == [0] * 4
        )
    finally:
        await service.close()


@pytest.mark.parametrize("case", ["source", "root", "resource", "mode", "scope", "capacity"])
async def test_bad_root_source_binding_and_capacity_roll_back_all_rows(runtime, tmp_path, case):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        expected = {
            "source": "workspace_tree_source_conflict",
            "root": "workspace_tree_root_mismatch",
            "resource": "workspace_binding_changed",
            "mode": "workspace_tree_root_mismatch",
            "scope": "execution_not_found",
            "capacity": "workspace_tree_step_limit",
        }[case]
        if case == "source":
            request = request.model_copy(update={"source_hash": "0" * 64})
        elif case == "root":
            request = request.model_copy(update={"root_path": "tree/sub"})
        elif case == "resource":
            prepared = replace(prepared, workspace_fingerprint="0" * 64)
        elif case == "mode":
            spec = prepared.spec.model_copy(update={"mode": 0o700})
            prepared = replace(prepared, spec_json=spec.model_dump_json())
        elif case == "scope":
            scope = "topic:" + (await runtime.repository.create_topic("foreign")).id
        else:
            request = request.model_copy(update={"max_entries": 2})
        with pytest.raises(ServiceError) as refused:
            await store.reserve(scope, binding, request, prepared)
        assert refused.value.code == expected
        assert (
            await tree_count(store.database) == [0, 0] and await counts(store.database) == [0] * 4
        )
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_nested_root_freezes_existing_anchor_and_only_selected_descendants(runtime, tmp_path):
    (tmp_path / "anchor").mkdir()
    service, agent, scope, binding, prepared, store, request = await tree_setup(
        runtime,
        tmp_path,
        root_path="anchor/tree",
        code="import os;os.makedirs('anchor/tree/sub');"
        "open('anchor/tree/sub/file.bin','wb').write(b'\\0')",
    )
    try:
        accepted, created = await store.reserve(scope, binding, request, prepared)
        assert created and accepted.step_count == 3
        assert [item.path for item in (await store.steps(scope, accepted.plan_id)).items] == [
            "anchor/tree",
            "anchor/tree/sub",
            "anchor/tree/sub/file.bin",
        ]
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceTreePlanRow, accepted.plan_id)
            assert row.frozen["anchor_identity"] == list(prepared.parent_identity)
            assert row.frozen["anchor_path"] == "anchor"
        assert (
            list((tmp_path / "anchor").iterdir()) == [] and await counts(store.database) == [0] * 4
        )
    finally:
        await service.close()


async def test_same_key_changed_request_two_connections_only_one_freeze(runtime, tmp_path):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    second_db = Database(str(store.database.engine.url))
    second_files = WorkspaceEditStore(second_db, owner_id=store.owner_id)
    second = WorkspaceTreePlanStore(
        second_files, WorkspaceDirectoryStore(second_db, owner_id=store.owner_id)
    )
    try:
        values = await asyncio.gather(
            *(item.reserve(scope, binding, request, prepared) for item in (store, second))
        )
        assert sum(created for _, created in values) == 1
        assert len({value.plan_id for value, _ in values}) == 1
        with pytest.raises(ServiceError) as conflict:
            await store.reserve(
                scope, binding, request.model_copy(update={"accept_failed": True}), prepared
            )
        assert conflict.value.code == "workspace_tree_idempotency_conflict"
        assert await tree_count(store.database) == [1, 3]
    finally:
        await second_db.close()
        await service.close()


async def test_step_insert_error_rolls_back_parent_and_every_step(runtime, tmp_path):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)

    def fail(_conn, _cursor, statement, _params, _context, _many):
        if statement.startswith("INSERT INTO workspace_tree_steps"):
            raise RuntimeError("test step insert failed")

    event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="test step insert"):
            await store.reserve(scope, binding, request, prepared)
        assert await tree_count(store.database) == [0, 0] and list(tmp_path.iterdir()) == []
    finally:
        event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
        await service.close()


@pytest.mark.parametrize("case", ["checksum", "step", "dependency"])
async def test_cold_read_rejects_corrupt_snapshot_or_step_links(runtime, tmp_path, case):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        plan, _ = await store.reserve(scope, binding, request, prepared)
        async with store.database.sessions.begin() as sql:
            if case == "checksum":
                await sql.execute(
                    update(WorkspaceTreePlanRow)
                    .where(WorkspaceTreePlanRow.plan_id == plan.plan_id)
                    .values(snapshot_revision="0" * 64)
                )
            else:
                await sql.execute(
                    update(WorkspaceTreeStepRow)
                    .where(
                        WorkspaceTreeStepRow.plan_id == plan.plan_id,
                        WorkspaceTreeStepRow.position == 2,
                    )
                    .values(**({"path": "other.bin"} if case == "step" else {"parent_position": 0}))
                )
        with pytest.raises(ServiceError) as corrupt:
            await store.steps(scope, plan.plan_id)
        assert corrupt.value.code == "workspace_tree_corrupt"
    finally:
        await service.close()


async def test_nonempty_tree_downgrade_refuses_without_losing_old_foreign_keys(runtime, tmp_path):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        plan, _ = await store.reserve(scope, binding, request, prepared)
        with pytest.raises(RuntimeError, match="目录树计划非空"):
            async with store.database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0040_mixed_batch_model_sources"
                    )
                )
        assert (await store.get(scope, plan.plan_id)).plan_id == plan.plan_id
        async with store.database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0053_directory_captures"
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
    finally:
        await service.close()


@pytest.mark.parametrize("field", ["owner_id", "root", "mode", "run_id", "token", "manifest"])
def test_tree_request_rejects_internal_authority_or_content_fields(field):
    with pytest.raises(ValidationError):
        TreePlanRequest.model_validate(
            {
                "command_id": "id",
                "source_hash": "a" * 64,
                "root_path": "tree",
                "request_key": "key",
                field: "forged",
            }
        )


@pytest.mark.parametrize("case", ["closed", "readonly", "execution", "metadata"])
async def test_freeze_refuses_closed_readonly_changed_execution_and_metadata_limit(
    runtime, tmp_path, case
):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        expected = {
            "closed": "agent_session_closed",
            "readonly": "workspace_read_only",
            "execution": "execution_binding_changed",
            "metadata": "workspace_tree_limit",
        }[case]
        if case == "closed":
            await agent.close()
        elif case == "readonly":
            monkey = pytest.MonkeyPatch()
            monkey.setattr(binding.workspace, "_writable", False)
        elif case == "execution":
            from krim_agent.persistence.database import AgentSessionRow

            async with store.database.sessions.begin() as sql:
                row = await sql.get(AgentSessionRow, agent.session_id)
                row.permissions = {**row.permissions, "execution": {"key": "other"}}
        else:
            store.max_metadata_bytes = 1024
        try:
            with pytest.raises(ServiceError) as refused:
                await store.reserve(scope, binding, request, prepared)
            assert refused.value.code == expected
        finally:
            if case == "readonly":
                monkey.undo()
        assert await tree_count(store.database) == [0, 0] and list(tmp_path.iterdir()) == []
    finally:
        await service.close()


async def test_existing_plan_is_cold_readable_at_history_limit_and_foreign_scope_is_hidden(
    runtime, tmp_path
):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        store.max_plans = 1
        plan, _ = await store.reserve(scope, binding, request, prepared)
        assert (await store.reserve(scope, binding, request, None))[0] == plan
        with pytest.raises(ServiceError) as full:
            await store.reserve(
                scope, binding, request.model_copy(update={"request_key": "other"}), prepared
            )
        assert full.value.code == "workspace_tree_limit"
        foreign = "topic:" + (await runtime.repository.create_topic("foreign")).id
        assert await store.by_key(foreign, request.request_key) is None
        with pytest.raises(ServiceError) as hidden:
            await store.get(foreign, plan.plan_id)
        assert hidden.value.code == "workspace_tree_not_found"
        for kwargs in ({"limit": 0}, {"limit": 33}, {"limit": True}, {"after": -1}, {"after": 3}):
            with pytest.raises(ServiceError) as invalid:
                await store.steps(scope, plan.plan_id, **kwargs)
            assert invalid.value.code == "workspace_tree_page"
    finally:
        await service.close()


@pytest.mark.parametrize("field", ["parent_position", "byte_size"])
async def test_sql_null_cannot_bypass_child_dependency_or_file_metadata(runtime, tmp_path, field):
    service, agent, scope, binding, prepared, store, request = await tree_setup(runtime, tmp_path)
    try:
        plan, _ = await store.reserve(scope, binding, request, prepared)
        with pytest.raises(IntegrityError):
            async with store.database.sessions.begin() as sql:
                await sql.execute(
                    update(WorkspaceTreeStepRow)
                    .where(
                        WorkspaceTreeStepRow.plan_id == plan.plan_id,
                        WorkspaceTreeStepRow.position == 2,
                    )
                    .values(**{field: None})
                )
        assert (await store.steps(scope, plan.plan_id)).items[-1].parent_position == 1
    finally:
        await service.close()


async def test_empty_0041_roundtrip_preserves_0040_host_and_model_tables(runtime):
    database = runtime.repository.database
    async with database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync: command.downgrade(migration_config(sync), "0040_mixed_batch_model_sources")
        )
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0040_mixed_batch_model_sources"
        assert (
            await connection.execute(
                text("SELECT count(*) FROM mixed_workspace_batch_model_sources")
            )
        ).scalar_one() == 0
        await connection.run_sync(lambda sync: command.upgrade(migration_config(sync), "head"))
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0053_directory_captures"
        assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
