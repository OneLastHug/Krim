"""文件锁内发布资格和动作来源的持久性；保留旧版本真实转移历史。"""

import time

import pytest
from alembic import command
from sqlalchemy import inspect, text, update

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import (
    AgentSessionRow,
    RunRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
)
from krim_agent.persistence.migration import migration_config
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeReceipt, FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_writer import WorkspaceWriter


@pytest.fixture
async def ledger(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'publication.sqlite'}")
    await database.initialize()
    topic = await KrimRepository(database).create_topic("发布校验")
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("old\n", encoding="utf-8")
    workspace = Workspace(root, writable=True)
    store = WorkspaceEditStore(database)
    try:
        yield database, store, workspace, "topic:" + topic.id
    finally:
        workspace.close()
        await database.close()


async def propose(store, workspace, scope):
    prepared = WorkspaceWriter(workspace).prepare(
        FileChangeSpec(
            path="file.txt",
            operation="write",
            expected_sha256=workspace.read("file.txt").sha256,
            content="new\n",
        )
    )
    change, _ = await store.preview(scope, "files", workspace.fingerprint, "preview", prepared)
    return prepared, change


async def reserve(store, workspace, scope, change, prepared, action="apply", run_id=None):
    current = await store.get(scope, change.view.proposal_id)
    attempt, _ = await store.reserve(
        scope,
        WorkspaceChangeAction(
            proposal_id=current.view.proposal_id,
            action=action,
            expected_revision=current.view.revision,
            request_key=f"{action}-{current.view.revision}",
        ),
        workspace.fingerprint,
        prepared,
        run_id=run_id,
    )
    return attempt


def receipt(prepared, status="done"):
    return FileChangeReceipt(
        path=prepared.spec.path,
        status=status,
        before_sha256=prepared.before_sha256,
        after_sha256=prepared.after_sha256,
        changed=False,
        error_code="workspace_version_changed" if status == "rejected" else None,
    )


@pytest.mark.parametrize(
    ("action", "kept", "source", "expected"),
    [
        ("apply", False, "proposed", "rejected"),
        ("keep", False, "applied", "applied"),
        ("undo", False, "applied", "applied"),
        ("undo", True, "kept", "kept"),
    ],
)
async def test_rejected_action_retains_durable_source_after_reopen(
    ledger, action, kept, source, expected
):
    database, store, workspace, scope = ledger
    prepared, change = await propose(store, workspace, scope)
    writer = WorkspaceWriter(workspace)
    if action != "apply":
        applied = await reserve(store, workspace, scope, change, prepared)
        effect = writer.apply(prepared)
        await store.settle(scope, applied.view.operation_id, effect.receipt, effect.recovery)
    if kept:
        keep = await reserve(store, workspace, scope, change, prepared, "keep")
        await store.settle(scope, keep.view.operation_id, receipt(prepared))
    executing = prepared
    if action == "undo":
        executing = writer.prepare(
            FileChangeSpec(
                path="file.txt",
                operation="write",
                expected_sha256=prepared.after_sha256,
                content="old\n",
            )
        )
    attempted = await reserve(store, workspace, scope, change, executing, action)
    async with database.sessions() as session:
        row = await session.get(WorkspaceEditAttemptRow, attempted.view.operation_id)
        assert row.previous_status == source
    cold = Database(str(database.engine.url))
    await cold.initialize()
    try:
        reopened = WorkspaceEditStore(cold, owner_id=store.owner_id)
        await reopened.settle(scope, attempted.view.operation_id, receipt(executing, "rejected"))
        current = await reopened.get(scope, change.view.proposal_id)
        assert current.view.status == expected
        assert current.view.active_operation_id is None
        assert (
            "previous_status"
            not in (await reopened.operation(scope, attempted.view.operation_id)).view.model_dump()
        )
        assert (workspace.root / "file.txt").read_text() == (
            "old\n" if action == "apply" else "new\n"
        )
    finally:
        await cold.close()


async def test_uncertain_kept_undo_resolves_to_kept_after_lease_expiry(ledger):
    database, store, workspace, scope = ledger
    prepared, change = await propose(store, workspace, scope)
    applied = await reserve(store, workspace, scope, change, prepared)
    effect = WorkspaceWriter(workspace).apply(prepared)
    await store.settle(scope, applied.view.operation_id, effect.receipt, effect.recovery)
    keep = await reserve(store, workspace, scope, change, prepared, "keep")
    await store.settle(scope, keep.view.operation_id, receipt(prepared))
    undo_plan = WorkspaceWriter(workspace).prepare(
        FileChangeSpec(
            path="file.txt",
            operation="write",
            expected_sha256=prepared.after_sha256,
            content="old\n",
        )
    )
    undo = await reserve(store, workspace, scope, change, undo_plan, "undo")
    async with database.sessions.begin() as session:
        await session.execute(
            update(WorkspaceEditAttemptRow)
            .where(WorkspaceEditAttemptRow.operation_id == undo.view.operation_id)
            .values(lease_expires_at=0)
        )
    assert await store.recover_interrupted() == 1
    await store.settle(scope, undo.view.operation_id, receipt(undo_plan, "rejected"), resolve=True)
    assert (await store.get(scope, change.view.proposal_id)).view.status == "kept"


@pytest.mark.parametrize(
    "failure", ["foreign_owner", "expired", "uncertain", "done", "pointer", "scope", "missing"]
)
async def test_execution_check_rejects_stale_or_foreign_operation(ledger, failure):
    database, store, workspace, scope = ledger
    prepared, change = await propose(store, workspace, scope)
    attempt = await reserve(store, workspace, scope, change, prepared)
    operation_id = attempt.view.operation_id
    await store.assert_execution(scope, operation_id)
    async with database.sessions() as session:
        before_lease = (await session.get(WorkspaceEditAttemptRow, operation_id)).lease_expires_at
    await store.assert_execution(scope, operation_id)
    async with database.sessions() as session:
        assert (
            await session.get(WorkspaceEditAttemptRow, operation_id)
        ).lease_expires_at == before_lease
    checker = store
    if failure == "foreign_owner":
        checker = WorkspaceEditStore(database)
    elif failure in {"expired", "uncertain", "done", "pointer"}:
        async with database.sessions.begin() as session:
            row = await session.get(WorkspaceEditAttemptRow, operation_id)
            if failure == "expired":
                row.lease_expires_at = time.time() - 1
            elif failure == "pointer":
                proposal = await session.get(WorkspaceEditProposalRow, change.view.proposal_id)
                proposal.active_operation_id = None
            else:
                row.status = failure
    elif failure == "scope":
        topic = await KrimRepository(database).create_topic("其他作用域")
        scope = "topic:" + topic.id
    else:
        operation_id = "missing"
    with pytest.raises(ServiceError) as rejected:
        await checker.assert_execution(scope, operation_id)
    assert rejected.value.code == {
        "pointer": "workspace_operation_conflict",
        "scope": "workspace_operation_not_found",
        "missing": "workspace_operation_not_found",
    }.get(failure, "workspace_operation_owner")


@pytest.mark.parametrize("failure", ["completed", "stopped", "wrong_topic"])
async def test_execution_check_revalidates_original_run(ledger, failure):
    database, store, workspace, scope = ledger
    prepared, change = await propose(store, workspace, scope)
    repository = KrimRepository(database)
    run, _ = await repository.begin_run(
        scope.removeprefix("topic:"), ChatRequest(text="发布", idempotency_key="run")
    )
    attempt = await reserve(store, workspace, scope, change, prepared, run_id=run.id)
    await store.assert_execution(scope, attempt.view.operation_id)
    other = await repository.create_topic("另一个话题")
    async with database.sessions.begin() as session:
        row = await session.get(RunRow, run.id)
        if failure == "completed":
            row.status = "completed"
        elif failure == "stopped":
            row.stop_requested = True
        else:
            row.topic_id = other.id
    with pytest.raises(ServiceError) as rejected:
        await store.assert_execution(scope, attempt.view.operation_id)
    assert rejected.value.code == (
        "workspace_run_mismatch" if failure == "wrong_topic" else "workspace_run_closed"
    )


@pytest.mark.parametrize("failure", ["closed", "expired", "foreign_owner"])
async def test_execution_check_revalidates_agent_session(runtime, tmp_path, failure):
    root = tmp_path / "agent-files"
    root.mkdir()
    (root / "file.txt").write_text("old\n", encoding="utf-8")
    service = AgentService(runtime, runtime.settings)
    agent = None
    try:
        binding = service.register_workspace("files", root, version="v1", writable=True)
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="writer",
                    description="发布资格",
                    workspace_key="files",
                    tool_names=["workspace_read"],
                ),
            )
        )
        agent = await service.spawn("writer")
        scope = "session:" + agent.session_id
        store = WorkspaceEditStore(runtime.repository.database, owner_id=service.manager.owner_id)
        prepared = WorkspaceWriter(binding.workspace).prepare(
            FileChangeSpec(
                path="file.txt",
                operation="write",
                expected_sha256=binding.workspace.read("file.txt").sha256,
                content="new\n",
            )
        )
        change, _ = await store.preview(scope, "files", binding.fingerprint, "preview", prepared)
        attempt, _ = await store.reserve(
            scope,
            WorkspaceChangeAction(
                proposal_id=change.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="apply",
            ),
            binding.fingerprint,
            prepared,
        )
        await store.assert_execution(scope, attempt.view.operation_id)
        async with runtime.repository.database.sessions.begin() as session:
            row = await session.get(AgentSessionRow, agent.session_id)
            if failure == "closed":
                row.status = "closed"
            elif failure == "expired":
                row.expires_at = time.time() - 1
            else:
                row.owner_id = "other-process"
        with pytest.raises(ServiceError) as rejected:
            await store.assert_execution(scope, attempt.view.operation_id)
        assert (
            rejected.value.code
            == {
                "closed": "agent_session_closed",
                "expired": "agent_session_expired",
                "foreign_owner": "agent_session_owner",
            }[failure]
        )
    finally:
        if agent is not None and failure == "foreign_owner":
            # 恢复故障注入的owner，允许测试夹具正常释放宿主资源。
            async with runtime.repository.database.sessions.begin() as session:
                row = await session.get(AgentSessionRow, agent.session_id)
                row.owner_id = service.manager.owner_id
        await service.close()


async def seed_legacy(connection, proposal_id, attempts, proposal_status):
    await connection.execute(
        text(
            "INSERT INTO workspace_edit_proposals "
            "(proposal_id,scope_id,topic_id,request_key,request_hash,resource_fingerprint,workspace_key,"
            "path,prepared,preview,status,revision,expires_at,active_operation_id,"
            "created_at,updated_at) "
            "VALUES (:id,'topic:topic','topic',:id,'hash','fingerprint','files',"
            "'file.txt','{}','{}',"
            ":status,:revision,9999999999,:active,'2026-10-08','2026-10-08')"
        ),
        {
            "id": proposal_id,
            "status": proposal_status,
            "revision": len(attempts) * 2,
            "active": f"{proposal_id}-{len(attempts) - 1}"
            if proposal_status in {"undoing", "uncertain"}
            else None,
        },
    )
    # 时间特意逆序：迁移必须采用动作revision而非created_at来恢复来源。
    for index, (action, status) in reversed(list(enumerate(attempts))):
        await connection.execute(
            text(
                "INSERT INTO workspace_edit_attempts "
                "(operation_id,proposal_id,scope_id,request_key,request_hash,action,expected_revision,status,"
                "prepared,receipt,cleanup_pending,receipt_history,created_at,updated_at) "
                "VALUES (:id,:proposal,'topic:topic',:id,'hash',:action,:revision,:status,"
                "'{}',:receipt,0,'[]',:created,'2026-10-08')"
            ),
            {
                "id": f"{proposal_id}-{index}",
                "proposal": proposal_id,
                "action": action,
                "revision": index * 2,
                "status": status,
                "created": f"2026-10-0{8 - index}",
                "receipt": '{"legacy":true}',
            },
        )
    if proposal_status in {"undoing", "uncertain"}:
        await connection.execute(
            text("INSERT INTO workspace_file_locks VALUES (:key,:operation)"),
            {"key": proposal_id, "operation": f"{proposal_id}-{len(attempts) - 1}"},
        )


async def test_0024_upgrade_replays_old_transitions_without_rewriting_terminal_history(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'legacy.sqlite'}")
    cases = {
        "rejected-apply": ([("apply", "rejected")], "rejected", ["proposed"]),
        "running-undo": (
            [("apply", "done"), ("keep", "done"), ("undo", "running")],
            "undoing",
            ["proposed", "applied", "kept"],
        ),
        "uncertain-undo": (
            [("apply", "done"), ("keep", "done"), ("undo", "uncertain")],
            "uncertain",
            ["proposed", "applied", "kept"],
        ),
        "legacy-rejected-undo": (
            [("apply", "done"), ("keep", "done"), ("undo", "rejected")],
            "applied",
            ["proposed", "applied", "kept"],
        ),
        "retry-after-old-rejection": (
            [("apply", "done"), ("keep", "done"), ("undo", "rejected"), ("undo", "running")],
            "undoing",
            ["proposed", "applied", "kept", "applied"],
        ),
    }
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.upgrade(migration_config(sync), "0024_workspace_edits")
            )
            await connection.execute(
                text(
                    "INSERT INTO topics (id,title,created_at,updated_at) "
                    "VALUES ('topic','迁移','2026-10-08','2026-10-08')"
                )
            )
            for proposal_id, (attempts, status, _sources) in cases.items():
                await seed_legacy(connection, proposal_id, attempts, status)
            proposals_before = (
                (
                    await connection.execute(
                        text("SELECT * FROM workspace_edit_proposals ORDER BY proposal_id")
                    )
                )
                .mappings()
                .all()
            )
            attempts_before = (
                (
                    await connection.execute(
                        text("SELECT * FROM workspace_edit_attempts ORDER BY operation_id")
                    )
                )
                .mappings()
                .all()
            )
        await database.initialize()
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(
                    text("SELECT * FROM workspace_edit_proposals ORDER BY proposal_id")
                )
            ).mappings().all() == proposals_before
            attempts_after = (
                (
                    await connection.execute(
                        text("SELECT * FROM workspace_edit_attempts ORDER BY operation_id")
                    )
                )
                .mappings()
                .all()
            )
            assert [
                {
                    key: value
                    for key, value in row.items()
                    if key not in {"previous_status", "keep_witness"}
                }
                for row in attempts_after
            ] == attempts_before
            for proposal_id, (_attempts, _status, sources) in cases.items():
                assert (
                    list(
                        (
                            await connection.execute(
                                text(
                                    "SELECT previous_status FROM workspace_edit_attempts "
                                    "WHERE proposal_id=:id ORDER BY expected_revision"
                                ),
                                {"id": proposal_id},
                            )
                        ).scalars()
                    )
                    == sources
                )
            assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
        with pytest.raises(RuntimeError, match="来源状态"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0024_workspace_edits")
                )
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0043_tree_move_ledger"
    finally:
        await database.close()


async def test_empty_attempt_ledger_roundtrips_without_losing_proposals(ledger):
    database, store, workspace, scope = ledger
    _, change = await propose(store, workspace, scope)
    async with database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync: command.downgrade(migration_config(sync), "0024_workspace_edits")
        )
    await database.initialize()
    assert (await store.get(scope, change.view.proposal_id)).view.status == "proposed"


async def test_invalid_legacy_history_fails_before_schema_mutation(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'invalid.sqlite'}")
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.upgrade(migration_config(sync), "0024_workspace_edits")
            )
            await connection.execute(
                text(
                    "INSERT INTO topics (id,title,created_at,updated_at) "
                    "VALUES ('topic','迁移','2026-10-08','2026-10-08')"
                )
            )
            await seed_legacy(connection, "invalid", [("undo", "running")], "undoing")
        with pytest.raises(RuntimeError, match="来源状态"):
            await database.initialize()
        async with database.engine.connect() as connection:
            columns = await connection.run_sync(
                lambda sync: inspect(sync).get_columns("workspace_edit_attempts")
            )
            assert "previous_status" not in {column["name"] for column in columns}
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0024_workspace_edits"
    finally:
        await database.close()
