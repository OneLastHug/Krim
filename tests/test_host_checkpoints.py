import asyncio
import json
import sys

import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import text

from krim_agent.application.checkpoints import CheckpointService
from krim_agent.checkpoint import (
    CheckpointCanRollback,
    CheckpointCheckResult,
    CheckpointCreate,
    CheckpointReceipt,
    CheckpointRollback,
)
from krim_agent.persistence import Database
from krim_agent.persistence.checkpoints import CheckpointRepository
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ServiceError


def receipt(execution, *, status="done", reference=None):
    return CheckpointReceipt(
        **execution.model_dump(exclude={"title", "metadata", "snapshot_metadata", "snapshot_ref"}),
        snapshot_ref=reference if reference is not None else execution.snapshot_ref,
        status=status,
        error_code=None if status == "done" else "resource_conflict",
    )


async def test_checkpoint_create_check_and_rollback_use_real_backup_file(repository, tmp_path):
    catalog = CheckpointRepository(repository.database)
    service = CheckpointService(catalog)
    topic = await repository.create_topic("真实宿主快照")
    resource, backup = tmp_path / "resource.txt", tmp_path / "backup.txt"
    resource.write_text("原值")
    metadata = {
        "purpose": "测试",
        "nested": {"version": 1},
        "url": "https://example.invalid/literal",
    }
    observed = []

    async def execute(execution):
        observed.append(execution)
        assert execution.metadata == (
            metadata if execution.action == "create" else {"reason": "测试回滚"}
        )
        if execution.action == "create":
            backup.write_text(resource.read_text())
            return receipt(execution, reference={"backup": "test-backup"})
        assert execution.snapshot_metadata == metadata and execution.snapshot_ref == {
            "backup": "test-backup"
        }
        resource.write_text(backup.read_text())
        return receipt(execution)

    async def can(check):
        assert check.snapshot_ref == {"backup": "test-backup"}
        assert check.snapshot_metadata == metadata
        return CheckpointCheckResult(
            topic_id=check.topic_id,
            checkpoint_id=check.checkpoint_id,
            revision=check.revision,
            can_rollback=backup.exists(),
        )

    try:
        request = CheckpointCreate(
            revision=0, title="初始快照", metadata=metadata, idempotency_key="create"
        )
        created = await service.create(topic.id, request, execute)
        assert created.status == "done" and backup.read_text() == "原值"
        current = await catalog.catalog(topic.id)
        checkpoint = current.checkpoints[0]
        assert (
            checkpoint.status == "available"
            and checkpoint.metadata == metadata
            and current.revision == 2
        )
        assert await service.create(topic.id, request, execute) == created and len(observed) == 1
        resource.write_text("修改后")
        check = CheckpointCanRollback(
            revision=2, checkpoint_id=checkpoint.id, metadata={"query": "只读"}
        )
        assert (await service.can_rollback(topic.id, check, can)).can_rollback
        rolled = await service.rollback(
            topic.id,
            CheckpointRollback(
                revision=2,
                checkpoint_id=checkpoint.id,
                metadata={"reason": "测试回滚"},
                idempotency_key="rollback",
            ),
            execute,
        )
        assert rolled.status == "done" and resource.read_text() == "原值"
        assert (await catalog.catalog(topic.id)).revision == 4
        assert (await catalog.checkpoint(topic.id, checkpoint.id)).status == "rolled_back"
        assert len(observed) == 2
    finally:
        await service.close()


@pytest.mark.parametrize("mode", ["exception", "timeout", "wrong_id", "invalid_output"])
async def test_checkpoint_unconfirmed_create_is_uncertain_and_cannot_replay(repository, mode):
    topic = await repository.create_topic(mode)
    catalog, observed = CheckpointRepository(repository.database), []
    service = CheckpointService(catalog)

    async def execute(execution):
        observed.append(execution)
        if mode == "exception":
            raise RuntimeError("private snapshot failure")
        if mode == "timeout":
            await asyncio.Event().wait()
        if mode == "invalid_output":
            return object()
        return receipt(execution, reference="test-backup").model_copy(
            update={"checkpoint_id": "wrong"}
        )

    try:
        request = CheckpointCreate(revision=0, idempotency_key="once", timeout_seconds=0.05)
        result = await service.create(topic.id, request, execute)
        assert (
            result.status == "uncertain"
            and (await catalog.catalog(topic.id)).checkpoints[0].status == "uncertain"
        )
        assert "private snapshot failure" not in result.model_dump_json()
        assert await service.create(topic.id, request, execute) == result and len(observed) == 1
        with pytest.raises(ServiceError):
            await service.create(
                topic.id, CheckpointCreate(revision=2, idempotency_key="other"), execute
            )
        resolved = await service.resolve(receipt(result.execution, reference="verified-reference"))
        assert (
            resolved.status == "done"
            and len(await catalog.history(topic.id, result.execution.operation_id)) == 2
        )
    finally:
        await service.close()


async def test_can_rollback_requires_true_typed_host_reply_and_live_reference(repository, tmp_path):
    topic = await repository.create_topic("宿主可回滚校验")
    catalog = CheckpointRepository(repository.database)
    service = CheckpointService(catalog)

    async def create(execution):
        return receipt(execution, reference={"backup": "literal"})

    try:
        created = await service.create(
            topic.id, CheckpointCreate(revision=0, idempotency_key="create"), create
        )
        request = CheckpointCanRollback(revision=2, checkpoint_id=created.execution.checkpoint_id)
        for mode in ("denied", "mismatch", "invalid"):

            async def can(check, selected=mode):
                if selected == "invalid":
                    return {"can_rollback": "true"}
                return CheckpointCheckResult(
                    topic_id=check.topic_id,
                    checkpoint_id=check.checkpoint_id if selected != "mismatch" else "wrong",
                    revision=check.revision,
                    can_rollback=selected != "denied",
                )

            assert not (await service.can_rollback(topic.id, request, can)).can_rollback
        assert (await catalog.checkpoint(topic.id, request.checkpoint_id)).status == "available"
    finally:
        await service.close()


async def test_checkpoint_interrupted_creation_recovers_without_replaying_callback(repository):
    topic = await repository.create_topic("恢复")
    catalog = CheckpointRepository(repository.database)
    prepared, created = await catalog.prepare_create(
        topic.id, CheckpointCreate(revision=0, idempotency_key="crashed")
    )
    assert created and await catalog.recover_interrupted() == 1
    assert await catalog.recover_interrupted() == 0
    recovered = await catalog.operation(topic.id, prepared.execution.operation_id)
    assert recovered.status == "uncertain" and recovered.receipt.error_code == "process_interrupted"
    assert (await catalog.catalog(topic.id)).revision == 2
    assert (
        await catalog.settle(receipt(recovered.execution, reference="verified"), resolve=True)
    ).status == "done"


async def test_host_checkpoint_migration_and_downgrade_refuse_snapshot_loss(repository):
    catalog = CheckpointRepository(repository.database)
    topic = await repository.create_topic("迁移保护")
    await catalog.prepare_create(topic.id, CheckpointCreate(revision=0, idempotency_key="create"))
    with pytest.raises(RuntimeError, match="审计"):
        async with repository.database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0013_change_lists")
            )
    async with repository.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar() == "0048_native_checkpoint_bytes"
    assert (await catalog.catalog(topic.id)).checkpoints[0].status == "creating"


def test_checkpoint_input_cannot_claim_done_and_references_are_bounded():
    with pytest.raises(ValidationError):
        CheckpointCreate(revision=0, idempotency_key="input", metadata={"value": "x" * 8192})
    with pytest.raises(ValidationError):
        CheckpointCreate.model_validate(
            {"revision": 0, "idempotency_key": "input", "status": "done"}
        )


async def test_local_sdk_checkpoint_create_can_and_rollback_use_actual_host(runtime, tmp_path):
    from krim_agent.app import LocalApp

    local = LocalApp(runtime)
    resource = tmp_path / "sdk.txt"
    resource.write_text("原值")
    snapshot = []

    async def create(execution):
        snapshot.append(resource.read_text())
        return receipt(execution, reference="local-snapshot")

    async def rollback(execution):
        assert execution.snapshot_ref == "local-snapshot"
        resource.write_text(snapshot[0])
        return receipt(execution)

    async def can(check):
        return CheckpointCheckResult(
            topic_id=check.topic_id,
            checkpoint_id=check.checkpoint_id,
            revision=check.revision,
            can_rollback=bool(snapshot),
        )

    local.handle.on_checkpoint_create(create)
    local.handle.on_checkpoint_rollback(rollback)
    local.handle.on_checkpoint_can_rollback(can)
    try:
        topic = await local.handle.create_topic("Local SDK")
        created = await local.handle.create_checkpoint(
            topic.id,
            CheckpointCreate(revision=0, metadata={"source": "local"}, idempotency_key="create"),
        )
        assert created.status == "done"
        resource.write_text("新值")
        assert (
            await local.handle.can_rollback_checkpoint(
                topic.id,
                CheckpointCanRollback(revision=2, checkpoint_id=created.execution.checkpoint_id),
            )
        ).can_rollback
        rolled = await local.handle.rollback_checkpoint(
            topic.id,
            CheckpointRollback(
                revision=2,
                checkpoint_id=created.execution.checkpoint_id,
                idempotency_key="rollback",
            ),
        )
        assert rolled.status == "done" and resource.read_text() == "原值"
    finally:
        await local.close()


async def test_remote_host_tcp_checkpoint_create_can_and_rollback(server_url, tmp_path):
    import httpx

    from krim_agent.app_sdk import RemoteHostHandle

    resource, backup = tmp_path / "remote.txt", tmp_path / "remote-backup.txt"
    resource.write_text("原值")
    async with httpx.AsyncClient(base_url=server_url, trust_env=False) as api:
        topic = (await api.post("/v1/topics", json={"title": "Remote checkpoint"})).json()["id"]
        sid = (await api.post("/v1/host/sessions")).json()["id"]
        await api.post(f"/v1/host/sessions/{sid}/ready")
        async with RemoteHostHandle(server_url, sid) as host:

            async def create(execution):
                assert execution.metadata == {"source": "remote"}
                backup.write_text(resource.read_text())
                return receipt(execution, reference="remote-backup")

            async def rollback(execution):
                assert execution.snapshot_ref == "remote-backup"
                resource.write_text(backup.read_text())
                return receipt(execution)

            async def can(check):
                return CheckpointCheckResult(
                    topic_id=check.topic_id,
                    checkpoint_id=check.checkpoint_id,
                    revision=check.revision,
                    can_rollback=backup.exists(),
                )

            host.on_checkpoint_create(create)
            host.on_checkpoint_rollback(rollback)
            host.on_checkpoint_can_rollback(can)

            async def listen():
                async for _event in host.events():
                    pass

            listener = asyncio.create_task(listen())
            try:
                from krim_agent.client import KrimClient

                def client_create():
                    with KrimClient(server_url) as client:
                        return client.create_checkpoint(
                            topic,
                            CheckpointCreate(
                                revision=0,
                                metadata={"source": "remote"},
                                idempotency_key="create",
                                host_session_id=sid,
                            ),
                        )

                created = await host.create_checkpoint(
                    topic,
                    CheckpointCreate(
                        revision=0, metadata={"source": "remote"}, idempotency_key="create"
                    ),
                )
                assert await asyncio.to_thread(client_create) == created
                assert created.status == "done"
                resource.write_text("新值")
                assert (await host.checkpoints(topic)).checkpoints[0].status == "available"

                def client_check():
                    with KrimClient(server_url) as client:
                        assert client.checkpoints(topic).checkpoints[0].status == "available"
                        assert (
                            client.checkpoint_operation(
                                topic, created.execution.operation_id
                            ).status
                            == "done"
                        )
                        return client.can_rollback_checkpoint(
                            topic,
                            CheckpointCanRollback(
                                revision=2,
                                checkpoint_id=created.execution.checkpoint_id,
                                host_session_id=sid,
                            ),
                        )

                assert (await asyncio.to_thread(client_check)).can_rollback
                assert (
                    await host.can_rollback_checkpoint(
                        topic,
                        CheckpointCanRollback(
                            revision=2, checkpoint_id=created.execution.checkpoint_id
                        ),
                    )
                ).can_rollback
                rolled = await host.rollback_checkpoint(
                    topic,
                    CheckpointRollback(
                        revision=2,
                        checkpoint_id=created.execution.checkpoint_id,
                        idempotency_key="rollback",
                    ),
                )
                assert rolled.status == "done" and resource.read_text() == "原值"
            finally:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)


async def test_checkpoint_cancel_after_real_snapshot_write_requires_resolution(
    repository, tmp_path
):
    topic = await repository.create_topic("取消快照")
    catalog = CheckpointRepository(repository.database)
    service = CheckpointService(catalog)
    resource, backup = tmp_path / "resource.txt", tmp_path / "backup.txt"
    resource.write_text("原值")
    started, stopped = asyncio.Event(), asyncio.Event()
    calls = []

    async def create(execution):
        calls.append(execution)
        backup.write_text(resource.read_text())
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()
        return receipt(execution, reference="actual-backup")

    request = CheckpointCreate(revision=0, idempotency_key="cancel")
    waiter = asyncio.create_task(service.create(topic.id, request, create))
    try:
        await asyncio.wait_for(started.wait(), 3)
        identifier = (await catalog.catalog(topic.id)).active_operation_id
        cancelled = await service.cancel(topic.id, identifier)
        await asyncio.wait_for(stopped.wait(), 3)
        await asyncio.gather(waiter, return_exceptions=True)
        assert cancelled.status == "uncertain" and backup.read_text() == "原值"
        assert await service.create(topic.id, request, create) == cancelled and len(calls) == 1
        resolved = await service.resolve(receipt(cancelled.execution, reference="actual-backup"))
        assert resolved.status == "done" and len(await catalog.history(topic.id, identifier)) == 2
    finally:
        await service.close()


async def test_can_rollback_reply_is_rejected_after_directory_revision_changes(repository):
    topic = await repository.create_topic("校验期间状态变化")
    catalog = CheckpointRepository(repository.database)
    service = CheckpointService(catalog)

    async def create(execution):
        return receipt(execution, reference="snapshot")

    created = await service.create(
        topic.id, CheckpointCreate(revision=0, idempotency_key="create"), create
    )
    entered, release = asyncio.Event(), asyncio.Event()

    async def can(check):
        entered.set()
        await release.wait()
        return CheckpointCheckResult(
            topic_id=check.topic_id,
            checkpoint_id=check.checkpoint_id,
            revision=check.revision,
            can_rollback=True,
        )

    waiter = asyncio.create_task(
        service.can_rollback(
            topic.id,
            CheckpointCanRollback(revision=2, checkpoint_id=created.execution.checkpoint_id),
            can,
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await service.create(topic.id, CheckpointCreate(revision=2, idempotency_key="new"), create)
        release.set()
        result = await waiter
        assert not result.can_rollback and result.reason == "checkpoint_changed"
    finally:
        release.set()
        await service.close()


async def test_checkpoint_receipt_store_failure_is_uncertain_after_snapshot_write(
    repository, tmp_path
):
    topic = await repository.create_topic("存储失败")
    catalog, service = CheckpointRepository(repository.database), None
    service = CheckpointService(catalog)
    actual = tmp_path / "snapshot.txt"
    settle = catalog.settle
    faulted = False

    async def fail_once(value, *, resolve=False):
        nonlocal faulted
        if not faulted:
            faulted = True
            raise RuntimeError("private sqlite fault")
        return await settle(value, resolve=resolve)

    catalog.settle = fail_once

    async def create(execution):
        actual.write_text("原值")
        return receipt(execution, reference="actual")

    try:
        result = await service.create(
            topic.id, CheckpointCreate(revision=0, idempotency_key="store"), create
        )
        assert result.status == "uncertain" and result.receipt.error_code == "receipt_store_failed"
        assert (
            actual.read_text() == "原值" and "private sqlite fault" not in result.model_dump_json()
        )
    finally:
        await service.close()


@pytest.mark.parametrize(
    "channel", ["checkpoint/create", "checkpoint/rollback", "checkpoint/canRollback"]
)
async def test_generic_checkpoint_channels_reject_unbound_snapshots(api, channel):
    sid = (await api.post("/v1/host/sessions")).json()["id"]
    await api.post(f"/v1/host/sessions/{sid}/ready")
    response = await api.post(
        f"/v1/host/sessions/{sid}/requests",
        json={
            "envelope": {
                "protocol_version": 2,
                "kind": "call",
                "id": "forged",
                "channel": channel,
                "data": {
                    "operation_id": "invented",
                    "checkpoint_id": "fake",
                    "snapshot_ref": "arbitrary",
                },
            }
        },
    )
    assert (
        response.status_code == 422
        and response.json()["error"]["code"] == "checkpoint_input_invalid"
    )


async def test_checkpoint_ids_and_operations_cannot_cross_topic_or_rewrite_resolved_receipt(
    repository,
):
    catalog = CheckpointRepository(repository.database)
    one, other = await repository.create_topic("one"), await repository.create_topic("other")
    prepared, created = await catalog.prepare_create(
        one.id, CheckpointCreate(revision=0, idempotency_key="create")
    )
    assert created
    with pytest.raises(ServiceError):
        await catalog.checkpoint(other.id, prepared.execution.checkpoint_id)
    with pytest.raises(ServiceError):
        await catalog.operation(other.id, prepared.execution.operation_id)
    result = await catalog.settle(receipt(prepared.execution, reference="snapshot"))
    assert result.status == "done"
    with pytest.raises(ServiceError):
        await catalog.settle(receipt(prepared.execution, reference="replaced-reference"))
    with pytest.raises(ServiceError):
        await catalog.prepare_create(
            one.id, CheckpointCreate(revision=2, title="different", idempotency_key="create")
        )


async def test_real_process_crash_after_backup_recovers_uncertain_with_actual_resource(tmp_path):
    path, backup = tmp_path / "crash.sqlite", tmp_path / "actual-backup.txt"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "tests/fixtures/host_checkpoint_crash_writer.py",
        str(path),
        str(backup),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, errors = await asyncio.wait_for(process.communicate(), 10)
    assert process.returncode == 0, errors.decode()
    identifiers = json.loads(output)
    assert backup.read_text() == "实际备份原值"
    database = Database(f"sqlite+aiosqlite:///{path}")
    try:
        await database.initialize()
        catalog = CheckpointRepository(database)
        assert await catalog.recover_interrupted() == 1 and await catalog.recover_interrupted() == 0
        operation = await catalog.operation(identifiers["topic_id"], identifiers["operation_id"])
        assert (
            operation.status == "uncertain"
            and operation.receipt.error_code == "process_interrupted"
        )
        assert backup.read_text() == "实际备份原值"
        assert (
            await catalog.settle(
                receipt(operation.execution, reference="actual-backup"), resolve=True
            )
        ).status == "done"
    finally:
        await database.close()


async def test_generic_checkpoint_request_respects_transport_deadline_and_retains_operation(api):
    topic = (await api.post("/v1/topics", json={"title": "传输截止时间"})).json()["id"]
    sid = (await api.post("/v1/host/sessions")).json()["id"]
    await api.post(f"/v1/host/sessions/{sid}/ready")
    command = {
        "envelope": {
            "protocol_version": 2,
            "kind": "call",
            "id": "short-deadline",
            "channel": "checkpoint/create",
            "data": {
                "topic_id": topic,
                "revision": 0,
                "idempotency_key": "deadline",
                "timeout_seconds": 300,
            },
        },
        "timeout_seconds": 0.05,
    }
    async with asyncio.timeout(2):
        response = await api.post(f"/v1/host/sessions/{sid}/requests", json=command)
    assert response.status_code == 200 and response.json()["data"]["status"] == "uncertain"
    operation = response.json()["data"]
    saved = (
        await api.get(
            f"/v1/topics/{topic}/checkpoints/operations/{operation['execution']['operation_id']}"
        )
    ).json()
    assert saved["status"] == "uncertain"
    async with asyncio.timeout(2):
        repeat = await api.post(f"/v1/host/sessions/{sid}/requests", json=command)
    assert repeat.json() == response.json()
