"""Real SQLite preview persistence, credential races and current access fences."""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import delete, insert, select, update

from krim_agent.contracts import ServiceError
from krim_agent.dev_preview.repository import BrowserGrant, PreviewRecord
from krim_agent.dev_preview.service import binding
from krim_agent.development.contracts import PreviewHandle
from krim_agent.development.ports import command_digest
from krim_agent.development.preview_ports import ServiceInstance
from krim_agent.project_access.service import command_intent
from krim_agent.project_access.types import ProjectGrant
from krim_agent.security import Membership, PrincipalContext
from krim_agent.storage.account_models import accounts
from krim_agent.storage.models import runs
from krim_agent.storage.preview import SQLitePreviewStore
from krim_agent.storage.preview_models import preview_credentials, preview_records
from krim_agent.storage.project_access import SQLiteAccessRepository
from krim_agent.storage.sandbox_models import sandbox_projects
from krim_agent.storage.sqlite import SQLiteStore
from tests.coding_agent.test_development_storage import prepare


async def setup(database: Path):
    store, development, _, request = await prepare(database)
    access = SQLiteAccessRepository(store)
    async with store.engine.begin() as conn:
        await conn.execute(update(sandbox_projects).where(
            sandbox_projects.c.project_id == request.environment.project_id,
        ).values(status="ready"))
        workspace = await conn.scalar(select(sandbox_projects.c.workspace_id).where(
            sandbox_projects.c.project_id == request.environment.project_id,
        ))
        actor = await access._actor(conn, request.authorization.actor_id)
    assert actor is not None
    request = request.model_copy(update={"lifecycle": "project_service", "timeout_seconds": None})
    intent = command_intent(request, workspace_id=workspace)
    request = request.model_copy(update={"authorization": request.authorization.model_copy(
        update={"arguments_digest": intent.digest(request.authorization)},
    )})
    grant = ProjectGrant(
        owner_id=request.environment.owner_id, workspace_id=workspace,
        project_id=request.environment.project_id, actor_id=actor.principal_id,
        actor_generation=actor.generation, revision=1, actions=("shell.execute",),
        requires_approval=True, expires_at=time.time() + 600,
    )
    await access.put(grant, expected_revision=0)
    reserved = await development.processes.reserve(request, command_digest(request))
    row = await development.processes.save(reserved.record, replace(
        reserved.record, revision=2, execution_id="e" * 64,
        handle=reserved.record.handle.model_copy(update={"state": "running"}),
    ))
    db = SQLitePreviewStore(store)
    await db.authority.registration(row.handle, 3000, actor)
    record = PreviewRecord(
        handle=PreviewHandle(
            preview_id="preview-one", environment=row.handle.environment,
            service_id=row.handle.process_id, port=3000, state="pending",
            access_path="/api/development/previews/preview-one/open",
        ), process=row.handle, authorization=request.authorization,
        instance=ServiceInstance(execution_id="e" * 64,
                                 started_at="2026-11-15T00:00:00.123456789Z"),
        request_digest=row.request_digest,
    )
    await db.authority.check(record, actor, "register")
    await db.records.create(record)
    return store, development, db, record, actor, grant


@pytest.mark.parametrize("legacy", ["missing", "revoked", "expired"])
async def test_current_project_acl_authorizes_without_legacy_grant(
    tmp_path: Path, legacy: str,
) -> None:
    from krim_agent.storage.development_models import project_access_records

    store, _, db, record, actor, grant = await setup(tmp_path / "preview.sqlite")
    try:
        if legacy == "missing":
            async with store.engine.begin() as conn:
                await conn.execute(delete(project_access_records).where(
                    project_access_records.c.project_id == record.handle.environment.project_id,
                    project_access_records.c.kind == "grant",
                ))
        else:
            updated = grant.model_copy(update={"revision": 2,
                **({"revoked": True} if legacy == "revoked" else {"expires_at": time.time() - 1})})
            await db.access.put(updated, expected_revision=1)
        await db.authority.registration(record.process, record.handle.port, actor)
        await db.authority.check(record, actor, "open")
        assert (await db.authority.resolve(binding(record), actor)).handle == record.process
    finally:
        await store.close()


@pytest.mark.parametrize("access", ["none", "read"])
async def test_service_creator_must_retain_current_project_write_access(
    tmp_path: Path, access: str,
) -> None:
    from krim_agent.storage.project_membership_models import project_memberships

    store, _, db, record, actor, _ = await setup(tmp_path / "preview.sqlite")
    try:
        async with store.engine.begin() as conn:
            await conn.execute(update(project_memberships).where(
                project_memberships.c.project_id == record.handle.environment.project_id,
                project_memberships.c.principal_id == actor.principal_id,
            ).values(access=access, revision=2))
        with pytest.raises(ServiceError):
            await db.authority.registration(record.process, record.handle.port, actor)
        with pytest.raises(ServiceError):
            await db.authority.resolve(binding(record), actor)
        with pytest.raises(ServiceError):
            await db.authority.resolve(binding(record), None)
    finally:
        await store.close()


async def test_preview_reader_acl_cannot_register_or_stop_and_revokes_immediately(
    tmp_path: Path,
) -> None:
    from krim_agent.storage.account_models import access_keys, account_workspaces
    from krim_agent.storage.project_membership_models import project_memberships

    store, _, db, record, actor, _ = await setup(tmp_path / "preview.sqlite")
    workspace = actor.memberships[0].workspace_id
    reader = PrincipalContext(
        "preview-reader", 1, (Membership(workspace_id=workspace, role="member"),),
    )
    try:
        async with store.engine.begin() as conn:
            await conn.execute(insert(accounts).values(principal_id=reader.principal_id,
                display_name="Preview reader", status="active", platform_role="member",
                authorization_generation=1, revision=1, legacy=False,
                created_at=time.time(), updated_at=time.time()))
            await conn.execute(insert(access_keys).values(key_id="a" * 32,
                principal_id=reader.principal_id, token_digest="b" * 64,
                credential_version=1, masked="test-reader", weak=False, status="active",
                issued_at=time.time(), request_key="test-reader-key"))
            await conn.execute(insert(account_workspaces).values(principal_id=reader.principal_id,
                workspace_id=workspace, role="member", approve=False))
            await conn.execute(insert(project_memberships).values(
                project_id=record.handle.environment.project_id, principal_id=reader.principal_id,
                access="read", revision=1, updated_at=time.time()))
        await db.authority.check(record, reader, "open")
        assert (await db.authority.resolve(binding(record), reader)).handle == record.process
        for action in ("register", "stop"):
            with pytest.raises(ServiceError, match="permission_denied"):
                await db.authority.check(record, reader, action)
        with pytest.raises(ServiceError, match="permission_denied"):
            await db.authority.registration(record.process, record.handle.port, reader)
        async with store.engine.begin() as conn:
            await conn.execute(update(project_memberships).where(
                project_memberships.c.project_id == record.handle.environment.project_id,
                project_memberships.c.principal_id == reader.principal_id,
            ).values(access="none", revision=2))
        with pytest.raises(ServiceError):
            await db.authority.check(record, reader, "open")
    finally:
        await store.close()


async def test_preview_reopen_and_service_outlives_run(tmp_path: Path) -> None:
    path = tmp_path / "preview.sqlite"
    store, development, db, record, actor, _ = await setup(path)
    try:
        async with store.engine.begin() as conn:
            await conn.execute(update(runs).where(
                runs.c.id == record.process.ownership.run_id,
            ).values(status="completed"))
        row = await db.authority.resolve(binding(record), actor)
        assert row.handle == record.process
        assert await db.authority.resolve(binding(record), None) == row
        await store.close()
        store = await SQLiteStore.open(path)
        db = SQLitePreviewStore(store)
        restored = await db.records.get(record.handle.preview_id)
        assert restored == record
        key = hashlib.sha256(record.handle.preview_id.encode()).hexdigest()[:40]
        assert await db.records.get_by_host_key(key) == record
        assert (await db.authority.resolve(binding(record), actor)).execution_id == "e" * 64
    finally:
        await store.close()


async def test_preview_cas_epoch_and_no_revival(tmp_path: Path) -> None:
    store, _, db, record, actor, _ = await setup(tmp_path / "preview.sqlite")
    try:
        ready = replace(record, handle=record.handle.model_copy(update={"state": "ready"}),
                        revision=2)
        assert await db.records.save(record, ready) == ready
        with pytest.raises(ServiceError, match="revision_conflict"):
            await db.records.save(record, ready)
        stopped = replace(ready, handle=ready.handle.model_copy(update={"state": "stopped"}),
                          route_epoch=2, revision=3)
        with pytest.raises(ServiceError, match="revision_conflict"):
            await db.records.save(ready, replace(stopped, route_epoch=1))
        await db.records.save(ready, stopped)
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            await db.authority.resolve(binding(record), actor)
        with pytest.raises(ServiceError, match="revision_conflict"):
            await db.records.save(stopped, replace(ready, revision=4, route_epoch=2))
        with pytest.raises(ServiceError, match="key_conflict"):
            await db.records.create(record)
    finally:
        await store.close()


@pytest.mark.parametrize("change", ["port", "instance", "process", "duplicate"])
async def test_reject_preview_retarget_and_duplicate_service(tmp_path: Path, change: str) -> None:
    store, _, db, record, _, _ = await setup(tmp_path / "preview.sqlite")
    try:
        if change == "duplicate":
            second = replace(record, handle=record.handle.model_copy(update={
                "preview_id": "second", "access_path": "/api/development/previews/second/open",
            }))
            with pytest.raises(ServiceError, match="key_conflict"):
                await db.records.create(second)
            return
        ready = replace(record, handle=record.handle.model_copy(update={"state": "ready"}),
                        revision=2)
        if change == "port":
            ready = replace(ready, handle=ready.handle.model_copy(update={"port": 3001}))
        elif change == "instance":
            ready = replace(ready, instance=ServiceInstance(
                execution_id="e" * 64, started_at="2026-11-15T00:00:00.123456790Z"))
        else:
            ready = replace(ready, process=ready.process.model_copy(update={"log_ref": "other"}))
        with pytest.raises(ServiceError, match="revision_conflict"):
            await db.records.save(record, ready)
    finally:
        await store.close()


async def test_ticket_consumed_once_under_concurrency_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "preview.sqlite"
    store, _, db, record, actor, _ = await setup(path)
    digest = hashlib.sha256(b"never-persist-this-ticket").hexdigest()
    grant = BrowserGrant(record.handle.preview_id, actor, time.time() + 25)
    try:
        await db.credentials.put_ticket(digest, grant)
        with pytest.raises(ServiceError, match="unauthorized"):
            await db.credentials.consume_ticket(digest, "other-preview", time.time())
        results = await asyncio.gather(*[
            db.credentials.consume_ticket(digest, record.handle.preview_id, time.time())
            for _ in range(8)
        ], return_exceptions=True)
        assert sum(isinstance(x, BrowserGrant) for x in results) == 1
        assert sum(isinstance(x, ServiceError) for x in results) == 7
        await store.close()
        store = await SQLiteStore.open(path)
        db = SQLitePreviewStore(store)
        with pytest.raises(ServiceError, match="unauthorized"):
            await db.credentials.consume_ticket(digest, record.handle.preview_id, time.time())
        with pytest.raises(ServiceError, match="key_conflict"):
            await db.credentials.put_ticket(digest, grant)
        assert b"never-persist-this-ticket" not in path.read_bytes()
    finally:
        await store.close()


@pytest.mark.parametrize("revocation", ["project_access", "generation", "account", "epoch"])
async def test_browser_session_and_route_recheck_current_authority(
    tmp_path: Path, revocation: str,
) -> None:
    store, _, db, record, actor, grant = await setup(tmp_path / "preview.sqlite")
    digest = "a" * 64
    try:
        await db.credentials.put_session(digest, BrowserGrant(
            record.handle.preview_id, actor, time.time() + 300,
        ))
        assert (await db.credentials.get_session(digest, record.handle.preview_id,
                                                time.time())).principal == actor
        if revocation == "project_access":
            from krim_agent.storage.project_membership_models import project_memberships

            async with store.engine.begin() as conn:
                await conn.execute(update(project_memberships).where(
                    project_memberships.c.project_id == record.handle.environment.project_id,
                    project_memberships.c.principal_id == actor.principal_id,
                ).values(access="none", revision=2))
        elif revocation == "epoch":
            await db.records.save(record, replace(
                record, handle=record.handle.model_copy(update={"state": "revoked"}),
                route_epoch=2, revision=2,
            ))
        else:
            values = ({"authorization_generation": actor.generation + 1}
                      if revocation == "generation" else {"status": "disabled"})
            async with store.engine.begin() as conn:
                await conn.execute(update(accounts).where(
                    accounts.c.principal_id == actor.principal_id,
                ).values(**values))
        with pytest.raises(ServiceError):
            await db.credentials.get_session(digest, record.handle.preview_id, time.time())
        with pytest.raises(ServiceError):
            await db.authority.resolve(binding(record), actor)
    finally:
        await store.close()


async def test_forged_viewer_membership_never_authorizes(tmp_path: Path) -> None:
    store, _, db, record, actor, _ = await setup(tmp_path / "preview.sqlite")
    try:
        forged = PrincipalContext("unknown-user", actor.generation, (
            Membership(workspace_id=actor.memberships[0].workspace_id, role="admin"),
        ))
        with pytest.raises(ServiceError, match="permission_denied"):
            await db.authority.resolve(binding(record), forged)
        with pytest.raises(ServiceError, match="permission_denied"):
            await db.credentials.put_ticket("c" * 64, BrowserGrant(
                record.handle.preview_id, forged, time.time() + 25,
            ))
    finally:
        await store.close()


async def test_ticket_expiry_and_session_cannot_be_ticket(tmp_path: Path) -> None:
    store, _, db, record, actor, _ = await setup(tmp_path / "preview.sqlite")
    try:
        await db.credentials.put_session("b" * 64, BrowserGrant(
            record.handle.preview_id, actor, time.time() + 300,
        ))
        with pytest.raises(ServiceError, match="unauthorized"):
            await db.credentials.consume_ticket("b" * 64, record.handle.preview_id, time.time())
        await db.credentials.put_ticket("c" * 64, BrowserGrant(
            record.handle.preview_id, actor, time.time() + 25,
        ))
        with pytest.raises(ServiceError, match="unauthorized"):
            await db.credentials.consume_ticket(
                "c" * 64, record.handle.preview_id, time.time() + 60,
            )
        async with store.engine.connect() as conn:
            assert await conn.scalar(select(preview_credentials.c.consumed_at).where(
                preview_credentials.c.digest == "c" * 64,
            )) is None
    finally:
        await store.close()


async def test_preview_index_corruption_rejected(tmp_path: Path) -> None:
    store, _, db, record, _, _ = await setup(tmp_path / "preview.sqlite")
    try:
        async with store.engine.begin() as conn:
            await conn.execute(update(preview_records).where(
                preview_records.c.preview_id == record.handle.preview_id,
            ).values(port=3999))
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await db.records.get(record.handle.preview_id)
    finally:
        await store.close()


async def test_ticket_lost_commit_reply_never_reissues_acceptance(
    tmp_path: Path, monkeypatch,
) -> None:
    store, _, db, record, actor, _ = await setup(tmp_path / "preview.sqlite")
    try:
        await db.credentials.put_ticket("d" * 64, BrowserGrant(
            record.handle.preview_id, actor, time.time() + 25,
        ))
        original = store._write

        async def lost(operation):
            await original(operation)
            raise ServiceError("storage_unavailable", 503)

        monkeypatch.setattr(store, "_write", lost)
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await db.credentials.consume_ticket("d" * 64, record.handle.preview_id, time.time())
        monkeypatch.setattr(store, "_write", original)
        with pytest.raises(ServiceError, match="unauthorized"):
            await db.credentials.consume_ticket("d" * 64, record.handle.preview_id, time.time())
    finally:
        await store.close()
