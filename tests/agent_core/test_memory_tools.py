"""Memory scope, opt-in, receipts, paging, source and migration regressions."""

from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import create_async_engine
from test_extensions_service import (
    ExtensionService,
    account_of,
    managed_extensions,
    new_session,
    personal_workspace,
    submit_turn,
)

from krim_agent.contracts import RunRequest, ServiceError, fingerprint
from krim_agent.extensions import (
    MemoryCreate,
    MemoryDelete,
    MemoryUpdate,
    Preference,
    SessionExtensions,
    body_hash,
)
from krim_agent.storage.extension_models import (
    extension_requests,
    memories,
    memory_preferences,
    migrate_memory_scopes,
)
from krim_agent.storage.models import turns
from krim_agent.storage.sandbox_models import (
    sandbox_environments,
    sandbox_projects,
    sandbox_session_bindings,
)
from krim_agent.tools.core import Invocation
from krim_agent.tools.memory import MemoryRead, MemoryRemove, MemoryToolService, MemoryWrite
from krim_agent.tools.session import SessionToolScope


@asynccontextmanager
async def memory_environment(path, *, enabled=True):
    async with managed_extensions(path) as (store, identities, admin):
        owner = await account_of(identities, admin, "owner")
        service = ExtensionService(store, identities)
        workspace = await personal_workspace(identities, owner)
        session = await new_session(store, owner, workspace, "session")
        await service.session_config(
            owner,
            workspace,
            session.id,
            SessionExtensions(request_key="config", expected_revision=0, memory_enabled=enabled),
        )
        if enabled:
            await service.preference(
                owner, Preference(request_key="enabled", expected_revision=0, enabled=True)
            )
        run, _ = await store.begin(
            RunRequest(text="喜欢深烘焙咖啡", request_key="run"), {"source": "test"}
        )
        turn = await submit_turn(store, owner, service, session, "turn", "记住：喜欢深烘焙咖啡", 0)
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(update(turns).where(turns.c.id == turn.id).values(run_id=run.id))
        scope = SessionToolScope(owner, workspace, session.id, turn.id, run.id)
        yield (
            store,
            identities,
            admin,
            service,
            MemoryToolService(store, identities, extensions=service),
            scope,
        )


async def bind_projects(store, scope):
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(
            insert(sandbox_environments).values(
                environment_id="environment",
                principal_id=scope.actor.principal_id,
                status="ready",
                created_at=1,
                updated_at=1,
            )
        )
        for project in ["project-one", "project-two"]:
            await conn.execute(
                insert(sandbox_projects).values(
                    project_id=project,
                    environment_id="environment",
                    principal_id=scope.actor.principal_id,
                    workspace_id=scope.workspace_id,
                    title=project,
                    status="ready",
                    created_at=1,
                    updated_at=1,
                )
            )
        await conn.execute(
            insert(sandbox_session_bindings).values(
                session_id=scope.session_id,
                principal_id=scope.actor.principal_id,
                project_id="project-one",
                revision=1,
                created_at=1,
                updated_at=1,
            )
        )
    return replace(scope, project_id="project-one")


def invocation(scope, name="memory_write", call="call"):
    return Invocation(scope.run_id, call, "span", 0, name, "{}", "0" * 64)


async def test_opt_in_disabled_tools_never_read_write_or_enable(tmp_path):
    async with memory_environment(tmp_path / "disabled.sqlite", enabled=False) as (
        _,
        _,
        _,
        ext,
        tools,
        scope,
    ):
        saved = await ext.memory(scope.actor, MemoryCreate(request_key="manual", body="coffee"))
        assert (await tools.recall(scope, "coffee"))["status"] == "disabled"
        assert (await tools.read(scope, MemoryRead(memory_id=saved["id"])))["status"] == "disabled"
        assert await tools.mutate(scope, MemoryWrite(body="ignored"), request_key="tool") == {
            "status": "disabled"
        }

        async def resolve(inv):
            return scope

        assert await tools.bundle(resolve).policy(invocation(scope)) == "deny"
        assert await ext.preference(scope.actor) == {"enabled": False, "revision": 0}
        assert len(await ext.list_memories(scope.actor)) == 1
        # Management stays usable while model memory is off.
        await ext.memory(
            scope.actor,
            MemoryDelete(
                request_key="remove",
                expected_revision=1,
                expected_content_hash=saved["content_hash"],
            ),
            saved["id"],
        )
        assert await ext.list_memories(scope.actor) == []


async def test_model_mutations_receipts_conflicts_provenance_and_disable(tmp_path):
    async with memory_environment(tmp_path / "tools.sqlite") as (_, _, _, ext, tools, scope):
        data = MemoryWrite(body="喜欢深烘焙咖啡", memory_kind="preference")
        saved = await tools.mutate(scope, data, request_key="write")
        assert await tools.mutate(scope, data, request_key="write") == saved
        item = await tools.read(scope, MemoryRead(memory_id=saved["id"]))
        assert item["body"] == data.body and item["source_ref"] == f"turn:{scope.turn_id}"
        assert item["authorization_turn_id"] == scope.turn_id
        with pytest.raises(ServiceError, match="key_conflict"):
            await tools.mutate(scope, MemoryWrite(body="different"), request_key="write")
        with pytest.raises(ServiceError, match="revision_conflict"):
            await tools.mutate(
                scope,
                MemoryWrite(
                    body="updated",
                    memory_id=saved["id"],
                    expected_revision=2,
                    expected_content_hash=saved["content_hash"],
                ),
                request_key="stale",
            )
        with pytest.raises(ServiceError, match="invalid_input"):
            await tools.mutate(
                scope,
                MemoryWrite(body="injected preference", memory_kind="preference"),
                request_key="inject",
            )
        with pytest.raises(ValidationError):
            MemoryWrite(
                body="web preference",
                source_kind="web",
                source_ref="https://example.test",
                memory_kind="preference",
            )
        with pytest.raises(ValidationError):
            MemoryCreate(
                request_key="web",
                body="web preference",
                source_kind="mcp",
                memory_kind="preference",
            )
        note = await tools.mutate(
            scope,
            MemoryWrite(body="external fact", source_kind="web", source_ref="https://example.test"),
            request_key="external",
        )
        assert (await tools.read(scope, MemoryRead(memory_id=note["id"])))["source_kind"] == "web"
        removed = await tools.mutate(
            scope,
            MemoryRemove(
                memory_id=saved["id"],
                expected_revision=1,
                expected_content_hash=saved["content_hash"],
            ),
            request_key="delete",
        )
        assert removed["removed"]
        assert not (await tools.recall(scope, "咖啡"))["items"]
        await ext.preference(
            scope.actor, Preference(request_key="off", expected_revision=1, enabled=False)
        )
        assert (await tools.mutate(scope, data, request_key="write"))["status"] == "disabled"
        assert (await tools.recall(scope, "external"))["items"] == []


async def test_private_project_scope_cross_session_and_wrong_scope(tmp_path):
    async with memory_environment(tmp_path / "scopes.sqlite") as (
        store,
        identities,
        admin,
        ext,
        tools,
        original,
    ):
        scope = await bind_projects(store, original)
        common = await ext.memory(
            scope.actor, MemoryCreate(request_key="account", body="coffee account")
        )
        first = await tools.mutate(
            scope, MemoryWrite(scope="project", body="coffee first"), request_key="project"
        )
        await ext.memory(
            scope.actor,
            MemoryCreate(request_key="second", body="coffee second"),
            workspace_id=scope.workspace_id,
            project_id="project-two",
        )
        found = await tools.recall(scope, "coffee")
        assert {item["id"] for item in found["items"]} == {common["id"], first["id"]}
        assert {item["scope"] for item in found["items"]} == {"account", "project"}
        digest = found.pop("fingerprint")
        assert fingerprint(found) == digest
        assert found["version"] == "memory-recall/1" and found["preference_revision"] == 1
        assert [item["id"] for item in await ext.list_memories(scope.actor)] == [common["id"]]
        with pytest.raises(ServiceError, match="not_found"):
            await tools.read(scope, MemoryRead(memory_id=first["id"]))
        with pytest.raises(ServiceError, match="context_binding_changed"):
            await tools.recall(original, "coffee")
        other = await account_of(identities, admin, "other")
        with pytest.raises(ServiceError):
            await ext.memory_page(other, workspace_id=scope.workspace_id, project_id="project-one")
        with pytest.raises(ServiceError):
            await tools.recall(replace(scope, actor=other), "coffee")
        second_session = await new_session(
            store, scope.actor, scope.workspace_id, "another-session"
        )
        await ext.session_config(
            scope.actor,
            scope.workspace_id,
            second_session.id,
            SessionExtensions(request_key="config-other", expected_revision=0, memory_enabled=True),
        )
        other_scope = replace(original, session_id=second_session.id)
        assert [item["id"] for item in (await tools.recall(other_scope, "coffee"))["items"]] == [
            common["id"]
        ]


async def test_no_storage_count_or_body_limit_and_versioned_paging(tmp_path):
    async with memory_environment(tmp_path / "paging.sqlite") as (store, _, _, ext, tools, scope):
        large_body = "needle " + "中" * 400000
        saved = await ext.memory(scope.actor, MemoryCreate(request_key="large", body=large_body))
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                insert(memories),
                [
                    dict(
                        id=f"row-{i}",
                        principal_id=scope.actor.principal_id,
                        body="noise",
                        content_hash=body_hash("noise"),
                        revision=1,
                        created_at=100 + i,
                        updated_at=100 + i,
                    )
                    for i in range(120)
                ],
            )
        await ext.memory(
            scope.actor, MemoryCreate(request_key="after120", body="after count limit")
        )
        pages, offset = [], 0
        while offset is not None:
            page = await ext.memory_page(scope.actor, offset=offset, limit=17)
            pages.extend(page["items"])
            offset = page["next_offset"]
        assert len(pages) == 122 and len({row["id"] for row in pages}) == 122
        long_query = " ".join(["needle", *(f"absent{i}" for i in range(1100))])
        found = (await tools.recall(scope, long_query, max_chars=100))["items"]
        assert len(found) == 1 and len(found[0]["body"]) == 100
        assert found[0]["next_offset"] == 100 and found[0]["content_hash"] == saved["content_hash"]
        assert found[0]["excerpt_hash"] == body_hash(found[0]["body"])
        first = await tools.read(scope, MemoryRead(memory_id=saved["id"], length=3000))
        with pytest.raises(ServiceError, match="invalid_input"):
            await tools.read(scope, MemoryRead(memory_id=saved["id"], offset=first["next_offset"]))
        text, offset = first["body"], first["next_offset"]
        while offset is not None:
            page = await tools.read(
                scope,
                MemoryRead(
                    memory_id=saved["id"],
                    offset=offset,
                    length=65536,
                    expected_revision=1,
                    expected_content_hash=saved["content_hash"],
                ),
            )
            text += page["body"]
            offset = page["next_offset"]
        assert text == large_body and body_hash(text) == saved["content_hash"]


async def test_bundle_policy_scope_checks_and_host_request_key(tmp_path):
    async with memory_environment(tmp_path / "bundle.sqlite") as (_, _, _, _, tools, scope):
        checks = []
        scope = replace(scope, check=lambda: checks.append(True))

        async def resolve(inv):
            assert inv.run_id == scope.run_id
            return scope

        bundle = tools.bundle(resolve)
        specs = {spec.name: spec for spec in bundle.specs}
        assert set(specs) == {"memory_read", "memory_search", "memory_write", "memory_delete"}
        assert await bundle.policy(invocation(scope)) == "ask"
        assert await bundle.policy(invocation(scope, "memory_search")) == "allow"
        assert specs["memory_write"].descriptor()["policy_version"] == "session/1"
        write = specs["memory_write"]
        assert "request_key" not in write.parameters.model_json_schema()["properties"]
        saved = await write.scoped_executor(
            invocation(scope), MemoryWrite(body="approved note"), None
        )
        assert (
            await write.scoped_executor(invocation(scope), MemoryWrite(body="approved note"), None)
            == saved
        )
        assert checks
        with pytest.raises(ServiceError, match="workspace_contract_required"):
            await tools.mutate(
                replace(scope, run_id="wrong-run"),
                MemoryWrite(body="note"),
                request_key="wrong-run",
            )


async def test_legacy_receipt_digest_survives_added_defaults(tmp_path):
    async with memory_environment(tmp_path / "legacy-receipt.sqlite") as (
        store,
        _,
        _,
        ext,
        _,
        scope,
    ):
        receipt = dict(id="old-id", revision=1, content_hash=body_hash("old body"))
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                insert(extension_requests).values(
                    principal_id=scope.actor.principal_id,
                    request_key="legacy-key",
                    request_hash=fingerprint(
                        {
                            "action": "memory:create",
                            "body": {
                                "request_key": "legacy-key",
                                "body": "old body",
                                "source_ref": None,
                            },
                        }
                    ),
                    result=receipt,
                )
            )
        assert (
            await ext.memory(scope.actor, MemoryCreate(request_key="legacy-key", body="old body"))
            == receipt
        )


async def test_memory_migration_preserves_legacy_data_and_switch(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'migration.sqlite'}")
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                "CREATE TABLE memories (id TEXT PRIMARY KEY, principal_id TEXT NOT NULL, "
                "body TEXT NOT NULL, content_hash TEXT NOT NULL, source_ref TEXT, "
                "revision INTEGER NOT NULL, created_at FLOAT NOT NULL, updated_at FLOAT NOT NULL)"
            )
            await conn.exec_driver_sql(
                "CREATE TABLE memory_preferences (principal_id TEXT PRIMARY KEY, "
                "enabled BOOLEAN NOT NULL, revision INTEGER NOT NULL)"
            )
            await conn.exec_driver_sql(
                "INSERT INTO memories VALUES ('id','owner','old',?,NULL,3,1,2)", (body_hash("old"),)
            )
            await conn.exec_driver_sql("INSERT INTO memory_preferences VALUES ('owner',0,4)")
            await conn.exec_driver_sql("PRAGMA user_version=17")
            await migrate_memory_scopes(conn)
            await migrate_memory_scopes(conn)
            columns = (await conn.exec_driver_sql("PRAGMA table_info(memories)")).mappings().all()
            assert [column["name"] for column in columns] == list(memories.c.keys())
            row = (await conn.execute(select(memories))).mappings().one()
            assert row["body"] == "old" and row["revision"] == 3
            assert row["project_id"] is row["workspace_id"] is row["authorization_turn_id"] is None
            assert row["source_kind"] == "user" and row["memory_kind"] == "note"
            preference = (await conn.execute(select(memory_preferences))).mappings().one()
            assert not preference["enabled"] and preference["revision"] == 4
            assert (await conn.exec_driver_sql("PRAGMA user_version")).scalar_one() == 17
    finally:
        await engine.dispose()


async def test_shared_project_does_not_expose_account_memory(tmp_path):
    async with memory_environment(tmp_path / "shared-project.sqlite") as (
        store,
        _,
        _,
        ext,
        tools,
        scope,
    ):
        await ext.memory(
            scope.actor, MemoryCreate(request_key="account-private", body="coffee account secret")
        )
        shared_session = await new_session(store, scope.actor, "owner-shared", "shared-session")
        shared = replace(scope, workspace_id="owner-shared", session_id=shared_session.id)
        shared = await bind_projects(store, shared)
        await ext.session_config(
            scope.actor,
            shared.workspace_id,
            shared.session_id,
            SessionExtensions(
                request_key="shared-enable", expected_revision=0, memory_enabled=True
            ),
        )
        saved = await ext.memory(
            scope.actor,
            MemoryCreate(request_key="shared-project", body="coffee project note"),
            workspace_id=shared.workspace_id,
            project_id=shared.project_id,
        )
        recalled = await tools.recall(shared, "coffee")
        assert [item["id"] for item in recalled["items"]] == [saved["id"]]
        with pytest.raises(ServiceError, match="private_memory_workspace"):
            await tools.read(shared, MemoryRead(memory_id=saved["id"]))
        await ext.session_config(
            scope.actor,
            shared.workspace_id,
            shared.session_id,
            SessionExtensions(
                request_key="shared-disable", expected_revision=1, memory_enabled=False
            ),
        )
        assert (await tools.recall(shared, "coffee"))["status"] == "disabled"


async def test_cancellation_guard_rolls_back_before_receipt(tmp_path):
    async with memory_environment(tmp_path / "rollback.sqlite") as (store, _, _, ext, tools, scope):
        checks = 0

        def cancelled():
            nonlocal checks
            checks += 1
            if checks >= 10:
                raise ServiceError("cancelled")

        scope = replace(scope, check=cancelled)
        with pytest.raises(ServiceError, match="cancelled"):
            await tools.mutate(scope, MemoryWrite(body="must roll back"), request_key="cancelled")
        assert await ext.list_memories(scope.actor) == []
        async with store.lock, store.engine.begin() as conn:
            receipt = await conn.scalar(
                select(extension_requests.c.request_key).where(
                    extension_requests.c.principal_id == scope.actor.principal_id,
                    extension_requests.c.request_key == "cancelled",
                )
            )
            assert receipt is None


async def test_current_read_detects_full_record_tampering(tmp_path):
    async with memory_environment(tmp_path / "tamper.sqlite") as (store, _, _, ext, tools, scope):
        saved = await ext.memory(scope.actor, MemoryCreate(request_key="tamper", body="coffee"))
        async with store.lock, store.engine.begin() as conn:
            await conn.execute(
                update(memories).where(memories.c.id == saved["id"]).values(content_hash="0" * 64)
            )
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await tools.recall(scope, "coffee")
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await tools.read(scope, MemoryRead(memory_id=saved["id"]))


async def test_legacy_update_preserves_external_provenance_and_unicode_search(tmp_path):
    async with memory_environment(tmp_path / "source-update.sqlite") as (
        _,
        _,
        _,
        ext,
        tools,
        scope,
    ):
        saved = await ext.memory(
            scope.actor,
            MemoryCreate(
                request_key="web-note",
                body="Straße source",
                source_kind="web",
                source_ref="https://example.test",
            ),
        )
        updated = await ext.memory(
            scope.actor,
            MemoryUpdate(
                request_key="legacy-update",
                body="Straße changed",
                expected_revision=1,
                expected_content_hash=saved["content_hash"],
            ),
            saved["id"],
        )
        row = await ext.read_memory(scope.actor, saved["id"])
        assert row["source_kind"] == "web" and row["source_ref"] == "https://example.test"
        assert [item["id"] for item in (await tools.recall(scope, "STRASSE"))["items"]] == [
            saved["id"]
        ]
        assert len((await ext.memory_page(scope.actor, query="STRASSE"))["items"]) == 1
        with pytest.raises(ValidationError):
            MemoryCreate(request_key="missing-source", body="fact", source_kind="web")
        with pytest.raises(ServiceError, match="invalid_input"):
            await ext.memory(
                scope.actor,
                MemoryUpdate(
                    request_key="bad-pref",
                    body="bad",
                    memory_kind="preference",
                    expected_revision=2,
                    expected_content_hash=updated["content_hash"],
                ),
                saved["id"],
            )
