import asyncio
import hashlib
import json
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from alembic import command
from cryptography.fernet import Fernet
from sqlalchemy import event, text

from krim_agent.mcp import (
    EncryptedTokenStore,
    MCPOAuthAuth,
    OAuthClient,
    OAuthConfig,
    OAuthError,
    OAuthHTTPRequester,
    OAuthStateStore,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
)
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest


def config(**changes):
    values = {
        "server": "demo",
        "client_id": "original-client",
        "client_secret": "original-client-secret",
        "issuer": "https://identity.example",
        "resource": "https://mcp.example/mcp",
        "authorization_endpoint": "https://identity.example/authorize",
        "token_endpoint": "https://identity.example/token",
        "redirect_uri": "http://127.0.0.1:8766/callback",
        "scopes": ["tools"],
        "allowed_hosts": {"identity.example"},
    }
    values.update(changes)
    return OAuthConfig(**values)


async def database_at(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'states.sqlite'}")
    await database.initialize()
    return database


async def state_rows(database):
    async with database.engine.connect() as connection:
        return (await connection.execute(text("SELECT * FROM oauth_states"))).mappings().all()


async def test_encrypted_state_survives_reopen_and_freezes_original_configuration(tmp_path):
    key = Fernet.generate_key()
    database = await database_at(tmp_path)
    original = config()
    try:
        request = await PersistentEncryptedStateStore(database, key).begin(original, subject="one")
        row = (await state_rows(database))[0]
        assert row["state_hash"] == hashlib.sha256(request.state.encode()).hexdigest()
        assert row["status"] == "pending"
        database_bytes = (tmp_path / "states.sqlite").read_bytes()
        for secret in (request.state, request.code_verifier, "original-client-secret"):
            assert secret.encode() not in database_bytes
        original.client_id = "changed"
        original.scopes.append("more")
    finally:
        await database.close()
    reopened = await database_at(tmp_path)
    try:
        stored, recovered = await PersistentEncryptedStateStore(reopened, key).consume(
            request.state, subject="one", expected_config=config()
        )
        assert recovered == request
        assert stored.client_id == "original-client" and stored.scopes == ["tools"]
        assert stored.client_secret.get_secret_value() == "original-client-secret"
        row = (await state_rows(reopened))[0]
        assert row["status"] == "consumed" and row["ciphertext"] is None
        with pytest.raises(OAuthError) as error:
            await PersistentEncryptedStateStore(reopened, key).consume(request.state, subject="one")
        assert error.value.code == "invalid_state"
    finally:
        await reopened.close()


@pytest.mark.parametrize(
    "changed",
    [
        {"server": "different"},
        {"client_id": "different"},
        {"redirect_uri": "http://127.0.0.1:8767/other"},
        {"resource": "https://mcp.example/other"},
        {"issuer": "https://identity.example/other"},
        {"scopes": ["other"]},
        {"authorization_endpoint": "https://identity.example/other"},
        {"token_endpoint": "https://identity.example/other"},
    ],
)
async def test_wrong_client_binding_does_not_consume_original_state(tmp_path, changed):
    database = await database_at(tmp_path)
    store = PersistentEncryptedStateStore(database, Fernet.generate_key())
    try:
        request = await store.begin(config(), subject="one")
        with pytest.raises(OAuthError) as error:
            await store.consume(request.state, subject="one", expected_config=config(**changed))
        assert error.value.code == "invalid_state"
        assert (await state_rows(database))[0]["status"] == "pending"
        assert (await store.consume(request.state, subject="one"))[1] == request
    finally:
        await database.close()


async def test_wrong_subject_cannot_consume_or_cancel_and_legacy_sync_still_works(tmp_path):
    database = await database_at(tmp_path)
    try:
        stores = (OAuthStateStore(), PersistentEncryptedStateStore(database, Fernet.generate_key()))
        for states in stores:
            client = OAuthClient(states, EncryptedTokenStore(Fernet.generate_key()))
            request = await client.begin_authorization(config(), "one")
            await client.discard_authorization(request.state, "other")
            with pytest.raises(OAuthError) as error:
                await client.exchange(request.state, "code", "other", lambda *_: None)
            assert error.value.code == "invalid_state"

            async def endpoint(_url, form, request=request):
                assert form["code_verifier"] == request.code_verifier
                return httpx.Response(200, json={"access_token": "valid"})

            assert (
                await client.exchange(request.state, "code", "one", endpoint)
            ).access_token.get_secret_value() == "valid"
        legacy = OAuthStateStore()
        request = legacy.begin(config())
        assert legacy.consume(request.state)[1] == request
        legacy.discard("unknown")
    finally:
        await database.close()


async def test_ttl_at_exact_boundary_commits_expiry_and_prune_is_subject_bound(
    tmp_path, monkeypatch
):
    database = await database_at(tmp_path)
    store = PersistentEncryptedStateStore(database, Fernet.generate_key())
    try:
        request = await store.begin(config(state_ttl_seconds=30), subject="one")
        other = await store.begin(config(state_ttl_seconds=30), subject="other")
        with monkeypatch.context() as clock:
            clock.setattr("time.time", lambda: max(request.created_at, other.created_at) + 30)
            with pytest.raises(OAuthError) as error:
                await store.consume(request.state, subject="one")
            assert error.value.code == "expired_state"
            row = next(row for row in await state_rows(database) if row["subject"] == "one")
            assert row["status"] == "expired" and row["ciphertext"] is None
            assert await store.prune(subject="one") == 1
            assert [row["subject"] for row in await state_rows(database)] == ["other"]
    finally:
        await database.close()


async def test_waiting_for_other_process_write_lock_cannot_extend_ttl(tmp_path, monkeypatch):
    first, second = await database_at(tmp_path), await database_at(tmp_path)
    key, entered = Fernet.generate_key(), asyncio.Event()
    store = PersistentEncryptedStateStore(first, key)
    task = None

    @event.listens_for(second.engine.sync_engine, "before_cursor_execute")
    def consuming(_connection, _cursor, statement, _params, _context, _many):
        if statement.startswith("UPDATE oauth_states"):
            entered.set()

    try:
        request = await store.begin(config(state_ttl_seconds=30), subject="one")
        now = [request.created_at + 29]
        with monkeypatch.context() as clock:
            clock.setattr("time.time", lambda: now[0])
            async with first.engine.begin() as connection:
                await connection.execute(text("UPDATE oauth_states SET ciphertext=ciphertext"))
                task = asyncio.create_task(
                    PersistentEncryptedStateStore(second, key).consume(request.state, subject="one")
                )
                await asyncio.wait_for(entered.wait(), 3)
                assert not task.done()
                now[0] = request.created_at + 30
            with pytest.raises(OAuthError) as error:
                await asyncio.wait_for(task, 3)
            assert error.value.code == "expired_state"
        assert (await state_rows(first))[0]["status"] == "expired"
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await first.close()
        await second.close()


@pytest.mark.parametrize("damage", ["wrong-key", "corrupt", "moved", "time", "envelope"])
async def test_corrupt_or_moved_ciphertext_is_terminal_and_never_exposes_secrets(tmp_path, damage):
    database = await database_at(tmp_path)
    key = Fernet.generate_key()
    store = PersistentEncryptedStateStore(database, key)
    try:
        request = await store.begin(config(), subject="one")
        row = (await state_rows(database))[0]
        replacement = row["ciphertext"]
        if damage == "wrong-key":
            store = PersistentEncryptedStateStore(database, Fernet.generate_key())
        elif damage == "corrupt":
            replacement = b"broken-original-client-secret"
        elif damage == "moved":
            await store.begin(config(), subject="other")
            replacement = (await state_rows(database))[1]["ciphertext"]
        elif damage == "envelope":
            data = json.loads(Fernet(key).decrypt(replacement))
            data["scope_hash"] = "changed"
            replacement = Fernet(key).encrypt(json.dumps(data).encode())
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE oauth_states SET ciphertext=:blob, expires_at=expires_at+:extend "
                    "WHERE subject='one'"
                ),
                {"blob": replacement, "extend": 100 if damage == "time" else 0},
            )
        with pytest.raises(OAuthError) as error:
            await store.consume(request.state, subject="one")
        assert error.value.code == "state_corrupt"
        assert "original-client-secret" not in str(error.value)
        assert request.code_verifier not in str(error.value)
        row = next(row for row in await state_rows(database) if row["subject"] == "one")
        assert row["status"] == "invalidated" and row["ciphertext"] is None
        with pytest.raises(OAuthError) as replay:
            await PersistentEncryptedStateStore(database, key).consume(request.state, subject="one")
        assert replay.value.code == "invalid_state"
    finally:
        await database.close()


async def test_two_database_connections_exchange_only_once_and_preserve_original_pkce(tmp_path):
    first, second = await database_at(tmp_path), await database_at(tmp_path)
    key, calls = Fernet.generate_key(), []
    try:
        clients = [
            OAuthClient(
                PersistentEncryptedStateStore(database, key),
                PersistentEncryptedTokenStore(database, key),
            )
            for database in (first, second)
        ]
        request = await clients[0].begin_authorization(config(), "one")

        async def endpoint(_url, form):
            calls.append(form)
            assert form["code_verifier"] == request.code_verifier
            assert form["client_secret"] == "original-client-secret"
            return httpx.Response(200, json={"access_token": "persisted"})

        results = await asyncio.gather(
            *(client.exchange(request.state, "code", "one", endpoint) for client in clients),
            return_exceptions=True,
        )
        assert len(calls) == 1
        assert (
            sum(
                isinstance(value, OAuthError) and value.code == "invalid_state" for value in results
            )
            == 1
        )
        assert (
            await clients[1].tokens.read(config(), "one")
        ).token.access_token.get_secret_value() == "persisted"
    finally:
        await first.close()
        await second.close()


@pytest.mark.parametrize("cancel", [False, True])
async def test_failed_or_cancelled_exchange_cannot_reuse_state(tmp_path, cancel):
    database = await database_at(tmp_path)
    key = Fernet.generate_key()
    states = PersistentEncryptedStateStore(database, key)
    tokens = PersistentEncryptedTokenStore(database, key)
    client = OAuthClient(states, tokens)
    entered = asyncio.Event()
    try:
        request = await client.begin_authorization(config(), "one")

        async def endpoint(_url, _form):
            entered.set()
            if cancel:
                await asyncio.Event().wait()
            return httpx.Response(400, text="secret identity failure")

        task = asyncio.create_task(client.exchange(request.state, "code", "one", endpoint))
        await asyncio.wait_for(entered.wait(), 3)
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else OAuthError):
            await task
        row = (await state_rows(database))[0]
        assert row["status"] == "consumed" and row["ciphertext"] is None
        with pytest.raises(OAuthError) as error:
            await client.exchange(request.state, "code", "one", endpoint)
        assert error.value.code == "invalid_state"
        assert (await tokens.read(config(), "one")).token is None
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT lease_owner FROM oauth_tokens"))
            ).scalar_one() is None
    finally:
        await database.close()


async def test_rotation_preserves_pkce_ttl_and_rolls_back_all_on_corruption(tmp_path):
    database = await database_at(tmp_path)
    old, new = Fernet.generate_key(), Fernet.generate_key()
    store = PersistentEncryptedStateStore(database, [old, new])
    try:
        request = await store.begin(config(), subject="one")
        await store.begin(config(), subject="two")
        rows = await state_rows(database)
        with pytest.raises(OAuthError) as error:
            await store.rotate_key(Fernet.generate_key())
        assert error.value.code == "rotation_key_missing"
        async with database.engine.begin() as connection:
            await connection.execute(
                text("UPDATE oauth_states SET ciphertext=X'00' WHERE subject='two'")
            )
        with pytest.raises(OAuthError) as error:
            await store.rotate_key(new)
        assert error.value.code == "state_corrupt"
        assert (await state_rows(database))[0]["ciphertext"] == rows[0]["ciphertext"]
        async with database.engine.begin() as connection:
            await connection.execute(
                text("UPDATE oauth_states SET ciphertext=:blob WHERE subject='two'"),
                {"blob": rows[1]["ciphertext"]},
            )
        assert await store.rotate_key(new) == 2
        rotated = await state_rows(database)
        assert [row["expires_at"] for row in rotated] == [row["expires_at"] for row in rows]
        assert (
            await PersistentEncryptedStateStore(database, new).consume(request.state, subject="one")
        )[1] == request
        assert await store.rotate_key(new) == 1
    finally:
        await database.close()


async def test_record_caps_are_atomic_across_connections_and_expired_slots_are_reclaimed(
    tmp_path, monkeypatch
):
    first, second = await database_at(tmp_path), await database_at(tmp_path)
    key = Fernet.generate_key()
    stores = [
        PersistentEncryptedStateStore(db, key, max_states=2, max_subject_states=1)
        for db in (first, second)
    ]
    try:
        results = await asyncio.gather(
            *(store.begin(config(state_ttl_seconds=30), subject="one") for store in stores),
            return_exceptions=True,
        )
        assert (
            sum(
                isinstance(value, OAuthError) and value.code == "state_store_full"
                for value in results
            )
            == 1
        )
        row = (await state_rows(first))[0]
        await stores[0].begin(config(), subject="two")
        with pytest.raises(OAuthError) as error:
            await stores[0].begin(config(), subject="three")
        assert error.value.code == "state_store_full"
        with monkeypatch.context() as clock:
            clock.setattr("time.time", lambda: row["expires_at"])
            await stores[1].begin(config(), subject="three")
        assert {row["subject"] for row in await state_rows(first)} == {"two", "three"}
    finally:
        await first.close()
        await second.close()


async def test_auth_consumes_injected_persistent_states_and_cancels_pending_login(tmp_path):
    database = await database_at(tmp_path)
    key, redirect, entered = Fernet.generate_key(), {}, asyncio.Event()
    store = PersistentEncryptedStateStore(database, key)
    tokens = PersistentEncryptedTokenStore(database, key)

    async def endpoint(request):
        if request.url.path == "/token":
            return httpx.Response(200, json={"access_token": "auth-access"})
        if request.headers.get("authorization") != "Bearer auth-access":
            return httpx.Response(401, headers={"WWW-Authenticate": "Bearer"})
        return httpx.Response(200, json={"ok": True})

    async def show(url):
        redirect["state"] = parse_qs(urlsplit(url).query)["state"][0]
        assert (await state_rows(database))[-1]["status"] == "pending"

    async def callback():
        return "code", redirect["state"]

    try:
        async with OAuthHTTPRequester(
            {"identity.example"}, transport=httpx.MockTransport(endpoint)
        ) as requester:
            auth = MCPOAuthAuth(
                "https://mcp.example/mcp",
                requester=requester,
                tokens=tokens,
                subject="one",
                server="demo",
                config=config(),
                states=store,
                redirect_handler=show,
                callback_handler=callback,
            )
            async with httpx.AsyncClient(
                auth=auth, transport=httpx.MockTransport(endpoint)
            ) as client:
                assert (
                    await client.post(auth.resource_url, json={"method": "initialize"})
                ).status_code == 200
                assert (await state_rows(database))[0]["status"] == "consumed"
                await tokens.delete(config(), "one")

                async def blocked():
                    entered.set()
                    await asyncio.Event().wait()

                auth.callback_handler = blocked
                task = asyncio.create_task(
                    client.post(auth.resource_url, json={"method": "initialize"})
                )
                await asyncio.wait_for(entered.wait(), 3)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                rows = await state_rows(database)
                assert [row["status"] for row in rows] == ["consumed", "cancelled"]
                assert all(row["ciphertext"] is None for row in rows)
    finally:
        await database.close()


async def test_repeated_cancellation_does_not_interrupt_persistent_secret_cleanup(tmp_path):
    database = await database_at(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()

    class DelayedStateStore(PersistentEncryptedStateStore):
        async def discard(self, state, *, subject):
            entered.set()
            await release.wait()
            await super().discard(state, subject=subject)

    try:
        store = DelayedStateStore(database, Fernet.generate_key())
        request = await store.begin(config(), subject="one")
        client = OAuthClient(store, EncryptedTokenStore(Fernet.generate_key()))
        task = asyncio.create_task(client.discard_authorization(request.state, "one"))
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await state_rows(database))[0]["status"] == "cancelled"
        assert (await state_rows(database))[0]["ciphertext"] is None
    finally:
        release.set()
        await database.close()


async def test_0031_upgrade_retains_history_and_nonempty_state_refuses_lossy_downgrade(tmp_path):
    database = await database_at(tmp_path)
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0031_command_artifact_proposals"
                )
            )
        repository = KrimRepository(database)
        topic = await repository.create_topic("原历史")
        run, _ = await repository.begin_run(
            topic.id, ChatRequest(text="retained", idempotency_key="original")
        )
        await repository.finish(run.id, "completed", None, [])
        await database.initialize()
        assert (await repository.list_messages(topic.id))[0].content == "retained"
        await PersistentEncryptedStateStore(database, Fernet.generate_key()).begin(
            config(), subject="one"
        )
        with pytest.raises(RuntimeError, match="OAuth 状态表非空"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0031_command_artifact_proposals"
                    )
                )
        assert len(await state_rows(database)) == 1
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one() == "0046_tree_exchange_origins"
    finally:
        await database.close()


@pytest.mark.parametrize("subject", ["", "x" * 257, "bad\nsubject"])
async def test_invalid_subject_is_rejected_before_state_is_persisted(tmp_path, subject):
    database = await database_at(tmp_path)
    try:
        with pytest.raises(OAuthError) as error:
            await PersistentEncryptedStateStore(database, Fernet.generate_key()).begin(
                config(), subject=subject
            )
        assert error.value.code == "invalid_token_subject"
        assert await state_rows(database) == []
    finally:
        await database.close()


async def test_invalid_key_and_oversize_payload_are_rejected_before_persistence(tmp_path):
    database = await database_at(tmp_path)
    try:
        for keys in (b"bad-key", []):
            with pytest.raises(OAuthError) as error:
                PersistentEncryptedStateStore(database, keys)
            assert error.value.code == "invalid_encryption_key"
        store = PersistentEncryptedStateStore(database, Fernet.generate_key())
        with pytest.raises(OAuthError) as error:
            await store.begin(config(client_secret="x" * 65536), subject="one")
        assert error.value.code == "state_payload_limit"
        for state in ("", "x" * 257, "\ud800"):
            with pytest.raises(OAuthError) as error:
                await store.consume(state, subject="one")
            assert error.value.code == "invalid_state"
        assert await state_rows(database) == []
    finally:
        await database.close()
