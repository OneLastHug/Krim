import asyncio
import time
from pathlib import Path

import httpx
import pytest
from alembic import command
from cryptography.fernet import Fernet
from sqlalchemy import text

from krim_agent.mcp import (
    EncryptedTokenStore,
    OAuthClient,
    OAuthConfig,
    OAuthError,
    OAuthStateStore,
    OAuthToken,
    PersistentEncryptedTokenStore,
)
from krim_agent.persistence import Database
from krim_agent.persistence.migration import migration_config


def config(**changes):
    values = {
        "server": "demo",
        "client_id": "client",
        "authorization_endpoint": "https://identity.example/authorize",
        "token_endpoint": "https://identity.example/token",
        "redirect_uri": "http://127.0.0.1:8766/callback",
        "scopes": ["tools"],
        "allowed_hosts": {"identity.example"},
    }
    values.update(changes)
    return OAuthConfig(**values)


def token(access="access", refresh="refresh", expires_at=None):
    return OAuthToken(
        access_token=access,
        refresh_token=refresh,
        expires_at=expires_at if expires_at is not None else time.time() + 600,
    )


async def make_database(tmp_path: Path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'oauth.sqlite'}")
    await database.initialize()
    return database


async def test_persistent_store_survives_reopen_and_never_stores_plaintext(tmp_path):
    key = Fernet.generate_key()
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, key)
        client = OAuthClient(OAuthStateStore(), store)
        attempt = client.states.begin(config())

        async def endpoint(_url, _form):
            return httpx.Response(
                200,
                json={
                    "access_token": "persist-access",
                    "refresh_token": "persist-refresh",
                    "expires_in": 600,
                },
            )

        await client.exchange(attempt.state, "code", "subject-a", endpoint)
        async with database.engine.connect() as connection:
            row = (
                await connection.execute(
                    text("SELECT ciphertext, revision FROM oauth_tokens WHERE subject='subject-a'")
                )
            ).one()
            assert (
                b"persist-access" not in row.ciphertext and b"persist-refresh" not in row.ciphertext
            )
            assert row.revision == 1
    finally:
        await database.close()

    reopened = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(reopened, key)
        snapshot = await store.read(config(), "subject-a")
        assert snapshot.revision == 1
        assert snapshot.token is not None
        assert snapshot.token.access_token.get_secret_value() == "persist-access"
        assert (await store.read(config(client_id="different"), "subject-a")).token is None
    finally:
        await reopened.close()


async def test_persistent_scope_hash_separates_client_and_endpoint(tmp_path):
    key = Fernet.generate_key()
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, key)
        await store.write(config(), "subject", token())
        assert (await store.read(config(), "subject")).token is not None
        assert (await store.read(config(client_id="other"), "subject")).token is None
        assert (
            await store.read(
                config(
                    token_endpoint="https://other.example/token",
                    allowed_hosts={"identity.example", "other.example"},
                ),
                "subject",
            )
        ).token is None
    finally:
        await database.close()


async def test_access_token_refresh_is_single_flight_across_two_stores(tmp_path):
    key = Fernet.generate_key()
    database_a = await make_database(tmp_path)
    database_b = Database(f"sqlite+aiosqlite:///{tmp_path / 'oauth.sqlite'}")
    await database_b.initialize()
    try:
        store_a = PersistentEncryptedTokenStore(database_a, key)
        store_b = PersistentEncryptedTokenStore(database_b, key)
        await store_a.write(config(), "subject", token(expires_at=time.time() - 1))
        calls = 0
        entered = asyncio.Event()
        release = asyncio.Event()

        async def endpoint(_url, _form):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return httpx.Response(
                200,
                json={"access_token": "fresh", "refresh_token": "new-refresh", "expires_in": 600},
            )

        first = asyncio.create_task(
            OAuthClient(OAuthStateStore(), store_a).access_token(
                config(), "subject", endpoint, refresh_skew_seconds=0
            )
        )
        second = asyncio.create_task(
            OAuthClient(OAuthStateStore(), store_b).access_token(
                config(), "subject", endpoint, refresh_skew_seconds=0
            )
        )
        await asyncio.wait_for(entered.wait(), 3)
        assert calls == 1
        release.set()
        values = await asyncio.gather(first, second)
        assert calls == 1
        assert [value.access_token.get_secret_value() for value in values] == ["fresh", "fresh"]
    finally:
        await database_a.close()
        await database_b.close()


async def test_cancelled_refresh_releases_lease_and_keeps_old_token(tmp_path):
    key = Fernet.generate_key()
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, key)
        await store.write(config(), "subject", token(expires_at=time.time() - 1))
        entered = asyncio.Event()

        async def blocked(_url, _form):
            entered.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(
            OAuthClient(OAuthStateStore(), store).access_token(
                config(), "subject", blocked, refresh_skew_seconds=0
            )
        )
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        calls = 0

        async def endpoint(_url, _form):
            nonlocal calls
            calls += 1
            return httpx.Response(
                200,
                json={
                    "access_token": "after-cancel",
                    "refresh_token": "refresh",
                    "expires_in": 600,
                },
            )

        refreshed = await OAuthClient(OAuthStateStore(), store).access_token(
            config(), "subject", endpoint, refresh_skew_seconds=0
        )
        assert refreshed.access_token.get_secret_value() == "after-cancel" and calls == 1
    finally:
        await database.close()


async def test_delete_revision_rejects_late_refresh_write(tmp_path):
    key = Fernet.generate_key()
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, key)
        await store.write(config(), "subject", token(expires_at=time.time() - 1))
        snapshot = await store.read(config(), "subject")
        await store.delete(config(), "subject")
        with pytest.raises(OAuthError) as error:
            await store.write(
                config(), "subject", token("late"), expected_revision=snapshot.revision
            )
        assert error.value.code == "token_conflict"
        assert (await store.read(config(), "subject")).token is None
    finally:
        await database.close()


async def test_key_rotation_reencrypts_all_tokens_and_requires_new_key_in_ring(tmp_path):
    old_key, new_key = Fernet.generate_key(), Fernet.generate_key()
    database = await make_database(tmp_path)
    try:
        old_store = PersistentEncryptedTokenStore(database, old_key)
        await old_store.write(config(), "subject", token())
        store = PersistentEncryptedTokenStore(database, [new_key, old_key])
        assert (await store.read(config(), "subject")).token is not None
        assert await store.rotate_key(new_key) == 1
        assert (await store.read(config(), "subject")).token is not None
        with pytest.raises(OAuthError, match="读密钥环"):
            await PersistentEncryptedTokenStore(database, new_key).rotate_key(Fernet.generate_key())
    finally:
        await database.close()


async def test_downgrade_refuses_to_erase_persistent_tokens(tmp_path):
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, Fernet.generate_key())
        await store.write(config(), "subject", token())
        with pytest.raises(RuntimeError, match="OAuth 密文"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0004_run_error_code")
                )
    finally:
        await database.close()


async def test_memory_store_uses_same_scope_and_refresh_lock_contract():
    store = EncryptedTokenStore(Fernet.generate_key())
    await store.write(config(), "subject", token())
    assert (await store.read(config(), "subject")).token is not None
    with pytest.raises(OAuthError) as error:
        await store.read(config(client_id="other"), "subject")
    assert error.value.code == "token_scope_mismatch"


@pytest.mark.parametrize(
    "change",
    [
        {"client_id": "other"},
        {"authorization_endpoint": "https://identity.example/other-authorize"},
        {"token_endpoint": "https://identity.example/other-token"},
        {"redirect_uri": "http://127.0.0.1:9999/callback"},
        {"scopes": ["other"]},
        {"server": "other"},
    ],
)
async def test_changed_configuration_never_reads_existing_credentials(tmp_path, change):
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, Fernet.generate_key())
        await store.write(config(), "subject", token("private"))
        assert (await store.read(config(**change), "subject")).token is None
        assert (await store.read(config(), "other-subject")).token is None
    finally:
        await database.close()


async def test_corrupt_ciphertext_rolls_back_key_rotation(tmp_path):
    old, new = Fernet.generate_key(), Fernet.generate_key()
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, [old, new])
        await store.write(config(), "a", token("valid"))
        await store.write(config(), "z", token("corrupt"))
        async with database.engine.begin() as connection:
            await connection.execute(
                text("UPDATE oauth_tokens SET ciphertext=:blob WHERE subject='z'"),
                {"blob": b"broken"},
            )
            before = (
                await connection.execute(
                    text("SELECT ciphertext, revision FROM oauth_tokens WHERE subject='a'")
                )
            ).one()
        with pytest.raises(OAuthError) as error:
            await store.rotate_key(new)
        assert error.value.code == "token_corrupt"
        async with database.engine.connect() as connection:
            after = (
                await connection.execute(
                    text("SELECT ciphertext, revision FROM oauth_tokens WHERE subject='a'")
                )
            ).one()
        assert before == after
        with pytest.raises(OAuthError) as error:
            await PersistentEncryptedTokenStore(database, Fernet.generate_key()).read(config(), "a")
        assert error.value.code == "token_corrupt"
    finally:
        await database.close()


async def test_rotation_requires_idle_leases_and_new_key_only_can_read_afterwards(tmp_path):
    old, new = Fernet.generate_key(), Fernet.generate_key()
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, [old, new])
        await store.write(config(), "subject", token())
        async with store.lock(config(), "subject"):
            with pytest.raises(OAuthError) as error:
                await store.rotate_key(new)
            assert error.value.code == "token_busy"
        assert await store.rotate_key(new) == 1
        assert (
            await PersistentEncryptedTokenStore(database, new).read(config(), "subject")
        ).token is not None
        with pytest.raises(OAuthError) as error:
            await PersistentEncryptedTokenStore(database, old).read(config(), "subject")
        assert error.value.code == "token_corrupt"
    finally:
        await database.close()


async def test_expired_lease_can_be_reclaimed_and_stale_owner_cannot_write(tmp_path):
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, Fernet.generate_key(), lease_seconds=2)
        await store.write(config(), "subject", token())
        async with database.engine.begin() as connection:
            await connection.execute(
                text("UPDATE oauth_tokens SET lease_owner='crashed', lease_expires_at=:expired"),
                {"expired": time.time() - 1},
            )
        async with store.lock(config(), "subject"):
            snapshot = await store.read(config(), "subject")
            async with database.engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE oauth_tokens SET lease_owner='new-owner', lease_expires_at=:future"
                    ),
                    {"future": time.time() + 60},
                )
            with pytest.raises(OAuthError) as error:
                await store.write(
                    config(), "subject", token("late"), expected_revision=snapshot.revision
                )
            assert error.value.code == "token_conflict"
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT lease_owner FROM oauth_tokens"))
            ).scalar() == "new-owner"
    finally:
        await database.close()


async def test_lease_wait_timeout_and_cancel_keep_original_ciphertext(tmp_path):
    database = await make_database(tmp_path)
    try:
        key = Fernet.generate_key()
        owner = PersistentEncryptedTokenStore(database, key)
        waiter = PersistentEncryptedTokenStore(database, key, acquire_timeout_seconds=0.05)
        await owner.write(config(), "subject", token())
        async with owner.lock(config(), "subject"):
            with pytest.raises(OAuthError) as error:
                async with waiter.lock(config(), "subject"):
                    pytest.fail("竞争者不应进入租约")
            assert error.value.code == "token_lock_timeout"
        async with database.engine.connect() as connection:
            row = (
                await connection.execute(
                    text("SELECT lease_owner, lease_expires_at, revision FROM oauth_tokens")
                )
            ).one()
            assert row.lease_owner is None and row.lease_expires_at is None and row.revision == 1
    finally:
        await database.close()


async def test_failed_refresh_and_concurrent_delete_never_replace_old_credentials(tmp_path):
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, Fernet.generate_key())
        await store.write(config(), "subject", token(expires_at=time.time() - 1))
        client = OAuthClient(OAuthStateStore(), store)

        async def failed(_url, _form):
            return httpx.Response(400, json={"error": "do not expose refresh"})

        with pytest.raises(OAuthError) as error:
            await client.refresh(config(), "subject", failed)
        assert error.value.code == "token_exchange_failed"
        assert (await store.read(config(), "subject")).revision == 1
        entered, release = asyncio.Event(), asyncio.Event()

        async def delayed(_url, _form):
            entered.set()
            await release.wait()
            return httpx.Response(200, json={"access_token": "late", "expires_in": 600})

        task = asyncio.create_task(client.refresh(config(), "subject", delayed))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            await store.delete(config(), "subject")
            release.set()
            with pytest.raises(OAuthError) as error:
                await task
            assert error.value.code == "token_conflict"
            snapshot = await store.read(config(), "subject")
            assert snapshot.token is None and snapshot.revision == 2
        finally:
            release.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        await database.close()


async def test_distinct_subjects_can_refresh_concurrently(tmp_path):
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, Fernet.generate_key())
        for subject in ("one", "two"):
            await store.write(config(), subject, token(expires_at=time.time() - 1))
        entered, release = asyncio.Event(), asyncio.Event()
        active = 0

        async def endpoint(_url, _form):
            nonlocal active
            active += 1
            if active == 2:
                entered.set()
            await release.wait()
            return httpx.Response(200, json={"access_token": "fresh", "expires_in": 600})

        tasks = [
            asyncio.create_task(
                OAuthClient(OAuthStateStore(), store).access_token(config(), subject, endpoint)
            )
            for subject in ("one", "two")
        ]
        try:
            await asyncio.wait_for(entered.wait(), 3)
            release.set()
            assert all(
                value.access_token.get_secret_value() == "fresh"
                for value in await asyncio.gather(*tasks)
            )
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await database.close()


async def test_empty_oauth_table_can_downgrade_then_upgrade(tmp_path):
    database = await make_database(tmp_path)
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0004_run_error_code")
            )
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0004_run_error_code"
        await database.initialize()
        async with database.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0009_user_interactions"
    finally:
        await database.close()


@pytest.mark.parametrize(
    "other_subject,other_client", [("other", "client"), ("subject", "different")]
)
async def test_moving_valid_ciphertext_to_another_scope_is_rejected(
    tmp_path, other_subject, other_client
):
    database = await make_database(tmp_path)
    try:
        store = PersistentEncryptedTokenStore(database, Fernet.generate_key())
        original, destination = config(), config(client_id=other_client)
        await store.write(original, "subject", token("original-secret"))
        await store.write(destination, other_subject, token("destination-secret"))
        from krim_agent.mcp.oauth import token_scope

        async with database.engine.begin() as connection:
            blob = (
                await connection.execute(
                    text(
                        "SELECT ciphertext FROM oauth_tokens "
                        "WHERE scope_hash=:scope AND subject=:subject"
                    ),
                    {"scope": token_scope(original, "subject"), "subject": "subject"},
                )
            ).scalar_one()
            await connection.execute(
                text(
                    "UPDATE oauth_tokens SET ciphertext=:blob "
                    "WHERE scope_hash=:scope AND subject=:subject"
                ),
                {
                    "blob": blob,
                    "scope": token_scope(destination, other_subject),
                    "subject": other_subject,
                },
            )
        with pytest.raises(OAuthError) as error:
            await store.read(destination, other_subject)
        assert error.value.code == "token_scope_mismatch"
        assert "original-secret" not in str(error.value)
    finally:
        await database.close()
