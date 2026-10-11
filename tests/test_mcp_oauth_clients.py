import asyncio

import httpx
import pytest
from alembic import command
from cryptography.fernet import Fernet
from sqlalchemy import text

from krim_agent.mcp import (
    MCPOAuthAuth,
    OAuthClientBinding,
    OAuthConfig,
    OAuthError,
    OAuthHTTPRequester,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
    PersistentOAuthClientRegistrationStore,
)
from krim_agent.mcp.discovery import RegisteredClient
from krim_agent.persistence import Database
from krim_agent.persistence.migration import migration_config


def binding(**changes):
    values = {
        "server": "demo",
        "subject": "subject",
        "resource": "https://mcp.example/mcp",
        "issuer": "https://identity.example",
        "redirect_uri": "http://127.0.0.1:9000/callback",
        "scopes": ["tools"],
        "allowed_hosts": {"identity.example", "mcp.example"},
    }
    values.update(changes)
    return OAuthClientBinding(**values)


def config(**changes):
    values = {
        "server": "demo",
        "client_id": "registered-client-identifier",
        "client_secret": "private-client-secret",
        "issuer": "https://identity.example",
        "resource": "https://mcp.example/mcp",
        "authorization_endpoint": "https://identity.example/authorize",
        "token_endpoint": "https://identity.example/token",
        "redirect_uri": "http://127.0.0.1:9000/callback",
        "scopes": ["tools"],
        "allowed_hosts": {"identity.example"},
    }
    values.update(changes)
    return OAuthConfig(**values)


def client():
    return RegisteredClient(
        client_id="client",
        client_secret="private-client-secret",
        issuer="https://identity.example",
        redirect_uris=["http://127.0.0.1:9000/callback"],
        scope="tools",
        token_endpoint_auth_method="client_secret_post",
    )


async def make_store(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'clients.sqlite'}")
    await database.initialize()
    return database, PersistentOAuthClientRegistrationStore(database, Fernet.generate_key())


async def test_registration_reopens_without_plaintext_and_binding_is_strict(tmp_path):
    database, store = await make_store(tmp_path)
    key = store._keys[0]
    try:
        binding_value, oauth = binding(), config()
        async with store.lock(binding_value):
            assert (await store.read(binding_value)).revision == 0
            await store.write(binding_value, oauth, expected_revision=0)
        snapshot = await store.read(binding_value)
        assert (
            snapshot.revision == 1 and snapshot.config.client_id == "registered-client-identifier"
        )
        raw = (tmp_path / "clients.sqlite").read_bytes()
        for secret in (
            "registered-client-identifier",
            "private-client-secret",
            "https://identity.example/token",
        ):
            assert secret.encode() not in raw
        reopened = PersistentOAuthClientRegistrationStore(database, key)
        assert (
            await reopened.read(binding_value)
        ).config.client_secret.get_secret_value() == "private-client-secret"
        assert (await reopened.read(binding(resource="https://mcp.example/other"))).config is None
    finally:
        await database.close()


async def test_0033_upgrade_keeps_resource_selection_and_guards_nonempty_client_downgrade(tmp_path):
    from krim_agent.mcp import MCPManager, MCPSelectionRequest, MCPServerConfig, TrustedMCPServer
    from krim_agent.tools import ToolRegistry

    database, store = await make_store(tmp_path)
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0033_mcp_resource_profiles")
            )
        manager = MCPManager(database, ToolRegistry())
        manager.register(
            TrustedMCPServer(
                MCPServerConfig(name="demo", transport="stdio", command="trusted"), "v1"
            )
        )
        view = await manager.view("demo")
        await manager.configure(
            "demo",
            MCPSelectionRequest(
                expected_revision=0, resource_hash=view.resource_hash, enabled=True
            ),
        )
        await database.initialize()
        assert (await manager.view("demo")).profile.revision == 1
        declared = binding()
        async with store.lock(declared):
            await store.write(declared, config(), expected_revision=0)
        with pytest.raises(RuntimeError, match="DCR客户端"):
            async with database.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(
                        migration_config(sync), "0033_mcp_resource_profiles"
                    )
                )
        assert (await store.read(declared)).revision == 1
        await manager.close()
    finally:
        await database.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"server": "other"},
        {"subject": "other"},
        {"issuer": "https://identity.example/other"},
        {"resource": "https://mcp.example/other"},
        {"redirect_uri": "http://127.0.0.1:9001/callback"},
        {"scopes": ["other"]},
        {"allowed_hosts": {"identity.example"}},
        {"client_name": "another client"},
    ],
)
async def test_declared_binding_change_never_reuses_original_registration(tmp_path, changes):
    database, store = await make_store(tmp_path)
    try:
        original = binding()
        async with store.lock(original):
            await store.write(original, config(), expected_revision=0)
        assert (await store.read(binding(**changes))).config is None
        assert (await store.read(original)).revision == 1
    finally:
        await database.close()


async def test_delete_and_expired_lease_reject_late_registration_write(tmp_path):
    database, store = await make_store(tmp_path)
    original = binding()
    try:
        async with store.lock(original):
            before = await store.read(original)
            await store.delete(original)
            with pytest.raises(OAuthError) as error:
                await store.write(original, config(), expected_revision=before.revision)
            assert error.value.code == "oauth_client_conflict"
        async with store.lock(original):
            before = await store.read(original)
            async with database.engine.begin() as connection:
                await connection.execute(
                    text("UPDATE oauth_client_registrations SET lease_expires_at=0")
                )
            with pytest.raises(OAuthError) as error:
                await store.write(original, config(), expected_revision=before.revision)
            assert error.value.code == "oauth_client_conflict"
        assert (await store.read(original)).config is None
    finally:
        await database.close()


async def test_rotation_rolls_back_all_clients_if_one_payload_is_corrupt(tmp_path):
    database, original_store = await make_store(tmp_path)
    new = Fernet.generate_key()
    store = PersistentOAuthClientRegistrationStore(database, [original_store._keys[0], new])
    one, two = binding(), binding(subject="two")
    try:
        for declared in (one, two):
            async with store.lock(declared):
                await store.write(declared, config(), expected_revision=0)
        async with database.engine.begin() as connection:
            prior = (
                await connection.execute(
                    text(
                        "SELECT ciphertext,revision FROM oauth_client_registrations "
                        "WHERE binding_hash=:hash"
                    ),
                    {"hash": one.digest()},
                )
            ).one()
            await connection.execute(
                text(
                    "UPDATE oauth_client_registrations SET ciphertext=X'00' "
                    "WHERE binding_hash=:hash"
                ),
                {"hash": two.digest()},
            )
        with pytest.raises(OAuthError) as error:
            await store.rotate_key(new)
        assert error.value.code == "oauth_client_corrupt"
        async with database.engine.connect() as connection:
            current = (
                await connection.execute(
                    text(
                        "SELECT ciphertext,revision FROM oauth_client_registrations "
                        "WHERE binding_hash=:hash"
                    ),
                    {"hash": one.digest()},
                )
            ).one()
        assert current == prior
    finally:
        await database.close()


async def test_two_connections_share_one_dcr_lease_and_only_one_write(tmp_path):
    first, store_a = await make_store(tmp_path)
    second = Database(f"sqlite+aiosqlite:///{tmp_path / 'clients.sqlite'}")
    await second.initialize()
    store_b = PersistentOAuthClientRegistrationStore(second, store_a._keys[0])
    calls = 0
    try:
        binding_value, oauth = binding(), config()

        async def register(store):
            nonlocal calls
            async with store.lock(binding_value):
                cached = await store.read(binding_value)
                if cached.config is None:
                    await asyncio.sleep(0.03)
                    calls += 1
                    await store.write(binding_value, oauth, expected_revision=cached.revision)
                return (await store.read(binding_value)).revision

        assert await asyncio.gather(register(store_a), register(store_b)) == [1, 1]
        assert calls == 1
    finally:
        await first.close()
        await second.close()


async def test_cancelled_or_failed_registration_releases_lease_and_does_not_write(tmp_path):
    database, store = await make_store(tmp_path)
    binding_value = binding()
    entered = asyncio.Event()
    try:

        async def blocked():
            async with store.lock(binding_value):
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(blocked())
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with store.lock(binding_value):
            with pytest.raises(OAuthError) as error:
                await store.write(
                    binding_value,
                    config(redirect_uri="http://127.0.0.1:9001/other"),
                    expected_revision=0,
                )
            assert error.value.code == "oauth_client_binding_mismatch"
            assert (await store.read(binding_value)).revision == 0
    finally:
        await database.close()


@pytest.mark.parametrize("damage", ["corrupt", "moved", "wrong-key"])
async def test_corrupt_or_moved_client_is_terminal_until_new_explicit_registration(
    tmp_path, damage
):
    database, store = await make_store(tmp_path)
    key = store._keys[0]
    try:
        binding_value = binding()
        async with store.lock(binding_value):
            await store.write(binding_value, config(), expected_revision=0)
        async with database.engine.begin() as connection:
            if damage == "corrupt":
                await connection.execute(
                    text("UPDATE oauth_client_registrations SET ciphertext=X'00'")
                )
            elif damage == "moved":
                blob = (
                    await connection.execute(
                        text("SELECT ciphertext FROM oauth_client_registrations")
                    )
                ).scalar_one()
        if damage == "moved":
            binding_value = binding(subject="other")
            async with store.lock(binding_value):
                await store.write(binding_value, config(), expected_revision=0)
            async with database.engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE oauth_client_registrations SET ciphertext=:blob "
                        "WHERE binding_hash=:hash"
                    ),
                    {"blob": blob, "hash": binding_value.digest()},
                )
        reader = PersistentOAuthClientRegistrationStore(
            database, Fernet.generate_key() if damage == "wrong-key" else key
        )
        with pytest.raises(OAuthError) as error:
            await reader.read(binding_value)
        assert error.value.code == "oauth_client_corrupt"
    finally:
        await database.close()


async def test_rotation_keeps_binding_and_rolls_back_damage(tmp_path):
    database, store = await make_store(tmp_path)
    old, new = store._keys[0], Fernet.generate_key()
    try:
        binding_value = binding()
        async with store.lock(binding_value):
            await store.write(binding_value, config(), expected_revision=0)
        with pytest.raises(OAuthError) as error:
            await store.rotate_key(Fernet.generate_key())
        assert error.value.code == "rotation_key_missing"
        store = PersistentOAuthClientRegistrationStore(database, [old, new])
        assert await store.rotate_key(new) == 1
        assert (
            await PersistentOAuthClientRegistrationStore(database, new).read(binding_value)
        ).revision == 2
    finally:
        await database.close()


async def test_new_auth_and_database_reopen_reuse_registered_client_and_token_without_dcr(tmp_path):
    from urllib.parse import parse_qs, urlsplit

    url = f"sqlite+aiosqlite:///{tmp_path / 'auth.sqlite'}"
    database = Database(url)
    await database.initialize()
    key, counts, redirect = Fernet.generate_key(), {}, {}
    resource, issuer = "https://mcp.example/mcp", "https://identity.example"

    async def endpoint(request):
        counts[request.url.path] = counts.get(request.url.path, 0) + 1
        path = request.url.path
        if path == "/mcp":
            if request.headers.get("authorization") == "Bearer registered-access":
                return httpx.Response(200, json={"ok": True})
            return httpx.Response(
                401,
                headers={
                    "WWW-Authenticate": (
                        'Bearer resource_metadata="https://mcp.example/'
                        '.well-known/oauth-protected-resource", scope="tools"'
                    )
                },
            )
        if path == "/.well-known/oauth-protected-resource":
            return httpx.Response(
                200,
                json={
                    "resource": resource,
                    "authorization_servers": [issuer],
                    "scopes_supported": ["tools"],
                },
            )
        if path == "/.well-known/oauth-authorization-server":
            return httpx.Response(
                200,
                json={
                    "issuer": issuer,
                    "authorization_endpoint": issuer + "/authorize",
                    "token_endpoint": issuer + "/token",
                    "registration_endpoint": issuer + "/register",
                    "response_types_supported": ["code"],
                    "grant_types_supported": ["authorization_code", "refresh_token"],
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["none"],
                    "scopes_supported": ["tools"],
                },
            )
        if path == "/register":
            body = __import__("json").loads(request.content)
            return httpx.Response(
                201,
                json={
                    "client_id": "persisted-client",
                    "redirect_uris": body["redirect_uris"],
                    "grant_types": body["grant_types"],
                    "response_types": ["code"],
                    "token_endpoint_auth_method": "none",
                },
            )
        assert path == "/token"
        return httpx.Response(
            200,
            json={
                "access_token": "registered-access",
                "refresh_token": "registered-refresh",
                "expires_in": 600,
            },
        )

    async def on_redirect(value):
        redirect["state"] = parse_qs(urlsplit(value).query)["state"][0]

    async def callback():
        return "local-code", redirect["state"]

    try:
        async with OAuthHTTPRequester(
            {"mcp.example", "identity.example"}, transport=httpx.MockTransport(endpoint)
        ) as requester:
            auth = MCPOAuthAuth(
                resource,
                requester=requester,
                tokens=PersistentEncryptedTokenStore(database, key),
                subject="subject",
                server="demo",
                redirect_uri="http://127.0.0.1:9000/callback",
                redirect_handler=on_redirect,
                callback_handler=callback,
                allow_dynamic_registration=True,
                scopes=["tools"],
                states=PersistentEncryptedStateStore(database, key),
                clients=PersistentOAuthClientRegistrationStore(database, key),
            )
            async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(endpoint)) as api:
                assert (await api.post(resource, json={"method": "initialize"})).status_code == 200
        assert counts["/register"] == counts["/token"] == 1
    finally:
        await database.close()
    reopened = Database(url)
    await reopened.initialize()
    try:
        async with OAuthHTTPRequester(
            {"mcp.example", "identity.example"}, transport=httpx.MockTransport(endpoint)
        ) as requester:
            auth = MCPOAuthAuth(
                resource,
                requester=requester,
                tokens=PersistentEncryptedTokenStore(reopened, key),
                subject="subject",
                server="demo",
                redirect_uri="http://127.0.0.1:9000/callback",
                allow_dynamic_registration=True,
                scopes=["tools"],
                clients=PersistentOAuthClientRegistrationStore(reopened, key),
            )
            async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(endpoint)) as api:
                assert (await api.post(resource, json={"method": "initialize"})).status_code == 200
        assert counts["/register"] == counts["/token"] == 1
        assert auth.config.client_id == "persisted-client" and auth.declared_config is None
    finally:
        await reopened.close()
