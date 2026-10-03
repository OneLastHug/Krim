import json
from urllib.parse import parse_qs

import httpx
import pytest
from cryptography.fernet import Fernet

from krim_agent.mcp import (
    EncryptedTokenStore,
    MCPAuthError,
    MCPOAuthAuth,
    OAuthConfig,
    OAuthHTTPRequester,
    PersistentEncryptedTokenStore,
)
from krim_agent.persistence import Database

RESOURCE = "https://mcp.example/mcp"
ISSUER = "https://identity.example"


def static_config():
    return OAuthConfig(
        server="demo",
        client_id="client",
        issuer=ISSUER,
        resource=RESOURCE,
        authorization_endpoint=ISSUER + "/authorize",
        token_endpoint=ISSUER + "/token",
        redirect_uri="http://127.0.0.1:9000/callback",
        scopes=["tools"],
        allowed_hosts={"identity.example"},
    )


def challenge():
    return {
        "WWW-Authenticate": (
            'Bearer resource_metadata="https://mcp.example/'
            '.well-known/oauth-protected-resource", scope="tools"'
        )
    }


async def test_static_auth_replays_exact_mcp_request_after_pkce_callback():
    seen = []
    redirect = {}

    async def endpoint(request):
        seen.append((request.url.path, request.headers.get("authorization"), request.content))
        if request.url.path == "/token":
            form = parse_qs(request.content.decode())
            assert form["client_id"] == ["client"] and form["code"] == ["local-code"]
            return httpx.Response(
                200, json={"access_token": "access", "refresh_token": "refresh", "expires_in": 600}
            )
        if request.url.path == "/mcp" and request.headers.get("authorization") != "Bearer access":
            return httpx.Response(401, headers=challenge())
        return httpx.Response(200, json={"ok": True, "body": request.content.decode()})

    async with OAuthHTTPRequester(
        {"identity.example", "mcp.example"},
        transport=httpx.MockTransport(endpoint),
    ) as requester:

        async def on_redirect(url):
            redirect["state"] = parse_qs(url.split("?", 1)[1])["state"][0]
            assert "code_challenge_method=S256" in url

        async def callback():
            return "local-code", redirect["state"]

        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            subject="subject",
            server="demo",
            config=static_config(),
            redirect_handler=on_redirect,
            callback_handler=callback,
        )
        async with httpx.AsyncClient(
            auth=auth, transport=httpx.MockTransport(endpoint), follow_redirects=False
        ) as client:
            payload = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
            response = await client.post(RESOURCE, json=payload)
            assert response.status_code == 200 and json.loads(response.json()["body"]) == payload
        assert [item[1] for item in seen if item[0] == "/mcp"] == [None, "Bearer access"]
        assert auth.last_error_code is None
        token_form = next(body for path, _header, body in seen if path == "/token")
        assert parse_qs(token_form.decode())["resource"] == [RESOURCE]


async def test_dynamic_auth_discovers_resource_metadata_dcr_and_retries_once():
    seen = []
    state = {}

    async def endpoint(request):
        seen.append((request.method, request.url.path, request.headers.get("authorization")))
        if request.url.path == "/mcp":
            if request.headers.get("authorization") != "Bearer dynamic":
                return httpx.Response(401, headers=challenge())
            return httpx.Response(200, json={"ok": True})
        if request.url.path == "/.well-known/oauth-protected-resource":
            return httpx.Response(
                200,
                json={
                    "resource": RESOURCE,
                    "authorization_servers": [ISSUER],
                    "scopes_supported": ["tools"],
                    "bearer_methods_supported": ["header"],
                },
            )
        if request.url.path == "/.well-known/oauth-authorization-server":
            return httpx.Response(
                200,
                json={
                    "issuer": ISSUER,
                    "authorization_endpoint": ISSUER + "/authorize",
                    "token_endpoint": ISSUER + "/token",
                    "registration_endpoint": ISSUER + "/register",
                    "response_types_supported": ["code"],
                    "grant_types_supported": ["authorization_code", "refresh_token"],
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["none"],
                    "scopes_supported": ["tools"],
                },
            )
        if request.url.path == "/register":
            body = json.loads(request.content)
            assert body["redirect_uris"] == ["http://127.0.0.1:9000/callback"]
            return httpx.Response(
                201,
                json={
                    "client_id": "dynamic-client",
                    "redirect_uris": body["redirect_uris"],
                    "grant_types": body["grant_types"],
                    "response_types": body["response_types"],
                    "token_endpoint_auth_method": "none",
                },
            )
        if request.url.path == "/token":
            form = parse_qs(request.content.decode())
            assert form["client_id"] == ["dynamic-client"]
            return httpx.Response(
                200,
                json={
                    "access_token": "dynamic",
                    "refresh_token": "dynamic-refresh",
                    "expires_in": 600,
                },
            )
        raise AssertionError(request.url)

    async with OAuthHTTPRequester(
        {"identity.example", "mcp.example"},
        transport=httpx.MockTransport(endpoint),
    ) as requester:

        async def redirect(url):
            state["state"] = parse_qs(url.split("?", 1)[1])["state"][0]

        async def callback():
            return "code", state["state"]

        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            subject="subject",
            server="demo",
            redirect_uri="http://127.0.0.1:9000/callback",
            redirect_handler=redirect,
            callback_handler=callback,
            allow_dynamic_registration=True,
            scopes=["tools"],
        )
        async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(endpoint)) as client:
            response = await client.post(
                RESOURCE, json={"jsonrpc": "2.0", "id": 1, "method": "initialize"}
            )
            assert response.status_code == 200
        paths = [path for _method, path, _auth in seen]
        assert paths.count("/mcp") == 2
        assert "/.well-known/oauth-protected-resource" in paths
        assert "/.well-known/oauth-authorization-server" in paths
        assert "/register" in paths and "/token" in paths


async def test_auth_rejects_without_callback_and_second_401():
    calls = 0

    async def endpoint(request):
        nonlocal calls
        calls += 1
        return httpx.Response(401, headers=challenge())

    async with OAuthHTTPRequester(
        {"identity.example", "mcp.example"},
        transport=httpx.MockTransport(endpoint),
    ) as requester:
        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            subject="subject",
            server="demo",
            config=static_config(),
        )
        async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(endpoint)) as client:
            with pytest.raises(MCPAuthError) as error:
                await client.post(RESOURCE, json={"jsonrpc": "2.0"})
        assert error.value.code == "oauth_interaction_required" and calls == 1


async def test_persistent_token_is_loaded_without_401_or_callback(tmp_path):
    from krim_agent.mcp import OAuthToken

    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'tokens.sqlite'}")
    await database.initialize()
    try:
        store = PersistentEncryptedTokenStore(database, Fernet.generate_key())
        config = static_config()
        await store.write(config, "subject", OAuthToken(access_token="stored", expires_at=10**12))
        async with OAuthHTTPRequester(
            {"identity.example"}, transport=httpx.MockTransport(lambda _: httpx.Response(500))
        ) as requester:
            auth = MCPOAuthAuth(
                RESOURCE,
                requester=requester,
                tokens=store,
                subject="subject",
                server="demo",
                config=config,
            )

            def resource(request):
                assert request.headers["authorization"] == "Bearer stored"
                return httpx.Response(200, json={"ok": True})

            async with httpx.AsyncClient(
                auth=auth, transport=httpx.MockTransport(resource)
            ) as client:
                assert (await client.get(RESOURCE)).json() == {"ok": True}
    finally:
        await database.close()


@pytest.mark.parametrize(
    "kind,code",
    [
        ("wrong-state", "oauth_state_mismatch"),
        ("repeat-401", "oauth_unauthorized"),
        ("timeout", "oauth_timeout"),
        ("no-bearer", "oauth_challenge_required"),
    ],
)
async def test_login_failure_never_reports_success_and_discards_state(kind, code):
    import asyncio

    attempts, grants, state = [], [], {}

    def endpoint(request):
        if request.url.path == "/token":
            grants.append(request)
            return httpx.Response(200, json={"access_token": "fresh", "expires_in": 600})
        attempts.append(request)
        headers = {} if kind == "no-bearer" else challenge()
        return httpx.Response(401, headers=headers)

    async with OAuthHTTPRequester(
        {"identity.example", "mcp.example"}, transport=httpx.MockTransport(endpoint)
    ) as requester:

        async def redirect(url):
            state["value"] = parse_qs(url.split("?", 1)[1])["state"][0]

        async def callback():
            if kind == "timeout":
                await asyncio.Event().wait()
            return "code", "wrong" if kind == "wrong-state" else state["value"]

        store = EncryptedTokenStore(Fernet.generate_key())
        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=store,
            server="demo",
            subject="subject",
            config=static_config(),
            redirect_handler=redirect,
            callback_handler=callback,
            timeout_seconds=0.05,
        )
        async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(endpoint)) as client:
            with pytest.raises(MCPAuthError) as error:
                await client.post(RESOURCE, json={"method": "initialize", "id": 1})
        assert error.value.code == code and auth.last_error_code == code
        assert len(attempts) == (2 if kind == "repeat-401" else 1)
        assert len(grants) == (1 if kind == "repeat-401" else 0)
        assert not auth.states._states


@pytest.mark.parametrize(
    "url",
    [
        "https://other.example/mcp",
        "https://mcp.example/other",
        "https://mcp.example:8443/mcp",
        "https://mcp.example/mcp?other=1",
    ],
)
async def test_bearer_never_leaves_fixed_endpoint(url):
    async def never(_request):
        pytest.fail("固定 endpoint 之外不能发送凭据")

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(never)
    ) as requester:
        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            server="demo",
            subject="subject",
            config=static_config(),
        )
        async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(never)) as client:
            with pytest.raises(MCPAuthError) as error:
                await client.get(url)
            assert error.value.code == "oauth_resource_mismatch"


async def test_valid_sse_is_streamed_and_401_body_is_discarded_without_reading():
    from krim_agent.mcp import OAuthToken

    class Stream(httpx.AsyncByteStream):
        def __init__(self):
            self.read = False
            self.closed = False

        async def __aiter__(self):
            self.read = True
            yield b"data: first\\n\\n"

        async def aclose(self):
            self.closed = True

    stream = Stream()
    store = EncryptedTokenStore(Fernet.generate_key())
    await store.write(static_config(), "subject", OAuthToken(access_token="stored"))
    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(lambda _: httpx.Response(500))
    ) as requester:
        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=store,
            server="demo",
            subject="subject",
            config=static_config(),
        )
        async with httpx.AsyncClient(
            auth=auth,
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, headers={"content-type": "text/event-stream"}, stream=stream
                )
            ),
        ) as client:
            async with client.stream("GET", RESOURCE) as response:
                assert not stream.read
                assert (await anext(response.aiter_bytes())).startswith(b"data:")
        assert stream.closed

    denied = Stream()
    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(lambda _: httpx.Response(500))
    ) as requester:
        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            server="demo",
            subject="subject",
            config=static_config(),
        )
        async with httpx.AsyncClient(
            auth=auth,
            transport=httpx.MockTransport(
                lambda _: httpx.Response(401, headers=challenge(), stream=denied)
            ),
        ) as client:
            with pytest.raises(MCPAuthError):
                await client.get(RESOURCE)
        assert denied.closed and not denied.read


async def test_concurrent_initializations_share_one_login():
    import asyncio

    state, logins, entered, release = {}, [], asyncio.Event(), asyncio.Event()

    def endpoint(request):
        if request.url.path == "/token":
            return httpx.Response(200, json={"access_token": "shared", "expires_in": 600})
        if request.headers.get("authorization") == "Bearer shared":
            return httpx.Response(200)
        return httpx.Response(401, headers=challenge())

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(endpoint)
    ) as requester:

        async def redirect(url):
            logins.append(url)
            state["value"] = parse_qs(url.split("?", 1)[1])["state"][0]
            entered.set()
            await release.wait()

        async def callback():
            return "code", state["value"]

        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            server="demo",
            subject="subject",
            config=static_config(),
            redirect_handler=redirect,
            callback_handler=callback,
        )
        async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(endpoint)) as client:
            tasks = [
                asyncio.create_task(
                    client.post(RESOURCE, json={"method": "initialize", "id": index})
                )
                for index in range(3)
            ]
            try:
                await asyncio.wait_for(entered.wait(), 3)
                release.set()
                assert all(response.status_code == 200 for response in await asyncio.gather(*tasks))
                assert len(logins) == 1
            finally:
                release.set()
                await asyncio.gather(*tasks, return_exceptions=True)


async def test_cancelled_login_drops_pending_state_and_does_not_exchange_token():
    import asyncio

    entered, grants = asyncio.Event(), []

    def endpoint(request):
        if request.url.path == "/token":
            grants.append(request)
            return httpx.Response(200, json={"access_token": "never"})
        return httpx.Response(401, headers=challenge())

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(endpoint)
    ) as requester:

        async def redirect(_url):
            entered.set()
            await asyncio.Event().wait()

        async def callback():
            return "never", "never"

        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            server="demo",
            subject="subject",
            config=static_config(),
            redirect_handler=redirect,
            callback_handler=callback,
        )
        async with httpx.AsyncClient(auth=auth, transport=httpx.MockTransport(endpoint)) as client:
            task = asyncio.create_task(client.post(RESOURCE, json={"method": "initialize"}))
            await asyncio.wait_for(entered.wait(), 3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not grants and not auth.states._states


async def test_connection_initialization_cancel_and_timeout_reclaim_owner():
    import asyncio

    from krim_agent.mcp import MCPConnection, MCPServerConfig

    entered, closed = asyncio.Event(), asyncio.Event()

    class Session:
        async def initialize(self):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.set()

    async def factory():
        return Session()

    connection = MCPConnection(
        MCPServerConfig(
            name="slow", transport="stdio", command="fake", request_timeout_seconds=0.1
        ),
        session_factory=factory,
    )
    task = asyncio.create_task(connection.connect())
    await asyncio.wait_for(entered.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set() and connection._owner.done()
    await connection.close()

    entered.clear()
    closed.clear()
    connection = MCPConnection(
        MCPServerConfig(
            name="slow", transport="stdio", command="fake", request_timeout_seconds=0.01
        ),
        session_factory=factory,
    )
    with pytest.raises(TimeoutError):
        await connection.connect()
    assert closed.is_set() and connection._owner.done()


async def test_auth_transport_rejects_conflicting_headers_stdio_and_endpoint():
    from pydantic import SecretStr

    from krim_agent.mcp import MCPConnection, MCPConnectionError, MCPServerConfig

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(lambda _: httpx.Response(500))
    ) as requester:
        auth = MCPOAuthAuth(
            RESOURCE,
            requester=requester,
            tokens=EncryptedTokenStore(Fernet.generate_key()),
            server="demo",
            subject="subject",
            config=static_config(),
        )
        for configured in (
            MCPServerConfig(name="demo", transport="stdio", command="fake"),
            MCPServerConfig(
                name="other",
                transport="streamable_http",
                url=RESOURCE,
                allowed_hosts={"mcp.example"},
            ),
            MCPServerConfig(
                name="demo",
                transport="streamable_http",
                url="https://mcp.example/other",
                allowed_hosts={"mcp.example"},
            ),
            MCPServerConfig(
                name="demo",
                transport="streamable_http",
                url=RESOURCE,
                headers={"Authorization": SecretStr("static")},
                allowed_hosts={"mcp.example"},
            ),
        ):
            with pytest.raises(MCPConnectionError):
                MCPConnection(configured, auth=auth)
