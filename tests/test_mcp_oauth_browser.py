from urllib.parse import parse_qs, urlsplit

import httpx
from cryptography.fernet import Fernet
from pydantic import SecretStr
from sqlalchemy import text

from krim_agent.api import create_app
from krim_agent.mcp import (
    MCPOAuthAuth,
    MCPServerConfig,
    OAuthConfig,
    OAuthHTTPRequester,
    PersistentEncryptedStateStore,
    PersistentEncryptedTokenStore,
    TrustedMCPServer,
)
from krim_agent.mcp.browser import CALLBACK_PATH, PersistentOAuthBrowserCallbackStore, cookie_name
from krim_agent.persistence import Database
from krim_agent.protocol.domain import ServiceError


async def fixture(runtime, origin="http://127.0.0.1"):
    database, key = runtime.repository.database, Fernet.generate_key()
    runtime.settings.api_key = SecretStr("browser-oauth-test-key")
    config = OAuthConfig(
        server="demo",
        client_id="web-client",
        client_secret="private-client-secret",
        issuer="https://identity.example",
        resource="https://mcp.example/mcp",
        authorization_endpoint="https://identity.example/authorize",
        token_endpoint="https://identity.example/token",
        redirect_uri=origin + CALLBACK_PATH,
        scopes=["tools"],
        allowed_hosts={"identity.example"},
    )
    calls = []

    async def token_endpoint(request):
        form = parse_qs(request.content.decode())
        calls.append(form)
        assert form["redirect_uri"] == [config.redirect_uri] and form["client_id"] == ["web-client"]
        return httpx.Response(
            200,
            json={
                "access_token": "browser-access",
                "refresh_token": "browser-refresh",
                "expires_in": 600,
            },
        )

    requester = OAuthHTTPRequester(
        {"identity.example", "mcp.example"}, transport=httpx.MockTransport(token_endpoint)
    )
    states, tokens = (
        PersistentEncryptedStateStore(database, key),
        PersistentEncryptedTokenStore(database, key),
    )
    auth = MCPOAuthAuth(
        config.resource,
        requester=requester,
        tokens=tokens,
        states=states,
        subject="api-subject",
        server="demo",
        config=config,
    )
    app = create_app(
        runtime=runtime,
        oauth_browser_key=key,
        mcp_servers=[
            TrustedMCPServer(
                MCPServerConfig(
                    name="demo",
                    transport="streamable_http",
                    url=config.resource,
                    allowed_hosts={"mcp.example"},
                ),
                "v1",
                auth=auth,
            )
        ],
    )
    return app, auth, requester, tokens, calls, config


async def begin(api):
    api.headers["Authorization"] = "Bearer browser-oauth-test-key"
    resource = (await api.get("/v1/mcp/servers")).json()[0]
    await api.put(
        "/v1/mcp/servers/demo",
        json={"expected_revision": 0, "resource_hash": resource["resource_hash"], "enabled": True},
    )
    response = await api.post(
        "/v1/mcp/servers/demo/oauth/begin",
        json={"expected_revision": 1, "resource_hash": resource["resource_hash"]},
    )
    assert response.status_code == 200, response.text
    view = response.json()
    state = parse_qs(urlsplit(view["authorization_url"]).query)["state"][0]
    return view, state, response


async def test_browser_cookie_original_state_exchange_and_clean_redirect(runtime):
    app, auth, requester, tokens, calls, config = await fixture(runtime)
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            view, state, response = await begin(api)
            assert (
                "HttpOnly" in response.headers["set-cookie"]
                and "SameSite=lax" in response.headers["set-cookie"]
            )
            assert response.headers["cache-control"] == "no-store"
            result = await api.get(
                CALLBACK_PATH,
                params={"state": state, "code": "private-browser-code"},
                headers={"Authorization": ""},
            )
            assert result.status_code == 303, result.text
            assert (
                "code=" not in result.headers["location"]
                and state not in result.headers["location"]
            )
            assert result.headers["referrer-policy"] == "no-referrer"
            saved = (
                await api.get(f"/v1/mcp/servers/demo/oauth/attempts/{view['attempt_id']}")
            ).json()
            assert saved["phase"] == "complete" and saved["authorization_url"] is None
            assert (
                len(calls) == 1
                and (await tokens.read(config, auth.subject)).token.access_token.get_secret_value()
                == "browser-access"
            )
            repeat = await api.get(
                CALLBACK_PATH,
                params={"state": state, "code": "private-browser-code"},
                headers={"Authorization": ""},
            )
            assert repeat.status_code in {400, 403, 409} and len(calls) == 1
    finally:
        await requester.close()


async def test_wrong_or_missing_cookie_cannot_consume_original_intent(runtime):
    app, auth, requester, tokens, calls, config = await fixture(runtime)
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            view, state, _response = await begin(api)
            correct = api.cookies.get(cookie_name(view["attempt_id"]))
            api.cookies.clear()
            for value in (None, "B" * 43):
                if value is not None:
                    api.cookies.set(cookie_name(view["attempt_id"]), value, path=CALLBACK_PATH)
                result = await api.get(
                    CALLBACK_PATH,
                    params={"state": state, "code": "code"},
                    headers={"Authorization": ""},
                )
                assert (
                    result.status_code == 403 and result.headers["referrer-policy"] == "no-referrer"
                )
            assert (
                await api.get(f"/v1/mcp/servers/demo/oauth/attempts/{view['attempt_id']}")
            ).json()["phase"] == "pending"
            assert not calls and (await tokens.read(config, auth.subject)).token is None
            api.cookies.clear()
            api.cookies.set(cookie_name(view["attempt_id"]), correct, path=CALLBACK_PATH)
            assert (
                await api.get(
                    CALLBACK_PATH,
                    params={"state": state, "code": "code"},
                    headers={"Authorization": ""},
                )
            ).status_code == 303
    finally:
        await requester.close()


async def test_cancel_denied_error_and_other_api_authorization_are_stable(runtime):
    app, _auth, requester, _tokens, calls, _config = await fixture(runtime)
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            view, state, _response = await begin(api)
            cancel = await api.post(
                f"/v1/mcp/servers/demo/oauth/attempts/{view['attempt_id']}/cancel"
            )
            assert cancel.json()["phase"] == "cancelled"
            assert (
                await api.get(
                    CALLBACK_PATH,
                    params={"state": state, "code": "code"},
                    headers={"Authorization": ""},
                )
            ).status_code in {400, 403, 409}
            resource = (await api.get("/v1/mcp/servers/demo")).json()
            response = await api.post(
                "/v1/mcp/servers/demo/oauth/begin",
                json={"expected_revision": 1, "resource_hash": resource["resource_hash"]},
            )
            second = response.json()
            state = parse_qs(urlsplit(second["authorization_url"]).query)["state"][0]
            returned = await api.get(
                CALLBACK_PATH,
                params={
                    "state": state,
                    "error": "access_denied",
                    "error_description": "private-provider-description",
                },
                headers={"Authorization": ""},
            )
            assert (
                returned.status_code == 303 and "private-provider-description" not in returned.text
            )
            saved = (
                await api.get(f"/v1/mcp/servers/demo/oauth/attempts/{second['attempt_id']}")
            ).json()
            assert (
                saved["phase"] == "failed" and saved["error_code"] == "oauth_authorization_denied"
            )
            assert not calls
            assert (
                await api.get("/v1/mcp/servers", headers={"Authorization": ""})
            ).status_code == 401
            assert (await api.post(CALLBACK_PATH, headers={"Authorization": ""})).status_code == 401
    finally:
        await requester.close()


async def test_resource_revision_change_rejects_code_and_cleans_original_pkce(runtime):
    app, _auth, requester, _tokens, calls, _config = await fixture(runtime)
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            view, state, _response = await begin(api)
            resource = (await api.get("/v1/mcp/servers/demo")).json()
            await api.put(
                "/v1/mcp/servers/demo",
                json={
                    "expected_revision": 1,
                    "resource_hash": resource["resource_hash"],
                    "enabled": False,
                },
            )
            returned = await api.get(
                CALLBACK_PATH,
                params={"state": state, "code": "code"},
                headers={"Authorization": ""},
            )
            assert returned.status_code == 303 and not calls
            saved = (
                await api.get(f"/v1/mcp/servers/demo/oauth/attempts/{view['attempt_id']}")
            ).json()
            assert saved["phase"] == "failed" and saved["error_code"] == "mcp_resource_disabled"
            async with runtime.repository.database.engine.connect() as connection:
                rows = (
                    await connection.execute(text("SELECT status,ciphertext FROM oauth_states"))
                ).all()
            assert all(row.status == "cancelled" and row.ciphertext is None for row in rows)
    finally:
        await requester.close()


async def test_query_is_scrubbed_before_asgi_access_log_and_early_origin_rejection(runtime):
    app, _auth, requester, _tokens, _calls, _config = await fixture(runtime)
    observed = []

    async def audited(scope, receive, send):
        async def record(message):
            if message["type"] == "http.response.start":
                observed.append(scope.get("query_string"))
            await send(message)

        await app(scope, receive, record)

    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=audited), base_url="http://127.0.0.1"
            ) as api,
        ):
            response = await api.get(
                CALLBACK_PATH,
                params={"state": "A" * 43, "code": "private-code-log"},
                headers={"Origin": "https://other.invalid"},
            )
            assert response.status_code == 403
            assert observed == [b""] and response.headers["cache-control"] == "no-store"
    finally:
        await requester.close()


async def test_two_database_claimants_one_winner_and_exchanging_cannot_be_cancelled(runtime):
    app, _auth, requester, tokens, _calls, _config = await fixture(runtime)
    other = Database(str(runtime.repository.database.engine.url))
    await other.initialize()
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            view, state, _response = await begin(api)
            secret = api.cookies.get(cookie_name(view["attempt_id"]))
            second = PersistentOAuthBrowserCallbackStore(other, tokens._keys[0])
            results = await __import__("asyncio").gather(
                app.state.oauth_browser.store.claim(state, secret),
                second.claim(state, secret),
                return_exceptions=True,
            )
            assert (
                sum(
                    isinstance(value, ServiceError) and value.code == "oauth_browser_binding"
                    for value in results
                )
                == 1
            )
            owner, intent = next(value for value in results if isinstance(value, tuple))
            refused = await api.post(
                f"/v1/mcp/servers/demo/oauth/attempts/{view['attempt_id']}/cancel"
            )
            assert refused.status_code == 409
            await app.state.oauth_browser.store.finish(
                intent, owner, error_code="oauth_authorization_interrupted"
            )
            await app.state.oauth_browser.store.discard_state(intent)
    finally:
        await other.close()
        await requester.close()


async def test_original_ttl_and_unknown_state_are_rejected_without_exchange(runtime, monkeypatch):
    app, _auth, requester, _tokens, calls, _config = await fixture(runtime)
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            view, state, _response = await begin(api)
            unknown = await api.get(
                CALLBACK_PATH,
                params={"state": "A" * 43, "code": "code"},
                headers={"Authorization": ""},
            )
            assert unknown.status_code == 400
            with monkeypatch.context() as clock:
                clock.setattr("time.time", lambda: view["expires_at"] + 1)
                expired = (
                    await api.get(f"/v1/mcp/servers/demo/oauth/attempts/{view['attempt_id']}")
                ).json()
                assert expired["phase"] == "expired" and expired["authorization_url"] is None
                result = await api.get(
                    CALLBACK_PATH,
                    params={"state": state, "code": "code"},
                    headers={"Authorization": ""},
                )
                assert result.status_code == 403 and not calls
    finally:
        await requester.close()
