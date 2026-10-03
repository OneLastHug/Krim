import base64
import hashlib
import time

import httpx
import pytest
from cryptography.fernet import Fernet

from krim_agent.mcp.oauth import (
    EncryptedTokenStore,
    OAuthClient,
    OAuthConfig,
    OAuthError,
    OAuthHTTPRequester,
    OAuthStateStore,
)


def config(**changes):
    values = {
        "server": "demo",
        "client_id": "client",
        "authorization_endpoint": "http://127.0.0.1:8766/authorize",
        "token_endpoint": "http://127.0.0.1:8766/token",
        "redirect_uri": "http://127.0.0.1:8766/callback",
        "scopes": ["tools"],
    }
    values.update(changes)
    return OAuthConfig(**values)


async def test_pkce_state_has_s256_and_is_one_time_with_fixed_redirect():
    states = OAuthStateStore()
    request = states.begin(config())
    query = request.authorization_url.split("?", 1)[1]
    params = dict(item.split("=", 1) for item in query.split("&"))
    expected = (
        base64.urlsafe_b64encode(hashlib.sha256(request.code_verifier.encode()).digest())
        .rstrip(b"=")
        .decode()
    )
    assert params["code_challenge"] == expected and params["code_challenge_method"] == "S256"
    assert params["redirect_uri"] == "http%3A%2F%2F127.0.0.1%3A8766%2Fcallback"
    assert states.consume(request.state)[0].server == "demo"
    with pytest.raises(OAuthError) as error:
        states.consume(request.state)
    assert error.value.code == "invalid_state"


async def test_exchange_refresh_encrypted_store_and_secret_never_plaintext():
    states = OAuthStateStore()
    store = EncryptedTokenStore(Fernet.generate_key())
    client = OAuthClient(states, store)
    request = states.begin(config())
    calls = []

    async def token_endpoint(url, form):
        calls.append((url, form))
        return httpx.Response(
            200, json={"access_token": "access", "refresh_token": "refresh", "expires_in": 60}
        )

    token = await client.exchange(request.state, "code", "subject", token_endpoint)
    assert token.access_token.get_secret_value() == "access"
    assert b"access" not in store._values[("demo", "subject")]
    stored = store.get("demo", "subject")
    assert stored.access_token.get_secret_value() == "access"
    assert stored.refresh_token.get_secret_value() == "refresh"
    refreshed = await client.refresh(config(), "subject", token_endpoint)
    assert refreshed.refresh_token.get_secret_value() == "refresh"
    assert [form["grant_type"] for _url, form in calls] == ["authorization_code", "refresh_token"]


async def test_error_response_expiry_and_endpoint_boundaries():
    with pytest.raises(OAuthError, match="HTTPS"):
        OAuthConfig(
            server="demo",
            client_id="client",
            authorization_endpoint="http://example.com/a",
            token_endpoint="https://example.com/t",
            redirect_uri="https://example.com/c",
        ).validate_endpoints()
    with pytest.raises(OAuthError) as error:
        OAuthConfig(
            server="demo",
            client_id="client",
            authorization_endpoint="https://example.com/a#bad",
            token_endpoint="https://example.com/t",
            redirect_uri="https://example.com/c",
        ).validate_endpoints()
    assert error.value.code == "invalid_oauth_endpoint"
    states = OAuthStateStore()
    request = states.begin(config(state_ttl_seconds=30))
    states._states[request.state] = (
        config(state_ttl_seconds=30),
        request.__class__(
            request.state, request.code_verifier, request.authorization_url, time.time() - 1000
        ),
    )
    with pytest.raises(OAuthError) as error:
        states.consume(request.state)
    assert error.value.code == "expired_state"
    store = EncryptedTokenStore(Fernet.generate_key())
    with pytest.raises(OAuthError) as error:
        await OAuthClient(states, store).refresh(config(), "missing", lambda *_: None)
    assert error.value.code == "refresh_unavailable"


async def test_http_requester_redirect_and_allowlist_have_real_token_flow():
    seen = []

    async def endpoint(request):
        seen.append(request)
        if request.url.path == "/redirect":
            return httpx.Response(302, headers={"Location": "https://evil.test/token"})
        return httpx.Response(
            200, json={"access_token": "fresh", "token_type": "Bearer", "expires_in": 60}
        )

    states = OAuthStateStore()
    store = EncryptedTokenStore(Fernet.generate_key())
    client = OAuthClient(states, store)
    async with OAuthHTTPRequester(
        {"localhost"}, transport=httpx.MockTransport(endpoint)
    ) as requester:
        configured = config(
            authorization_endpoint="http://localhost/authorize",
            token_endpoint="http://localhost/token",
        )
        attempt = states.begin(configured)
        token = await client.exchange(attempt.state, "code", "user", requester)
        assert token.access_token.get_secret_value() == "fresh"
        assert b"code_verifier" in seen[0].content
        assert (await requester("http://localhost/redirect", {})).status_code == 302
        assert len(seen) == 2
        with pytest.raises(OAuthError, match="白名单"):
            await requester("https://evil.test/token", {})
        assert len(seen) == 2
