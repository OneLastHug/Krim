import asyncio
import json
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from cryptography.fernet import Fernet

from krim_agent.mcp import (
    EncryptedTokenStore,
    OAuthClient,
    OAuthDiscoveryClient,
    OAuthError,
    OAuthHTTPRequester,
    OAuthMetadata,
    OAuthStateStore,
    validate_oauth_endpoint,
)

ISSUER = "https://identity.example/tenant"
REDIRECT = "http://127.0.0.1:9000/callback"


def metadata(**changes):
    values = {
        "issuer": ISSUER,
        "authorization_endpoint": "https://identity.example/authorize",
        "token_endpoint": "https://identity.example/token",
        "registration_endpoint": "https://identity.example/register",
        "response_types_supported": ["code"],
        "code_challenge_methods_supported": ["S256"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
        "scopes_supported": ["tools"],
    }
    values.update(changes)
    return values


def registration(**changes):
    values = {
        "client_id": "registered",
        "redirect_uris": [REDIRECT],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "issuer": "https://ignored-wire-issuer.example",
    }
    values.update(changes)
    return values


async def test_discovery_registration_configuration_and_pkce_exchange_are_consumed():
    requests = []

    async def endpoint(request):
        requests.append(request)
        if request.method == "GET":
            assert request.url.path == "/.well-known/oauth-authorization-server/tenant"
            assert "Authorization" not in request.headers
            return httpx.Response(200, json=metadata())
        if request.url.path == "/register":
            payload = json.loads(request.content)
            assert payload["redirect_uris"] == [REDIRECT] and payload["scope"] == "tools"
            assert payload["client_name"] == "Krim Agent"
            return httpx.Response(201, json=registration())
        form = parse_qs(request.content.decode())
        assert form["client_id"] == ["registered"]
        assert form["redirect_uri"] == [REDIRECT]
        assert len(form["code_verifier"][0]) >= 43
        return httpx.Response(
            200, json={"access_token": "access", "refresh_token": "refresh", "expires_in": 600}
        )

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(endpoint)
    ) as request:
        discovery = OAuthDiscoveryClient(request)
        server = await discovery.discover(ISSUER)
        client = await discovery.register(server, redirect_uri=REDIRECT, scopes=["tools"])
        assert client.issuer == ISSUER
        configured = discovery.configuration(server, client, server="demo")
        assert configured.issuer == ISSUER
        assert configured.scopes == ["tools"]
        states = OAuthStateStore()
        attempt = states.begin(configured)
        query = parse_qs(urlsplit(attempt.authorization_url).query)
        assert query["client_id"] == ["registered"] and query["code_challenge_method"] == ["S256"]
        tokens = EncryptedTokenStore(Fernet.generate_key())
        received = await OAuthClient(states, tokens).exchange(
            attempt.state, "code", "subject", request
        )
        assert received.access_token.get_secret_value() == "access"
        assert len(requests) == 3


async def test_oidc_discovery_fallback_keeps_issuer_path():
    seen = []

    async def endpoint(request):
        seen.append(request.url.path)
        return httpx.Response(404) if len(seen) < 3 else httpx.Response(200, json=metadata())

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(endpoint)
    ) as request:
        server = await OAuthDiscoveryClient(request).discover(ISSUER)
        assert server.issuer == ISSUER
    assert seen == [
        "/.well-known/oauth-authorization-server/tenant",
        "/.well-known/openid-configuration/tenant",
        "/tenant/.well-known/openid-configuration",
    ]


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"issuer": "https://identity.example/other"}, "oauth_issuer_mismatch"),
        ({"issuer": "https://["}, "oauth_issuer_mismatch"),
        ({"token_endpoint": "https://untrusted.example/token"}, "oauth_host_denied"),
        ({"authorization_endpoint": "http://identity.example/auth"}, "insecure_oauth_endpoint"),
        ({"code_challenge_methods_supported": ["plain"]}, "unsupported_oauth_metadata"),
        ({"code_challenge_methods_supported": None}, "unsupported_oauth_metadata"),
        ({"response_types_supported": ["token"]}, "unsupported_oauth_metadata"),
        ({"grant_types_supported": ["authorization_code"]}, "unsupported_oauth_metadata"),
        (
            {"token_endpoint": "https://user:secret@identity.example/token"},
            "invalid_oauth_endpoint",
        ),
    ],
)
async def test_metadata_mismatch_and_unsupported_capabilities_fail_closed(changes, code):
    async with OAuthHTTPRequester(
        {"identity.example"},
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=metadata(**changes))
        ),
    ) as request:
        with pytest.raises(OAuthError) as error:
            await OAuthDiscoveryClient(request).discover(ISSUER)
        assert error.value.code == code and "secret" not in str(error.value)


@pytest.mark.parametrize(
    "url",
    [
        "https://identity.example:bad/token",
        "https://identity.example/token?",
        "https://identity.example/token#",
        "https://identity.example/\n",
        "https://user@identity.example/token",
        "file:///token",
    ],
)
def test_shared_endpoint_validator_rejects_ambiguous_urls(url):
    with pytest.raises(OAuthError):
        validate_oauth_endpoint(url, {"identity.example"})


async def test_remote_redirect_error_and_invalid_json_never_expose_body():
    responses = [
        (httpx.Response(302, headers={"Location": "https://evil.example/steal"}), "oauth_redirect"),
        (httpx.Response(500, content=b"private secret"), "metadata_http_error"),
        (httpx.Response(200, json=[]), "invalid_oauth_metadata"),
        (httpx.Response(200, content=b"not-json"), "invalid_oauth_metadata"),
        (httpx.Response(200, content=b"x" * 2000), "metadata_response_limit"),
    ]
    for response, code in responses:
        async with OAuthHTTPRequester(
            {"identity.example"},
            transport=httpx.MockTransport(lambda _request, value=response: value),
        ) as request:
            with pytest.raises(OAuthError) as error:
                await OAuthDiscoveryClient(request, max_response_bytes=1024).discover(ISSUER)
            assert error.value.code == code and "private secret" not in str(error.value)


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"redirect_uris": ["https://evil.example/callback"]}, "invalid_registration_response"),
        ({"token_endpoint_auth_method": "client_secret_basic"}, "invalid_registration_response"),
        ({"client_secret": "do-not-expose"}, "invalid_registration_response"),
        (
            {"grant_types": ["authorization_code", "refresh_token", "client_credentials"]},
            "invalid_registration_response",
        ),
        ({"scope": "admin"}, "invalid_registration_response"),
        ({"client_id": ""}, "invalid_registration_response"),
    ],
)
async def test_registration_rejects_changed_client_contract(changes, code):
    async with OAuthHTTPRequester(
        {"identity.example"},
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(201, json=registration(**changes))
        ),
    ) as request:
        discovery = OAuthDiscoveryClient(request)
        with pytest.raises(OAuthError) as error:
            await discovery.register(
                OAuthMetadata.model_validate(metadata()), redirect_uri=REDIRECT, scopes=["tools"]
            )
        assert error.value.code == code and "do-not-expose" not in str(error.value)


async def test_confidential_client_secret_is_masked_and_sent_only_to_validated_token_endpoint():
    seen = []

    async def endpoint(request):
        seen.append(request)
        return httpx.Response(
            201,
            json=registration(
                token_endpoint_auth_method="client_secret_post",
                client_secret="client-secret",
            ),
        )

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(endpoint)
    ) as request:
        discovery = OAuthDiscoveryClient(request)
        server = OAuthMetadata.model_validate(metadata())
        client = await discovery.register(
            server, redirect_uri=REDIRECT, token_endpoint_auth_method="client_secret_post"
        )
        assert "client-secret" not in repr(client)
        configured = discovery.configuration(server, client, server="demo")
        assert configured.client_secret.get_secret_value() == "client-secret"
        assert "client-secret" not in repr(configured)
        assert len(seen) == 1


async def test_registration_unavailable_bad_scopes_and_cross_issuer_config_do_not_send_request():
    async def never(_request):
        pytest.fail("无效配置不得联网")

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(never)
    ) as request:
        discovery = OAuthDiscoveryClient(request)
        with pytest.raises(OAuthError) as error:
            await discovery.register(
                OAuthMetadata.model_validate(metadata(registration_endpoint=None)),
                redirect_uri=REDIRECT,
            )
        assert error.value.code == "registration_unavailable"
        for scopes in (["admin"], ["two words"], ['quote"'], ["slash\\"]):
            with pytest.raises(OAuthError) as error:
                await discovery.register(
                    OAuthMetadata.model_validate(metadata()), redirect_uri=REDIRECT, scopes=scopes
                )
            assert error.value.code == "invalid_scope"
        with pytest.raises(OAuthError) as error:
            await discovery.discover(ISSUER, metadata_url="https://other.example/metadata")
        assert error.value.code == "oauth_host_denied"


async def test_metadata_request_network_failure_and_cancellation_keep_semantics():
    async def timeout(request):
        raise httpx.ReadTimeout("private secret", request=request)

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(timeout)
    ) as request:
        with pytest.raises(OAuthError) as error:
            await OAuthDiscoveryClient(request).discover(ISSUER)
        assert error.value.code == "metadata_request_failed"

    entered = asyncio.Event()

    async def waiting(_request):
        entered.set()
        await asyncio.Event().wait()

    async with OAuthHTTPRequester(
        {"identity.example"}, transport=httpx.MockTransport(waiting)
    ) as request:
        task = asyncio.create_task(OAuthDiscoveryClient(request).discover(ISSUER))
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_discovered_issuer_is_part_of_persistent_token_scope(repository):
    from krim_agent.mcp import PersistentEncryptedTokenStore
    from krim_agent.mcp.oauth import OAuthToken

    async with OAuthHTTPRequester(
        {"identity.example"},
        transport=httpx.MockTransport(lambda _request: httpx.Response(201, json=registration())),
    ) as request:
        discovery = OAuthDiscoveryClient(request)
        original = OAuthMetadata.model_validate(metadata())
        registered = await discovery.register(original, redirect_uri=REDIRECT)
        first = discovery.configuration(original, registered, server="demo")
        changed_metadata = OAuthMetadata.model_validate(
            metadata(issuer="https://identity.example/other")
        )
        with pytest.raises(OAuthError) as error:
            discovery.configuration(changed_metadata, registered, server="demo")
        assert error.value.code == "oauth_issuer_mismatch"
        second = discovery.configuration(
            changed_metadata,
            registered.model_copy(update={"issuer": changed_metadata.issuer}),
            server="demo",
        )
        store = PersistentEncryptedTokenStore(repository.database, Fernet.generate_key())
        await store.write(first, "subject", OAuthToken(access_token="issuer-bound"))
        assert (await store.read(second, "subject")).token is None
