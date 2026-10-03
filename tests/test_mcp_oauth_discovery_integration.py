import asyncio
import base64
import hashlib
import socket
from urllib.parse import parse_qs, urlsplit

import uvicorn
from cryptography.fernet import Fernet
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from krim_agent.mcp import (
    EncryptedTokenStore,
    OAuthClient,
    OAuthDiscoveryClient,
    OAuthHTTPRequester,
    OAuthStateStore,
)


async def test_real_tcp_metadata_registration_and_pkce_exchange():
    app = FastAPI()
    records = []
    challenge = ""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    base = f"http://127.0.0.1:{listener.getsockname()[1]}"
    issuer, redirect = base + "/realm", base + "/callback"

    @app.get("/.well-known/oauth-authorization-server/realm")
    async def metadata():
        records.append("metadata")
        return {
            "issuer": issuer,
            "authorization_endpoint": base + "/authorize",
            "token_endpoint": base + "/token",
            "registration_endpoint": base + "/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "scopes_supported": ["tools"],
        }

    @app.post("/register")
    async def register(request: Request):
        payload = await request.json()
        records.append("register")
        assert payload["redirect_uris"] == [redirect]
        assert payload["token_endpoint_auth_method"] == "none"
        assert payload["scope"] == "tools"
        return JSONResponse({"client_id": "registered", **payload}, status_code=201)

    @app.post("/token")
    async def token(request: Request):
        form = parse_qs((await request.body()).decode())
        records.append("token")
        assert form["client_id"] == ["registered"]
        assert form["redirect_uri"] == [redirect]
        proof = (
            base64.urlsafe_b64encode(hashlib.sha256(form["code_verifier"][0].encode()).digest())
            .rstrip(b"=")
            .decode()
        )
        assert proof == challenge
        return {
            "access_token": "local-access",
            "refresh_token": "local-refresh",
            "expires_in": 600,
        }

    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        async with OAuthHTTPRequester({"127.0.0.1"}) as request:
            discovery = OAuthDiscoveryClient(request)
            discovered = await discovery.discover(issuer)
            registered = await discovery.register(
                discovered, redirect_uri=redirect, scopes=["tools"]
            )
            config = discovery.configuration(discovered, registered, server="demo")
            states = OAuthStateStore()
            attempt = states.begin(config)
            query = parse_qs(urlsplit(attempt.authorization_url).query)
            challenge = query["code_challenge"][0]
            assert query["code_challenge_method"] == ["S256"]
            store = EncryptedTokenStore(Fernet.generate_key())
            received = await OAuthClient(states, store).exchange(
                attempt.state, "local-code", "subject", request
            )
            assert received.access_token.get_secret_value() == "local-access"
            assert (await store.read(config, "subject")).token is not None
            assert records == ["metadata", "register", "token"]
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 5)
        finally:
            listener.close()
