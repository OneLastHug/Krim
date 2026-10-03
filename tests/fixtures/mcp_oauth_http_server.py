"""需要 Bearer OAuth 的本机 Streamable HTTP MCP 测试服务。"""

from __future__ import annotations

import base64
import hashlib
import os
import secrets
from urllib.parse import parse_qs

from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse
from starlette.routing import Route

HOST = "127.0.0.1"
PORT = int(os.environ.get("KRIM_TEST_OAUTH_MCP_PORT", "8767"))
BASE = f"http://{HOST}:{PORT}"
RESOURCE = BASE + "/mcp"
ISSUER = BASE

server = FastMCP("Krim OAuth MCP", host=HOST, port=PORT)
codes = {}


@server.tool()
def echo(text: str) -> str:
    """回声工具。"""
    return text


async def protected_resource(_request: Request):
    return JSONResponse(
        {
            "resource": RESOURCE,
            "authorization_servers": [ISSUER],
            "scopes_supported": ["tools"],
            "bearer_methods_supported": ["header"],
        }
    )


async def auth_metadata(_request: Request):
    return JSONResponse(
        {
            "issuer": ISSUER,
            "authorization_endpoint": BASE + "/authorize",
            "token_endpoint": BASE + "/token",
            "registration_endpoint": BASE + "/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "scopes_supported": ["tools"],
        }
    )


async def register(request: Request):
    body = await request.json()
    return JSONResponse(
        {
            "client_id": "mcp-test-client",
            "redirect_uris": body["redirect_uris"],
            "grant_types": body["grant_types"],
            "response_types": body["response_types"],
            "token_endpoint_auth_method": "none",
            "scope": body.get("scope"),
        },
        status_code=201,
    )


async def authorize(request: Request):
    query = dict(parse_qs(request.url.query))
    if (
        query.get("code_challenge_method") != ["S256"]
        or query.get("resource") != [RESOURCE]
        or query.get("client_id") != ["mcp-test-client"]
    ):
        return JSONResponse({"error": "invalid_request"}, status_code=400)
    code = secrets.token_urlsafe(16)
    codes[code] = query
    location = query["redirect_uri"][0] + "?code=" + code + "&state=" + query["state"][0]
    return RedirectResponse(location)


async def token(request: Request):
    form = parse_qs((await request.body()).decode())
    stored = codes.pop(form.get("code", [""])[0], None)
    verifier = form.get("code_verifier", [""])[0]
    proof = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
    proof = proof.rstrip(b"=").decode()
    if (
        stored is None
        or stored["code_challenge"] != [proof]
        or stored["redirect_uri"] != form.get("redirect_uri")
        or form.get("resource") != [RESOURCE]
    ):
        return JSONResponse({"error": "invalid_grant"}, status_code=400)
    return JSONResponse(
        {
            "access_token": "mcp-test-access",
            "refresh_token": "mcp-test-refresh",
            "expires_in": 600,
        }
    )


class RequireBearer(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if (
            request.url.path == "/mcp"
            and request.headers.get("authorization") != "Bearer mcp-test-access"
        ):
            return JSONResponse(
                {"error": "unauthorized"},
                status_code=401,
                headers={
                    "WWW-Authenticate": (
                        f'Bearer resource_metadata="{BASE}/.well-known/'
                        'oauth-protected-resource", scope="tools"'
                    )
                },
            )
        return await call_next(request)


app = server.streamable_http_app()
app.add_middleware(RequireBearer)
app.routes.extend(
    [
        Route("/.well-known/oauth-protected-resource", protected_resource, methods=["GET"]),
        Route("/.well-known/oauth-authorization-server", auth_metadata, methods=["GET"]),
        Route("/register", register, methods=["POST"]),
        Route("/authorize", authorize, methods=["GET"]),
        Route("/token", token, methods=["POST"]),
    ]
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT, log_level="error")
