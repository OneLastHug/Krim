"""固定OAuth回调；原发起Cookie/state/资源绑定校验由Python服务执行。"""

import time
from urllib.parse import parse_qs, quote

from fastapi import FastAPI, Request, Response
from fastapi.responses import RedirectResponse

from krim_agent.mcp.browser import CALLBACK_PATH, OAuthBrowserService, OAuthBrowserView, cookie_name
from krim_agent.mcp.management import MCPControlRequest
from krim_agent.protocol.domain import ServiceError

_HEADERS = {
    "Cache-Control": "no-store",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
}


def mount_browser_oauth(app: FastAPI, service: OAuthBrowserService | None) -> None:
    def available() -> OAuthBrowserService:
        if service is None:
            raise ServiceError("oauth_browser_unavailable", "宿主未启用浏览器OAuth回调", 503)
        return service

    @app.get("/v1/mcp/oauth/capabilities")
    async def capabilities() -> dict[str, bool | list[str]]:
        resources = []
        if service is not None:
            for name in service.manager._resources:
                try:
                    service._auth(name)
                    resources.append(name)
                except ServiceError:
                    pass
        return {"enabled": service is not None, "resources": resources}

    @app.post("/v1/mcp/servers/{name}/oauth/begin")
    async def begin(
        name: str, payload: MCPControlRequest, request: Request, response: Response
    ) -> OAuthBrowserView:
        view, secret = await available().begin(name, payload, str(request.base_url).rstrip("/"))
        response.set_cookie(
            cookie_name(view.attempt_id),
            secret,
            max_age=max(1, int(view.expires_at - time.time())),
            path=CALLBACK_PATH,
            httponly=True,
            secure=request.url.scheme == "https",
            samesite="lax",
        )
        response.headers.update(_HEADERS)
        return view

    @app.get("/v1/mcp/servers/{name}/oauth/attempts")
    async def attempts(name: str, response: Response) -> list[OAuthBrowserView]:
        selected = available()
        response.headers.update(_HEADERS)
        return await selected.store.list(selected.manager.store.scope, name)

    @app.get("/v1/mcp/servers/{name}/oauth/attempts/{attempt_id}")
    async def attempt(name: str, attempt_id: str, response: Response) -> OAuthBrowserView:
        selected = available()
        response.headers.update(_HEADERS)
        return await selected.store.view(selected.manager.store.scope, name, attempt_id)

    @app.post("/v1/mcp/servers/{name}/oauth/attempts/{attempt_id}/cancel")
    async def cancel(name: str, attempt_id: str, response: Response) -> OAuthBrowserView:
        view = await available().cancel(name, attempt_id)
        response.delete_cookie(cookie_name(attempt_id), path=CALLBACK_PATH)
        response.headers.update(_HEADERS)
        return view

    @app.get(CALLBACK_PATH, include_in_schema=False)
    async def callback(request: Request) -> RedirectResponse:
        raw = request.scope.pop("krim.oauth_query", request.scope.get("query_string", b""))
        request.scope["query_string"] = b""
        try:
            if len(raw) > 16384:
                raise ValueError
            values = parse_qs(raw.decode("ascii"), keep_blank_values=True, max_num_fields=8)
            if any(len(value) != 1 for value in values.values()):
                raise ValueError
            if "state" not in values or set(values) - {
                "state",
                "code",
                "error",
                "error_description",
                "iss",
                "session_state",
            }:
                raise ValueError
            view = await available().callback(
                values["state"][0],
                values.get("code", [None])[0],
                values.get("error", [None])[0],
                dict(request.cookies),
                values.get("iss", [None])[0],
            )
        except (ValueError, UnicodeError):
            raise ServiceError("oauth_callback_invalid", "OAuth回程字段或编码无效", 400) from None
        response = RedirectResponse(
            "/ui/?mcp=" + quote(view.name) + "&oauth_attempt=" + quote(view.attempt_id),
            status_code=303,
            headers=_HEADERS,
        )
        response.delete_cookie(cookie_name(view.attempt_id), path=CALLBACK_PATH)
        return response
