"""本机/同源/Key 与有限 JSON 边界；不提供多租户或 OS 沙箱。"""

import ipaddress
import secrets
from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from krim_agent.config import Settings
from krim_agent.contracts import parse_json

from .web import PUBLIC_UI_PATHS


def local(value: str | None) -> bool:
    if value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value or "").is_loopback
    except ValueError:
        return False


class LocalBoundary:
    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        self.app, self.settings = app, settings

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def reject(code: str, status: int) -> None:
            await JSONResponse(
                {"error": {"code": code, "message": "请求未完成，请检查操作及运行状态。"}},
                status_code=status,
            )(scope, receive, send)

        headers = Headers(scope=scope)
        try:
            host = urlsplit("http://" + headers.get("host", "")).hostname
        except ValueError:
            host = None
        key = self.settings.api_key.get_secret_value()
        client = scope.get("client")
        if not key and (not local(host) or client is None or not local(client[0])):
            await reject("local_only", 403)
            return
        origin = headers.get("origin")
        if origin is not None and origin != scope["scheme"] + "://" + headers.get("host", ""):
            await reject("origin_denied", 403)
            return
        public = scope["method"] in {"GET", "HEAD"} and (
            scope["path"] in PUBLIC_UI_PATHS or scope["path"] == "/healthz"
        )
        if (
            key
            and not public
            and not secrets.compare_digest(
                headers.get("authorization", "").encode(), ("Bearer " + key).encode()
            )
        ):
            await reject("unauthorized", 401)
            return
        body = bytearray()
        while True:
            part = await receive()
            if part["type"] == "http.disconnect":
                return
            body.extend(part.get("body", b""))
            if len(body) > 65536:
                await reject("body_too_large", 413)
                return
            if not part.get("more_body", False):
                break
        if body:
            try:
                parse_json(body.decode("utf-8"))
            except (UnicodeError, ValueError, RecursionError):
                await reject("invalid_json", 422)
                return
        delivered = False
        started = False

        async def replay() -> Message:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        async def tracked(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, replay, tracked)
        except Exception:
            # 最外层不让原始异常/私有 payload 进入服务器 traceback 日志。
            if not started:
                await reject("execution_error", 500)
