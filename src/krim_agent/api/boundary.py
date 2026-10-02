"""本地 API 边界；EXT 安全补全，不宣称提供租户隔离或沙箱。"""

import secrets
from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from krim_agent.config import Settings


def local_host(host: str | None) -> bool:
    return host in {"localhost", "127.0.0.1", "::1"}


class LocalBoundary:
    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        self.app = app
        self.key = settings.api_key.get_secret_value()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)

        async def reject(code: str, message: str, status: int) -> None:
            response = JSONResponse({"error": {"code": code, "message": message}}, status)
            await response(scope, receive, send)

        try:
            host = urlsplit("http://" + headers.get("host", "")).hostname
        except ValueError:
            host = None
        if not self.key and not local_host(host):
            await reject("local_only", "无密钥模式仅允许本机 Host", 403)
            return
        client = scope.get("client")
        if not self.key and client is not None and not local_host(client[0]):
            await reject("local_only", "无密钥模式仅允许本机客户端", 403)
            return
        origin = headers.get("origin")
        expected_origin = f"{scope['scheme']}://{headers.get('host', '')}"
        if origin and origin != expected_origin:
            await reject("origin_denied", "不允许跨站请求", 403)
            return
        if self.key and scope["path"] != "/healthz":
            actual = headers.get("authorization", "")
            if not secrets.compare_digest(actual.encode(), f"Bearer {self.key}".encode()):
                await reject("unauthorized", "需要有效的 Bearer API Key", 401)
                return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > 65536:
                await reject("body_too_large", "请求体超过 64 KiB", 413)
                return
            if not message.get("more_body", False):
                break
        delivered = False

        async def replay() -> Message:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)
