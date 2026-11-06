"""本机/同源/Key 与有限 JSON 边界；不提供多租户或 OS 沙箱。"""

import ipaddress
import secrets
import time
from collections import OrderedDict, deque
from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from krim_agent.config import Settings
from krim_agent.contracts import ServiceError, parse_json
from krim_agent.security import LOCAL, AccessPolicy

from .web import PUBLIC_UI_PATHS


def local(value: str | None) -> bool:
    if value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value or "").is_loopback
    except ValueError:
        return False


class _FailGate:
    """按client address的有界认证失败节流：最多256个地址桶。

    单一坏客户端不能锁死其他地址的合法认证；桶容量固定，写入满时先
    清理窗口外过期桶，仍满则按LRU逐出。不按token建桶。
    """

    _LIMIT = 64
    _WINDOW = 60.0
    _BUCKETS = 256

    def __init__(self) -> None:
        self._buckets: OrderedDict[str, deque[float]] = OrderedDict()

    def throttled(self, address: str, now: float) -> bool:
        bucket = self._buckets.get(address)
        return (
            bucket is not None
            and len(bucket) == bucket.maxlen
            and now - bucket[0] < self._WINDOW
        )

    def record(self, address: str, now: float) -> None:
        bucket = self._buckets.get(address)
        if bucket is None:
            if len(self._buckets) >= self._BUCKETS:
                for key, old in list(self._buckets.items()):
                    if now - old[-1] >= self._WINDOW:
                        del self._buckets[key]
                while len(self._buckets) >= self._BUCKETS:
                    self._buckets.popitem(last=False)
            bucket = deque(maxlen=self._LIMIT)
            self._buckets[address] = bucket
        self._buckets.move_to_end(address)
        bucket.append(now)


class LocalBoundary:
    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        self.app, self.settings = app, settings
        self._gate = _FailGate()

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
        application = getattr(getattr(scope.get("app"), "state", None), "application", None)
        access = getattr(application, "access_policy", None)
        identities = getattr(application, "identities", None)
        managed = identities is not None and bool(getattr(identities, "managed", False))
        key = self.settings.api_key.get_secret_value()
        client = scope.get("client")
        if (
            not key
            and access is None
            and not managed
            and (not local(host) or client is None or not local(client[0]))
        ):
            await reject("local_only", 403)
            return
        origin = headers.get("origin")
        if origin is not None and origin != scope["scheme"] + "://" + headers.get("host", ""):
            await reject("origin_denied", 403)
            return
        public_paths = getattr(
            getattr(scope.get("app"), "state", None), "public_ui_paths", PUBLIC_UI_PATHS
        )
        public = scope["method"] in {"GET", "HEAD"} and (
            scope["path"] in public_paths or scope["path"] == "/healthz"
        )
        if (
            key
            and not managed
            and not public
            and not secrets.compare_digest(
                headers.get("authorization", "").encode(), ("Bearer " + key).encode()
            )
        ):
            await reject("unauthorized", 401)
            return
        if not public:
            if managed:
                # 托管身份只认IdentityService；认证失败不回落LOCAL或旧全局key。
                # 节流按来源地址分桶：坏客户端不阻其他地址的合法认证。
                address = str(client[0]) if client is not None else ""
                now = time.monotonic()
                if self._gate.throttled(address, now):
                    await reject("unauthorized", 401)
                    return
                authorization = headers.get("authorization", "")
                try:
                    principal = identities.authenticate(authorization)
                except ServiceError:
                    self._gate.record(address, time.monotonic())
                    await reject("unauthorized", 401)
                    return
                state = scope.setdefault("state", {})
                state["principal"] = principal
                # 供SSE发送/心跳复核复用；不写入任何日志或响应。
                state["authorization"] = authorization
            elif isinstance(access, AccessPolicy):
                try:
                    principal = access.authenticate(headers.get("authorization", ""))
                except ServiceError:
                    await reject("unauthorized", 401)
                    return
                scope.setdefault("state", {})["principal"] = principal
            else:
                scope.setdefault("state", {})["principal"] = LOCAL
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
