"""HTTPS downloads only, using the existing pinned HTTP transport."""
from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import httpx

from krim_agent.network.egress_policy import EgressPolicy, EgressTarget, EgressViolation
from krim_agent.network.http_transport import (
    AddressResolver,
    PinnedAsyncTransport,
    SystemResolver,
)
from krim_agent.project_access.egress import ProjectEgressPolicy

from .policy import PolicyBinding

MAX_LINE = 131072
MAX_CHUNK = 32768
MAX_DOWNLOAD = 512 * 1024 * 1024
MAX_ACTIVE = 8
Send = Callable[[dict[str, Any]], Awaitable[None]]
_HEADERS = frozenset({
    "accept", "accept-encoding", "accept-language", "user-agent", "range",
    "if-none-match", "if-modified-since", "cache-control", "npm-command",
    "npm-in-ci", "npm-scope", "npm-session",
})
_RESPONSE_HEADERS = frozenset({
    "content-type", "content-length", "content-encoding", "content-range",
    "accept-ranges", "etag", "last-modified", "cache-control", "expires",
    "location", "vary", "date",
})


class _Policy(EgressPolicy):
    def __init__(self, policy: ProjectEgressPolicy) -> None:
        self.policy = policy

    def validate_target(self, url: str) -> EgressTarget:
        return self.policy.validate_target(url)

    def approve_addresses(self, addresses: Sequence[str]) -> tuple[str, ...]:
        return self.policy.approve_addresses(addresses)


class _Resolver:
    def __init__(self, binding: PolicyBinding, inner: AddressResolver) -> None:
        self.binding, self.inner = binding, inner

    async def resolve(self, host: str) -> Sequence[str]:
        await self.binding.current()
        result = await self.inner.resolve(host)
        await self.binding.current()
        return result


class DownloadBroker:
    def __init__(self, binding: PolicyBinding, *,
                 resolver: AddressResolver | None = None,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.binding = binding
        self.transport = transport or PinnedAsyncTransport(
            _Policy(binding.policy),
            resolver=_Resolver(binding, resolver or SystemResolver()),
        )
        self.client = httpx.AsyncClient(
            transport=self.transport, trust_env=False, follow_redirects=False,
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        self.closed = False
        self.active: set[asyncio.Task[Any]] = set()
        self.total_bytes = 0

    async def close(self) -> None:
        self.closed = True
        current = asyncio.current_task()
        tasks = [task for task in self.active if task is not current]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.client.aclose()

    async def watch(self) -> None:
        try:
            while not self.closed:
                await self.binding.current()
                await asyncio.sleep(0.25)
        finally:
            await self.close()

    async def download(self, message: dict[str, Any], send: Send) -> None:
        task = asyncio.current_task()
        assert task is not None
        if self.closed or len(self.active) >= MAX_ACTIVE:
            raise EgressViolation("download broker unavailable")
        self.active.add(task)
        try:
            if set(message) != {"id", "method", "url", "headers"}:
                raise EgressViolation("invalid download request")
            request_id = message["id"]
            method, url, headers = message["method"], message["url"], message["headers"]
            if (type(request_id) is not int or not 1 <= request_id <= 2**31 - 1
                    or method not in {"GET", "HEAD"} or not isinstance(url, str)
                    or not isinstance(headers, dict) or len(headers) > 32):
                raise EgressViolation("invalid download request")
            policy = await self.binding.current()
            target = policy.validate_target(url)
            # The URL authority is reconstructed by the trusted transport. A raw
            # Host, TLS SNI, proxy, Upgrade, body or credential is never forwarded.
            # Preserve content negotiation: httpx otherwise adds gzip even for
            # clients (curl) that did not request or enable decompression.
            filtered = {"accept-encoding": "identity"}
            for key, value in headers.items():
                if (not isinstance(key, str) or not isinstance(value, str)
                        or len(key) > 128 or len(value) > 8192
                        or any(ord(c) < 32 or ord(c) == 127 for c in key + value)):
                    raise EgressViolation("invalid download headers")
                if key.lower() in _HEADERS:
                    filtered[key.lower()] = value
            async with self.client.stream(method, url, headers=filtered) as response:
                await self.binding.current()
                outgoing = {k: v for k, v in response.headers.items()
                            if k.lower() in _RESPONSE_HEADERS}
                location = outgoing.get("location")
                if location is not None:
                    redirect = str(response.url.join(location))
                    policy.validate_target(redirect)
                    # Absolute redirects are validated here and again when the
                    # client opens its next CONNECT/request, including DNS.
                    outgoing["location"] = redirect
                # No upstream connection is reused (PinnedAsyncTransport), and
                # redirect following/HTTP2/Upgrade are disabled.
                await send({"id": request_id, "status": response.status_code,
                            "headers": outgoing})
                count = 0
                async for data in response.aiter_raw():
                    for offset in range(0, len(data), MAX_CHUNK):
                        await self.binding.current()
                        chunk = data[offset:offset + MAX_CHUNK]
                        count += len(chunk)
                        self.total_bytes += len(chunk)
                        if count > MAX_DOWNLOAD or self.total_bytes > MAX_DOWNLOAD * 4:
                            raise EgressViolation("download size exceeded")
                        await send({"id": request_id,
                                    "data": base64.b64encode(chunk).decode("ascii")})
                await self.binding.current()
                await send({"id": request_id, "end": True})
            del target
        finally:
            self.active.discard(task)

    async def serve(self, reader: asyncio.StreamReader, send: Send) -> None:
        """Treat all shim output as hostile; duplicate IDs fail the whole lease."""
        tasks: dict[int, asyncio.Task[None]] = {}

        async def run(message: dict[str, Any]) -> None:
            rid = message["id"]
            try:
                async with asyncio.timeout(120):
                    await self.download(message, send)
            except Exception:
                await send({"id": rid, "error": "download_denied"})
            finally:
                tasks.pop(rid, None)

        try:
            while not self.closed:
                line = await reader.readline()
                if not line:
                    return
                if len(line) > MAX_LINE:
                    raise EgressViolation("invalid broker frame")
                message = json.loads(line)
                if (not isinstance(message, dict) or type(message.get("id")) is not int
                        or not 1 <= message["id"] <= 2**31 - 1
                        or message["id"] in tasks or len(tasks) >= MAX_ACTIVE):
                    raise EgressViolation("invalid broker frame")
                rid = message["id"]
                tasks[rid] = asyncio.create_task(run(message))
        finally:
            for pending in tuple(tasks.values()):
                pending.cancel()
            await asyncio.gather(*tuple(tasks.values()), return_exceptions=True)
            await self.close()
