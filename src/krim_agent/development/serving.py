"""Separate TLS listeners over one application; protocol handling stays in Uvicorn."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import stat
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any

import uvicorn

from krim_agent.api.server import create_app
from krim_agent.composition import Application, open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.development.configuration import DevelopmentListenerConfiguration
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.runtime.contracts import settled


class _Server(uvicorn.Server):
    @contextmanager
    def capture_signals(self) -> Iterator[None]:
        # One owner handles shutdown of both listeners.
        yield


def _tls_file(path: Path, *, private: bool) -> None:
    try:
        info = path.lstat()
        if (
            path.resolve() != path or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid() or info.st_mode & 0o022
            or private and info.st_mode & 0o077
        ):
            raise ValueError("untrusted TLS file")
    except (OSError, ValueError):
        raise ServiceError("development_tls_invalid", 422) from None


def listener_configs(
    application: Application, listener: DevelopmentListenerConfiguration,
) -> tuple[uvicorn.Config, uvicorn.Config]:
    development = application.development
    if development is None or development.preview_app is None:
        raise ServiceError("preview_unavailable", 503)
    for certificate, key in (
        (listener.control_certificate, listener.control_private_key),
        (listener.preview_certificate, listener.preview_private_key),
    ):
        _tls_file(certificate, private=False)
        _tls_file(key, private=True)
    common: dict[str, Any] = {
        "access_log": False, "log_level": "critical", "proxy_headers": False,
        "server_header": False, "lifespan": "on", "http": "h11",
        "timeout_graceful_shutdown": 5, "limit_concurrency": 128,
    }
    control = uvicorn.Config(
        create_app(application=application), host=listener.control_host,
        port=listener.control_port, ws="none",
        ssl_certfile=str(listener.control_certificate),
        ssl_keyfile=str(listener.control_private_key), **common,
    )
    preview = uvicorn.Config(
        development.preview_app, host=listener.preview_host, port=listener.preview_port,
        ws="websockets-sansio", ws_max_size=1_048_576, ws_max_queue=16,
        ws_per_message_deflate=False,
        ssl_certfile=str(listener.preview_certificate),
        ssl_keyfile=str(listener.preview_private_key), **common,
    )
    try:
        control.load()
        preview.load()
    except (OSError, ValueError):
        raise ServiceError("development_tls_invalid", 422) from None
    return control, preview


def _bind(config: uvicorn.Config) -> socket.socket:
    sock = socket.socket(socket.AF_INET6 if config.host == "::1" else socket.AF_INET)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((config.host, config.port))
        sock.listen(128)
        sock.setblocking(False)
        return sock
    except BaseException:
        sock.close()
        raise


@contextmanager
def _signals(stop: asyncio.Event) -> Iterator[None]:
    loop = asyncio.get_running_loop()
    previous = {}
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.getsignal(sig)
            loop.add_signal_handler(sig, stop.set)
        yield
    finally:
        for sig, handler in previous.items():
            loop.remove_signal_handler(sig)
            signal.signal(sig, handler)


async def _serve(server: _Server, sock: socket.socket) -> None:
    try:
        await server.serve(sockets=[sock])
    except SystemExit:
        raise ServiceError("development_listener_failed", 503) from None


async def serve_development(application: Application, *, stop: asyncio.Event | None = None) -> None:
    """Borrow one application; the caller closes it after both listeners stop."""
    development = application.development
    preview = development.configuration.preview if development is not None else None
    if preview is None or preview.listener is None:
        raise ServiceError("development_listener_unconfigured", 422)
    configs = listener_configs(application, preview.listener)
    servers = tuple(_Server(config) for config in configs)
    sockets: list[socket.socket] = []
    tasks: list[asyncio.Task[None]] = []
    waiter: asyncio.Task[bool] | None = None
    event = stop if stop is not None else asyncio.Event()
    try:
        for config in configs:
            sockets.append(_bind(config))
        with _signals(event) if stop is None else nullcontext():
            tasks = [asyncio.create_task(_serve(server, sock))
                     for server, sock in zip(servers, sockets, strict=True)]
            waiter = asyncio.create_task(event.wait())
            done, _ = await asyncio.wait([*tasks, waiter], return_when=asyncio.FIRST_COMPLETED)
            for task in tasks:
                if task in done:
                    await task
                    if not event.is_set():
                        raise ServiceError("development_listener_stopped", 503)
    finally:
        for server in servers:
            server.should_exit = True
        if waiter is not None:
            waiter.cancel()

        async def drain() -> None:
            if not tasks:
                return
            try:
                async with asyncio.timeout(12):
                    await asyncio.gather(*tasks, return_exceptions=True)
            except TimeoutError:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        try:
            await settled(drain())
        finally:
            for sock in sockets:
                sock.close()
            if waiter is not None:
                await asyncio.gather(waiter, return_exceptions=True)


async def run_development(settings: Settings, http_config: OpenAICompatibleConfig) -> None:
    async with await open_application(
        settings.database, settings=settings, http_config=http_config,
    ) as application:
        await serve_development(application)
