"""One owner task per isolated browser; no model-controlled Docker or network."""

from __future__ import annotations

import asyncio
import json
import os
import re
import stat
import tempfile
import time
from collections.abc import Awaitable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import JsonValue

from krim_agent.contracts import ServiceError
from krim_agent.dev_environment.docker import DockerCLI
from krim_agent.dev_preview.app import PreviewApp
from krim_agent.development.configuration import DevelopmentBrowserConfiguration
from krim_agent.development.contracts import EnvironmentHandle, RunOwnership
from krim_agent.runtime.contracts import settled
from krim_agent.security import PrincipalContext

_MAX_LINE = 1_500_000
_ROOT = Path("/run/krim-browser")
_ID = re.compile(r"[0-9a-f]{64}\Z")


@dataclass
class _Action:
    body: dict[str, Any]
    result: asyncio.Future[dict[str, Any]]


@dataclass
class BrowserSession:
    session_id: str
    principal: PrincipalContext
    environment: EnvironmentHandle
    ownership: RunOwnership
    preview_id: str
    ready: asyncio.Future[None]
    queue: asyncio.Queue[_Action] = field(default_factory=lambda: asyncio.Queue(maxsize=1))
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    container_id: str | None = None
    process: asyncio.subprocess.Process | None = None
    failure: str | None = None
    spec: Any = None
    directory: Path | None = None
    directory_identity: tuple[int, int] | None = None
    cleanup_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    busy: bool = False
    created_at: float = field(default_factory=time.monotonic)


class BrowserWorkers:
    def __init__(self, app: PreviewApp, configuration: DevelopmentBrowserConfiguration):
        self.app, self.configuration = app, configuration
        self.runner = DockerCLI()
        self.sessions: dict[str, BrowserSession] = {}
        self.closed = False

    async def open(
        self, principal: PrincipalContext, environment: EnvironmentHandle,
        ownership: RunOwnership, preview_id: str,
    ) -> dict[str, JsonValue]:
        if self.closed or self.sessions:
            raise ServiceError("capacity", 429)
        session = BrowserSession(
            "browser-" + uuid4().hex, principal, environment, ownership, preview_id,
            asyncio.get_running_loop().create_future(),
        )
        self.sessions[session.session_id] = session
        session.task = asyncio.create_task(self._own(session))
        try:
            async with asyncio.timeout(25):
                await asyncio.shield(session.ready)
        except BaseException:
            await settled(self._close(session))
            raise
        return {"browser_id": session.session_id, "preview_id": preview_id, "state": "ready"}

    def _bound(
        self, browser_id: str, principal: PrincipalContext,
        environment: EnvironmentHandle, ownership: RunOwnership,
    ) -> BrowserSession:
        session = self.sessions.get(browser_id)
        if session is None:
            raise ServiceError("browser_closed", 410)
        if (session.principal != principal or session.ownership != ownership
                or session.environment.model_dump(exclude={"state"})
                != environment.model_dump(exclude={"state"})):
            raise ServiceError("permission_denied", 403)
        if session.failure is not None or session.stop.is_set():
            raise ServiceError("browser_closed", 410)
        return session

    async def action(
        self, browser_id: str, principal: PrincipalContext, environment: EnvironmentHandle,
        ownership: RunOwnership, command: dict[str, Any],
    ) -> dict[str, Any]:
        session = self._bound(browser_id, principal, environment, ownership)
        if command.get("op") not in {
            "goto", "click", "fill", "wait", "assert", "upload", "screenshot",
            "diagnostics", "resize", "download",
        } or session.busy:
            raise ServiceError("browser_invalid_input", 422)
        session.busy = True
        future = asyncio.get_running_loop().create_future()
        try:
            session.queue.put_nowait(_Action(command, future))
            async with asyncio.timeout(20):
                return await asyncio.shield(future)
        except BaseException:
            await settled(self._close(session))
            raise
        finally:
            session.busy = False

    async def close_session(
        self, browser_id: str, principal: PrincipalContext, environment: EnvironmentHandle,
        ownership: RunOwnership,
    ) -> None:
        await self._close(self._bound(browser_id, principal, environment, ownership))

    async def _close(self, session: BrowserSession) -> None:
        session.stop.set()
        if session.task is not None:
            if not session.task.done():
                await settled(session.task)
                return
            # A previous cleanup failure retains its exact resource identities.
            session.task.exception()
        async with session.cleanup_lock:
            if session.session_id not in self.sessions:
                return
            if session.spec is not None:
                await settled(self._cleanup(session, session.spec))
            self._remove_directory(session)
            self.sessions.pop(session.session_id, None)

    @staticmethod
    def _remove_directory(session: BrowserSession) -> None:
        path = session.directory
        if path is None:
            return
        info = path.lstat()
        if (info.st_dev, info.st_ino) != session.directory_identity:
            raise ServiceError("browser_worker_failed", 503)
        path.rmdir()
        session.directory = None

    async def close_run(self, run_id: str) -> None:
        await asyncio.gather(*(self._close(s) for s in tuple(self.sessions.values())
                               if s.ownership.run_id == run_id))

    async def revoke(self, owner_id: str) -> None:
        await asyncio.gather(*(self._close(s) for s in tuple(self.sessions.values())
                               if s.environment.owner_id == owner_id
                               or s.principal.principal_id == owner_id))

    async def close(self) -> None:
        self.closed = True
        await asyncio.gather(*(self._close(s) for s in tuple(self.sessions.values())))

    @staticmethod
    def _directory() -> Path:
        _ROOT.mkdir(mode=0o700, exist_ok=True)
        info = _ROOT.lstat()
        if (_ROOT.resolve() != _ROOT or not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise ServiceError("browser_worker_failed", 503)
        return Path(tempfile.mkdtemp(prefix="session-", dir=_ROOT))

    @staticmethod
    def _absent(reply: Any, container_id: str) -> bool:
        messages = {
            prefix + kind + container_id
            for prefix in ("", "error: ", "error response from daemon: ")
            for kind in ("no such object: ", "no such container: ")
        }
        return (reply.code == 1 and reply.stdout.strip() in {"", "[]"}
                and reply.stderr.strip().casefold() in messages)

    async def _verify_container(
        self, session: BrowserSession, spec: Any, *, allow_absent: bool = False,
    ) -> bool:
        if session.container_id is None or not _ID.fullmatch(session.container_id):
            raise ServiceError("browser_worker_failed", 503)
        reply = await self.runner.run(("inspect", session.container_id))
        if allow_absent and self._absent(reply, session.container_id):
            return False
        if reply.code:
            raise ServiceError("browser_worker_failed", 503)
        try:
            value, = json.loads(reply.stdout)
            config, host = value["Config"], value["HostConfig"]
            if (value["Id"] != session.container_id or value["Image"] != spec.image_id
                    or value["Name"] != "/" + spec.name or config["User"] != "1000:1000"
                    or host["NetworkMode"] != "none" or host["Runtime"] != "runc"
                    or host.get("Privileged") or not host.get("ReadonlyRootfs")
                    or host.get("CapAdd") or host.get("CapDrop") != ["ALL"]
                    or host.get("PortBindings")
                    or host["Memory"] != 1073741824 or host["MemorySwap"] != 1073741824
                    or host["NanoCpus"] != 2000000000 or host["PidsLimit"] != 128):
                raise ValueError("worker binding changed")
            expected = {"/run/krim-preview/bridge.sock": str(spec.socket_path),
                        "/run/krim-ca/preview-ca.pem": str(spec.ca_path)}
            mounts = [m for m in value["Mounts"] if m["Type"] == "bind"]
            if len(mounts) != 2 or any(
                expected.get(m["Destination"]) != m["Source"] or m["RW"] for m in mounts
            ):
                raise ValueError("worker mount changed")
            security = host.get("SecurityOpt", [])
            if (not any(s in {"no-new-privileges", "no-new-privileges:true"}
                        for s in security)
                    or host.get("IpcMode") != "private"
                    or host.get("PidMode") not in {None, ""}
                    or host.get("Devices") or host.get("DeviceRequests")
                    or host.get("ExtraHosts") or host.get("Links")
                    or host.get("VolumesFrom")
                    or host.get("RestartPolicy", {}).get("Name") != "no"
                    or host.get("LogConfig", {}).get("Type") != "none"
                    or host.get("ShmSize") != 134217728
                    or any(m["Type"] not in {"bind", "tmpfs"} for m in value["Mounts"])):
                raise ValueError("worker security changed")
            profiles = [s.removeprefix("seccomp=") for s in security
                        if s.startswith("seccomp=")]
            if len(profiles) != 1:
                raise ValueError("worker seccomp changed")
            expected_profile = json.loads(spec.seccomp_path.read_text())
            if json.loads(profiles[0]) != expected_profile:
                raise ValueError("worker seccomp changed")
            environment = dict(item.split("=", 1) for item in config["Env"])
            if any(environment.get(key) != expected for key, expected in {
                "KRIM_PREVIEW_ORIGIN": spec.origin,
                "KRIM_CONTROL_ORIGIN": spec.control_origin,
                "KRIM_CA_SHA256": spec.ca_sha256,
                "HOME": "/tmp/krim-home",
            }.items()):
                raise ValueError("worker environment changed")
        except (KeyError, ValueError, TypeError):
            raise ServiceError("browser_worker_failed", 503) from None
        return True

    async def _exchange(self, session: BrowserSession, command: dict[str, Any]) -> dict[str, Any]:
        process = session.process
        if process is None or process.stdin is None or process.stdout is None:
            raise ServiceError("browser_closed", 410)
        data = json.dumps(command, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        if len(data) > _MAX_LINE:
            raise ServiceError("browser_output_limit", 422)
        async with asyncio.timeout(15):
            process.stdin.write(data)
            await process.stdin.drain()
            line = await process.stdout.readline()
        if not line or len(line) > _MAX_LINE or not line.endswith(b"\n"):
            raise ServiceError("browser_worker_failed", 503)
        try:
            response = json.loads(line)
            if not isinstance(response, dict) or response.get("ok") is not True:
                raise ValueError("worker denied")
            return response
        except (ValueError, TypeError):
            raise ServiceError("browser_worker_failed", 503) from None

    async def _create(self, session: BrowserSession, spec: Any) -> None:
        reply = await self.runner.run(spec.create_args())
        if reply.code or not _ID.fullmatch(reply.stdout.strip()):
            raise ServiceError("browser_worker_failed", 503)
        session.container_id = reply.stdout.strip()

    async def _attach(self, session: BrowserSession) -> None:
        assert session.container_id is not None
        session.process = await asyncio.create_subprocess_exec(
            *self.runner.prefix, "start", "--attach", "--interactive",
            session.container_id, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            limit=_MAX_LINE + 1,
            env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
        )

    async def _cleanup(self, session: BrowserSession, spec: Any) -> None:
        try:
            if session.container_id is not None:
                exists = await self._verify_container(session, spec, allow_absent=True)
                if exists:
                    removed = await self.runner.run(spec.remove_args(session.container_id))
                    if removed.code:
                        raise ServiceError("browser_worker_failed", 503)
                    checked = await self.runner.run(("inspect", session.container_id))
                    if not self._absent(checked, session.container_id):
                        raise ServiceError("browser_worker_failed", 503)
                session.container_id = None
        finally:
            process = session.process
            if process is not None:
                if process.stdin is not None:
                    process.stdin.close()
                try:
                    await asyncio.wait_for(process.wait(), 5)
                except TimeoutError:
                    if process.returncode is None:
                        process.kill()
                    await process.wait()

    async def _active_exchange(
        self, session: BrowserSession, command: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._active_operation(session, self._exchange(session, command))

    async def _active_operation(
        self, session: BrowserSession, operation: Awaitable[dict[str, Any]],
    ) -> dict[str, Any]:
        exchange = asyncio.ensure_future(operation)
        stopped = asyncio.create_task(session.stop.wait())
        try:
            done, _ = await asyncio.wait((exchange, stopped), return_when=asyncio.FIRST_COMPLETED)
            if stopped in done:
                raise ServiceError("browser_closed", 410)
            return await exchange
        finally:
            exchange.cancel()
            stopped.cancel()
            await asyncio.gather(exchange, stopped, return_exceptions=True)

    async def _own(self, session: BrowserSession) -> None:
        directory: Path | None = None
        current_action: _Action | None = None
        clean = True
        try:
            from krim_agent.dev_browser.host_bridge import PreviewConnectBridge
            from krim_agent.dev_browser.launcher import WorkerSpec

            async with AsyncExitStack() as stack:
                record, lease = await stack.enter_async_context(
                    self.app.service.route(session.preview_id, session.principal),
                )
                if record.handle.environment.model_dump(exclude={"state"}) != (
                    session.environment.model_dump(exclude={"state"})
                ):
                    raise ServiceError("permission_denied", 403)

                async def authorize() -> None:
                    if session.stop.is_set() or time.monotonic() - session.created_at >= 120:
                        raise ServiceError("browser_closed", 410)
                    async with asyncio.timeout(3):
                        await self.app.service.recheck(record, session.principal, lease)

                directory = self._directory()
                session.directory = directory
                info = directory.lstat()
                session.directory_identity = (info.st_dev, info.st_ino)
                await stack.enter_async_context(PreviewConnectBridge(
                    str(directory / "bridge.sock"), self.app.origins.host(session.preview_id),
                    authorize, expected_uid=1000, expected_gid=1000,
                ))
                cfg = self.configuration
                spec = WorkerSpec(
                    cfg.image_id, "krim-browser-" + uuid4().hex,
                    self.app.origins.origin(session.preview_id), self.app.origins.control_origin,
                    directory / "bridge.sock", cfg.ca_path, cfg.ca_sha256, cfg.seccomp_path,
                )
                session.spec = spec

                async def watch() -> None:
                    try:
                        while not session.stop.is_set():
                            await authorize()
                            try:
                                await asyncio.wait_for(session.stop.wait(), 0.25)
                            except TimeoutError:
                                pass
                    except Exception:
                        session.stop.set()

                watcher = asyncio.create_task(watch())
                try:
                    await authorize()
                    await settled(self._create(session, spec))
                    await self._verify_container(session, spec)
                    await authorize()
                    await settled(self._attach(session))
                    bootstrap = await self.app.issue_bootstrap(
                        session.preview_id, session.principal,
                    )
                    await authorize()
                    response = await self._active_exchange(
                        session, {"op": "bootstrap", "ticket": bootstrap.ticket},
                    )
                    if response.get("ready") is not True:
                        raise ServiceError("browser_worker_failed", 503)
                    await authorize()
                    session.ready.set_result(None)
                    while not session.stop.is_set():
                        try:
                            current_action = await asyncio.wait_for(session.queue.get(), 0.25)
                        except TimeoutError:
                            continue
                        await authorize()
                        body = current_action.body
                        download = body.get("op") == "download"
                        if download and (set(body) != {"op", "path", "cookie_mode"}
                                or not isinstance(body["path"], str)
                                or body["cookie_mode"] not in {"context", "preview_only"}):
                            raise ServiceError("browser_invalid_input", 422)
                        response = await self._active_exchange(
                            session, {**body, "op": "download_context"} if download else body,
                        )
                        await authorize()
                        result = response.get("result")
                        if not isinstance(result, dict):
                            raise ServiceError("browser_protocol", 503)
                        if download:
                            from .browser_downloads import inspect_download

                            async with asyncio.timeout(10):
                                result = await self._active_operation(session, inspect_download(
                                    self.app.service.routes, lease, authorize,
                                    host=self.app.origins.host(session.preview_id),
                                    origin=self.app.origins.origin(session.preview_id),
                                    path=body["path"], cookie_mode=body["cookie_mode"],
                                    descriptor=result,
                                ))
                            await authorize()
                        if not current_action.result.done():
                            current_action.result.set_result(result)
                        current_action = None
                finally:
                    session.stop.set()
                    watcher.cancel()
                    await asyncio.gather(watcher, return_exceptions=True)
                    clean = False
                    await settled(self._cleanup(session, spec))
                    clean = True
        except BaseException:
            session.failure = "browser_worker_failed"
        finally:
            session.stop.set()
            error = ServiceError(session.failure or "browser_closed", 410)
            if not session.ready.done():
                session.ready.set_exception(error)
            if current_action is not None and not current_action.result.done():
                current_action.result.set_exception(error)
            while not session.queue.empty():
                action = session.queue.get_nowait()
                if not action.result.done():
                    action.result.set_exception(error)
            if clean:
                self._remove_directory(session)
                self.sessions.pop(session.session_id, None)
            else:
                raise ServiceError("browser_worker_failed", 503)
