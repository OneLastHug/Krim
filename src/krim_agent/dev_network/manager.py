"""One bounded, fail-closed download session per exact Docker execution unit."""
from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import dataclass, field
from typing import Any

from krim_agent.dev_environment import docker
from krim_agent.dev_environment.model import EnvironmentError, RuntimeSpec, identity

from .broker import MAX_LINE, DownloadBroker
from .lifecycle import settle
from .policy import PolicyBinding, PolicyResolver, bind
from .shim import SHIM_CODE
from .tls import certificate


@dataclass
class _Lease:
    spec: RuntimeSpec
    task: asyncio.Task[None] | None = None
    process: asyncio.subprocess.Process | None = None
    broker: DownloadBroker | None = None
    closed: bool = False
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    release_task: asyncio.Task[None] | None = None


class DownloadNetworks:
    """I injects current D policy resolution and awaits invalidate at revocation.

    No independent grants, persistence records, network IDs or proxy credentials.
    A dead host/CLI, malformed shim, denied resolution or expired session closes
    the pipe; the container always retains Docker/runsc network=none.
    """

    def __init__(self, resolver: PolicyResolver, *, runner: docker.Runner | None = None,
                 max_sessions: int = 64, max_age: float = 900) -> None:
        if (type(max_sessions) is not int or not 1 <= max_sessions <= 256
                or not math.isfinite(max_age) or not 1 <= max_age <= 900):
            raise ValueError("invalid download limits")
        self.resolver = resolver
        self.runner = runner or docker.DockerCLI()
        if self.runner.prefix != docker.DockerCLI.prefix:
            raise ValueError("download runner requires the fixed local Docker socket")
        self.max_sessions, self.max_age = max_sessions, max_age
        self._leases: dict[str, _Lease] = {}
        self._epochs: dict[tuple[str, str], int] = {}
        self._closed = False

    async def check(self, spec: RuntimeSpec) -> None:
        if spec.network.network_id is not None:
            raise EnvironmentError("uncontrolled_network_forbidden")
        if spec.network.policy_ref is not None:
            await bind(spec, self.resolver)

    async def arm(self, spec: RuntimeSpec, container_id: str) -> None:
        if spec.network.policy_ref is None:
            return
        scope = identity(spec.handle)[:2]
        epoch = self._epochs.get(scope, 0)
        binding = await bind(spec, self.resolver)
        if self._closed or self._epochs.get(scope, 0) != epoch:
            raise EnvironmentError("network_authorization_expired")
        existing = self._leases.get(container_id)
        if existing is not None:
            if existing.closed or existing.spec.fingerprint != spec.fingerprint:
                raise EnvironmentError("network_session_unavailable")
            return
        if len(self._leases) >= self.max_sessions:
            raise EnvironmentError("network_session_capacity")
        # Register before the next await so invalidate also catches pending arms.
        lease = _Lease(spec)
        self._leases[container_id] = lease
        try:
            value = await docker.inspect(self.runner, spec)
            if value is None or value["Id"] != container_id:
                raise EnvironmentError("network_container_missing")
            docker.verify(spec, value)
            if lease.closed:
                raise EnvironmentError("network_authorization_expired")
            await binding.current()
            if lease.closed:
                raise EnvironmentError("network_authorization_expired")
            lease.task = asyncio.create_task(self._run(lease, container_id, binding))
        except BaseException:
            lease.closed = True
            lease.ready.set()
            if self._leases.get(container_id) is lease:
                self._leases.pop(container_id)
            raise

    async def wait_ready(self, container_id: str) -> None:
        lease = self._leases.get(container_id)
        if lease is None:
            raise EnvironmentError("network_session_missing")
        try:
            async with asyncio.timeout(15):
                await lease.ready.wait()
        except TimeoutError:
            await self.release(container_id)
            raise EnvironmentError("network_session_unavailable") from None
        if lease.closed:
            raise EnvironmentError("network_session_unavailable")

    async def _run(self, lease: _Lease, container_id: str, binding: PolicyBinding) -> None:
        process = None
        tasks: list[asyncio.Task[Any]] = []
        try:
            await binding.current()
            age = min(self.max_age, binding.policy.grant.expires_at - time.time())
            if age < 1:
                raise EnvironmentError("network_authorization_expired")
            cert, key = await asyncio.to_thread(certificate, binding.policy, age)
            # C persists create-only executions before launching them. No exec
            # runs until that exact verified unit has actually started.
            async with asyncio.timeout(30):
                while True:
                    await binding.current()
                    value = await docker.inspect(self.runner, lease.spec)
                    if value is None or value["Id"] != container_id:
                        raise EnvironmentError("network_container_missing")
                    state = docker.verify(lease.spec, value)
                    if state == "ready":
                        break
                    if value["State"]["Status"] != "created":
                        raise EnvironmentError("network_container_stopped")
                    await asyncio.sleep(.05)
            argv = (*self.runner.prefix, "exec", "--interactive", "--detach-keys=",
                    "--user=65532:65532", "--workdir=/", container_id,
                    "/usr/bin/python3", "-I", "-c", SHIM_CODE)
            spawn = asyncio.create_task(asyncio.create_subprocess_exec(
                *argv, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, limit=MAX_LINE,
                env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
            ))
            try:
                process = await asyncio.shield(spawn)
            except BaseException:
                try:
                    await settle(spawn)
                finally:
                    if spawn.done() and not spawn.cancelled() and spawn.exception() is None:
                        process = spawn.result()
                raise
            lease.process = process
            assert process.stdin and process.stdout and process.stderr
            lock = asyncio.Lock()

            async def send(message: dict[str, Any]) -> None:
                async with lock:
                    assert process.stdin
                    process.stdin.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")
                    async with asyncio.timeout(5):
                        await process.stdin.drain()

            async def heartbeat() -> None:
                while True:
                    await binding.current()
                    await send({"ping": True})
                    await asyncio.sleep(.5)

            async def stderr() -> None:
                assert process.stderr
                count = 0
                while data := await process.stderr.read(4096):
                    count += len(data)
                    if count > 16384:
                        raise EnvironmentError("network_proxy_failed")

            tasks.append(asyncio.create_task(stderr()))
            await send({"cert": cert, "key": key, "age": age})
            del key
            async with asyncio.timeout(10):
                line = await process.stdout.readline()
            if line != b'{"ready":true}\n':
                raise EnvironmentError("network_proxy_failed")
            lease.broker = DownloadBroker(binding)
            lease.ready.set()
            tasks += [asyncio.create_task(heartbeat()),
                      asyncio.create_task(lease.broker.watch()),
                      asyncio.create_task(lease.broker.serve(process.stdout, send)),
                      asyncio.create_task(process.wait()),
                      asyncio.create_task(asyncio.sleep(age))]
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except Exception:
            pass  # Raw process/URL/TLS data never escapes to a model or logs.
        finally:
            lease.closed = True
            lease.ready.set()
            try:
                await settle(asyncio.create_task(self._cleanup(lease, process, tasks)))
            finally:
                if self._leases.get(container_id) is lease:
                    self._leases.pop(container_id)

    async def _cleanup(self, lease: _Lease, process: asyncio.subprocess.Process | None,
                       tasks: list[asyncio.Task[Any]]) -> None:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if lease.broker is not None:
            await lease.broker.close()
        if process is not None:
            if process.stdin:
                process.stdin.close()
            try:
                async with asyncio.timeout(2):
                    await process.wait()
            except TimeoutError:
                if process.returncode is None:
                    process.kill()
                await process.wait()

    async def _release(self, container_id: str, lease: _Lease) -> None:
        lease.closed = True
        lease.ready.set()
        if lease.broker is not None:
            await lease.broker.close()
        if lease.task is not None:
            lease.task.cancel()
            await asyncio.gather(lease.task, return_exceptions=True)
        if self._leases.get(container_id) is lease:
            self._leases.pop(container_id)

    async def release(self, container_id: str) -> None:
        lease = self._leases.get(container_id)
        if lease is None:
            return
        if lease.release_task is None:
            lease.release_task = asyncio.create_task(self._release(container_id, lease))
        await settle(lease.release_task)

    async def invalidate(self, owner_id: str, project_id: str, *,
                         policy_id: str | None = None) -> None:
        """Trusted I revoke/rotate boundary: close existing streams before return."""
        scope = (owner_id, project_id)
        self._epochs[scope] = self._epochs.get(scope, 0) + 1
        targets = [cid for cid, lease in self._leases.items()
                   if identity(lease.spec.handle)[:2] == (owner_id, project_id)
                   and (policy_id is None or lease.spec.network.policy_ref == policy_id)]
        await asyncio.gather(*(self.release(cid) for cid in targets))

    async def close(self) -> None:
        self._closed = True
        await asyncio.gather(*(self.release(cid) for cid in tuple(self._leases)))
