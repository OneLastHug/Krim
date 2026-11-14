"""Preview-route-v1 adapter over bound Docker exec relays; no published ports."""
from __future__ import annotations

import asyncio
import socket
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import ProcessHandle
from krim_agent.development.ports import ProcessRecord, command_digest
from krim_agent.development.preview_ports import (
    PreviewRouteError,
    PreviewRouteSource,
    RouteBinding,
    RouteLease,
    ServiceInstance,
)
from krim_agent.security import PrincipalContext

from . import docker
from .backend import DevelopmentBackend
from .model import RuntimeSpec, identity
from .preview_control import PreviewControl, PreviewLimits
from .preview_transport import ExecRelay, RelayNotListening, RelayUnavailable


@dataclass
class _Lease:
    public: RouteLease
    principal: PrincipalContext | None
    deadline: float
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    relays: set[ExecRelay] = field(default_factory=set)
    timer: asyncio.Task[None] | None = None
    closed: bool = False


class DockerPreviewRoutes:
    """I owns registrations/authorization. This table only owns ephemeral resources."""

    def __init__(self, environments: DevelopmentBackend, source: PreviewRouteSource, *,
                 limits: PreviewLimits | None = None, control: docker.Runner | None = None,
                 relay_factory: Callable[..., ExecRelay] = ExecRelay) -> None:
        self.environments = environments
        self.source = source
        self.limits = limits or PreviewLimits()
        self.control = control or PreviewControl(self.limits.control_seconds)
        self._relay_factory = relay_factory
        self._leases: dict[str, _Lease] = {}
        self._connections: set[ExecRelay] = set()
        self._allocations: dict[ExecRelay, tuple[_Lease, tuple[str, str, str, int, str]]] = {}
        self._service_counts: dict[tuple[str, str, str, int, str], int] = {}
        self._closed = False

    @staticmethod
    def _scope(binding: RouteBinding) -> tuple[str, str, str, int]:
        return (binding.owner_id, binding.project_id, binding.environment_id, binding.generation)

    @staticmethod
    def _row(row: ProcessRecord, binding: RouteBinding | None = None) -> None:
        try:
            row.handle.require_binding(row.request)
        except ValueError:
            raise PreviewRouteError("preview_route_invalidated") from None
        if (row.request.lifecycle != "project_service" or row.handle.lifecycle != "project_service"
                or row.handle.state != "running" or row.execution_id is None
                or row.request_digest != command_digest(row.request)):
            raise PreviewRouteError("preview_route_invalidated")
        if binding is not None and (
            identity(row.request.environment) != DockerPreviewRoutes._scope(binding)
            or row.handle.process_id != binding.process_id
            or row.handle.operation_id != binding.operation_id
            or row.request_digest != binding.request_digest
            or row.execution_id != binding.execution_id
        ):
            raise PreviewRouteError("preview_route_invalidated")

    async def _runtime(self, row: ProcessRecord) -> ServiceInstance:
        env = await self.environments.repository.resolve(row.request.environment)
        if (identity(env.environment) != identity(row.request.environment)
                or env.environment.state != "ready"):
            raise PreviewRouteError("preview_route_invalidated")
        await self.environments.authorizer(env.environment, "inspect")
        idle = RuntimeSpec(env)
        if env.configuration_digest not in (None, idle.fingerprint):
            raise PreviewRouteError("preview_route_invalidated")
        self.environments.mount_check(idle)
        idle_value = await docker.inspect(self.control, idle)
        if idle_value is None or docker.verify(idle, idle_value) != "ready":
            raise PreviewRouteError("preview_route_invalidated")
        spec = RuntimeSpec(env, row.request, row.handle.process_id, row.execution_id)
        await docker.check_network(self.control, spec)
        value = await docker.inspect(self.control, spec)
        if value is None or docker.verify(spec, value) != "ready":
            raise PreviewRouteError("preview_route_invalidated")
        return ServiceInstance(execution_id=str(value["Id"]),
                               started_at=value["State"]["StartedAt"])

    @asynccontextmanager
    async def _validation(self, owner: str, project: str) -> AsyncIterator[None]:
        try:
            async with asyncio.timeout(self.limits.control_seconds):
                async with self.environments.locks.hold(owner, project):
                    yield
        except PreviewRouteError:
            raise
        except ServiceError as error:
            if error.status in {401, 403, 404}:
                raise PreviewRouteError("preview_route_denied") from None
            raise PreviewRouteError("preview_route_unavailable") from None
        except Exception:
            raise PreviewRouteError("preview_route_unavailable") from None

    async def observe(self, process: ProcessHandle, port: int,
                      principal: PrincipalContext) -> ServiceInstance:
        if self._closed or type(port) is not int or not 1 <= port <= 65535 or principal is None:
            raise PreviewRouteError("preview_route_denied")
        async with self._validation(process.environment.owner_id, process.environment.project_id):
            row = await self.source.registration(process, port, principal)
            self._row(row)
            try:
                row.handle.require_binding(process)
            except ValueError:
                raise PreviewRouteError("preview_route_invalidated") from None
            return await self._runtime(row)

    async def _validate(self, binding: RouteBinding,
                        principal: PrincipalContext | None) -> None:
        async with self._validation(binding.owner_id, binding.project_id):
            row = await self.source.resolve(binding, principal)
            self._row(row, binding)
            instance = await self._runtime(row)
            if (instance.execution_id != binding.execution_id
                    or instance.started_at != binding.started_at):
                raise PreviewRouteError("preview_route_invalidated")

    def _live(self, lease: RouteLease) -> _Lease:
        state = self._leases.get(lease.lease_id)
        if (self._closed or state is None or state.closed or state.public != lease
                or time.monotonic() >= state.deadline):
            if state is not None:
                self._invalidate(state)
            raise PreviewRouteError("preview_route_invalidated")
        return state

    def _invalidate(self, state: _Lease) -> None:
        if state.closed:
            return
        state.closed = True
        self._leases.pop(state.public.lease_id, None)
        state.wake.set()
        for relay in tuple(state.relays):
            relay.invalidate()  # synchronous peer.shutdown; no drain or project lock.

    async def _expiry(self, state: _Lease) -> None:
        while not state.closed:
            state.wake.clear()
            remaining = state.deadline - time.monotonic()
            if remaining <= 0:
                self._invalidate(state)
                return
            try:
                async with asyncio.timeout(remaining):
                    await state.wake.wait()
            except TimeoutError:
                pass

    async def _close_lease(self, state: _Lease) -> None:
        self._invalidate(state)
        relays = tuple(state.relays)
        if relays:
            await asyncio.gather(*(relay.aclose() for relay in relays), return_exceptions=True)
        if state.timer is not None:
            await state.timer

    @asynccontextmanager
    async def acquire(self, binding: RouteBinding,
                      principal: PrincipalContext | None) -> AsyncIterator[RouteLease]:
        if self._closed:
            raise PreviewRouteError("preview_route_unavailable")
        await self._validate(binding, principal)
        if self._closed:
            raise PreviewRouteError("preview_route_unavailable")
        # Bound even acquire-only callers that never open a connection.
        if len(self._leases) >= 4 * self.limits.adapter_connections:
            raise PreviewRouteError("preview_route_capacity")
        public = RouteLease(binding.model_copy(deep=True), uuid.uuid4().hex)
        state = _Lease(public, principal, time.monotonic() + self.limits.lease_seconds)
        self._leases[public.lease_id] = state
        state.timer = asyncio.create_task(self._expiry(state))
        try:
            yield public  # never under the project launch lock
        finally:
            await self._close_lease(state)

    async def check(self, lease: RouteLease) -> None:
        state = self._live(lease)
        try:
            await self._validate(state.public.binding, state.principal)
            self._live(lease)  # expiry during a blocked lookup cannot be revived
        except BaseException:
            self._invalidate(state)
            raise
        state.deadline = time.monotonic() + self.limits.lease_seconds
        state.wake.set()

    def _release_connection(self, relay: ExecRelay) -> None:
        allocation = self._allocations.pop(relay, None)
        if allocation is None:
            return
        state, key = allocation
        state.relays.discard(relay)
        self._connections.discard(relay)
        count = self._service_counts[key] - 1
        if count:
            self._service_counts[key] = count
        else:
            self._service_counts.pop(key, None)

    @asynccontextmanager
    async def _connect(self, lease: RouteLease) -> AsyncIterator[socket.socket]:
        state = self._live(lease)
        try:
            await self._validate(state.public.binding, state.principal)
            self._live(lease)
        except BaseException:
            self._invalidate(state)
            raise
        key = (*self._scope(lease.binding), lease.binding.execution_id)
        if (len(self._connections) >= self.limits.adapter_connections
                or self._service_counts.get(key, 0) >= self.limits.service_connections):
            raise PreviewRouteError("preview_route_capacity")
        # Reserve atomically in this event-loop turn, before any relay await.
        try:
            relay = self._relay_factory(lease.binding.execution_id, lease.binding.port,
                                        self.limits, self._release_connection)
        except Exception:
            raise PreviewRouteError("preview_route_unavailable") from None
        self._service_counts[key] = self._service_counts.get(key, 0) + 1
        self._connections.add(relay)
        state.relays.add(relay)
        self._allocations[relay] = (state, key)
        try:
            try:
                await relay.start()  # TCP connect only; no E endpoint/business bytes yet
                self._live(lease)
                await self._validate(state.public.binding, state.principal)
                self._live(lease)
                endpoint = relay.attach_socket()
            except RelayNotListening:
                raise
            except (RelayUnavailable, ConnectionError, OSError, TimeoutError):
                raise PreviewRouteError("preview_route_unavailable") from None
            except PreviewRouteError:
                self._invalidate(state)
                raise
            yield endpoint  # caller errors are not rewritten as routing errors
        finally:
            # The relay's settled callback releases capacity. Cancellation or a
            # delayed subprocess spawn must not release it before actual reap.
            await relay.aclose()

    @asynccontextmanager
    async def connect(self, lease: RouteLease) -> AsyncIterator[socket.socket]:
        if self._live(lease).principal is None:
            raise PreviewRouteError("preview_route_denied")
        try:
            async with self._connect(lease) as endpoint:
                yield endpoint
        except RelayNotListening:
            raise PreviewRouteError("preview_route_unavailable") from None

    async def ready(self, lease: RouteLease) -> bool:
        try:
            async with self._connect(lease):
                return True  # no health request or payload
        except RelayNotListening:
            return False

    async def aclose(self) -> None:
        self._closed = True
        states = tuple(self._leases.values())
        for state in states:
            self._invalidate(state)
        await asyncio.gather(*(self._close_lease(state) for state in states))
        await asyncio.gather(*(relay.aclose() for relay in tuple(self._connections)),
                             return_exceptions=True)
