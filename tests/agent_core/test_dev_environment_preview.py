from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

_shared_src = os.environ.get("KRIM_PREVIEW_SHARED_SRC")

import krim_agent.development as _development_package  # noqa: E402

if _shared_src:
    if not Path(_shared_src).is_absolute():
        raise RuntimeError("KRIM_PREVIEW_SHARED_SRC must be an absolute shared src path")
    _development_package.__path__.append(
        str(Path(_shared_src) / "krim_agent" / "development")
    )

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402

try:  # noqa: E402
    from krim_agent.development.preview_ports import (  # noqa: E402
        PreviewRouteError,
        RouteBinding,
        RouteLease,
        ServiceInstance,
    )
except ModuleNotFoundError as error:  # noqa: E402
    if error.name != "krim_agent.development.preview_ports":
        raise
    pytest.skip("preview_ports is provided by the shared I worktree", allow_module_level=True)

from test_dev_environment_execution import ExecutionRunner, make_process  # noqa: E402

from krim_agent.contracts import ServiceError  # noqa: E402
from krim_agent.dev_environment import DevelopmentBackend  # noqa: E402
from krim_agent.dev_environment.model import RuntimeSpec  # noqa: E402
from krim_agent.dev_environment.preview import DockerPreviewRoutes  # noqa: E402
from krim_agent.dev_environment.preview_control import (  # noqa: E402
    PreviewControl,
    PreviewLimits,
)
from krim_agent.dev_environment.preview_transport import (  # noqa: E402
    RelayNotListening,
    RelayUnavailable,
)
from krim_agent.development.contracts import ProcessHandle  # noqa: E402
from krim_agent.development.ports import ProcessRecord  # noqa: E402
from krim_agent.security import Membership, PrincipalContext  # noqa: E402

pytest_plugins = ("test_dev_environment_execution",)


PORT = 3000
SECOND_PORT = 3001
EXECUTION_ID = "a" * 64
SECOND_EXECUTION_ID = "b" * 64
STARTED_AT = "2026-11-15T00:00:00.000000001Z"
SECOND_STARTED_AT = "2026-11-15T00:00:01.000000001Z"
OWNER_PRINCIPAL = PrincipalContext(
    "owner", 1, (Membership(workspace_id="local", role="member"),)
)
VIEWER_PRINCIPAL = PrincipalContext(
    "viewer", 7, (Membership(workspace_id="local", role="viewer"),)
)
OTHER_PRINCIPAL = PrincipalContext(
    "other", 1, (Membership(workspace_id="local", role="member"),)
)


class PreviewRunner(ExecutionRunner):
    """ExecutionRunner with the Docker StartedAt field required by preview."""

    def __init__(self) -> None:
        super().__init__()
        self.started_at: dict[str, str] = {}
        self.block_inspect = False
        self.inspect_started = asyncio.Event()
        self.inspect_release = asyncio.Event()
        self.inspect_release.set()

    async def run(self, args: tuple[str, ...]):
        if args[:2] == ("container", "inspect") and self.block_inspect:
            self.inspect_started.set()
            await self.inspect_release.wait()
        return await super().run(args)

    def inspect_value(
        self,
        spec: RuntimeSpec,
        cid: str,
        state: str,
        exit_code: int | None = None,
    ) -> dict[str, Any]:
        value = super().inspect_value(spec, cid, state, exit_code)
        value["State"]["StartedAt"] = self.started_at.get(cid, STARTED_AT)
        return value


class RelayFactory:
    def __init__(self) -> None:
        self.instances: list[FakeRelay] = []
        self.factory_error: BaseException | None = None
        self.start_error: BaseException | None = None
        self.listening = True

    def __call__(
        self,
        execution_id: str,
        port: int,
        limits: PreviewLimits,
        ended: Callable[[Any], None],
    ) -> FakeRelay:
        if self.factory_error is not None:
            error = self.factory_error
            self.factory_error = None
            raise error
        relay = FakeRelay(execution_id, port, limits, ended, self)
        self.instances.append(relay)
        return relay


class FakeRelay:
    """A real socketpair endpoint with no Docker process behind it."""

    def __init__(
        self,
        execution_id: str,
        port: int,
        limits: PreviewLimits,
        ended: Callable[[Any], None],
        factory: RelayFactory,
    ) -> None:
        self.execution_id = execution_id
        self.port = port
        self.limits = limits
        self._ended = ended
        self._factory = factory
        self.endpoint: socket.socket | None = None
        self.peer: socket.socket | None = None
        self.started = False
        self.invalidated = False
        self.peer_shutdown = False
        self.closed = False

    async def start(self) -> None:
        if self._factory.start_error is not None:
            error = self._factory.start_error
            self._factory.start_error = None
            raise error
        if not self._factory.listening:
            raise RelayNotListening
        self.started = True

    def attach_socket(self) -> socket.socket:
        if not self.started or self.invalidated:
            raise RelayUnavailable
        self.endpoint, self.peer = socket.socketpair()
        self.endpoint.setblocking(False)
        self.peer.setblocking(False)
        return self.endpoint

    def invalidate(self) -> None:
        if self.invalidated:
            return
        self.invalidated = True
        if self.peer is not None:
            try:
                self.peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.peer_shutdown = True
        for endpoint in (self.endpoint, self.peer):
            if endpoint is not None:
                endpoint.close()
        self.closed = True
        self._ended(self)

    async def aclose(self) -> None:
        self.invalidate()


@dataclass
class FakePreviewSource:
    row: ProcessRecord
    records: dict[str, ProcessRecord] = field(default_factory=dict)
    registration_calls: list[tuple[ProcessHandle, int, PrincipalContext]] = field(
        default_factory=list
    )
    resolve_calls: list[tuple[RouteBinding, PrincipalContext | None]] = field(
        default_factory=list
    )
    expected_bindings: dict[str, RouteBinding] = field(default_factory=dict)
    failure: BaseException | None = None
    blocked: bool = False
    gate: asyncio.Event | None = None
    hook: Callable[[str, int], None] | None = None

    def __post_init__(self) -> None:
        self.records.setdefault(self.row.handle.process_id, self.row)

    async def _before(self, method: str, count: int) -> None:
        if self.hook is not None:
            self.hook(method, count)
        if self.blocked:
            if self.gate is None:
                self.gate = asyncio.Event()
            await self.gate.wait()
        if self.failure is not None:
            raise self.failure

    async def registration(
        self, process: ProcessHandle, port: int, principal: PrincipalContext
    ) -> ProcessRecord:
        self.registration_calls.append((process, port, principal))
        await self._before("registration", len(self.registration_calls))
        return self.records.get(process.process_id, self.row)

    async def resolve(
        self, binding: RouteBinding, principal: PrincipalContext | None
    ) -> ProcessRecord:
        self.resolve_calls.append((binding, principal))
        await self._before("resolve", len(self.resolve_calls))
        expected = self.expected_bindings.get(binding.process_id)
        if expected is not None and (
            binding.preview_id != expected.preview_id
            or binding.port != expected.port
            or binding.route_epoch != expected.route_epoch
        ):
            raise ServiceError("not_found", 404)
        return self.records.get(binding.process_id, self.row)


@dataclass
class PreviewFixture:
    backend: DevelopmentBackend
    runner: PreviewRunner
    source: FakePreviewSource
    factory: RelayFactory
    record: Any
    request: Any
    process: ProcessRecord
    binding: RouteBinding
    routes: DockerPreviewRoutes


def _binding(
    process: ProcessRecord, *, execution_id: str, started_at: str, port: int
) -> RouteBinding:
    environment = process.request.environment
    return RouteBinding(
        execution_id=execution_id,
        started_at=started_at,
        preview_id="preview-1",
        owner_id=environment.owner_id,
        project_id=environment.project_id,
        environment_id=environment.environment_id,
        generation=environment.generation,
        service_id=process.handle.process_id,
        process_id=process.handle.process_id,
        operation_id=process.handle.operation_id,
        request_digest=process.request_digest,
        port=port,
        route_epoch=1,
    )


def _short_limits(**updates: Any) -> PreviewLimits:
    values: dict[str, Any] = {
        "control_seconds": 0.5,
        "connect_seconds": 0.5,
        "lease_seconds": 0.2,
        "heartbeat_seconds": 0.05,
        "max_age_seconds": 1.0,
        "idle_seconds": 1.0,
        "service_connections": 2,
        "adapter_connections": 4,
    }
    values.update(updates)
    return PreviewLimits(**values)


@pytest_asyncio.fixture
async def preview_env(execution_env) -> PreviewFixture:
    (
        _executions,
        backend,
        _bindings,
        processes,
        original_runner,
        record,
        original_request,
        _original_process,
    ) = execution_env

    runner = PreviewRunner()
    runner.containers.update(original_runner.containers)
    runner.specs_by_name.update(original_runner.specs_by_name)
    backend.runner = runner
    runner.started_at[record.container_id or ""] = STARTED_AT

    request = original_request.model_copy(
        update={"lifecycle": "project_service", "timeout_seconds": None}
    )
    process = make_process(request, state="running")
    process = replace(process, execution_id=EXECUTION_ID)
    processes.set_row(process)
    spec = RuntimeSpec(record, request, process.handle.process_id, EXECUTION_ID)
    runner.add_container(spec, state="running", container_id=EXECUTION_ID)
    runner.started_at[EXECUTION_ID] = STARTED_AT

    source = FakePreviewSource(process)
    factory = RelayFactory()
    binding = _binding(
        process,
        execution_id=EXECUTION_ID,
        started_at=STARTED_AT,
        port=PORT,
    )
    source.expected_bindings[process.handle.process_id] = binding
    routes = DockerPreviewRoutes(
        backend,
        source,
        limits=_short_limits(),
        control=runner,
        relay_factory=factory,
    )
    value = PreviewFixture(
        backend, runner, source, factory, record, request, process, binding, routes
    )
    try:
        yield value
    finally:
        await routes.aclose()


def _add_second_service(value: PreviewFixture) -> RouteBinding:
    request = value.request.model_copy(update={"operation_id": "operation-2"})
    process = make_process(request, "process-2", state="running")
    process = replace(process, execution_id=SECOND_EXECUTION_ID)
    value.source.records[process.handle.process_id] = process
    spec = RuntimeSpec(value.record, request, process.handle.process_id, SECOND_EXECUTION_ID)
    value.runner.add_container(spec, state="running", container_id=SECOND_EXECUTION_ID)
    value.runner.started_at[SECOND_EXECUTION_ID] = SECOND_STARTED_AT
    binding = _binding(
        process,
        execution_id=SECOND_EXECUTION_ID,
        started_at=SECOND_STARTED_AT,
        port=SECOND_PORT,
    )
    value.source.expected_bindings[process.handle.process_id] = binding
    return binding


async def _finish_lock(
    locks, owner: str, project: str, entered: asyncio.Event, release: asyncio.Event
):
    async with locks.hold(owner, project):
        entered.set()
        await release.wait()


@pytest.mark.asyncio
async def test_observe_only_authorizes_and_inspects_without_start_or_connect(preview_env):
    value = preview_env

    instance = await value.routes.observe(value.process.handle, PORT, OWNER_PRINCIPAL)

    assert instance == ServiceInstance(execution_id=EXECUTION_ID, started_at=STARTED_AT)
    assert value.source.registration_calls == [(value.process.handle, PORT, OWNER_PRINCIPAL)]
    assert not any(call and call[0] in {"start", "exec"} for call in value.runner.calls)
    assert value.factory.instances == []


@pytest.mark.asyncio
async def test_acquire_and_connect_revalidate_and_yield_without_project_lock(preview_env):
    value = preview_env

    async with value.routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
        assert isinstance(lease, RouteLease)
        assert value.source.resolve_calls == [(value.binding, OWNER_PRINCIPAL)]

        async def get_lock() -> bool:
            async with value.backend.locks.hold("owner-1", "project-1"):
                return True

        assert await asyncio.wait_for(asyncio.create_task(get_lock()), timeout=0.5)
        async with value.routes.connect(lease) as endpoint:
            assert endpoint.getblocking() is False
            assert len(value.source.resolve_calls) == 3
            assert all(principal == OWNER_PRINCIPAL for _, principal in value.source.resolve_calls)
            assert len(value.factory.instances) == 1

    assert not value.routes._connections
    assert not value.routes._service_counts


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "updated", "expected"),
    [
        ("owner_id", "other-owner", "preview_route_invalidated"),
        ("project_id", "other-project", "preview_route_invalidated"),
        ("environment_id", "other-environment", "preview_route_invalidated"),
        ("generation", 2, "preview_route_invalidated"),
        ("port", 3001, "preview_route_denied"),
        ("route_epoch", 2, "preview_route_denied"),
        ("execution_id", SECOND_EXECUTION_ID, "preview_route_invalidated"),
        ("operation_id", "other-operation", "preview_route_invalidated"),
        ("request_digest", "c" * 64, "preview_route_invalidated"),
        ("preview_id", "preview-2", "preview_route_denied"),
    ],
)
async def test_binding_drift_is_rejected(
    field: str, updated: Any, expected: str, preview_env
):
    value = preview_env
    forged = value.binding.model_copy(update={field: updated})

    with pytest.raises(PreviewRouteError) as raised:
        async with value.routes.acquire(forged, OWNER_PRINCIPAL):
            raise AssertionError("drift must fail before yielding")

    assert raised.value.code == expected
    assert not value.routes._leases


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "updated"),
    [
        ("lifecycle", "run"),
        ("state", "accepted"),
        ("state", "failed"),
    ],
)
async def test_canonical_record_lifecycle_and_state_drift_is_rejected(
    field: str, updated: Any, preview_env
):
    value = preview_env
    handle = value.process.handle.model_copy(update={field: updated})
    value.source.records[value.process.handle.process_id] = replace(value.process, handle=handle)

    with pytest.raises(PreviewRouteError) as raised:
        async with value.routes.acquire(value.binding, OWNER_PRINCIPAL):
            raise AssertionError("record drift must fail before yielding")

    assert raised.value.code == "preview_route_invalidated"


@pytest.mark.asyncio
async def test_started_at_and_execution_observation_drift_invalidates(preview_env):
    value = preview_env

    async with value.routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
        value.runner.started_at[EXECUTION_ID] = "2026-11-15T00:00:02.000000001Z"
        with pytest.raises(PreviewRouteError) as raised:
            await value.routes.check(lease)
        assert raised.value.code == "preview_route_invalidated"


@pytest.mark.asyncio
async def test_original_viewer_is_retained_for_renewal_and_none_is_readiness_only(preview_env):
    value = preview_env

    async with value.routes.acquire(value.binding, VIEWER_PRINCIPAL) as viewer_lease:
        await value.routes.check(viewer_lease)
        assert value.source.resolve_calls[-1][1] == VIEWER_PRINCIPAL
        assert value.routes._leases[viewer_lease.lease_id].principal == VIEWER_PRINCIPAL

    value.source.resolve_calls.clear()
    async with value.routes.acquire(value.binding, None) as readiness_lease:
        await value.routes.check(readiness_lease)
        assert await value.routes.ready(readiness_lease) is True
        assert value.source.resolve_calls[-1][1] is None
        assert value.routes._leases[readiness_lease.lease_id].principal is None
        with pytest.raises(PreviewRouteError) as raised:
            async with value.routes.connect(readiness_lease):
                raise AssertionError("readiness leases cannot become browser access")
        assert raised.value.code == "preview_route_denied"


@pytest.mark.asyncio
async def test_forged_lease_is_rejected_and_expiry_cannot_be_revived(preview_env):
    value = preview_env
    forged = RouteLease(value.binding, "not-a-live-lease")

    with pytest.raises(PreviewRouteError) as raised:
        await value.routes.check(forged)
    assert raised.value.code == "preview_route_invalidated"

    short_routes = DockerPreviewRoutes(
        value.backend,
        value.source,
        limits=_short_limits(lease_seconds=0.05),
        control=value.runner,
        relay_factory=value.factory,
    )
    try:
        async with short_routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
            await asyncio.sleep(0.08)
            with pytest.raises(PreviewRouteError) as expired:
                await short_routes.check(lease)
            assert expired.value.code == "preview_route_invalidated"
            assert lease.lease_id not in short_routes._leases
    finally:
        await short_routes.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("block_kind", ["resolve", "control", "lock"])
async def test_ttl_expires_independently_of_blocked_resolve_control_or_lock(
    block_kind, preview_env
):
    value = preview_env
    limits = _short_limits(lease_seconds=0.05, control_seconds=0.5)
    routes = DockerPreviewRoutes(
        value.backend,
        value.source,
        limits=limits,
        control=value.runner,
        relay_factory=value.factory,
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    holder: asyncio.Task[None] | None = None
    try:
        async with routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
            if block_kind == "resolve":
                value.source.blocked = True
            elif block_kind == "control":
                value.runner.block_inspect = True
                value.runner.inspect_release.clear()
            else:
                holder = asyncio.create_task(
                    _finish_lock(value.backend.locks, "owner-1", "project-1", entered, release)
                )
                await asyncio.wait_for(entered.wait(), timeout=0.5)

            checking = asyncio.create_task(routes.check(lease))
            await asyncio.sleep(0.08)
            assert lease.lease_id not in routes._leases

            if block_kind == "resolve":
                value.source.gate.set()
            elif block_kind == "control":
                value.runner.inspect_release.set()
            else:
                release.set()
            with pytest.raises(PreviewRouteError) as raised:
                await checking
            assert raised.value.code == "preview_route_invalidated"
    finally:
        value.source.blocked = False
        value.runner.block_inspect = False
        value.runner.inspect_release.set()
        release.set()
        if holder is not None:
            await holder
        await routes.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [(401, "preview_route_denied"), (403, "preview_route_denied"),
     (404, "preview_route_denied"), (409, "preview_route_unavailable"),
     (500, "preview_route_unavailable")],
)
async def test_source_failures_map_to_bounded_errors(status: int, expected: str, preview_env):
    value = preview_env
    value.source.failure = ServiceError("permission_denied", status)

    with pytest.raises(PreviewRouteError) as raised:
        await value.routes.observe(value.process.handle, PORT, OWNER_PRINCIPAL)

    assert raised.value.code == expected
    assert str(raised.value) == expected


@pytest.mark.asyncio
async def test_connect_uses_two_checks_and_never_yields_business_bytes_before_second_check(
    preview_env,
):
    value = preview_env
    seen: list[int] = []

    def hook(method: str, count: int) -> None:
        if method == "resolve":
            seen.append(count)

    value.source.hook = hook
    async with value.routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
        async with value.routes.connect(lease) as endpoint:
            relay = value.factory.instances[-1]
            assert seen == [1, 2, 3]
            await asyncio.get_running_loop().sock_sendall(relay.peer, b"reply")
            assert await asyncio.get_running_loop().sock_recv(endpoint, 6) == b"reply"


@pytest.mark.asyncio
async def test_half_close_uses_real_socketpair_and_adapter_close_shuts_peer(preview_env):
    value = preview_env
    async with value.routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
        connect = value.routes.connect(lease)
        endpoint = await connect.__aenter__()
        relay = value.factory.instances[-1]
        await asyncio.get_running_loop().sock_sendall(relay.peer, b"data")
        assert await asyncio.get_running_loop().sock_recv(endpoint, 4) == b"data"
        endpoint.shutdown(socket.SHUT_WR)
        assert await asyncio.get_running_loop().sock_recv(relay.peer, 1) == b""
        await connect.__aexit__(None, None, None)

    async with value.routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
        connect = value.routes.connect(lease)
        endpoint = await connect.__aenter__()
        relay = value.factory.instances[-1]
        endpoint.close()  # Simulate E transferring and closing the transport socket.
        await value.routes.aclose()
        assert relay.peer_shutdown
        assert relay.closed
        await connect.__aexit__(None, None, None)
        break_out = lease
        assert break_out.lease_id not in value.routes._leases


@pytest.mark.asyncio
async def test_capacity_is_released_for_service_adapter_and_failed_relays(preview_env):
    value = preview_env
    binding2 = _add_second_service(value)
    routes = DockerPreviewRoutes(
        value.backend,
        value.source,
        limits=_short_limits(service_connections=1, adapter_connections=2),
        control=value.runner,
        relay_factory=value.factory,
    )
    try:
        async with routes.acquire(value.binding, OWNER_PRINCIPAL) as first_lease:
            async with routes.acquire(value.binding, OWNER_PRINCIPAL) as second_lease:
                first = routes.connect(first_lease)
                first_endpoint = await first.__aenter__()
                with pytest.raises(PreviewRouteError) as raised:
                    async with routes.connect(second_lease):
                        raise AssertionError("service capacity must reject")
                assert raised.value.code == "preview_route_capacity"
                await first.__aexit__(None, None, None)
                async with routes.connect(second_lease):
                    pass
                assert not routes._service_counts
                first_endpoint.close()

        routes2 = DockerPreviewRoutes(
            value.backend,
            value.source,
            limits=_short_limits(service_connections=1, adapter_connections=1),
            control=value.runner,
            relay_factory=value.factory,
        )
        try:
            async with routes2.acquire(value.binding, OWNER_PRINCIPAL) as lease1:
                async with routes2.acquire(binding2, OWNER_PRINCIPAL) as lease2:
                    first = routes2.connect(lease1)
                    await first.__aenter__()
                    with pytest.raises(PreviewRouteError) as raised:
                        async with routes2.connect(lease2):
                            raise AssertionError("adapter capacity must reject")
                    assert raised.value.code == "preview_route_capacity"
                    await first.__aexit__(None, None, None)
                    async with routes2.connect(lease2):
                        pass
                    assert not routes2._connections
        finally:
            await routes2.aclose()

        value.factory.factory_error = RuntimeError("factory")
        async with routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
            with pytest.raises(PreviewRouteError) as raised:
                async with routes.connect(lease):
                    pass
            assert raised.value.code == "preview_route_unavailable"
            assert not routes._connections and not routes._service_counts

            value.factory.start_error = RelayUnavailable()
            with pytest.raises(PreviewRouteError) as raised:
                async with routes.connect(lease):
                    pass
            assert raised.value.code == "preview_route_unavailable"
            assert not routes._connections and not routes._service_counts

            value.factory.start_error = RelayNotListening()
            with pytest.raises(PreviewRouteError) as raised:
                async with routes.connect(lease):
                    pass
            assert raised.value.code == "preview_route_unavailable"
            assert not routes._connections and not routes._service_counts
    finally:
        await routes.aclose()


@pytest.mark.asyncio
async def test_ready_true_false_and_auth_errors_are_distinct(preview_env):
    value = preview_env
    async with value.routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
        assert await value.routes.ready(lease) is True
        value.factory.listening = False
        assert await value.routes.ready(lease) is False
        value.factory.listening = True
        value.source.failure = ServiceError("permission_denied", 403)
        with pytest.raises(PreviewRouteError) as raised:
            await value.routes.ready(lease)
        assert raised.value.code == "preview_route_denied"
        assert not value.routes._connections and not value.routes._service_counts


@pytest.mark.parametrize(
    "kwargs",
    [
        {"control_seconds": 0.009},
        {"control_seconds": 2.001},
        {"connect_seconds": 2.001},
        {"lease_seconds": 5.001},
        {"heartbeat_seconds": 1.001},
        {"max_age_seconds": 3600.001},
        {"idle_seconds": 120.001},
        {"service_connections": 0},
        {"service_connections": 9},
        {"adapter_connections": 0},
        {"adapter_connections": 65},
        {"service_connections": 3, "adapter_connections": 2},
    ],
)
def test_preview_limits_reject_out_of_range_parameters(kwargs):
    with pytest.raises(ValueError):
        _short_limits(**kwargs)


@pytest.mark.parametrize("seconds", [0.009, 2.001])
def test_preview_control_rejects_out_of_range_budget(seconds: float):
    with pytest.raises(ValueError):
        PreviewControl(seconds)


@pytest.mark.asyncio
async def test_acquire_does_not_stop_service_and_releases_without_connection(preview_env):
    value = preview_env
    async with value.routes.acquire(value.binding, OWNER_PRINCIPAL) as lease:
        assert lease.binding.execution_id == EXECUTION_ID
    assert value.runner.stop_calls == []
    assert value.runner.removed == []
    assert not value.routes._leases
    assert not value.routes._connections
    assert not value.routes._service_counts
