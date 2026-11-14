"""Host-only ports shared by B/C and implemented or assembled by I.

These values never become model tool arguments. A repository protects durable
bindings; a backend owns actual runtime state. No port grants a principal access
simply because a caller presents a matching dataclass.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol

from krim_agent.contracts import fingerprint
from krim_agent.development.contracts import (
    CommandRequest,
    EnvironmentHandle,
    ProcessHandle,
    RunOwnership,
)


def command_digest(request: CommandRequest) -> str:
    """Bind intent while excluding only the observed environment state."""
    values = request.model_dump(mode="json")
    values["environment"].pop("state")
    return fingerprint(values)


@dataclass(frozen=True)
class EnvironmentRecord:
    """Trusted registered project mount and one selected runtime configuration."""

    environment: EnvironmentHandle
    image_id: str
    content_path: Path = field(repr=False)
    content_device: int
    content_inode: int
    revision: int = 1
    container_id: str | None = None
    configuration_digest: str | None = None
    uid: int = 65532
    gid: int = 65532
    cpu_cores: float = 1.0
    memory_bytes: int = 1_073_741_824
    pids_limit: int = 256
    max_processes: int = 4
    network_id: str | None = None
    network_policy_ref: str | None = None


class EnvironmentRepository(Protocol):
    """I persists records; resolving a stale identity/generation fails closed."""

    async def resolve(self, environment: EnvironmentHandle) -> EnvironmentRecord: ...

    async def compare_and_swap(
        self, expected: EnvironmentRecord, updated: EnvironmentRecord
    ) -> EnvironmentRecord: ...


class EnvironmentAuthorizer(Protocol):
    async def __call__(
        self,
        environment: EnvironmentHandle,
        action: Literal["create", "start", "inspect", "stop", "recreate", "execute"],
    ) -> None:
        """Revalidate current permission. Cleanup uses a separately scoped host authority."""
        ...


@dataclass(frozen=True)
class ProcessRecord:
    request: CommandRequest
    handle: ProcessHandle
    request_digest: str
    revision: int = 1
    execution_id: str | None = None


@dataclass(frozen=True)
class ProcessReservation:
    record: ProcessRecord
    execute: bool


class ProcessRepository(Protocol):
    """Single I-owned admission transaction and immutable request binding.

    reserve returns execute=True only for a first process reservation. Existing
    operation rows and their ownership are joined in the same transaction; C
    does not maintain another operation ledger. Repeated reservations must have
    identical command/scope/lifecycle/authorization. Missing execution receipts
    after admission are unknown, never a reason to call launch again.
    """

    async def reserve(self, request: CommandRequest, request_digest: str) -> ProcessReservation: ...
    async def get(self, process_id: str) -> ProcessRecord: ...
    async def save(self, expected: ProcessRecord, updated: ProcessRecord) -> ProcessRecord: ...
    async def list_environment(
        self, environment: EnvironmentHandle
    ) -> tuple[ProcessRecord, ...]: ...
    async def list_run(self, ownership: RunOwnership) -> tuple[ProcessRecord, ...]: ...
    async def list_environment_for_cleanup(
        self, previous: EnvironmentHandle
    ) -> tuple[ProcessRecord, ...]:
        """Host cleanup only; previous must match the persisted retired-generation binding."""
        ...


@dataclass(frozen=True)
class RuntimeExecution:
    """Trusted B/C launch descriptor. No credentials or user-supplied host argv."""

    execution_id: str
    environment: EnvironmentHandle
    operation_id: str
    process_id: str
    attach_argv: tuple[str, ...] = field(repr=False)
    pty: bool = False


@dataclass(frozen=True)
class RuntimeStatus:
    execution_id: str
    state: Literal["created", "running", "exited", "unknown"]
    exit_code: int | None = None


class ExecutionBackend(Protocol):
    """B owns runtime identity, C owns host supervisor and durable process handles.

    prepare only creates an execution unit and returns its host-only attach argv;
    it never starts the user's command. After durable process admission, C starts
    attach_argv exactly once under its trusted supervisor. Recovery uses inspect
    and must not execute attach_argv on an existing stopped execution unit.
    cancel must stop the entire execution unit, including detached descendants;
    terminating a local CLI attachment alone does not meet this contract.
    """

    def prepare(
        self, request: CommandRequest, process_id: str
    ) -> AbstractAsyncContextManager[RuntimeExecution]:
        """Hold the project launch fence until C persists and completes the launch handshake."""
        ...

    async def inspect(self, execution: RuntimeExecution) -> RuntimeStatus: ...
    async def cancel(self, execution: RuntimeExecution) -> RuntimeStatus: ...
    async def release(self, execution: RuntimeExecution) -> None: ...
