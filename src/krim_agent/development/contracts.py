"""C0 v1 shared by the SDK and development environment modules.

These are transport values and narrow ports, not a second run store. Run state,
approval checkpoints and stop precedence remain in runtime.contracts.RunStore.
No contract authorizes an operation merely because it validates successfully.
"""

from __future__ import annotations

from typing import Annotated, Literal, Protocol, Self

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import StrictModel

CONTRACT_VERSION = "c0-v1.1"
Identifier = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Lifecycle = Literal["run", "project_service"]
ProcessState = Literal["accepted", "running", "exited", "cancelled", "failed", "unknown"]


class EnvironmentHandle(StrictModel):
    owner_id: Identifier
    project_id: Identifier
    environment_id: Identifier
    generation: int = Field(ge=1)
    state: Literal["creating", "stopped", "starting", "ready", "stopping", "failed", "unknown"]


class RunOwnership(StrictModel):
    """Origin is retained even when a service outlives its creating run."""

    owner_id: Identifier
    session_id: Identifier
    run_id: Identifier


class AuthorizationRef(StrictModel):
    """Opaque, project-bound references; resolving/revalidating them belongs to D/I."""

    actor_id: Identifier
    actor_generation: int = Field(ge=1)
    project_id: Identifier
    approval_id: Identifier | None = None
    arguments_digest: Digest
    secret_refs: tuple[Identifier, ...] = ()
    network_policy_ref: Identifier | None = None


class CommandRequest(StrictModel):
    operation_id: Identifier
    environment: EnvironmentHandle
    ownership: RunOwnership
    authorization: AuthorizationRef
    command: str = Field(min_length=1, max_length=32768)
    cwd: str = Field(default=".", min_length=1, max_length=4096)
    pty: bool = False
    lifecycle: Lifecycle = "run"
    timeout_seconds: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    wait_seconds: float = Field(default=1.0, ge=0, le=30)

    @field_validator("command")
    @classmethod
    def no_nul_command(cls, value: str) -> str:
        if "\x00" in value or not value.strip():
            raise ValueError("command must be nonempty and contain no NUL")
        return value

    @field_validator("cwd")
    @classmethod
    def project_relative_cwd(cls, value: str) -> str:
        # The backend must additionally resolve symlinks inside the project mount.
        if (value.startswith("/") or "\\" in value or ":" in value
                or any(ord(c) < 32 for c in value)
                or any(part == ".." for part in value.split("/"))):
            raise ValueError("cwd must be relative to the project")
        return value

    @model_validator(mode="after")
    def coherent_scope(self) -> Self:
        if self.authorization.project_id != self.environment.project_id:
            raise ValueError("authorization and environment project differ")
        if self.ownership.owner_id != self.environment.owner_id:
            raise ValueError("run and environment owner differ")
        return self


class ProcessHandle(StrictModel):
    process_id: Identifier
    operation_id: Identifier
    environment: EnvironmentHandle
    ownership: RunOwnership
    lifecycle: Lifecycle
    state: ProcessState
    exit_code: int | None = None
    log_ref: Identifier

    def require_binding(self, original: CommandRequest | ProcessHandle) -> None:
        """Reject a backend reply that changes the request's immutable ownership."""
        environment = self.environment
        expected = original.environment
        if (
            (environment.owner_id, environment.project_id, environment.environment_id,
             environment.generation)
            != (expected.owner_id, expected.project_id, expected.environment_id,
                expected.generation)
            or self.ownership != original.ownership
            or self.operation_id != original.operation_id
            or self.lifecycle != original.lifecycle
            or (isinstance(original, ProcessHandle) and (
                self.process_id != original.process_id or self.log_ref != original.log_ref
            ))
        ):
            raise ValueError("process binding changed")

    @model_validator(mode="after")
    def coherent_result(self) -> Self:
        if self.environment.owner_id != self.ownership.owner_id:
            raise ValueError("process and environment owner differ")
        if self.state == "exited" and self.exit_code is None:
            raise ValueError("exited process requires an exit code")
        if self.state in {"accepted", "running", "unknown"} and self.exit_code is not None:
            raise ValueError("unfinished process cannot have an exit code")
        return self


class OutputPage(StrictModel):
    """Cursors are byte offsets into the backend log, independent of decoded text."""

    process_id: Identifier
    cursor: int = Field(ge=0)
    next_cursor: int = Field(ge=0)
    text: str = Field(max_length=65536)
    truncated: bool = False
    eof: bool = False

    @model_validator(mode="after")
    def forward_cursor(self) -> Self:
        if self.next_cursor < self.cursor:
            raise ValueError("output cursor cannot move backwards")
        return self


class OperationIdentity(StrictModel):
    """Persistent binding, checked before replay or side effects by the host."""

    operation_id: Identifier
    call_id: Identifier
    environment: EnvironmentHandle
    ownership: RunOwnership
    request_digest: Digest

    @model_validator(mode="after")
    def coherent_owner(self) -> Self:
        if self.environment.owner_id != self.ownership.owner_id:
            raise ValueError("operation and environment owner differ")
        return self


class OperationReceipt(StrictModel):
    identity: OperationIdentity
    state: Literal["accepted", "running", "completed", "failed", "cancelled", "unknown"]
    result_ref: Identifier | None = None
    error_code: Identifier | None = None


class OperationAdmission(StrictModel):
    receipt: OperationReceipt
    execute: bool

    @model_validator(mode="after")
    def execute_only_new_intent(self) -> Self:
        if self.execute and self.receipt.state != "accepted":
            raise ValueError("only a newly accepted intent may execute")
        return self


class OperationStore(Protocol):
    """Atomic admission with durable ownership and request binding.

    lookup(None) means never admitted, not an unknown outcome. admit returns
    execute=True only for the first durable admission. Binding drift is rejected.
    An accepted/running intent found after a crash becomes unknown unless the
    backend can recover its receipt; it must never be dispatched a second time.
    """

    async def lookup(self, identity: OperationIdentity) -> OperationReceipt | None: ...
    async def admit(self, identity: OperationIdentity) -> OperationAdmission: ...
    async def save_receipt(self, receipt: OperationReceipt) -> None: ...


class EnvironmentBackend(Protocol):
    """One selected backend. Host paths and runtime credentials stay behind this port."""

    async def start(self, environment: EnvironmentHandle) -> EnvironmentHandle: ...
    async def inspect(self, environment: EnvironmentHandle) -> EnvironmentHandle: ...
    async def stop(self, environment: EnvironmentHandle) -> EnvironmentHandle: ...


class ProcessBackend(Protocol):
    """C consumes B's concrete execution facility; no generic multi-backend router.

    Consumers call result.require_binding(original) after submit/inspect/cancel.
    Implementations authenticate every request against the persisted binding;
    caller-supplied handles do not themselves authorize input, reads or cleanup.
    """

    async def submit(self, request: CommandRequest) -> ProcessHandle: ...
    async def inspect(self, process: ProcessHandle) -> ProcessHandle: ...
    async def read_output(self, process: ProcessHandle, cursor: int) -> OutputPage: ...
    async def write_stdin(
        self, process: ProcessHandle, data: str, *, close: bool = False
    ) -> None: ...
    async def cancel(self, process: ProcessHandle) -> ProcessHandle: ...
    async def stop_run(self, ownership: RunOwnership) -> None: ...
    async def stop_environment(self, environment: EnvironmentHandle) -> None: ...


class PreviewHandle(StrictModel):
    """The proxy resolves this binding after authorization; no raw upstream URL."""

    preview_id: Identifier
    environment: EnvironmentHandle
    service_id: Identifier
    port: int = Field(ge=1, le=65535)
    state: Literal["pending", "ready", "stopped", "revoked", "unknown"]
    access_path: str = Field(pattern=r"^/[^\s?#]*$", max_length=2048)

    @field_validator("access_path")
    @classmethod
    def same_origin(cls, value: str) -> str:
        if value.startswith("//") or "\\" in value:
            raise ValueError("preview access path must stay on the authorized origin")
        return value
